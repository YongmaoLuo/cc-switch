//! 暴露边界配置：绑定范围、CORS 模式、Origin 白名单。
//!
//! 判据与状态都在这里，命令层（`commands::remote`）只是薄壳。
//!
//! 绑定范围**不是**一个可以直接 `set` 的字段。它走两段式：
//!
//! 1. [`BindScopeGate::request_bind_scope_change`] 只记下一条待确认请求并返回
//!    一个 confirmation id —— 渲染层能做的仅止于此。
//! 2. 原生弹窗确认后，确认事件回到 Rust 侧，
//!    [`BindScopeGate::confirm_native`] 才兑现并落盘。
//!
//! 第二段缺了任何一条，第二段之前配置都不变，也没有文件被写。
//!
//! CORS 则相反：收紧（选白名单）是随时可做的，放宽不在这个文件的能力范围内 —
//! [`CorsMode`] 本身就没有「任意 origin」那个变体。

use super::{BindScope, CorsMode, RemoteConfig};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// 原生弹窗确认后回到 Rust 侧的事件名。载荷见 [`NativeConfirmation`]。
pub const BIND_SCOPE_CONFIRM_EVENT: &str = "remote-bind-scope-native-confirmation";

/// 一次点火的有效期。用户没管那个弹窗时，待确认记录过期，不得事后兑现。
pub const BIND_SCOPE_CONFIRM_TTL: Duration = Duration::from_secs(300);

/// 落盘文件名（在凭据目录里，与配对凭据同目录）。
const FILE_NAME: &str = "exposure.json";

/// 落盘文件路径。测试与生产走同一条路径，测试因此能断言「有没有写文件」。
fn exposure_file(dir: &Path) -> PathBuf {
    dir.join(FILE_NAME)
}

/// 原生弹窗确认事件的载荷。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NativeConfirmation {
    pub confirmation_id: String,
    /// 弹窗是否被点了「确认」。
    pub approved: bool,
}

/// 落盘形态。三个字段就是全部的可配置暴露边界。
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
struct Persisted {
    bind_scope: BindScope,
    cors_mode: CorsMode,
    origin_whitelist: Vec<String>,
}

/// 白名单里的单个 origin 是否可接受。
///
/// 三条判据都不是「洁癖」，每一条都对应一类真实存在、能被利用的来源：
///
/// - `Origin: null` — sandbox iframe、`file:`、`data:` 页面发出的请求都带它。
///   它不归属任何站点，把它列进白名单等于把整个浏览器-extension / sandbox
///   面交给使用者。
/// - `*` — 它就是「任意 origin」的另一种写法；既然 [`CorsMode`] 上不放这条路，
///   列表里也不给。
/// - 空串 / 非 `http(s)` — 要么是白名单写错了，要么是想用 `file:` 那一类
///   本地 scheme 绕过归属判断。
fn is_allowed_origin(origin: &str) -> bool {
    let trimmed = origin.trim();
    if trimmed.is_empty() || trimmed == "*" || trimmed == "null" {
        return false;
    }
    // scheme 前缀之外若还有空白/通配，一并拒。
    if trimmed.contains(char::is_whitespace) {
        return false;
    }
    trimmed.starts_with("http://") || trimmed.starts_with("https://")
}

/// 校验整份白名单。全过或全不过 —— 半份写入的列表比一份都没有更难推理。
pub fn validate_whitelist(origins: &[String]) -> Result<(), String> {
    for origin in origins {
        if !is_allowed_origin(origin) {
            return Err(format!(
                "origin whitelist entry rejected: {origin:?} \
                 (allowed: explicit http(s) origins; \"null\", \"*\" and blank are never allowed)"
            ));
        }
    }
    Ok(())
}

/// 暴露边界的持久化与两段式闸门。
///
/// `dir` 是凭据目录（与配对凭据同一个）：暴露边界和凭据同属「这台机器上的远程
/// 服务开放到什么程度」，分开存只会让两条线的答案对不上。
pub struct BindScopeGate {
    dir: PathBuf,
    inner: Arc<Mutex<Inner>>,
}

impl Clone for BindScopeGate {
    /// 克隆的是同一份状态，不是快照：待确认请求登记在一处，就得能被另一处兑现。
    fn clone(&self) -> Self {
        Self {
            dir: self.dir.clone(),
            inner: self.inner.clone(),
        }
    }
}

#[derive(Debug)]
struct Inner {
    persisted: Persisted,
    /// confirmation id → (请求的范围, 点火时刻)。
    pending: HashMap<String, (BindScope, Instant)>,
}

/// 拿锁。毒了就当作配置读不出来 —— 一份被 panic 过的锁不值得让整个命令层挂掉。
fn lock(inner: &Mutex<Inner>) -> std::sync::MutexGuard<'_, Inner> {
    inner.lock().unwrap_or_else(|e| e.into_inner())
}

impl BindScopeGate {
    /// 全部默认（回环 / 同源 / 空白名单），不读盘。
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Self {
            dir: dir.into(),
            inner: Arc::new(Mutex::new(Inner {
                persisted: Persisted::default(),
                pending: HashMap::new(),
            })),
        }
    }

    /// 从 `dir` 读出已落盘的暴露边界；没有文件就是默认值。
    ///
    /// 文件存在但读不出来 / 字段不合法时同样回默认值：这三条判据都是「收紧」
    /// 方向的默认值，宁可退回最紧的一档，也不要因为一个坏文件让服务带着更宽的
    /// 边界起来。
    pub fn load(dir: impl Into<PathBuf>) -> Self {
        let dir = dir.into();
        let gate = Self::new(dir.clone());
        let persisted = read_persisted(&exposure_file(&dir)).unwrap_or_default();
        lock(&gate.inner).persisted = persisted;
        gate
    }

    /// 当前暴露边界的一份快照。
    pub fn read_remote_config(&self) -> RemoteConfig {
        let persisted = lock(&self.inner).persisted.clone();
        RemoteConfig {
            bind_scope: persisted.bind_scope,
            cors_mode: persisted.cors_mode,
            origin_whitelist: persisted.origin_whitelist,
            ..RemoteConfig::default()
        }
    }

    /// 第一段：登记一条「想把绑定范围改成 `scope`」的请求，返回 confirmation id。
    ///
    /// **不改配置、不落盘** —— 这是本函数存在的全部理由。它返回的 id 是原生弹窗
    /// 回话时带的凭据。
    pub fn request_bind_scope_change(&self, scope: BindScope) -> String {
        let id = uuid::Uuid::new_v4().to_string();
        let mut guard = lock(&self.inner);
        purge_expired(&mut guard.pending);
        guard.pending.insert(id.clone(), (scope, Instant::now()));
        id
    }

    /// 第二段：原生弹窗确认事件回来后兑现那次点火。
    ///
    /// 认不出来的 id（不存在 / 已用过 / 过期）与 `approved = false` 都返回错误，
    /// 两种情况都不动配置。
    pub fn confirm_native(&self, confirmation_id: &str, now: Instant) -> Result<BindScope, String> {
        let mut guard = lock(&self.inner);
        let Some((scope, requested_at)) = guard.pending.remove(confirmation_id) else {
            return Err("unknown or expired bind-scope confirmation".to_string());
        };
        if now.saturating_duration_since(requested_at) > BIND_SCOPE_CONFIRM_TTL {
            return Err("bind-scope confirmation expired".to_string());
        }

        guard.persisted.bind_scope = scope;
        let snapshot = guard.persisted.clone();
        drop(guard);
        write_persisted(&self.dir, &snapshot)?;

        log::info!(
            "[Remote] bind scope confirmed natively: {:?} (confirmation={confirmation_id})",
            scope
        );
        Ok(scope)
    }

    /// 显式拒绝一次点火（用户关掉了弹窗）。同样不动配置。
    pub fn reject_native(&self, confirmation_id: &str) {
        lock(&self.inner).pending.remove(confirmation_id);
    }

    /// 设置 CORS 模式与白名单。**不含绑定范围** —— 那条路只有两段式。
    pub fn set_cors(&self, cors_mode: CorsMode, origin_whitelist: &[String]) -> Result<(), String> {
        validate_whitelist(origin_whitelist)?;
        let mut guard = lock(&self.inner);
        guard.persisted.cors_mode = cors_mode;
        guard.persisted.origin_whitelist = origin_whitelist.to_vec();
        let snapshot = guard.persisted.clone();
        drop(guard);
        write_persisted(&self.dir, &snapshot)
    }
}

/// 丢掉过期的待确认记录，让 `pending` 不随点火次数无限增长。
fn purge_expired(pending: &mut HashMap<String, (BindScope, Instant)>) {
    pending.retain(|_, (_, at)| {
        Instant::now().saturating_duration_since(*at) <= BIND_SCOPE_CONFIRM_TTL
    });
}

fn read_persisted(path: &Path) -> Option<Persisted> {
    let raw = std::fs::read_to_string(path).ok()?;
    let persisted: Persisted = serde_json::from_str(&raw).ok()?;
    // 读回来的白名单同样要过一遍判据：文件是上一版程序或手改出来的，
    // 不能因为「已经落盘了」就绕开 `Origin: null` 这条。
    validate_whitelist(&persisted.origin_whitelist).ok()?;
    Some(persisted)
}

fn write_persisted(dir: &Path, persisted: &Persisted) -> Result<(), String> {
    let path = exposure_file(dir);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| format!("{}: {e}", parent.display()))?;
    }
    let body = serde_json::to_string_pretty(persisted).map_err(|e| e.to_string())?;
    let tmp = path.with_extension("json.tmp");
    std::fs::write(&tmp, body).map_err(|e| format!("{}: {e}", tmp.display()))?;
    // temp + rename：写到一半断电不会留下半个 JSON，下次启动读不出就退回默认值。
    std::fs::rename(&tmp, &path).map_err(|e| format!("{}: {e}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::time::{Duration, Instant};

    /// 任何 origin 的取值在**枚举层面**就不存在。
    #[test]
    fn cors_mode_has_no_wildcard_variant() {
        assert_eq!(
            CorsMode::ALL.len(),
            2,
            "CorsMode 的取值集合必须恰为 {{SameOrigin, ExplicitWhitelist}}，\
             多出来的每一个都是一条「放宽到任意 origin」的路"
        );
        assert!(CorsMode::ALL.contains(&CorsMode::SameOrigin));
        assert!(CorsMode::ALL.contains(&CorsMode::ExplicitWhitelist));

        // 线上的名字也一样只有两个：反序列化是唯一能让「配置文件里塞一个通配值」
        // 进到程序里的入口，它认不出来才算真的堵住。
        assert_eq!(
            serde_json::from_value::<CorsMode>(json!("sameOrigin")).unwrap(),
            CorsMode::SameOrigin
        );
        assert_eq!(
            serde_json::from_value::<CorsMode>(json!("explicitWhitelist")).unwrap(),
            CorsMode::ExplicitWhitelist
        );
        for loose in ["any", "anyOrigin", "wildcard", "*", "All"] {
            assert!(
                serde_json::from_value::<CorsMode>(json!(loose)).is_err(),
                "CorsMode 反序列化不得接受 {loose}：本枚举里不存在「任意 origin」"
            );
        }

        // 默认收紧。
        assert_eq!(CorsMode::default(), CorsMode::SameOrigin);
    }

    /// 点火不落盘；只有原生弹窗确认事件回来之后才落盘。
    #[test]
    fn bind_scope_change_requires_native_confirmation() {
        let dir = tempfile::tempdir().expect("tempdir");
        let gate = BindScopeGate::new(dir.path());

        assert_eq!(gate.read_remote_config().bind_scope, BindScope::Loopback);

        let id = gate.request_bind_scope_change(BindScope::Lan);
        assert!(!id.is_empty(), "点火必须返回一个 confirmation id");

        // 只点火：配置不变，也没有落盘。
        assert_eq!(
            gate.read_remote_config().bind_scope,
            BindScope::Loopback,
            "点火命令不得直接改配置"
        );
        assert!(
            !exposure_file(dir.path()).exists(),
            "点火命令不得落盘：落盘必须发生在原生弹窗确认之后"
        );

        // 对不上的 id（猜的、旧的、重复的）同样不得改配置。
        assert!(gate
            .confirm_native("not-a-real-confirmation-id", Instant::now())
            .is_err());
        assert_eq!(gate.read_remote_config().bind_scope, BindScope::Loopback);

        // 原生弹窗确认之后才真正落盘。
        gate.confirm_native(&id, Instant::now())
            .expect("原生确认事件必须能兑现这次点火");
        assert_eq!(gate.read_remote_config().bind_scope, BindScope::Lan);
        assert!(exposure_file(dir.path()).exists(), "确认之后必须落盘");

        // 重新打开目录读得回来：落盘是真的，不是内存里那份。
        let reopened = BindScopeGate::load(dir.path());
        assert_eq!(reopened.read_remote_config().bind_scope, BindScope::Lan);

        // 同一个 id 只能兑现一次。
        assert!(gate.confirm_native(&id, Instant::now()).is_err());

        // 过期的点火（用户没管那个弹窗）不得兑现。
        let stale = gate.request_bind_scope_change(BindScope::Loopback);
        assert!(gate
            .confirm_native(
                &stale,
                Instant::now() + BIND_SCOPE_CONFIRM_TTL + Duration::from_secs(1)
            )
            .is_err());
        assert_eq!(gate.read_remote_config().bind_scope, BindScope::Lan);
    }

    /// 白名单里不得出现 `Origin: null`。
    #[test]
    fn whitelist_rejects_null_origin() {
        let dir = tempfile::tempdir().expect("tempdir");
        let gate = BindScopeGate::new(dir.path());

        for null_origin in ["null", "NULL", "Null", " null", "null "] {
            assert!(
                validate_whitelist(&[null_origin.to_string()]).is_err(),
                "白名单不得接受 {null_origin:?}：sandbox / file: / data: 页面发出的 \
                 Origin: null 不是可归属的来源"
            );
        }
        assert!(gate
            .set_cors(CorsMode::ExplicitWhitelist, &["null".to_string()])
            .is_err());
        assert_eq!(gate.read_remote_config().cors_mode, CorsMode::SameOrigin);
        assert!(gate.read_remote_config().origin_whitelist.is_empty());

        // 通配同样不接受：它就是「任意 origin」另一种写法。
        assert!(validate_whitelist(&["*".to_string()]).is_err());

        // 合法的显式白名单能进，并跟着落盘。
        let allowed = vec![
            "http://127.0.0.1:4000".to_string(),
            "https://desk.example.invalid".to_string(),
        ];
        gate.set_cors(CorsMode::ExplicitWhitelist, &allowed)
            .expect("显式白名单应当被接受");
        assert_eq!(
            gate.read_remote_config().cors_mode,
            CorsMode::ExplicitWhitelist
        );
        assert_eq!(gate.read_remote_config().origin_whitelist, allowed);
        assert_eq!(
            BindScopeGate::load(dir.path())
                .read_remote_config()
                .origin_whitelist,
            allowed
        );

        // 白名单里混进一个 null，整份一起拒，不做部分写入。
        let mut mixed = allowed.clone();
        mixed.push("null".to_string());
        assert!(gate.set_cors(CorsMode::ExplicitWhitelist, &mixed).is_err());
        assert_eq!(
            gate.read_remote_config().origin_whitelist,
            allowed,
            "被拒的白名单不得留下部分写入"
        );
    }
}
