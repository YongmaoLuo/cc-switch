use tauri::{command, AppHandle};

async fn apply_remote_config(
    app: &AppHandle,
    port: u16,
    tailscale_enabled: bool,
    action: &str,
) -> Result<String, String> {
    log::info!(
        "[Remote] {action}_remote_server called with port: {port}, tailscale: {tailscale_enabled}"
    );

    let config = crate::remote::RemoteConfig {
        enabled: true,
        port,
        tailscale_enabled,
        // 暴露边界（绑定范围 / CORS / 白名单）不从这里进：start / restart 只管
        // 端口与 Tailscale，那三项一律从 gate 读当前生效值 —— 否则「改了端口
        // 重启一下」会把先前确认过的绑定范围悄悄退回回环。
        ..exposure_of(app).await
    };

    let urls = crate::remote::start_remote(app, config).await?;
    log::info!("[Remote] Server {action}ed on URLs: {:?}", urls);
    Ok(format!("Remote server {action}ed on: {}", urls.join(", ")))
}

#[command]
pub async fn start_remote_server(
    app: AppHandle,
    port: u16,
    tailscale_enabled: bool,
) -> Result<String, String> {
    apply_remote_config(&app, port, tailscale_enabled, "start").await
}

#[command]
pub async fn stop_remote_server(app: AppHandle) -> Result<String, String> {
    crate::remote::stop_remote(&app).await?;
    Ok("Remote server stopped".to_string())
}

/// Restart the remote server without broadcasting SSE shutdown.
/// Used for Tailscale toggle and port changes to avoid "closed" page on browsers.
#[command]
pub async fn restart_remote_server(
    app: AppHandle,
    port: u16,
    tailscale_enabled: bool,
) -> Result<String, String> {
    apply_remote_config(&app, port, tailscale_enabled, "restart").await
}

#[command]
pub fn check_tailscale_available() -> bool {
    crate::remote::is_tailscale_available()
}

#[command]
pub fn get_tailscale_ip() -> Option<String> {
    crate::remote::get_tailscale_ip()
}

// --- 对外暴露边界（绑定范围 / CORS / Origin 白名单）---
//
// 一条不可协商的分界：**收紧**随时可做，**放宽**只有一条路。
//
// 收紧 = 改 CORS 模式、往白名单里加一条具体 origin、走两段式把绑定范围退回
// 回环。放宽 = 让别的机器能连上（`BindScope::Lan`）或让别的源能发请求
// （`ExplicitWhitelist`）。
//
// 渲染层能直接做的事因此只有两件：调 `setRemoteConfig` 收紧，或者调
// `requestBindScopeChange` **点火**。点火返回的是 confirmation id，不改任何
// 配置；真正的落盘发生在原生弹窗确认事件回到 Rust 侧之后
// （[`BIND_SCOPE_CONFIRM_EVENT`]）。
//
// 如果「二次确认」只是渲染层一个 `confirm()` 再调一次 `set_remote_config`，
// 那么任何能执行渲染层代码的路径都能跳过它 —— 确认就成了装饰品。

use crate::remote::config::{
    BindScopeGate, NativeConfirmation, BIND_SCOPE_CONFIRM_EVENT, BIND_SCOPE_CONFIRM_TTL,
};
use crate::remote::{BindScope, RemoteConfig};
use std::time::Duration;
use tauri::{Listener, Runtime};

/// 取当前生效的暴露边界。
///
/// 服务没起时没有「生效值」可言，直接给默认（回环 / 同源 / 空白名单）—— 起服务
/// 的那条路读的是同一份落盘文件，所以不会因为这里返回默认而丢配置。
async fn exposure_of<R: Runtime>(app: &AppHandle<R>) -> RemoteConfig {
    use tauri::Manager;
    let managed = app.try_state::<ManagedRemoteServer>();
    let Some(managed) = managed else {
        return RemoteConfig::default();
    };
    let guard = managed.lock().read().await;
    match guard.as_ref() {
        Some(server) => server.exposure.read_remote_config(),
        None => BindScopeGate::load(auth_config_dir()).read_remote_config(),
    }
}

/// 读当前暴露边界。
///
/// 泛型化的理由与安全无关，纯粹是可测性：`AppHandle<Wry>` 会把这族命令钉死在
/// 生产那一个 runtime 上，而行为级闸门要用 `tauri::test::mock_builder` 的
/// runtime 无关 mock 夹具把它们真调一遍（见 `lib.rs::remote_invoke_handler`）。
#[command]
pub async fn get_remote_config<R: Runtime>(app: AppHandle<R>) -> Result<RemoteConfig, String> {
    Ok(exposure_of(&app).await)
}

/// 暴露边界写入的判定内核：收窄实参、校验白名单、落盘，全在这一行。
///
/// 拆出来的唯一理由是可测性。`#[tauri::command]` 那一层要 `AppHandle<R>`，而
/// 「传进来的 `bind_scope` 一律不认」这条判据必须能在没有 Tauri runtime 的情况
/// 下被真调一遍 —— 判据的强度不该取决于本轮能不能新增一条 crate。
///
/// 名字上没有 `remote`：本文件里另有一个私有的 `apply_remote_config`（start /
/// restart 用的端口与 Tailscale 那条路），两个函数管的是两回事，同名只会让调用
/// 点读起来像同一件事。
///
/// 签名里也没有「绑定范围」这个参数：整份 `config` 进来，被取走的只有
/// `cors_mode` 与 `origin_whitelist`。[`BindScopeGate::set_cors`] 里没有
/// `bind_scope` 那一行 —— 那不是「读出来再丢回去」，是压根不读。
pub fn apply_exposure_config(gate: &BindScopeGate, config: &RemoteConfig) -> Result<(), String> {
    // 白名单校验在这里发生，`Origin: null` / `*` 到不了落盘。
    gate.set_cors(config.cors_mode, &config.origin_whitelist)
}

/// 写 CORS 模式与 Origin 白名单。
///
/// **绑定范围不在这里**：本命令只接受 `cors_mode` 与 `origin_whitelist`，传进来
/// 的 `bind_scope` 一律忽略。要改绑定范围只能走
/// [`request_bind_scope_change`] 的两段式。
///
/// 本函数只负责「找到闸门 + 转发」，判定本身在 [`apply_exposure_config`]。
#[command]
pub async fn set_remote_config<R: Runtime>(
    app: AppHandle<R>,
    config: RemoteConfig,
) -> Result<(), String> {
    use tauri::Manager;

    let managed = app.state::<ManagedRemoteServer>();
    let gate = {
        let guard = managed.lock().read().await;
        guard.as_ref().map(|s| s.exposure.clone())
    };
    let gate = match gate {
        Some(gate) => gate,
        None => BindScopeGate::load(auth_config_dir()),
    };

    apply_exposure_config(&gate, &config)
}

/// 点火：请求把绑定范围改成 `scope`，返回 confirmation id。
///
/// 这一步**不改配置、不落盘**，只是唤起原生弹窗并登记一条待确认请求。落盘发生在
/// 原生弹窗确认之后，由 [`BIND_SCOPE_CONFIRM_EVENT`] 把确认结果带回 Rust 侧。
///
/// 同理，为什么不放一条 `confirm_bind_scope_change` 命令给渲染层调用：那样「确认」
/// 又变回渲染层的一行代码，这条闸门就只剩形式。
#[command]
pub async fn request_bind_scope_change<R: Runtime>(
    app: AppHandle<R>,
    scope: BindScope,
) -> Result<String, String> {
    use tauri::Manager;

    let managed = app.state::<ManagedRemoteServer>();
    let gate = {
        let guard = managed.lock().read().await;
        guard.as_ref().map(|s| s.exposure.clone())
    };
    let gate = match gate {
        Some(gate) => gate,
        None => BindScopeGate::load(auth_config_dir()),
    };

    let confirmation_id = gate.request_bind_scope_change(scope);

    // 等原生弹窗的确认事件。只认这一个 id，只作一次响应。
    let listener_app = app.clone();
    let expected_id = confirmation_id.clone();
    tauri::async_runtime::spawn(async move {
        let listener_id = listener_app.listen(BIND_SCOPE_CONFIRM_EVENT, move |event| {
            let Ok(payload) = serde_json::from_str::<NativeConfirmation>(event.payload()) else {
                return;
            };
            // 别人的 id、旧的 id：不是这次点火的确认，一律不兑。
            if payload.confirmation_id != expected_id {
                return;
            }
            let gate = gate.clone();
            tauri::async_runtime::spawn(async move {
                if payload.approved {
                    if let Err(e) = gate.confirm_native(&payload.confirmation_id, Instant::now()) {
                        log::warn!("[Remote] bind scope confirmation failed: {e}");
                    }
                } else {
                    gate.reject_native(&payload.confirmation_id);
                    log::info!("[Remote] bind scope change declined in native dialog");
                }
            });
        });

        // 用户没管那个弹窗：到点收尸，不留一条永远能兑现的待确认记录。
        tokio::time::sleep(BIND_SCOPE_CONFIRM_TTL + Duration::from_secs(1)).await;
        listener_app.unlisten(listener_id);
        log::debug!("[Remote] bind scope confirmation listener retired");
    });

    Ok(confirmation_id)
}

// --- 配对审批与设备管理（桌面端侧）---
//
// 这四条与 HTTP 那四条（`/pairing/approve`、`/pairing/revoke` …）作用在同一份状态
// 上，但入口是 Tauri 命令而不是网络路由：**审批是 human-in-the-loop 的那一环**，
// 把它留在 GUI 进程内比再开一条 loopback HTTP 路由少一整层可能被误配的鉴权。
//
// 三条边界贯穿下面每一条：
//
// 1. 返回值只有元数据（客户端名、创建时间、最近使用）。凭据库里存的摘要**不**
//    跟着发到前端 —— 它是一份「可复现 token」的判定材料。
// 2. 失败一律是同一类错误字符串，不区分「配对码不存在 / 已过期 / 已用掉」。
//    分开报等于给出一个 oracle，让调用方能反推某个配对码的存活状态。
// 3. 证据缺失（`whois` 超时、找不到 tailscale 二进制）**不会**让审批失败，见
//    [`crate::remote::pairing::approve`]。证据由后端**重算**，不用前端送来的快照 ——
//    快照是谁都能伪造的，见 [`approve_bound`]。

use crate::remote::pairing::{
    approve, list_client_meta, revoke_client_by_name, ClientMeta, PairingApiState,
};
use crate::remote::ManagedRemoteServer;
use std::time::Instant;

/// 凭据目录。与 `RemoteServer` 用的是同一个 `get_app_config_dir()` —— 中间件读
/// 的就是这里的 `remote-auth/clients.json`。
fn auth_config_dir() -> std::path::PathBuf {
    crate::config::get_app_config_dir()
}

/// 取运行中的配对状态。
///
/// 远程服务没起就没有待批请求可言，因此这里报「未运行」而不是自己另建一个
/// store —— 另建的那个 CLI 永远看不到，只能让人以为批了。
async fn pairing_state(app: &AppHandle) -> Result<PairingApiState, String> {
    use tauri::Manager;
    let managed = app.state::<ManagedRemoteServer>();
    let guard = managed.lock().read().await;
    guard
        .as_ref()
        .map(|server| server.pairing.clone())
        .ok_or_else(|| "remote server is not running".to_string())
}

/// 所有失败共用的一句话。
///
/// 「配对码不存在 / 已过期 / 已被用掉 / 不属于这条请求」必须折叠成同一个出口：
/// 分开报就等于给出一个 oracle，让调用方能反推某个配对码的存活状态。
const DENIED: &str = "unknown or no longer pending pairing request";

/// 后端重算出的来源核不出来、而界面没交回确认时说的话。
///
/// 刻意**不**并进 [`DENIED`]：那条说的是「这条请求已经不在了」，把「你没勾那个
/// 确认」说成「请求不见了」，用户会去重开一次配对，而不是把勾补上。这条命令只
/// 在 GUI 进程内可达，不构成对配对请求发起方的 oracle。
const UNCONFIRMED: &str =
    "source of this pairing request could not be verified; explicit confirmation required";

/// 批准一条待审请求。
///
/// `request_id` 与 `user_code` 都收：配对码是状态转移的键，`request_id` 用来核对
/// 这条码确实属于用户刚看过的那条请求（界面上两者来自同一条审批事件）。
///
/// 证据**在后端重算**，不用前端送来的快照：快照是谁都能伪造的，而重算要的是
/// `tailscale whois <发起方 IP>` 与 `tailscale status --json` 的一次现场比对
/// （见 [`crate::remote::pairing::evidence_for`]）。`whois` 与 `status` 各自 2s
/// 上限，最坏 4s —— 这一条命令本来就跑在 `async fn` 上，不占 GUI 线程。
///
/// `whois` 超时导致的证据缺失**不会**让本命令变成一条死路，但它也不再默认放行：
/// 重算出来的来源核不出来时，必须由界面勾过一次「我已知悉来源无法核实」
/// （`confirmed_unverified_source`）才放行，否则报 [`UNCONFIRMED`] 且请求仍待审。
///
/// 这个标志位**只能由界面勾选结果决定**：后端重算看到的来源可能与用户在界面上
/// 看到的那一份不同，命令层若替用户默认同意，就等于给一条从未被确认过的授权开了
/// 后门。
///
/// `confirmed_foreign_account` 是**第二道**独立的门：对外来账号（`tailscale whois`
/// 的 `Node.User.ID` 与 `Self.UserID` 不等）的默认拒绝由状态机自己执行，见
/// [`crate::remote::pairing::approve`]。界面上那个勾是提示，不是防线 —— 绕开渲染
/// 层的调用同样必须被这道闸门拦下。缺省成 `true` 则是给所有请求开后门，故前端
/// 必须如实传值。
#[command]
pub async fn approve_pairing(
    app: AppHandle,
    request_id: String,
    user_code: String,
    confirmed_unverified_source: bool,
    confirmed_foreign_account: bool,
) -> Result<(), String> {
    let pairing = pairing_state(&app).await?;
    approve_bound(
        &pairing.store,
        &request_id,
        &user_code,
        confirmed_unverified_source,
        confirmed_foreign_account,
    )
    .await?;
    Ok(())
}

/// 核对 `request_id` ↔ `user_code` 的绑定，后端重算一次证据，然后批准。
///
/// 拆出来是为了能直接测：命令层要 `AppHandle` + 正在运行的远程服务，而这里只要
/// 一个 [`PairingStore`]。返回重算出来的决策供调用方记审计。
async fn approve_bound(
    store: &crate::remote::pairing::PairingStore,
    request_id: &str,
    user_code: &str,
    confirmed_unverified_source: bool,
    confirmed_foreign_account: bool,
) -> Result<Option<crate::remote::pairing::ApprovalDecision>, String> {
    let Some(bound_code) = store.user_code_for_request(request_id) else {
        return Err(DENIED.to_string());
    };
    if bound_code != user_code {
        return Err(DENIED.to_string());
    }

    // 重算要用「当初是谁发起的」—— 批准命令的 peer 是桌面端自己，那个地址查不出
    // 任何东西。来源 IP 在请求落库时就记下了。
    let peer = store
        .peer_for_request(request_id)
        .ok_or_else(|| DENIED.to_string())?;
    let evidence = crate::remote::pairing::evidence_for(std::net::SocketAddr::new(peer, 0)).await;

    let decision = approve(
        store,
        user_code,
        Some(&evidence),
        confirmed_unverified_source,
        confirmed_foreign_account,
        Instant::now(),
    )
    .map_err(|e| match e {
        crate::remote::pairing::PairingError::Unconfirmed => UNCONFIRMED.to_string(),
        _ => DENIED.to_string(),
    })?;

    log::info!(
        "[Remote] pairing: 已批准请求 {request_id}，证据来源 {:?} / 外来账号 {} / 决策 {:?}",
        evidence.source_kind,
        evidence.is_foreign_account,
        decision
    );
    Ok(decision)
}

/// 拒绝一条待审请求。`Rejected` 是终态，不可逆。
#[command]
pub async fn reject_pairing(app: AppHandle, request_id: String) -> Result<(), String> {
    let pairing = pairing_state(&app).await?;

    let Some(user_code) = pairing.store.user_code_for_request(&request_id) else {
        return Err(DENIED.to_string());
    };
    pairing
        .store
        .reject(&user_code, Instant::now())
        .map_err(|_| DENIED.to_string())
}

/// 列出已配对客户端。**只回元数据**，没有摘要、没有明文。
#[command]
pub fn list_paired_clients() -> Result<Vec<ClientMeta>, String> {
    list_client_meta(&auth_config_dir())
}

/// 按客户端名撤销其全部凭据。返回是否摘到了东西（撤销是幂等的）。
#[command]
pub fn revoke_client(client_name: String) -> Result<bool, String> {
    if client_name.is_empty() {
        return Err("client name must not be empty".to_string());
    }
    revoke_client_by_name(&auth_config_dir(), &client_name)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::collections::BTreeSet;

    /// 远程服务这一族命令（既有 5 条 + 审批 4 条 + 配置 3 条）。
    pub(crate) const REMOTE_COMMANDS: [&str; 12] = [
        "start_remote_server",
        "stop_remote_server",
        "restart_remote_server",
        "check_tailscale_available",
        "get_tailscale_ip",
        "approve_pairing",
        "reject_pairing",
        "list_paired_clients",
        "revoke_client",
        "get_remote_config",
        "set_remote_config",
        "request_bind_scope_change",
    ];

    /// 读 `lib.rs` 源码，逐名收集 `generate_handler![...]` 内的标识符。
    ///
    /// 不走编译期反射：这里要验证的恰恰是「源码里有没有登记」，读源码才是
    /// 真正的判据。任何一处旁路（写了命令但忘了进 handler）都会在这里露出来。
    pub(crate) fn registered_command_names() -> BTreeSet<String> {
        let lib_rs = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/lib.rs");
        let source = std::fs::read_to_string(&lib_rs)
            .unwrap_or_else(|e| panic!("cannot read {}: {e}", lib_rs.display()));

        let start = source
            .find("generate_handler![")
            .unwrap_or_else(|| panic!("no generate_handler! macro found in {}", lib_rs.display()));
        let body_start = start + "generate_handler![".len();
        let end = source[body_start..]
            .find(']')
            .map(|i| body_start + i)
            .unwrap_or_else(|| panic!("unterminated generate_handler! in {}", lib_rs.display()));

        source[body_start..end]
            .lines()
            // 先逐行去掉行注释（注释里也有逗号，不能按逗号盲切）。
            .map(|line| line.split("//").next().unwrap_or_default())
            .collect::<Vec<_>>()
            .join(" ")
            .split(',')
            .filter_map(|entry| {
                // 登记项写成 `commands::foo`，只取最后一段路径分量作为命令名。
                let ident = entry.trim().rsplit("::").next().unwrap_or_default().trim();
                if ident.is_empty() {
                    None
                } else {
                    Some(ident.to_string())
                }
            })
            .collect()
    }

    #[test]
    fn generate_handler_registers_exactly_twelve_commands() {
        let expected: BTreeSet<String> = REMOTE_COMMANDS.iter().map(|s| s.to_string()).collect();
        let registered = registered_command_names();

        // 集合相等（逐名），不接受「至少 12 条」这种弱断言。
        let missing: Vec<&String> = expected.difference(&registered).collect();
        assert!(
            missing.is_empty(),
            "generate_handler! is missing remote commands: {missing:?}"
        );

        // 反向：收集到的每一项都必须是合法标识符，否则上面的比较建立在垃圾数据上。
        for name in &registered {
            assert!(
                !name.is_empty() && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_'),
                "unexpected non-identifier entry collected from generate_handler!: {name:?}"
            );
        }

        assert_eq!(
            expected.intersection(&registered).count(),
            REMOTE_COMMANDS.len()
        );
    }

    #[test]
    fn existing_five_commands_are_registered() {
        let existing_five = [
            "start_remote_server",
            "stop_remote_server",
            "restart_remote_server",
            "check_tailscale_available",
            "get_tailscale_ip",
        ];
        let registered = registered_command_names();

        for name in existing_five {
            assert!(
                registered.contains(name),
                "pre-existing command {name} must remain registered in generate_handler!"
            );
        }
    }

    /// 批准命令必须在**后端重算**证据，不能把 `None` 传给 `approve`。
    ///
    /// 前端送来的证据是快照，谁都能伪造；重算一次（`tailscale whois` + `status`）
    /// 才拿得到没人能代填的结论。传 `None` 等于让这条链路上最该有判据的地方
    /// 恰好是空的。
    #[tokio::test]
    async fn approve_pairing_recomputes_evidence_on_the_backend() {
        use crate::remote::pairing::PairingStore;

        let store = PairingStore::new();
        let ticket = store
            .request_from_peer(
                "100.64.0.9".parse().expect("valid peer ip"),
                "cli",
                &["read"],
                Instant::now(),
            )
            .expect("配对请求必须放行");

        let decision = approve_bound(&store, &ticket.request_id, &ticket.user_code, true, false)
            .await
            .expect("后端重算证据后必须能批准");

        assert!(
            decision.is_some(),
            "批准必须带回后端重算出来的决策，不能是「没有证据」的空洞 None"
        );

        // 配对码必须与 request_id 成对：错配一律折叠成同一个出口。
        let other = store.request("other-cli", &[], Instant::now());
        assert_eq!(
            approve_bound(&store, &other.request_id, &ticket.user_code, true, false).await,
            Err(DENIED.to_string()),
            "配对码不属于这条请求时必须拒绝"
        );
        assert_eq!(
            approve_bound(&store, "no-such-request", &ticket.user_code, true, false).await,
            Err(DENIED.to_string())
        );
    }

    /// 命令层不得替用户「默认同意」：重算出的证据核不出来时，没勾确认就不能批。
    ///
    /// 这条钉的是**接线**：后端自己重算证据，它看到的可能与界面上看到的那一份
    /// 不同（用户在界面上看到的是 `Tailnet`，重算时 `whois` 超时了）。这种时候
    /// 命令层若自己把确认当成「已经有了」，就等于给了一条从未被确认过的授权开
    /// 后门。失败之后请求必须仍待审，用户重勾一次即可。
    #[tokio::test]
    async fn approve_bound_refuses_to_skip_the_degraded_confirmation() {
        use crate::remote::pairing::{PairingStore, PollOutcome};

        let store = PairingStore::new();
        let now = Instant::now();
        let ticket = store
            .request_from_peer(
                "198.51.100.7".parse().expect("valid peer ip"),
                "cli",
                &["read"],
                now,
            )
            .expect("配对请求必须放行");

        assert_eq!(
            approve_bound(&store, &ticket.request_id, &ticket.user_code, false, false).await,
            Err(UNCONFIRMED.to_string()),
            "重算出「来源核不出来」时，未确认的批准必须失败"
        );
        assert!(
            matches!(store.poll(&ticket.poll_token, now), PollOutcome::Pending),
            "这次失败不得把待审请求消费掉"
        );

        assert!(
            approve_bound(&store, &ticket.request_id, &ticket.user_code, true, false)
                .await
                .expect("勾了确认就必须放行")
                .is_some(),
            "确认之后必须带回决策，而不是空洞的 None"
        );
    }

    /// `approve_bound(` 的实参表，只取调用点上的那几个字。
    ///
    /// 刻意把**签名**排除在外：`confirmed_foreign_account` 作为形参出现在签名里，
    /// 于是一句「这个标识符在函数体里出现过吗」的朴素检查，在调用点被换成字面量
    /// `true` 时照样为真 —— 那正是这道锁要抓的改动。
    fn approve_pairing_bound_args(src: &str) -> String {
        let body = src
            .split("pub async fn approve_pairing(")
            .nth(1)
            .and_then(|rest| rest.split("pub async fn reject_pairing(").next())
            .expect("approve_pairing body must be delimited by reject_pairing");

        body.split("approve_bound(")
            .nth(1)
            .and_then(|rest| rest.split(')').next())
            .expect("approve_pairing must call approve_bound(")
            .to_string()
    }

    /// 结构锁：`approve_pairing` 必须把外来账号确认**原样递下去**。
    ///
    /// 外来账号的默认拒绝有两道门：界面上的勾，和状态机里 `record.foreign_account`
    /// 对 `ack_foreign` 的核对（[`crate::remote::pairing::PairingStore::approve`]）。
    /// 第二道才是防线 —— 第一道只管渲染层，任何绕开它的调用都摸不到。
    ///
    /// 所以这条钉的是**接线**：`approve_pairing` 若把这个标志位换成字面量（无论
    /// `true` 还是 `false`），状态机那道门就再也收不到真实意图了。写成 `true` 时
    /// 更糟：外来账号从此默认放行 —— 本命令要守住的正是「外来账号默认拒绝」。
    ///
    /// 用结构锁而不是行为锁，是因为命令层要 `AppHandle` + 在跑的远程服务；而且判定
    /// 「是不是外来账号」要真去跑 `tailscale whois` 与 `status` 比对，测试里给不出
    /// 一条外来账号。这与紧挨着的 `approve_pairing_does_not_pass_a_none_evidence`
    /// 是同一类锁，量的是同一件事的两头：那边管证据，这边管确认。
    #[test]
    fn approve_pairing_forwards_the_foreign_acknowledgement() {
        let args = approve_pairing_bound_args(include_str!("remote.rs"));

        assert!(
            args.contains("confirmed_foreign_account"),
            "approve_pairing 必须把外来账号确认原样传给 approve_bound，不能换成一个 \
             字面量：调用点写死会让状态机那道默认拒绝失效（写死 true 等于外来账号 \
             默认放行）。实际实参：\n{args}"
        );
    }

    /// 反向夹具：探测器必须量的是**调用点**，不是签名。
    ///
    /// 没有这条，上面那条锁就可能因为形参同名而恒绿 —— 一个永远抓不到东西的锁，
    /// 比没有锁更糟。
    #[test]
    fn forwarded_ack_detector_ignores_the_signature_parameter() {
        let fixture = "pub async fn approve_pairing(\n    \
             confirmed_foreign_account: bool,\n) {\n    \
             approve_bound(&store, true);\n}\n\
             pub async fn reject_pairing(";

        assert!(
            !approve_pairing_bound_args(fixture).contains("confirmed_foreign_account"),
            "探测器把签名里的形参当成了接线：调用点写死字面量时它仍报已转发"
        );
        assert!(
            approve_pairing_bound_args(
                "pub async fn approve_pairing() {
                    approve_bound(&store, confirmed_foreign_account);
                 }
                 pub async fn reject_pairing(",
            )
            .contains("confirmed_foreign_account"),
            "探测器连真正的转发都认不出来了"
        );
    }

    /// 结构锁：`approve_pairing` 不得把证据形参写成 `None`。
    ///
    /// `approve_bound` 的行为锁管住了「重算」这件事，这条管住**接线**：有人日后
    /// 把命令层改回直接调 `approve(.., None, ..)` 时，这里先红。
    #[test]
    fn approve_pairing_does_not_pass_a_none_evidence() {
        let source = include_str!("remote.rs");
        let body = source
            .split("pub async fn approve_pairing(")
            .nth(1)
            .and_then(|rest| rest.split("pub async fn reject_pairing(").next())
            .expect("approve_pairing body must be delimited by reject_pairing");

        assert!(
            !body.contains(", None,"),
            "approve_pairing 不得再把证据形参传成 None：\n{body}"
        );
        assert!(
            body.contains("approve_bound("),
            "approve_pairing 必须走 approve_bound（内部重算证据）：\n{body}"
        );
    }

    // --- 暴露边界的行为级闸门 ---------------------------------------------
    //
    // 这一族原先挂在 Tauri IPC 上验：app 由 `tauri::test::mock_builder` 构建，载荷
    // 走 `get_ipc_response` 的真实反序列化。那条路要求 `tauri` 的 `test` feature，
    // 也就是 `[dev-dependencies]` 里多出一条 crate —— 而本仓的判据是依赖树
    // 零新增。
    //
    // 于是判定下沉到 [`apply_exposure_config`]：命令层退化成「找到闸门 + 转发」的
    // 薄壳，实参收窄的判定、校验、落盘全在这一层之内，不需要任何 Tauri runtime。
    // 保留的是本族真正要验的那条负向断言 —— `{bind_scope:"lan"}` 的载荷既改不了
    // 生效中的绑定范围，也不会把越权夹带的字段写进 `exposure.json`。
    //
    // 让出去的是两样：「原生弹窗事件确实经 IPC 回到了 Rust 侧」那段只剩结构锁
    // （下面的 `bind_scope_command_wires_the_native_confirmation_event`），以及
    // 「测试 handler 登记齐全」那一条（由本模块既有的
    // `generate_handler_registers_exactly_twelve_commands` 覆盖）。

    /// 落盘文件名。生产侧是 `remote::config` 里的私有常量，这里照抄一份：断言要能
    /// 独立指出「磁盘上那个文件」，而不是跟着生产常量一起变。
    const EXPOSURE_FILE: &str = "exposure.json";

    /// 一份隔离的凭据目录，外加「远程服务没起时」命令层拿到的那扇门。
    ///
    /// 生产上 `set_remote_config` 在服务未启动时会 `BindScopeGate::load(auth_config_dir())`
    /// 重新读盘，因此这里每次都重新 `load`：读回值来自磁盘，不是内存里那份残留。
    struct Exposure {
        dir: tempfile::TempDir,
    }

    impl Exposure {
        fn new() -> Self {
            Self {
                dir: tempfile::tempdir().expect("create isolated credential dir"),
            }
        }

        fn gate(&self) -> BindScopeGate {
            BindScopeGate::load(self.dir.path())
        }

        /// 线上格式的载荷 → `RemoteConfig`，走 serde 反序列化。IPC 层把
        /// `InvokeBody::Json` 变成实参走的也是这一步，不是另一条路。
        fn payload(&self, raw: serde_json::Value) -> RemoteConfig {
            serde_json::from_value(raw)
                .expect("payload must deserialize into RemoteConfig（线上格式）")
        }

        fn bytes(&self) -> Option<Vec<u8>> {
            std::fs::read(self.dir.path().join(EXPOSURE_FILE)).ok()
        }
    }

    /// 对抗载荷：一次调用里同时夹带「放宽」意图的每一个字段。
    ///
    /// `bind_scope` / `bind_address` / `cors_origins` 三项都属于越权覆盖：
    /// 前者是本命令明令不接的绑定范围，后两者在这份契约里根本不存在。
    fn escalation_payload() -> serde_json::Value {
        json!({
            "enabled": true,
            "port": 4000,
            "tailscale_enabled": true,
            "bind_scope": "lan",
            "cors_mode": "explicitWhitelist",
            "origin_whitelist": ["*"],
            "cors_origins": ["*"],
            "bind_address": "0.0.0.0"
        })
    }

    /// 渲染层直接调 `set_remote_config({bind_scope:"lan"})` 之后，配置必须没变、
    /// 也没落盘。
    #[test]
    fn set_remote_config_cannot_widen_the_exposure_boundary() {
        let e = Exposure::new();
        assert_eq!(
            e.gate().read_remote_config().bind_scope,
            BindScope::Loopback,
            "起点必须是默认的仅回环"
        );

        // 先走一条**合法**的收紧调用把落盘文件建起来。判据③要比较的是「落盘前后
        // 逐字节一致」，基线必须是磁盘上一个真实存在、有真实内容的文件 —— 拿
        // `None` 当基线比出来的 `None == None` 是空断言，删掉落盘逻辑它照样绿。
        apply_exposure_config(
            &e.gate(),
            &e.payload(json!({
                "enabled": true,
                "port": 4000,
                "tailscale_enabled": false,
                // `bind_scope` 是 `RemoteConfig` 的必填字段，反序列化要求它在场；
                // 「在场」不等于「被采纳」—— 这正是本闸门要验的那件事。
                "bind_scope": "loopback",
                "cors_mode": "explicitWhitelist",
                "origin_whitelist": ["http://127.0.0.1:4000"]
            })),
        )
        .expect("收紧白名单必须被接受");
        let baseline = e
            .bytes()
            .expect("收紧之后必须落盘，否则后面的逐字节比较是空断言");

        // 载荷里的 `origin_whitelist: ["*"]` 是通配 origin，判据本身就该拒。
        let err = apply_exposure_config(&e.gate(), &e.payload(escalation_payload()))
            .expect_err("通配 origin 的白名单必须被拒");
        assert!(
            err.contains("origin whitelist entry rejected"),
            "拒绝理由必须来自白名单判据，而不是某条别的兜底：{err}"
        );

        // 判据①：读回来的绑定范围仍是默认值（仅回环）。
        assert_eq!(
            e.gate().read_remote_config().bind_scope,
            BindScope::Loopback,
            "set_remote_config 不得改绑定范围"
        );

        // 判据③：落盘文件一个字节都没动。
        assert_eq!(
            e.bytes().as_deref(),
            Some(baseline.as_slice()),
            "被拒的调用不得落盘：落盘前后的文件内容必须逐字节一致"
        );

        // 再走一条**会成功**的、同样夹带越权字段的调用：绑定范围照样不动。
        apply_exposure_config(
            &e.gate(),
            &e.payload(json!({
                "enabled": true,
                "port": 4000,
                "tailscale_enabled": false,
                "cors_mode": "explicitWhitelist",
                "origin_whitelist": ["http://127.0.0.1:4000"],
                "bind_scope": "lan",
                "bind_address": "0.0.0.0",
                "cors_origins": ["*"]
            })),
        )
        .expect("收紧白名单必须被接受");

        assert_eq!(
            e.gate().read_remote_config().bind_scope,
            BindScope::Loopback,
            "一次成功的收紧调用同样不得顺带把绑定范围放宽"
        );

        // 收紧写的是同一份配置，落盘后 bindScope 仍应是回环 —— 越权字段没有被
        // 「顺手带上」写进去。
        let raw = e.bytes().expect("收紧之后必须落盘，否则收紧也成了空操作");
        let persisted: serde_json::Value =
            serde_json::from_slice(&raw).expect("exposure.json must be JSON");
        assert_eq!(
            persisted
                .get("bindScope")
                .and_then(serde_json::Value::as_str),
            Some("loopback")
        );
        assert!(
            !String::from_utf8_lossy(&raw).contains("0.0.0.0"),
            "bind_address 不是这份契约里的字段，不得被写进落盘文件：{raw:?}"
        );
    }

    /// `set_remote_config` 只落 CORS 两项，绑定范围一个字节都不跟着走。
    ///
    /// 判据要能分辨两件事，而只看「`bind_scope` 没变」分辨不了：
    ///
    /// * 这条命令**真的执行了**（白名单确实被写进磁盘）；
    /// * 它**只**写了白名单，越权夹带的 `bind_scope: "lan"` 被原地丢弃。
    ///
    /// 所以基线与越权调用送的是**两份不同的白名单**：若落盘内容等于越权调用送的那份，
    /// 判定确实跑过；若等于基线那份，越权调用就是一次提前 return 的假绿。两者都不
    /// 成立时 `bindScope` 才真的是「被刻意忽略了」，而不是「整条调用没发生」。
    #[test]
    fn set_remote_config_persists_cors_but_drops_the_bind_scope() {
        let e = Exposure::new();

        // 起点：没有落盘文件，也没有内存态的越权。
        assert!(
            e.bytes().is_none(),
            "起点必须没有落盘文件，否则「未落盘」这条断言无从判断"
        );
        assert_eq!(
            e.gate().read_remote_config().bind_scope,
            BindScope::Loopback,
            "起点必须是默认的仅回环"
        );

        // --- 第一段：被白名单判据拒掉的越权调用，整份配置一个字节都不许落盘 ---
        let err = apply_exposure_config(&e.gate(), &e.payload(escalation_payload()))
            .expect_err("通配 origin 的白名单必须被拒");
        assert!(
            err.contains("origin whitelist entry rejected"),
            "拒绝理由必须来自白名单判据，而不是某条别的兜底：{err}"
        );
        assert!(e.bytes().is_none(), "被拒的调用不得留下任何落盘痕迹");
        assert_eq!(
            e.gate().read_remote_config().bind_scope,
            BindScope::Loopback,
            "被拒的调用不得改绑定范围"
        );

        // --- 第二段：一条**会成功**的调用，夹带的越权字段只多不少 ---
        //
        // 白名单故意与上面那份不同：落盘内容等于它，才能证明判定真的执行到了
        // `set_cors`，而不是在看到 `bind_scope` 之后就提前 return 了。
        let whitelist = ["http://127.0.0.1:4000", "https://desk.example.invalid"];
        apply_exposure_config(
            &e.gate(),
            &e.payload(json!({
                "enabled": true,
                "port": 4000,
                "tailscale_enabled": false,
                "bind_scope": "lan",
                "bind_address": "0.0.0.0",
                "cors_origins": ["*"],
                "cors_mode": "explicitWhitelist",
                "origin_whitelist": whitelist,
            })),
        )
        .expect("合法的显式白名单必须被接受");

        let raw = e
            .bytes()
            .expect("合法的收紧调用必须落盘，否则这条用例什么都没验到");
        let persisted: serde_json::Value =
            serde_json::from_slice(&raw).expect("exposure.json must be JSON");

        // 判据(c)：判定确实执行了 —— 送进去的白名单原样落盘。
        assert_eq!(
            persisted
                .get("corsMode")
                .and_then(serde_json::Value::as_str),
            Some("explicitWhitelist"),
            "set_remote_config 确实执行到了落盘这一步（否则下面几条都是空断言）：{persisted}"
        );
        let on_disk: Vec<String> = persisted
            .get("originWhitelist")
            .and_then(serde_json::Value::as_array)
            .map(|entries| {
                entries
                    .iter()
                    .filter_map(|e| e.as_str().map(str::to_string))
                    .collect()
            })
            .expect("originWhitelist must be an array");
        assert_eq!(
            on_disk,
            whitelist.to_vec(),
            "送进去的白名单必须原样落盘：{persisted}"
        );

        // 判据(a)：越权夹带的绑定范围在磁盘上、在读回接口上都仍是回环。
        assert_eq!(
            persisted
                .get("bindScope")
                .and_then(serde_json::Value::as_str),
            Some("loopback"),
            "set_remote_config 一律不得落盘 bind_scope：{persisted}"
        );
        assert_eq!(
            e.gate().read_remote_config().bind_scope,
            BindScope::Loopback,
            "set_remote_config 一律不得改生效中的绑定范围"
        );

        // 越权夹带的其余字段在这份契约里根本不存在，序列化时必须被静默丢弃。
        assert!(
            !String::from_utf8_lossy(&raw).contains("0.0.0.0"),
            "bind_address 不是这份契约里的字段，不得被写进落盘文件：{raw:?}"
        );
        assert!(
            !String::from_utf8_lossy(&raw).contains("cors_origins"),
            "cors_origins 不是这份契约里的字段，不得被写进落盘文件：{raw:?}"
        );
    }

    /// `BindScope` 上不存在「所有网卡」这个取值。
    ///
    /// 线上格式（`"lan"`）是反序列化的唯一入口，它认不出来的名字才是真的堵住了。
    /// 枚举多一个变体，这里先红。
    #[test]
    fn bind_scope_has_no_all_interfaces_variant() {
        assert_eq!(
            serde_json::from_value::<BindScope>(json!("loopback")).unwrap(),
            BindScope::Loopback
        );
        assert_eq!(
            serde_json::from_value::<BindScope>(json!("lan")).unwrap(),
            BindScope::Lan
        );

        for loose in ["all", "allInterfaces", "any", "public", "0.0.0.0", "*"] {
            assert!(
                serde_json::from_value::<BindScope>(json!(loose)).is_err(),
                "BindScope 反序列化不得接受 {loose:?}：本枚举里不存在「所有网卡」这个取值"
            );
        }

        assert_eq!(BindScope::default(), BindScope::Loopback);
        assert_eq!(RemoteConfig::default().bind_scope, BindScope::Loopback);
    }

    /// 点火不改配置、不落盘；兑现只能由原生弹窗那一侧发起。
    ///
    /// 两段式的**行为**在 `remote::config` 里有逐条断言（过期、猜错 id、重复兑现）。
    /// 这里钉的是命令层的接线：渲染层能碰到的只有 `request_bind_scope_change`，
    /// 它的返回值是 id 而不是配置。命令体若自己把点火兑现了，两段式就没了。
    fn bind_scope_only_registers(src: &str) -> Result<(), String> {
        let body = command_body(src, "request_bind_scope_change", "approve_pairing");

        if !body.contains("gate.request_bind_scope_change(scope)") {
            return Err(format!(
                "点火命令必须把范围登记到闸门上，而不是就地改配置：\n{body}"
            ));
        }
        if body.contains("set_cors(") {
            return Err(format!("点火命令不得写 CORS：\n{body}"));
        }

        // 兑现只能出现在后台监听器里。命令体在派发监听器之前的那一段里出现
        // `confirm_native`，就等于「点火即兑现」。
        let Some((dispatch, listener)) = body.split_once("tauri::async_runtime::spawn") else {
            return Err(format!(
                "request_bind_scope_change must spawn the confirmation listener：\n{body}"
            ));
        };
        if dispatch.contains("confirm_native(") {
            return Err(format!(
                "点火命令在派出监听器之前不得兑现这次点火：\n{dispatch}"
            ));
        }
        if !listener.contains("confirm_native(") {
            return Err(format!(
                "兑现必须发生在原生弹窗确认事件回来的监听器里：\n{listener}"
            ));
        }
        if !listener.contains("reject_native(") {
            return Err(format!(
                "弹窗被关掉同样要有明确的出口，而不是让记录悬着：\n{listener}"
            ));
        }
        Ok(())
    }

    /// 确认事件的接线不得被改成「谁的 id 都兑」。
    ///
    /// 监听器里那句 `payload.confirmation_id != expected_id` 是两段式的身份判据：
    /// 别人的 id、旧的 id 不是这次点火的确认。去掉它，任何一次确认事件都能兑现
    /// 一条还没被用户看的待确认记录。
    fn bind_scope_checks_the_confirmation_id(src: &str) -> Result<(), String> {
        let body = command_body(src, "request_bind_scope_change", "approve_pairing");

        if !body.contains("listen(BIND_SCOPE_CONFIRM_EVENT") {
            return Err(format!("确认事件必须由这条命令亲自监听：\n{body}"));
        }
        if !body.contains("payload.confirmation_id != expected_id") {
            return Err(format!(
                "监听器必须核对 confirmation id，别人的 id 不得兑现这次点火：\n{body}"
            ));
        }
        if !body.contains("NativeConfirmation") {
            return Err(format!(
                "事件载荷必须按 NativeConfirmation 反序列化：\n{body}"
            ));
        }
        Ok(())
    }

    /// `set_remote_config` 只能是转发。
    ///
    /// 这一族真正的判定住在 [`apply_exposure_config`] 里，命令层负责找闸门。命令层
    /// 若自己再写一份「取哪两个字段、怎么校验」，那就已经有两条判定路径了 —— 日后
    /// 改其中一条，另一条悄悄放行，而两边看上去都还对。
    fn set_remote_config_only_forwards(src: &str) -> Result<(), String> {
        let body = command_body(src, "set_remote_config", "request_bind_scope_change");

        if !body.contains("apply_exposure_config(&gate, &config)") {
            return Err(format!(
                "set_remote_config 必须把整份 config 原样转给 apply_exposure_config：\n{body}"
            ));
        }
        if body.contains("set_cors(") {
            return Err(format!("set_remote_config 不得自己再写一份判定：\n{body}"));
        }
        Ok(())
    }

    #[test]
    fn bind_scope_command_only_registers_and_never_cashes_in() {
        if let Err(reason) = bind_scope_only_registers(include_str!("remote.rs")) {
            panic!("{reason}");
        }
    }

    #[test]
    fn bind_scope_command_wires_the_native_confirmation_event() {
        if let Err(reason) = bind_scope_checks_the_confirmation_id(include_str!("remote.rs")) {
            panic!("{reason}");
        }
    }

    #[test]
    fn set_remote_config_forwards_to_apply_exposure_config() {
        if let Err(reason) = set_remote_config_only_forwards(include_str!("remote.rs")) {
            panic!("{reason}");
        }
    }

    /// 反向夹具：上面三条结构锁必须真的量得到东西。
    ///
    /// 读源码的锁天生有个致命失效模式 —— 切错地方就恒绿，而恒绿的锁比没有锁更
    /// 糟（它让人以为这一族钉住了）。所以每条锁都配一份「改成它该抓的样子」的
    /// 夹具：夹具必须报出失败，否则探测器已经失效。
    #[test]
    fn the_exposure_locks_catch_the_wiring_they_claim_to() {
        let src = include_str!("remote.rs");

        // 反向基线：没改动的源码，三条锁都通过。
        assert!(bind_scope_only_registers(src).is_ok());
        assert!(bind_scope_checks_the_confirmation_id(src).is_ok());
        assert!(set_remote_config_only_forwards(src).is_ok());

        // ① 判定被内联回命令层 —— 出现了第二条判定路径。
        let inlined = src.replace(
            "apply_exposure_config(&gate, &config)",
            "gate.set_cors(config.cors_mode, &config.origin_whitelist)",
        );
        assert!(
            set_remote_config_only_forwards(&inlined).is_err(),
            "判定被内联回 set_remote_config 时，转发锁必须先红"
        );

        // ② 去掉 id 核对 —— 任何一次确认事件都能兑现别人的点火。
        let id_checkless = src.replace("payload.confirmation_id != expected_id", "false");
        assert!(
            bind_scope_checks_the_confirmation_id(&id_checkless).is_err(),
            "监听器不再核对 confirmation id 时，身份锁必须先红"
        );

        // ③ 点火即兑现 —— 两段式塌成一段式。
        let eager = src.replace(
            "let confirmation_id = gate.request_bind_scope_change(scope);",
            "let confirmation_id = gate.request_bind_scope_change(scope);\n    \
             gate.confirm_native(&confirmation_id, Instant::now()).ok();",
        );
        assert!(
            bind_scope_only_registers(&eager).is_err(),
            "点火命令当场兑现时，两段式锁必须先红"
        );
    }

    /// 读 `remote.rs` 源码，取两个命令之间的那截函数体。
    ///
    /// 切分点只认 `pub async fn <name>`、不认后面的参数表：这族命令多带一个
    /// `<R: Runtime>`，按带括号的整串切会在泛型化之后静默找不到（`nth(1)` 返回
    /// `None`，直接变成一条读不懂的失败信息）。
    fn command_body(src: &str, name: &str, next: &str) -> String {
        src.split(&format!("pub async fn {name}"))
            .nth(1)
            .and_then(|rest| rest.split(&format!("pub async fn {next}")).next())
            .unwrap_or_else(|| panic!("{name} body must be delimited by {next}"))
            .to_string()
    }
}
