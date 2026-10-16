//! Remote Management HTTP Server
//!
//! 轻量 Web 服务，通过手机/远程浏览器切换 provider。
//! 直接调用 ProviderService::switch()，确保 backfill 和状态一致性。
//! 彻底解决独立 Python remote 绕过桌面端状态管理导致的竞态覆盖问题。

mod auth;
pub(crate) mod config;
mod events;
mod handlers;
mod html;
pub(crate) mod redact;
// 配对子系统要向桌面端暴露命令（`approve` / `reject` / 设备管理），所以可见性
// 从 crate 内放宽到 `pub(crate)`；仍然是 crate 私有，不对外成 API。
pub(crate) mod pairing;
mod whois;

// 下面是整个 remote 子系统唯一的 E2E 夹具挂载点（不是 `lib.rs`、不是
// `src-tauri/tests/`）：`mod e2e;` 只在 test 配置下编译。四层授权规则的 12 格
// 逐格用例就住在 `src-tauri/src/remote/e2e.rs`，其模块文档里也逐字写回了本挂载点。
#[cfg(test)]
mod e2e;

use auth::{auth_layer, AuthScope, REMOTE_LISTENER_IS_TLS};
use axum::http::Method;
use axum::routing::{MethodFilter, MethodRouter};
use axum::Router;
use events::{AppHandleSink, EventSink};
use pairing::{PairingApiState, PairingStore};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::{broadcast, RwLock};
use tokio::task::JoinHandle;
use tower_http::cors::{Any, CorsLayer};

/// 服务绑在哪个范围上。
///
/// 只有两档，**没有**「所有网卡」这个取值：要往外放必须显式选 [`BindScope::Lan`]，
/// 而那一步本身走两段式确认（见 [`config::BindScopeGate`]）。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum BindScope {
    /// 只绑回环地址（本机可达）
    #[default]
    Loopback,
    /// 额外绑局域网/tailnet 地址（外部可达）
    Lan,
}

/// 跨域策略。取值集合恰为两个，**不存在** `Any` / 通配变体 —— 放宽到任意
/// origin 这件事在本类型上不可构造，而不是靠调用方自觉不填。
///
/// [`CorsMode::ALL`] 是这个集合的唯一来源：枚举多一个变体，断言它长度的
/// 那个测试就先红。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum CorsMode {
    /// 只接受与被访问服务同源的浏览器请求
    #[default]
    SameOrigin,
    /// 只接受 [`crate::remote::config`] 白名单里逐条列出的 origin
    ExplicitWhitelist,
}

impl CorsMode {
    /// 全部取值的集合。新增变体时这里和
    /// `config::tests::cors_mode_has_no_wildcard_variant` 会一起挡下来。
    pub const ALL: [CorsMode; 2] = [CorsMode::SameOrigin, CorsMode::ExplicitWhitelist];
}

/// Remote server 配置
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RemoteConfig {
    /// 是否启用远程管理服务
    pub enabled: bool,
    /// 监听端口
    pub port: u16,
    /// 是否同时监听 Tailscale IP（远程访问）
    pub tailscale_enabled: bool,
    /// 绑定范围，默认回环。只能经两段式确认变更。
    pub bind_scope: BindScope,
    /// 跨域策略，默认同源。
    pub cors_mode: CorsMode,
    /// 显式 Origin 白名单，仅 [`CorsMode::ExplicitWhitelist`] 下生效。
    pub origin_whitelist: Vec<String>,
}

impl Default for RemoteConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            port: 4000,
            tailscale_enabled: false,
            bind_scope: BindScope::Loopback,
            cors_mode: CorsMode::SameOrigin,
            origin_whitelist: Vec::new(),
        }
    }
}

/// Tauri webview 的 origin：两个平台固有值，**不是**配置项。
///
/// 为什么它们必须显式在列：webview 里跑的正是那个要读响应的页面，而
/// `auth::policy` 的第②层判据对不在名单里的 Origin 一律 403 —— 漏掉这两条，
/// 桌面端自己的 UI 会落进第 11 格，表现是「打开就是白屏」。
///
/// 它们是常量而不是 [`RemoteConfig`] 的字段：它们描述的是「谁在读」，不是
/// 「用户允许了什么」，让用户去配它等于把一个非选择题伪装成选择题。
pub const TAURI_WEBVIEW_ORIGINS: [&str; 2] = ["tauri://localhost", "https://tauri.localhost"];

/// debug 构建下 Vite dev server 的 origin。
///
/// release 构建里不存在这个来源，因此用 `#[cfg]` 去掉而不是留着当一条永不
/// 命中的死条目 —— 后者会让人误以为「开发环境的白名单」在生产上也一样。
#[cfg(debug_assertions)]
pub const DEV_SERVER_ORIGIN: &str = "http://localhost:3000";

/// 本服务自身的 origin：`(scheme, 127.0.0.1, port)`。
///
/// 端口取 [`RemoteConfig::port`] 而不是实际绑定值：origin 是**浏览器**看到的
/// 串，`127.0.0.1:0` 起起来的临时端口上没有任何浏览器会来请求，把它写进
/// 白名单只会在文档里留一个假 origin。
fn self_origin(cfg: &RemoteConfig, tls: bool) -> String {
    format!("{}://127.0.0.1:{}", scheme_of(tls), cfg.port)
}

/// 当前 listener 的 scheme。
///
/// 与 [`auth::cookie::REMOTE_LISTENER_IS_TLS`] 同源：cookie 上的 `Secure` 与
/// CORS 白名单里的 scheme 说的是同一件事实（这个 listener 走不走 TLS），
/// 两处分岔就会得到「cookie 带了 Secure 但 origin 是 http」这种浏览器直接
/// 静默拒收的组合。
fn scheme_of(tls: bool) -> &'static str {
    if tls {
        "https"
    } else {
        "http"
    }
}

/// CORS `Access-Control-Allow-Origin` 的白名单。
///
/// **这是装配层唯一的 origin 来源**：[`RemoteServer::build_router`] 只经它取
/// CORS 名单，测试与将来的其它装配点也必须经它 —— 两处各写一份清单，
/// 漏一条的后果是「内嵌 webview 被浏览器按跨源掐掉」，而这类故障只在
/// 生产构建里表现为白屏。
///
/// 名单由三部分拼成，来源各自独立、互不覆盖：
///
/// 1. **本服务自身**（`self_origin`）：服务自己发出去的页面的 origin。
/// 2. **内嵌 webview**（[`TAURI_WEBVIEW_ORIGINS`] + [`DEV_SERVER_ORIGIN`]）：
///    谁在读响应，不受用户配置影响。
/// 3. **Tailscale origin**：仅当 [`RemoteConfig::tailscale_enabled`]，远程浏览器
///    打开的正是那个地址。
/// 4. **用户白名单**：仅当 [`CorsMode::ExplicitWhitelist`]，逐条过
///    [`config::validate_whitelist`] —— 与 `set_cors` 那道闸门**同一份判据**。
///
/// 三条硬约束：
///
/// - **没有通配**。`CorsLayer` 的 `allow_origin(Any)` 与白名单里的 `*` 是同一句
///   「任意 origin」，而 Origin 是攻击者完全可控的字段：放宽到任意 origin，
///   任意网页都读得到响应内容，等于把鉴权结果交给攻击者页面。
/// - **没有 `Origin: null`**。它不归属任何站点（sandbox iframe / `file:` /
///   `data:` 都发它），列进去等于放行整个 sandbox 面。
/// - **`RemoteConfig` 可以被直接构造**。`set_cors` 会拒毒条目，但命令层、测试
///   夹具、将来的迁移代码都能绕过那道闸门，所以本函数自己再过一遍。
pub fn allowed_origins(cfg: &RemoteConfig, tls: bool) -> Vec<axum::http::HeaderValue> {
    use axum::http::HeaderValue;

    let scheme = scheme_of(tls);
    let mut origins: Vec<String> = vec![self_origin(cfg, tls)];
    origins.extend(TAURI_WEBVIEW_ORIGINS.iter().map(|o| o.to_string()));
    #[cfg(debug_assertions)]
    origins.push(DEV_SERVER_ORIGIN.to_string());

    if cfg.tailscale_enabled {
        if let Some(ip) = get_tailscale_ip() {
            origins.push(format!("{scheme}://{ip}:{}", cfg.port));
        }
    }

    if cfg.cors_mode == CorsMode::ExplicitWhitelist {
        origins.extend(
            cfg.origin_whitelist
                .iter()
                .filter(|o| config::validate_whitelist(std::slice::from_ref(o)).is_ok())
                .cloned(),
        );
    }

    origins
        .into_iter()
        // 装不进 header 的串（内嵌 CR/LF 等）直接丢：一份读不出内容的名单
        // 塞给 `CorsLayer` 只会 panic，而 panic 发生在起服务的路径上。
        .filter_map(|o| HeaderValue::from_str(o.trim()).ok())
        .collect()
}

/// Tauri 运行时句柄，可能缺席（headless）。
///
/// 为什么包一层而不是把字段改成 `Option<AppHandle>`：`handlers.rs` 有 12 处
/// 直接以 `state.app_handle.state::<AppState>()` / `&state.app_handle` 的形式
/// 使用它。靠 `Deref` 转发，这些调用点一行都不用改。
///
/// 生产构建下 `inner` 恒为 `Some`：`headless()` 带 `#[cfg(test)]`，非测试构建
/// 根本构造不出空句柄，所以「GUI 专属路由在 headless 下取句柄」这条路径是
/// 测试构建独有的。
pub struct RemoteApp {
    inner: Option<tauri::AppHandle>,
}

impl RemoteApp {
    /// 生产路径：总是持有真实运行时
    pub fn gui(app: tauri::AppHandle) -> Self {
        Self { inner: Some(app) }
    }

    /// 无 GUI 运行时的构造入口，仅测试构建可用（E2E 基座用它起服务）
    #[cfg(test)]
    pub fn headless() -> Self {
        Self { inner: None }
    }
}

impl std::ops::Deref for RemoteApp {
    type Target = tauri::AppHandle;

    fn deref(&self) -> &Self::Target {
        self.inner.as_ref().expect(
            "headless remote server: this route needs the Tauri runtime \
             (managed AppState / tray); it is unavailable without an AppHandle",
        )
    }
}

impl Clone for RemoteApp {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

/// Remote server 共享状态
pub struct RemoteState {
    /// Tauri AppHandle，用于访问 managed AppState 和刷新托盘。
    /// headless 构造下缺席，此时依赖 GUI 的路由不可用。
    pub app_handle: RemoteApp,
    /// 事件出口：把审批事件/前端通知发出去，可在测试中替换
    pub sink: Arc<dyn EventSink>,
    /// SSE 广播通道发送端
    pub sse_tx: broadcast::Sender<String>,
    /// 凭据落盘目录：配对签发与 `auth_layer` 验凭据读的是同一个。
    ///
    /// 显式随 server 走而不是各自去读全局：测试夹具因此能指向自己的临时目录，
    /// 生产端则是 `~/.cc-switch`。
    pub config_dir: PathBuf,
    /// 运行状态标志：stop() 后设为 false，拒绝新请求
    pub running: std::sync::atomic::AtomicBool,
}

struct ListenerEntry {
    shutdown_tx: tokio::sync::oneshot::Sender<()>,
    handle: JoinHandle<()>,
}

/// Remote 管理服务器
pub struct RemoteServer {
    state: Arc<RemoteState>,
    /// 配对子系统的独立状态：不持有 AppHandle，因此能与 GUI 生命周期脱钩。
    ///
    /// 桌面端的审批命令要读写同一个 store（见 `commands::remote`），故对 crate 内
    /// 可见 —— 与 server 另起一个 store 会让「批了」落不到 HTTP 路由那边。
    pub(crate) pairing: PairingApiState,
    /// 对外暴露边界（绑定范围 / CORS / 白名单）。同样对 crate 内可见 —— 桌面端的
    /// `get/set_remote_config` 与两段式点火读写的是同一个闸门。
    pub(crate) exposure: config::BindScopeGate,
    /// 按地址独立管理的 listener：支持动态增删，Tailscale 切换时不中断 localhost
    listeners: Arc<RwLock<HashMap<SocketAddr, ListenerEntry>>>,
    config: RwLock<RemoteConfig>,
}

/// Tauri managed state wrapper
pub struct ManagedRemoteServer(Arc<RwLock<Option<RemoteServer>>>);

/// 业务路由的鉴权分区表：`(路径, 该路径的凭据通道)`。
///
/// 分区依据两条**互相独立**的事实，都不是「这条路由要不要保护」：
///
/// 1. 无凭据时是否必须可达 —— 只有配对端点与根页面是 `Public`。
/// 2. 载体在协议层有没有地方放 token —— `<img src>` / `EventSource` 放不了，
///    只能走会话 cookie，因此 `/api/icon`、`/api/provider-icons/:name`、
///    `/api/events` 是 `BrowserSafe`（`html.rs` 里这三处调用点为证）。
///    其余都是 `Protected`，接受 `Authorization: Bearer` **与会话 cookie 两条
///    通道**（顺序与语义见 `auth::middleware` 里的 `AuthScope::credential_channels`；
///    某个载体一旦在请求里出现，它就必须验过，不靠另一条通道回落过关）。
///
/// 三类 scope **全部**经过同一个 [`auth_layer`]；`BrowserSafe` 绝不等于跳过
/// 中间件，它只是换了一条凭据通道。
///
/// 配对端点（`/pairing/*`）是「拿到凭据之前」那一方的入口，属于配对子系统
/// 自带的一次性凭据，不在本表内、也不计入这 14 条。
pub const ROUTE_SCOPE_TABLE: &[(&str, AuthScope)] = &[
    ("/", AuthScope::Public),
    ("/api/providers", AuthScope::Protected),
    ("/api/usage", AuthScope::Protected),
    ("/api/switch", AuthScope::Protected),
    ("/api/reorder", AuthScope::Protected),
    ("/api/current", AuthScope::Protected),
    ("/api/events", AuthScope::BrowserSafe),
    ("/api/health", AuthScope::Protected),
    ("/api/icon", AuthScope::BrowserSafe),
    ("/api/provider-icons/:name", AuthScope::BrowserSafe),
    ("/api/proxy/control", AuthScope::Protected),
    ("/api/proxy/takeover", AuthScope::Protected),
    ("/api/proxy/failover", AuthScope::Protected),
    ("/api/provider/test", AuthScope::Protected),
];

/// 业务路由进入 Router 的**唯一**入口。
///
/// 裸 `.route("/…")` 一旦允许，某条新路由就能绕开 [`ROUTE_SCOPE_TABLE`]，
/// 也就绕开了鉴权分区 —— 所以路径是 `&'static str` 而不是
/// `impl AsRef<str>`：动态拼出来的路径在编译期就走不进这个入口。
///
/// `cfg` 与 CORS 装配读的是**同一个值**（[`build_router`] 里那一份），这样中间件
/// 的 Origin 判据与 `Access-Control-Allow-Origin` 永远同源。少传一个参数就没
/// 法从这一层保证「同一份配置来源」。
#[allow(clippy::too_many_arguments)]
fn register<S>(
    router: Router<S>,
    path: &'static str,
    method: Method,
    scope: AuthScope,
    handler: MethodRouter<S>,
    config_dir: &Path,
    cfg: &RemoteConfig,
) -> Router<S>
where
    S: Clone + Send + Sync + 'static,
{
    assert!(
        ROUTE_SCOPE_TABLE.iter().any(|(p, _)| *p == path),
        "路由 {path} 没有登记在 ROUTE_SCOPE_TABLE 里：新增路由必须先在表中声明它的凭据通道"
    );
    // method 必须能映射成一个 MethodFilter。映射不出来说明装配处写了非标准动词，
    // 装配期就红，不留到某个请求打过来时才发现这条路由的方法是空的。
    MethodFilter::try_from(method.clone())
        .unwrap_or_else(|_| panic!("路由 {path} 声明了无法映射到 MethodFilter 的方法 {method}"));

    router.route(
        path,
        handler.layer(auth_layer(scope, config_dir.to_path_buf(), cfg)),
    )
}

impl ManagedRemoteServer {
    pub fn new(inner: Arc<RwLock<Option<RemoteServer>>>) -> Self {
        Self(inner)
    }

    pub fn lock(&self) -> &Arc<RwLock<Option<RemoteServer>>> {
        &self.0
    }

    /// 获取 sse_tx 广播通道（仅当 server 已启动时返回 Some）
    pub async fn sse_tx(&self) -> Option<broadcast::Sender<String>> {
        let guard = self.0.read().await;
        guard.as_ref().map(|s| s.state().sse_tx.clone())
    }
}

/// 获取 Tailscale IPv4 地址
///
/// 走 `whois::resolve_bin()` 的三级解析阶梯（覆盖变量 → 绝对候选 → PATH），
/// 而不是裸 `Command::new("tailscale")`：GUI 启动的进程常常不带 `/usr/local/bin`，
/// 裸命令名会在这台机器上「每次都查不到」。
pub fn get_tailscale_ip() -> Option<String> {
    let bin = whois::resolve_bin()?;
    std::process::Command::new(bin)
        .args(["ip", "-4"])
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        .filter(|ip| !ip.is_empty())
}

/// Check if Tailscale CLI is available and returns an IP
pub fn is_tailscale_available() -> bool {
    get_tailscale_ip().is_some()
}

/// Start or update the remote server.
/// If a server already exists, performs an incremental update:
/// - keeps existing listeners (e.g. localhost) that are still needed
/// - only starts/stops the changed addresses (e.g. Tailscale IP)
pub async fn start_remote(
    app: &tauri::AppHandle,
    config: RemoteConfig,
) -> Result<Vec<String>, String> {
    use tauri::Manager;

    let managed = app.state::<ManagedRemoteServer>();
    let mut guard = managed.lock().write().await;

    if let Some(server) = guard.as_ref() {
        // 已有 server：增量更新，不中断保留的地址
        log::info!("[Remote] Updating remote server configuration...");
        let urls = server.update(config).await?;
        log::info!("[Remote] Server updated on: {}", urls.join(", "));
        Ok(urls)
    } else {
        // 首次启动
        let new_server = RemoteServer::new(
            RemoteApp::gui(app.clone()),
            Arc::new(AppHandleSink::new(app.clone())),
            config,
            crate::config::get_app_config_dir(),
        )?;
        let urls = new_server.start().await?;
        *guard = Some(new_server);
        log::info!("[Remote] Server started on: {}", urls.join(", "));
        Ok(urls)
    }
}

/// Stop the remote server
pub async fn stop_remote(app: &tauri::AppHandle) -> Result<(), String> {
    use tauri::Manager;

    let managed = app.state::<ManagedRemoteServer>();
    let mut guard = managed.lock().write().await;

    if let Some(server) = guard.take() {
        // 用户显式关闭：先广播 shutdown 让浏览器主动销毁页面
        match server
            .state()
            .sse_tx
            .send(r#"{"type":"shutdown"}"#.to_string())
        {
            Ok(n) => log::info!("[Remote] Broadcasted shutdown to {} SSE clients", n),
            Err(e) => log::warn!("[Remote] No SSE clients to broadcast shutdown: {e}"),
        }
        // 给 SSE 消息一点时间到达客户端
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        server.stop().await?;
    }

    log::info!("[Remote] Server stopped via command");
    Ok(())
}

impl RemoteServer {
    /// 构造 server。
    ///
    /// `config_dir` 是凭据落盘目录，必须与 `auth_layer` 读的是同一个：配对在
    /// 这里签发的摘要，中间件要能在那里读到，反之亦然。传错目录的后果不是
    /// 「配对不生效」，而是**签发出去一把中间件永远不认的死 token**。
    ///
    /// 顺带在这里建 `remote-auth/secret`（0600）：签发摘要要用它，吊销时反查
    /// 也要用。凭据文件过宽（0644）时这里就报错，远程服务拒绝带着一个别人
    /// 也读得到的 secret 起来。
    fn new(
        app_handle: RemoteApp,
        sink: Arc<dyn EventSink>,
        config: RemoteConfig,
        config_dir: PathBuf,
    ) -> Result<Self, String> {
        let (sse_tx, _) = broadcast::channel(16);
        let secret = auth::load_or_create(&config_dir).map_err(|e| e.to_string())?;

        let state = Arc::new(RemoteState {
            sink,
            app_handle,
            sse_tx,
            config_dir: config_dir.clone(),
            running: std::sync::atomic::AtomicBool::new(true),
        });

        Ok(Self {
            // 审批事件与其它前端事件共用同一个出口：桌面端只需订阅一处。
            pairing: PairingApiState {
                // 必须与下面 `secret` 是同一把：状态机交给撤销的那份摘要要拿去
                // `clients.json` 里逐条 `ct_eq` 比对，而那份库正是用这把 secret
                // 写的。状态机自己另生成一把的话，撤销会受理成功却一条没摘。
                store: PairingStore::with_secret(secret),
                sink: state.sink.clone(),
                config_dir: config_dir.clone(),
                secret,
            },
            state,
            exposure: config::BindScopeGate::load(config_dir.clone()),
            listeners: Arc::new(RwLock::new(HashMap::new())),
            config: RwLock::new(config),
        })
    }

    /// 根据配置计算目标监听地址列表
    fn compute_target_addrs(config: &RemoteConfig) -> Vec<SocketAddr> {
        let mut addrs = Vec::new();

        // 始终包含 localhost
        if let Ok(addr) = format!("127.0.0.1:{}", config.port).parse() {
            addrs.push(addr);
        }

        // 可选：添加 Tailscale IP
        if config.tailscale_enabled {
            if let Some(ts_ip) = get_tailscale_ip() {
                if let Ok(addr) = format!("{}:{}", ts_ip, config.port).parse() {
                    addrs.push(addr);
                }
            }
        }

        addrs
    }

    /// 启动指定地址的 listener（如果已在运行则跳过）
    async fn start_addrs(&self, addrs: Vec<SocketAddr>) -> Vec<String> {
        let app = self.build_router();
        let mut started_urls = Vec::new();
        let mut listeners = self.listeners.write().await;

        for addr in addrs {
            if listeners.contains_key(&addr) {
                started_urls.push(format!("http://{addr}"));
                continue;
            }

            match tokio::net::TcpListener::bind(addr).await {
                Ok(listener) => {
                    // 端口 0 时内核会分配真实端口，必须以 listener 实际绑定的
                    // 地址对外报 URL，否则会把 ":0" 泄漏给调用方。
                    let bound = listener.local_addr().unwrap_or(addr);
                    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();

                    let app_clone = app.clone();
                    let handle = tokio::spawn(async move {
                        // `into_make_service_with_connect_info`：配对限流按来源
                        // peer IP 计，缺了它 handler 拿不到 `ConnectInfo`。
                        let serve = axum::serve(
                            listener,
                            app_clone.into_make_service_with_connect_info::<SocketAddr>(),
                        );
                        let graceful = serve.with_graceful_shutdown(async move {
                            let _ = shutdown_rx.await;
                        });
                        if let Err(e) = graceful.await {
                            log::warn!("[Remote] Server error on {addr}: {e}");
                        }
                    });

                    listeners.insert(
                        addr,
                        ListenerEntry {
                            shutdown_tx,
                            handle,
                        },
                    );
                    started_urls.push(format!("http://{bound}"));
                    log::info!("[Remote] Listening on {bound}");
                }
                Err(e) => {
                    log::warn!("[Remote] Failed to bind {addr}: {e}");
                }
            }
        }

        started_urls
    }

    /// 停止指定地址的 listener
    async fn stop_addrs(&self, addrs: Vec<SocketAddr>) {
        let mut listeners = self.listeners.write().await;

        for addr in addrs {
            if let Some(entry) = listeners.remove(&addr) {
                let _ = entry.shutdown_tx.send(());
                let abort_handle = entry.handle.abort_handle();

                match tokio::time::timeout(std::time::Duration::from_secs(3), entry.handle).await {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) => log::warn!("[Remote] Listener task on {addr} panicked: {e}"),
                    Err(_) => {
                        log::warn!("[Remote] Listener on {addr} stop timeout, aborting");
                        abort_handle.abort();
                    }
                }

                log::info!("[Remote] Stopped listening on {addr}");
            }
        }
    }

    /// 启动所有配置中的地址（首次启动用）
    async fn start(&self) -> Result<Vec<String>, String> {
        let config = self.config.read().await.clone();
        let addrs = Self::compute_target_addrs(&config);
        let urls = self.start_addrs(addrs).await;

        if urls.is_empty() {
            return Err("Failed to bind any address".to_string());
        }

        Ok(urls)
    }

    /// 增量更新：根据新配置增删 listener，不中断保留的地址
    async fn update(&self, new_config: RemoteConfig) -> Result<Vec<String>, String> {
        let current_addrs: Vec<SocketAddr> = self.listeners.read().await.keys().cloned().collect();
        let target_addrs = Self::compute_target_addrs(&new_config);

        // 停止不再需要的地址
        let to_stop: Vec<SocketAddr> = current_addrs
            .iter()
            .filter(|a| !target_addrs.contains(a))
            .cloned()
            .collect();
        if !to_stop.is_empty() {
            self.stop_addrs(to_stop).await;
        }

        // 启动新地址
        let to_start: Vec<SocketAddr> = target_addrs
            .iter()
            .filter(|a| !current_addrs.contains(a))
            .cloned()
            .collect();
        let urls: Vec<String> = target_addrs.iter().map(|a| format!("http://{a}")).collect();

        if !to_start.is_empty() {
            self.start_addrs(to_start).await;
        }

        // 更新配置
        *self.config.write().await = new_config;

        Ok(urls)
    }

    async fn stop(&self) -> Result<(), String> {
        // 先标记为 stopped，拒绝已建立连接上的新请求
        self.state
            .running
            .store(false, std::sync::atomic::Ordering::SeqCst);
        log::info!("[Remote] Running flag set to false");

        let addrs: Vec<SocketAddr> = self.listeners.read().await.keys().cloned().collect();
        self.stop_addrs(addrs).await;

        log::info!("[Remote] Server stopped");
        Ok(())
    }

    fn state(&self) -> Arc<RemoteState> {
        self.state.clone()
    }

    /// 配对路由。状态是 [`PairingApiState`] 而不是 [`RemoteState`] —— 配对是
    /// 「还没拿到凭据的人」走的路，不该经过持有 AppHandle 的那套状态。
    ///
    /// 但**闸门照挂不误**：这里与 [`build_router`] 里 14 条业务路由走的是同一个
    /// [`auth_layer`]，scope 取 [`AuthScope::Public`]。scope 决定的是「凭据闸门
    /// 要不要」，不是「Origin 否决要不要」—— [`auth::middleware`] 的判定链把
    /// Origin 否决排在最前，`Public` 只豁免第三段。于是配对端的语义与
    /// `GET /`（同样是 `Public`）逐条对齐：
    ///
    /// - `Origin` 在场且不在白名单 → 403（凭据再对也不救）；
    /// - `Origin` 缺失或合法 → 放行，绝不因为「没带凭据」而 401。
    ///
    /// 少了这一层，四条 `/pairing/*` 就成了 Origin 白名单上唯一的缺口：任意网页
    /// 能让浏览器带着攻击者的 `Origin` 打 `POST /pairing/request`，而那一条
    /// 路由的作用正是**签发长期凭据**。`cfg` 必须由 [`build_router`] 传进来，
    /// 好让中间件读到的 Origin 名录与 CORS 的 `Access-Control-Allow-Origin`
    /// 仍然同源（同一个 `allowed_origins(&cfg, ..)`）。
    ///
    /// 刻意不进 [`register`]：那条入口断言路径已登记在 [`ROUTE_SCOPE_TABLE`]，
    /// 而配对路由不计入那 14 条 —— 这里挂的是同一段中间件，不是同一张分区表。
    fn pairing_router(&self, cfg: &RemoteConfig) -> Router<Arc<RemoteState>> {
        pairing::router(self.pairing.clone())
            .with_state(self.pairing.clone())
            .layer(auth_layer(
                AuthScope::Public,
                self.state.config_dir.clone(),
                cfg,
            ))
    }

    fn build_router(&self) -> Router {
        // Origin 白名单只有一份来源：`allowed_origins` 按 `RemoteConfig` 算。
        // 这里若自己另列一份，中间件那边就永远漏掉其中几条 —— 两份清单各自
        // 漂移，而症状是「内嵌 webview 被浏览器按跨源掐掉」，只在生产构建里
        // 表现为白屏。读不到配置时退回默认配置（收紧的一档），绝不放宽。
        let cfg = self
            .config
            .try_read()
            .map(|c| c.clone())
            .unwrap_or_default();
        let origins = allowed_origins(&cfg, REMOTE_LISTENER_IS_TLS);

        let cors = CorsLayer::new()
            .allow_origin(origins)
            .allow_methods(Any)
            .allow_headers(Any);

        // 业务路由只能经受控的 register() 进入 Router：路径与 scope 一并
        // 声明在本函数里，鉴权中间件由 register 挂在每条路由上。
        let mut router: Router<Arc<RemoteState>> = Router::new();
        router = register(
            router,
            "/",
            Method::GET,
            AuthScope::Public,
            axum::routing::get(handlers::index),
            self.state.config_dir.as_path(),
            &cfg,
        );
        router = register(
            router,
            "/api/providers",
            Method::GET,
            AuthScope::Protected,
            axum::routing::get(handlers::get_providers),
            self.state.config_dir.as_path(),
            &cfg,
        );
        router = register(
            router,
            "/api/usage",
            Method::GET,
            AuthScope::Protected,
            axum::routing::get(handlers::get_usage),
            self.state.config_dir.as_path(),
            &cfg,
        );
        router = register(
            router,
            "/api/switch",
            Method::POST,
            AuthScope::Protected,
            axum::routing::post(handlers::switch_provider),
            self.state.config_dir.as_path(),
            &cfg,
        );
        router = register(
            router,
            "/api/reorder",
            Method::POST,
            AuthScope::Protected,
            axum::routing::post(handlers::reorder_providers),
            self.state.config_dir.as_path(),
            &cfg,
        );
        router = register(
            router,
            "/api/current",
            Method::GET,
            AuthScope::Protected,
            axum::routing::get(handlers::get_current),
            self.state.config_dir.as_path(),
            &cfg,
        );
        router = register(
            router,
            "/api/events",
            Method::GET,
            AuthScope::BrowserSafe,
            axum::routing::get(handlers::sse_events),
            self.state.config_dir.as_path(),
            &cfg,
        );
        router = register(
            router,
            "/api/health",
            Method::GET,
            AuthScope::Protected,
            axum::routing::get(handlers::health_check),
            self.state.config_dir.as_path(),
            &cfg,
        );
        router = register(
            router,
            "/api/icon",
            Method::GET,
            AuthScope::BrowserSafe,
            axum::routing::get(handlers::get_icon),
            self.state.config_dir.as_path(),
            &cfg,
        );
        router = register(
            router,
            "/api/provider-icons/:name",
            Method::GET,
            AuthScope::BrowserSafe,
            axum::routing::get(handlers::get_provider_icon),
            self.state.config_dir.as_path(),
            &cfg,
        );
        router = register(
            router,
            "/api/proxy/control",
            Method::POST,
            AuthScope::Protected,
            axum::routing::post(handlers::proxy_control),
            self.state.config_dir.as_path(),
            &cfg,
        );
        router = register(
            router,
            "/api/proxy/takeover",
            Method::POST,
            AuthScope::Protected,
            axum::routing::post(handlers::proxy_takeover),
            self.state.config_dir.as_path(),
            &cfg,
        );
        router = register(
            router,
            "/api/proxy/failover",
            Method::POST,
            AuthScope::Protected,
            axum::routing::post(handlers::proxy_failover),
            self.state.config_dir.as_path(),
            &cfg,
        );
        router = register(
            router,
            "/api/provider/test",
            Method::POST,
            AuthScope::Protected,
            axum::routing::post(handlers::test_provider),
            self.state.config_dir.as_path(),
            &cfg,
        );

        // 配对是「还没拿到凭据」一方唯一的入口，走配对子系统自带的一次性凭据，
        // 不在本表内、不计入 14 条业务路由 —— 但 Origin 白名单与业务路由共用同一段
        // 中间件（见 `pairing_router`），白名单外的 Origin 一律 403。
        router
            .merge(self.pairing_router(&cfg))
            .layer(cors)
            .with_state(self.state.clone())
    }
}

/// App 内切换 provider 后，直接广播 SSE 给所有已连接的远程浏览器（App → Web 方向）。
/// 在 tray.rs / failover.rs 等切换点调用此函数，替代无效的 app_handle.listen() 方案
/// （app_handle.emit() 只发给 JS 前端，不触发 Rust listen 回调）。
///
/// 函数内部会从 AppState 查询 provider 名称，调用方只需传入 provider_id 和 app_type。
pub async fn broadcast_provider_switch(
    app: &tauri::AppHandle,
    app_type_str: &str,
    provider_id: &str,
) {
    use tauri::Manager;

    let managed = match app.try_state::<ManagedRemoteServer>() {
        Some(m) => m,
        None => {
            log::warn!("[Remote] broadcast_provider_switch: ManagedRemoteServer not found");
            return;
        }
    };

    let guard = managed.lock().read().await;
    let server = match guard.as_ref() {
        Some(s) => s,
        None => {
            log::warn!("[Remote] broadcast_provider_switch: RemoteServer not started");
            return;
        }
    };

    let name: String = app
        .try_state::<crate::store::AppState>()
        .and_then(|s| {
            s.db.get_all_providers(app_type_str)
                .ok()
                .and_then(|map| map.get(provider_id).map(|p| p.name.clone()))
        })
        .unwrap_or_else(|| provider_id.to_string());

    let msg = serde_json::json!({
        "type": "switch",
        "provider_id": provider_id,
        "name": name,
    })
    .to_string();

    match server.state().sse_tx.send(msg) {
        Ok(_) => {
            log::info!(
                "[Remote] Broadcasted provider switch: {} (id={})",
                name,
                provider_id
            );
        }
        Err(e) => {
            log::error!("[Remote] Failed to broadcast provider switch: {}", e);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::auth::origin_is_allowed;
    use super::{allowed_origins, CorsMode, RemoteConfig};

    /// 剥掉行尾注释（区分字符串字面量），让断言读的是代码而不是说明。
    fn strip_comments(src: &str) -> String {
        let mut out = String::new();
        for line in src.lines() {
            let bytes = line.as_bytes();
            let mut in_str = false;
            let mut cut = bytes.len();
            let mut i = 0;
            while i < bytes.len() {
                match bytes[i] {
                    b'\\' if in_str => i += 1,
                    b'"' => in_str = !in_str,
                    b'/' if !in_str && bytes.get(i + 1) == Some(&b'/') => {
                        cut = i;
                        break;
                    }
                    _ => {}
                }
                i += 1;
            }
            let code = line[..cut].trim();
            if code.is_empty() {
                continue;
            }
            if !out.is_empty() {
                out.push('\n');
            }
            out.push_str(code);
        }
        out
    }

    /// 本文件的生产代码部分：`mod tests {` 之前的全部内容，且已剥掉行尾注释。
    ///
    /// 两处收窄缺一不可：
    ///
    /// - 剥注释：装配点上方那段注释里也写着 `into_make_service_with_connect_info`，
    ///   直接 `contains` 会被纯说明文字骗过去。
    /// - 截到测试模块之前：断言用的字面量本身就是源文件里的一行，自指的
    ///   needle 会把自己数进去，让断言恒真。
    fn production_code(src: &str) -> String {
        let head = src
            .split("mod tests {")
            .next()
            .expect("源码里总有 mod tests {");
        strip_comments(head)
    }

    /// serve 必须经 `into_make_service_with_connect_info::<SocketAddr>()` 装配。
    ///
    /// 缺了它，axum 不会往 request 的 extensions 里塞对端地址，中间件的
    /// `peer_of` 永远读到 `None` → 一律按 NonLoopback 算，四层规则就退化成
    /// 「碰运气」：服务将来绑到 `0.0.0.0` 时，外部请求的 peer 应当非 loopback
    /// 并自动落到要 token 的分支，而这个前提只有在装配了这一层时才成立。
    #[test]
    fn serve_uses_connect_info_service() {
        let code = production_code(include_str!("mod.rs"));
        assert!(
            code.contains("into_make_service_with_connect_info::<SocketAddr>()"),
            "axum::serve 必须走 into_make_service_with_connect_info::<SocketAddr>()，\
             否则中间件拿不到真实 TCP peer"
        );
        // 两个 listener 共用同一个 Router，共享的 serve 装配点就是
        // `for addr in addrs` 循环里的那一次调用；出现两次说明有人按地址各写了一份。
        assert_eq!(
            code.matches("axum::serve(").count(),
            1,
            "两个 listener 必须共用同一处 axum::serve 装配：ConnectInfo 的装配不能按地址分叉"
        );
    }

    /// CORS 白名单里既没有通配，也没有 `Origin: null`。
    ///
    /// 这两条各对应一类真实来源：`null` 是 sandbox iframe / `file:` / `data:`
    /// 页面发出的请求，`*` 则**就是**「任意 origin」的另一种拼写。放进白名单
    /// 等于把「有页面在跨源读这个服务」这件事从「需要逐条批准」降级成「默认允许」，
    /// 而 Origin 是浏览器给的、攻击者完全可控的字段。
    ///
    /// 反证输入是「最毒」的白名单：`validate_whitelist` 在 `set_cors` 那道闸门上
    /// 已经会拒它们，但 [`RemoteConfig`] 是个可以直接构造的普通结构体 ——
    /// 命令层、测试夹具、未来的迁移代码都能绕过那道闸门。`allowed_origins`
    /// 因此自己再过一遍同一份判据（[`config::validate_whitelist`]），
    /// 而不是靠「入口已经校验过」。
    #[test]
    fn cors_origins_exclude_wildcard_and_null() {
        use axum::http::HeaderValue;

        /// 白名单里「看起来像能放行」的毒条目，以及一条合法对照。
        const POISON: [&str; 5] = ["*", "null", "NULL", "", "   "];
        const OK_ENTRY: &str = "http://ok.example";

        let cfg = RemoteConfig {
            cors_mode: CorsMode::ExplicitWhitelist,
            origin_whitelist: POISON
                .iter()
                .map(|s| s.to_string())
                .chain(std::iter::once(OK_ENTRY.to_string()))
                .collect(),
            ..Default::default()
        };

        for tls in [false, true] {
            let origins = allowed_origins(&cfg, tls);
            assert!(
                !origins.is_empty(),
                "白名单不得为空：内嵌 webview 的 origin 不在列就会落第 11 格 403"
            );

            for origin in &origins {
                let s = origin.to_str().expect("origin 必须是可见 ASCII");
                let trimmed = s.trim();
                assert_ne!(trimmed, "*", "白名单里不得有通配 origin：{s:?}");
                assert_ne!(trimmed, "null", "白名单里不得有 Origin: null：{s:?}");
                assert!(!s.contains('*'), "白名单里不得有带通配的 origin：{s:?}");
                assert!(!trimmed.is_empty(), "白名单里不得有空白 origin");
                // 与中间件第②层判据同源：CORS 面放行的每一条，中间件都必须放行。
                // 清单是同一个函数算出来的（`allowed_origins`），两层不可能分叉 ——
                // 这条断言盯的是「有人又在本模块外另列了一份」。
                assert!(
                    origin_is_allowed(trimmed, &cfg, tls),
                    "CORS 面放行的 {trimmed:?} 必须被中间件第②层判据接受，\
                     否则 CORS 放行、403 否决，两层各说各话"
                );
            }

            // 结构性的内嵌 webview origin 必须显式在列 —— 不在列就是 403，
            // 而 Tauri webview 是桌面端唯一的 UI 通道。
            assert!(
                origins.contains(&HeaderValue::from_static("tauri://localhost")),
                "tauri://localhost（macOS/Linux webview）必须在白名单里，实际 {origins:?}"
            );
            assert!(
                origins.contains(&HeaderValue::from_static("https://tauri.localhost")),
                "https://tauri.localhost（Windows webview）必须在白名单里，实际 {origins:?}"
            );
            // TLS listener 上 scheme 跟着换，否则 https 页面发来的请求
            // 匹配不上任何一条，CORS 面直接失效。
            let own = if tls {
                "https://127.0.0.1:4000"
            } else {
                "http://127.0.0.1:4000"
            };
            assert!(
                origins.contains(&HeaderValue::from_str(own).unwrap()),
                "TLS={tls} 时本服务自身的 origin 应当是 {own}，实际 {origins:?}"
            );
        }

        // 合法条目进得来，毒的一个都进不来（否则上面的否定断言是空的）。
        let origins = allowed_origins(&cfg, false);
        assert!(
            origins.contains(&HeaderValue::from_static(OK_ENTRY)),
            "合法白名单条目必须真的放行，否则上面的否定断言验不到东西：{origins:?}"
        );
        for poison in POISON {
            assert!(
                !origins.iter().any(|o| o == poison),
                "{poison:?} 混进了 CORS 白名单：{origins:?}"
            );
        }
    }

    /// 中间件第②层判据与 CORS 装配读的是**同一份来源**，不是各写一份。
    ///
    /// 这条锁的是**配置来源唯一**。只做源码包含检查的那一版
    /// 是空转的：`allowed_origins(` 出现在文件里，与「中间件读的是不是这份名单」
    /// 毫无关系 —— 硬编码一份 `["127.0.0.1", "localhost"]` 的中间件同样能通过。
    /// 所以这里用三条**负向**自测把「同源」钉死：每一条在两层各写一份时都会红，
    /// 而只有当两层真的算自同一份 `(RemoteConfig, tls)` 时才全绿。
    #[test]
    fn cors_assembly_goes_through_allowed_origins() {
        let code = production_code(include_str!("mod.rs"));
        assert!(
            code.contains("allowed_origins("),
            "build_router 必须经 allowed_origins(&cfg, ...) 装配 CORS"
        );
        assert!(
            !code.contains("allow_origin(Any)"),
            "allow_origin(Any) 就是通配 origin：本服务不得表达「任意来源」"
        );

        // 负向一：用户白名单里的 origin 必须**同时**过 CORS 面与中间件。
        // 这是本条要挡的真实故障：显式白名单模式下 origin 被 CORS 层放行、却被
        // 中间件 403，用户的 CORS 配置静默失效。
        let user_origin = "https://desk.example.invalid";
        let whitelist_cfg = RemoteConfig {
            cors_mode: CorsMode::ExplicitWhitelist,
            origin_whitelist: vec![user_origin.to_string()],
            ..Default::default()
        };
        assert!(
            allowed_origins(&whitelist_cfg, false)
                .iter()
                .any(|o| o == user_origin),
            "CORS 面必须放行用户白名单里的 {user_origin}"
        );
        assert!(
            origin_is_allowed(user_origin, &whitelist_cfg, false),
            "中间件必须放行同一条 {user_origin}：两层算自同一份来源"
        );

        // 负向二：端口一改，两层一起换名单。硬编码 `127.0.0.1:4000` 的那份
        // 中间件白名单过不了这条 —— 它对配置无感。
        let moved = RemoteConfig {
            port: 4001,
            ..Default::default()
        };
        assert!(
            !allowed_origins(&moved, false)
                .iter()
                .any(|o| o == "http://127.0.0.1:4000"),
            "端口改成 4001 之后 CORS 面不得再放行 http://127.0.0.1:4000"
        );
        assert!(
            !origin_is_allowed("http://127.0.0.1:4000", &moved, false),
            "端口改成 4001 之后中间件也不得再放行 http://127.0.0.1:4000：\
             判定必须由配置算出，不能另有一份写死的 host 表"
        );

        // 负向三：中间件判据的实现里不得再留一份独立的 host 白名单常量。
        let policy = production_code(include_str!("auth/policy.rs"));
        assert!(
            policy.contains("allowed_origins("),
            "中间件的 Origin 判定必须经 allowed_origins(&cfg, tls) 取名单"
        );
        assert!(
            !policy.contains("ALLOWED_HOSTS") && !policy.contains("ALLOWED_ORIGINS"),
            "policy.rs 里不得另立一份 origin 白名单：那是第二份来源"
        );
    }

    /// 中间件的 peer 只来自连接属性，不读监听地址、不读配置开关。
    ///
    /// 读监听地址是最容易写出的错法：服务一旦绑到 `0.0.0.0`，listener 的
    /// local_addr 就是 `0.0.0.0`，据此判 loopback 会把整台机器的外部请求
    /// 全放行。
    #[test]
    fn peer_is_taken_from_connect_info_only() {
        let code = production_code(include_str!("auth/middleware.rs"));
        let body = code
            .split("fn peer_of(")
            .nth(1)
            .and_then(|rest| rest.split("\n}").next())
            .expect("middleware.rs 里必须有 peer_of");
        assert!(
            body.contains("ConnectInfo<SocketAddr>"),
            "peer_of 必须从 ConnectInfo<SocketAddr> 取对端地址"
        );
        for forbidden in ["local_addr", "config", "tailscale"] {
            assert!(
                !body.contains(forbidden),
                "peer_of 里出现 {forbidden}：peer 只能来自连接属性，\
                 读监听地址/配置会让 0.0.0.0 绑定下的外部请求被误判成 loopback"
            );
        }
    }
}
