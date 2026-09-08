//! 熔断器模块
//!
//! 实现熔断器模式，用于防止向不健康的供应商发送请求

use super::log_codes::cb as log_cb;
use super::types::AppProxyConfig;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;

/// 熔断器状态
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CircuitState {
    /// 关闭状态 - 正常工作
    Closed,
    /// 打开状态 - 熔断激活，拒绝请求
    Open,
    /// 半开状态 - 尝试恢复，允许部分请求通过
    HalfOpen,
}

impl std::fmt::Display for CircuitState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CircuitState::Closed => write!(f, "closed"),
            CircuitState::Open => write!(f, "open"),
            CircuitState::HalfOpen => write!(f, "half_open"),
        }
    }
}

/// 熔断器配置
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CircuitBreakerConfig {
    /// 失败阈值 - 连续失败多少次后打开熔断器
    pub failure_threshold: u32,
    /// 成功阈值 - 半开状态下成功多少次后关闭熔断器
    pub success_threshold: u32,
    /// 超时时间 - 熔断器打开后多久尝试半开（秒）
    pub timeout_seconds: u64,
    /// 错误率阈值 - 错误率超过此值时打开熔断器 (0.0-1.0)
    pub error_rate_threshold: f64,
    /// 最小请求数 - 计算错误率前的最小请求数
    pub min_requests: u32,
}

impl From<&AppProxyConfig> for CircuitBreakerConfig {
    fn from(config: &AppProxyConfig) -> Self {
        Self {
            failure_threshold: config.circuit_failure_threshold,
            success_threshold: config.circuit_success_threshold,
            timeout_seconds: config.circuit_timeout_seconds as u64,
            error_rate_threshold: config.circuit_error_rate_threshold,
            min_requests: config.circuit_min_requests,
        }
    }
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            failure_threshold: 4,
            success_threshold: 2,
            timeout_seconds: 60,
            error_rate_threshold: 0.6,
            min_requests: 10,
        }
    }
}

/// 熔断器实例
pub struct CircuitBreaker {
    /// 当前状态
    state: Arc<RwLock<CircuitState>>,
    /// 连续失败计数
    consecutive_failures: Arc<AtomicU32>,
    /// 连续成功计数（半开状态）
    consecutive_successes: Arc<AtomicU32>,
    /// 总请求计数
    total_requests: Arc<AtomicU32>,
    /// 失败请求计数
    failed_requests: Arc<AtomicU32>,
    /// 上次打开时间
    last_opened_at: Arc<RwLock<Option<Instant>>>,
    /// 配置（支持热更新）
    config: Arc<RwLock<CircuitBreakerConfig>>,
    /// 半开状态已放行的请求数（用于限流）
    half_open_requests: Arc<AtomicU32>,
    /// Provider 标识，格式: "app_type:provider_id"，用于日志前缀
    circuit_key: Arc<str>,
    /// Provider 显示名称（例如 "P1 MiniMax"），在熔断器创建时从 DB 拉取并缓存。
    /// 重命名 Provider 后日志里仍是旧名，直到下次重启或熔断器重建。
    provider_name: Arc<str>,
}

/// RAII 守卫：持有 HalfOpen 探测 permit，确保在 Drop 时自动释放。
///
/// **核心目的**：消除"探测请求响应丢失导致 permit 永久泄漏"的 bug 类。
/// 之前 `used_half_open_permit: bool` 是值类型，drop 时啥也不做——如果 forwarder
/// 在 await 期间被 cancel（客户端断连 / tower timeout / panic unwinding），
/// record_success/failure/neutral 都不会被调用，permit 卡在占用状态，
/// 该 Provider 永远无法再发新探测。
///
/// **使用方式**：
/// ```ignore
/// let result = breaker.allow_request().await;
/// if !result.allowed { continue; }
/// let permit = result.permit.expect("HalfOpen 必须返回 permit");
///
/// // 成功路径：显式 disarm（Drop no-op）+ record_success（仅更新状态）
/// permit.disarm();
/// breaker.record_success().await;
///
/// // 异常路径：permit 随 future drop → Drop 自动 release
/// ```
pub struct HalfOpenPermitGuard {
    breaker: Arc<CircuitBreaker>,
    /// true → Drop 时调用 release_half_open_permit
    /// false → Drop 是 no-op（disarm 后）
    armed: bool,
}

impl std::fmt::Debug for HalfOpenPermitGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HalfOpenPermitGuard")
            .field("armed", &self.armed)
            .field("circuit_key", &self.breaker.circuit_key)
            .finish()
    }
}

impl HalfOpenPermitGuard {
    /// 标记 permit 已通过显式 record_success/failure/neutral 处理。
    /// 立即释放 permit + 设置 armed=false 让 Drop 变 no-op，避免与显式 release 双重释放。
    /// 注意：disarm 消费 self，所以调用后不能再访问 permit。
    ///
    /// 关键：disarm **必须**释放 permit，否则 forwarder 'guard.disarm() + record_success()'
    /// 的 happy path 会让 half_open_requests 计数泄漏——必须等下一次 transition_to_half_open()
    /// 才被清零，期间探测被错误挡掉。
    pub fn disarm(mut self) {
        if self.armed {
            self.armed = false;
            self.breaker.release_half_open_permit();
        }
    }
}

impl Drop for HalfOpenPermitGuard {
    fn drop(&mut self) {
        if self.armed {
            // 改用 trace 级别：happy path（forwarder.rs 显式 disarm 后 Drop no-op）不走这里，
            // 走这里意味着 guard 跨过 forward() 调用未被 disarm——通常是 future cancel / panic unwinding。
            // 把这一事件从 debug 降到 trace，避免污染生产日志（之前 forwarder.rs 提前 drop 守卫
            // 时这条日志每秒打几十次）。
            log::trace!(
                "[{} {}] HalfOpenPermitGuard::drop 释放 permit (RAII 兜底: future cancel / panic / 漏 disarm)",
                self.breaker.circuit_key,
                self.breaker.provider_name
            );
            self.breaker.release_half_open_permit();
        }
    }
}

/// 熔断器放行结果
///
/// `permit` 在 HalfOpen 探测成功占用时返回 `Some(HalfOpenPermitGuard)`，
/// 调用方应在请求结束时通过 guard 的 Drop 自动释放（或显式 `disarm()` +
/// `record_*` 处理后让 Drop 变 no-op）。
#[derive(Debug)]
pub struct AllowRequestResult {
    pub allowed: bool,
    /// RAII guard，drop 时自动释放 permit。
    /// 若调用方希望"释放时机由我控制"，应将 `permit` 绑定到一个跨过请求生命周期的局部变量；
    /// 若调用方希望"完全放手给 Drop"，则让 `AllowRequestResult` 在末尾自然 drop。
    pub permit: Option<HalfOpenPermitGuard>,
}

impl CircuitBreaker {
    /// 创建新的熔断器
    ///
    /// - `circuit_key`：格式 `"app_type:provider_id"`，用于在日志里精确匹配 Provider ID
    /// - `provider_name`：Provider 的显示名称（如 `"P1 MiniMax"`），用于日志可读性
    ///
    /// 调用方负责保证 key 在 (app_type, provider_id) 维度上唯一。
    pub fn new(
        config: CircuitBreakerConfig,
        circuit_key: impl Into<Arc<str>>,
        provider_name: impl Into<Arc<str>>,
    ) -> Self {
        Self {
            state: Arc::new(RwLock::new(CircuitState::Closed)),
            consecutive_failures: Arc::new(AtomicU32::new(0)),
            consecutive_successes: Arc::new(AtomicU32::new(0)),
            total_requests: Arc::new(AtomicU32::new(0)),
            failed_requests: Arc::new(AtomicU32::new(0)),
            last_opened_at: Arc::new(RwLock::new(None)),
            config: Arc::new(RwLock::new(config)),
            half_open_requests: Arc::new(AtomicU32::new(0)),
            circuit_key: circuit_key.into(),
            provider_name: provider_name.into(),
        }
    }

    /// 更新熔断器配置（热更新，不重置状态）
    pub async fn update_config(&self, new_config: CircuitBreakerConfig) {
        *self.config.write().await = new_config;
    }

    /// 判断当前 Provider 是否“可被纳入候选链路”
    ///
    /// 这个方法不会占用 HalfOpen 探测名额，仅用于路由选择阶段的“可用性判断”：
    /// - Closed / HalfOpen：可用（返回 true）
    /// - Open：若超时到达则切到 HalfOpen 并返回 true，否则返回 false
    ///
    /// 注意：真正发起请求前仍需调用 `allow_request()` 来获取 HalfOpen 探测名额，
    /// 并在请求结束后通过 `record_success()` / `record_failure()` 释放。
    pub async fn is_available(&self) -> bool {
        let state = *self.state.read().await;
        let config = self.config.read().await;

        match state {
            CircuitState::Closed | CircuitState::HalfOpen => true,
            CircuitState::Open => {
                if let Some(opened_at) = *self.last_opened_at.read().await {
                    let elapsed_secs = opened_at.elapsed().as_secs();
                    if elapsed_secs >= config.timeout_seconds {
                        let timeout_seconds = config.timeout_seconds;
                        drop(config); // 释放读锁再转换状态
                        log::info!(
                            "[{} {}] [{}] 熔断器 Open → HalfOpen (超时恢复, 已等待 {}s / 阈值 {}s)",
                            self.circuit_key,
                            self.provider_name,
                            log_cb::OPEN_TO_HALF_OPEN,
                            elapsed_secs,
                            timeout_seconds
                        );
                        self.transition_to_half_open().await;
                        return true;
                    } else {
                        log::debug!(
                            "[{} {}] 熔断器仍处 Open 状态：已等待 {}s / 阈值 {}s，本轮不可用",
                            self.circuit_key,
                            self.provider_name,
                            elapsed_secs,
                            config.timeout_seconds
                        );
                    }
                }
                false
            }
        }
    }

    /// 检查是否允许请求通过
    pub async fn allow_request(self: &Arc<Self>) -> AllowRequestResult {
        let state = *self.state.read().await;

        match state {
            CircuitState::Closed => AllowRequestResult {
                allowed: true,
                permit: None,
            },
            CircuitState::Open => {
                let config = self.config.read().await;
                // 检查是否应该尝试半开
                if let Some(opened_at) = *self.last_opened_at.read().await {
                    let elapsed_secs = opened_at.elapsed().as_secs();
                    if elapsed_secs >= config.timeout_seconds {
                        let timeout_seconds = config.timeout_seconds;
                        drop(config); // 释放读锁再转换状态
                        log::info!(
                            "[{} {}] [{}] 熔断器 Open → HalfOpen (超时恢复, 已等待 {}s / 阈值 {}s)",
                            self.circuit_key,
                            self.provider_name,
                            log_cb::OPEN_TO_HALF_OPEN,
                            elapsed_secs,
                            timeout_seconds
                        );
                        self.transition_to_half_open().await;

                        // 转换后按当前状态决定是否需要获取 HalfOpen 探测名额
                        let current_state = *self.state.read().await;
                        return match current_state {
                            CircuitState::Closed => AllowRequestResult {
                                allowed: true,
                                permit: None,
                            },
                            CircuitState::HalfOpen => self.allow_half_open_probe(),
                            CircuitState::Open => AllowRequestResult {
                                allowed: false,
                                permit: None,
                            },
                        };
                    } else {
                        log::debug!(
                            "[{} {}] 熔断器拒绝请求: Open 且未到超时 (已等待 {}s / 阈值 {}s)",
                            self.circuit_key,
                            self.provider_name,
                            elapsed_secs,
                            config.timeout_seconds
                        );
                    }
                }

                AllowRequestResult {
                    allowed: false,
                    permit: None,
                }
            }
            CircuitState::HalfOpen => self.allow_half_open_probe(),
        }
    }

    /// 记录成功
    ///
    /// **职责**：无条件释放 HalfOpen permit（兼容 RAII 兜底语义），并按状态机
    /// 推进 HalfOpen → Closed 转换或重置失败计数。
    ///
    /// **变更**：原 `used_half_open_permit: bool` 参数已被删除——参数之前总是被忽略
    /// （无论传 true/false，函数都调用 `release_half_open_permit`），保留它只会误导
    /// reader 以为有语义差异。调用方在调用前必须对 guard 显式 `disarm()` 以避免
    /// Drop 二次释放（但二次释放是 no-op，仅增加 trace 日志）。
    pub async fn record_success(&self) {
        let state = *self.state.read().await;
        let config = self.config.read().await;

        // 释放 permit
        self.release_half_open_permit();

        // 重置失败计数
        self.consecutive_failures.store(0, Ordering::SeqCst);
        self.total_requests.fetch_add(1, Ordering::SeqCst);

        if state == CircuitState::HalfOpen {
            let successes = self.consecutive_successes.fetch_add(1, Ordering::SeqCst) + 1;

            if successes >= config.success_threshold {
                let success_threshold = config.success_threshold;
                drop(config); // 释放读锁再转换状态
                log::info!(
                    "[{} {}] [{}] 熔断器 HalfOpen → Closed (恢复正常, 连续成功 {}/{})",
                    self.circuit_key,
                    self.provider_name,
                    log_cb::HALF_OPEN_TO_CLOSED,
                    successes,
                    success_threshold
                );
                self.transition_to_closed().await;
            }
        }
    }

    /// 记录失败
    ///
    /// **变更**：原 `used_half_open_permit: bool` 参数已被删除（理由同 `record_success`）。
    /// 无条件释放 HalfOpen permit，并按状态机推进 Closed → HalfOpen 或 Open 转换。
    pub async fn record_failure(&self) {
        let state = *self.state.read().await;
        let config = self.config.read().await;

        // 释放 permit
        self.release_half_open_permit();

        // 更新计数器
        let failures = self.consecutive_failures.fetch_add(1, Ordering::SeqCst) + 1;
        self.total_requests.fetch_add(1, Ordering::SeqCst);
        self.failed_requests.fetch_add(1, Ordering::SeqCst);

        // 重置成功计数
        self.consecutive_successes.store(0, Ordering::SeqCst);

        // 检查是否应该打开熔断器
        match state {
            CircuitState::HalfOpen => {
                // HalfOpen 状态下失败，立即转为 Open
                log::warn!(
                    "[{} {}] [{}] 熔断器 HalfOpen 探测失败 → Open",
                    self.circuit_key,
                    self.provider_name,
                    log_cb::HALF_OPEN_PROBE_FAILED
                );
                drop(config);
                self.transition_to_open().await;
            }
            CircuitState::Closed => {
                // 检查连续失败次数
                if failures >= config.failure_threshold {
                    log::warn!(
                        "[{} {}] [{}] 熔断器触发: 连续失败 {}/{} 次 → Open",
                        self.circuit_key,
                        self.provider_name,
                        log_cb::TRIGGERED_FAILURES,
                        failures,
                        config.failure_threshold
                    );
                    drop(config); // 释放读锁再转换状态
                    self.transition_to_open().await;
                } else {
                    // 检查错误率
                    let total = self.total_requests.load(Ordering::SeqCst);
                    let failed = self.failed_requests.load(Ordering::SeqCst);

                    if total >= config.min_requests {
                        let error_rate = failed as f64 / total as f64;

                        if error_rate >= config.error_rate_threshold {
                            log::warn!(
                                "[{} {}] [{}] 熔断器触发: 错误率 {}/{} = {:.1}% (阈值 {:.1}%, 最小请求数 {}) → Open",
                                self.circuit_key, self.provider_name,
                                log_cb::TRIGGERED_ERROR_RATE,
                                failed,
                                total,
                                error_rate * 100.0,
                                config.error_rate_threshold * 100.0,
                                config.min_requests
                            );
                            drop(config); // 释放读锁再转换状态
                            self.transition_to_open().await;
                        }
                    }
                }
            }
            _ => {}
        }
    }

    /// 获取当前状态
    #[allow(dead_code)]
    pub async fn get_state(&self) -> CircuitState {
        *self.state.read().await
    }

    /// 获取统计信息
    #[allow(dead_code)]
    pub async fn get_stats(&self) -> CircuitBreakerStats {
        CircuitBreakerStats {
            state: *self.state.read().await,
            consecutive_failures: self.consecutive_failures.load(Ordering::SeqCst),
            consecutive_successes: self.consecutive_successes.load(Ordering::SeqCst),
            total_requests: self.total_requests.load(Ordering::SeqCst),
            failed_requests: self.failed_requests.load(Ordering::SeqCst),
            half_open_requests: self.half_open_requests.load(Ordering::SeqCst),
        }
    }

    /// 测试专用：直接读取 half_open_requests 计数（不经过 guard）
    #[cfg(test)]
    pub fn get_half_open_requests_for_test(&self) -> u32 {
        self.half_open_requests.load(Ordering::SeqCst)
    }

    /// 重置熔断器（手动恢复）
    #[allow(dead_code)]
    pub async fn reset(&self) {
        log::info!(
            "[{} {}] [{}] 熔断器手动重置 → Closed",
            self.circuit_key,
            self.provider_name,
            log_cb::MANUAL_RESET
        );
        self.transition_to_closed().await;
    }

    fn allow_half_open_probe(self: &Arc<Self>) -> AllowRequestResult {
        // 半开状态限流：只允许有限请求通过进行探测
        let max_half_open_requests = 1u32;
        let current = self.half_open_requests.fetch_add(1, Ordering::SeqCst);

        if current < max_half_open_requests {
            AllowRequestResult {
                allowed: true,
                permit: Some(HalfOpenPermitGuard {
                    breaker: Arc::clone(self),
                    armed: true,
                }),
            }
        } else {
            // 超过限额，回退计数，拒绝请求
            self.half_open_requests.fetch_sub(1, Ordering::SeqCst);
            AllowRequestResult {
                allowed: false,
                permit: None,
            }
        }
    }

    /// 仅释放 HalfOpen permit，不影响健康统计
    ///
    /// 用于整流器等场景：请求结果不应计入 Provider 健康度，
    /// 但仍需释放占用的探测名额，避免 HalfOpen 状态卡死
    pub fn release_half_open_permit(&self) {
        let mut current = self.half_open_requests.load(Ordering::SeqCst);
        loop {
            if current == 0 {
                // 改用 trace：这是常见情况（forwarder.rs 已经在 Drop 里释放过 permit，
                // 这里再调一次就是重复 release）。原 debug 级别让生产日志污染严重。
                log::trace!(
                    "[{} {}] release_half_open_permit: 计数已为 0，无需释放",
                    self.circuit_key,
                    self.provider_name
                );
                return;
            }

            match self.half_open_requests.compare_exchange(
                current,
                current - 1,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => {
                    log::debug!(
                        "[{} {}] release_half_open_permit: 计数 {} → {}",
                        self.circuit_key,
                        self.provider_name,
                        current,
                        current - 1
                    );
                    return;
                }
                Err(actual) => current = actual,
            }
        }
    }

    /// 转换到打开状态
    async fn transition_to_open(&self) {
        *self.state.write().await = CircuitState::Open;
        *self.last_opened_at.write().await = Some(Instant::now());
        self.consecutive_failures.store(0, Ordering::SeqCst);
        self.consecutive_successes.store(0, Ordering::SeqCst);
    }

    /// 转换到半开状态
    async fn transition_to_half_open(&self) {
        let mut state = self.state.write().await;
        if *state != CircuitState::Open {
            return;
        }

        *state = CircuitState::HalfOpen;
        self.consecutive_successes.store(0, Ordering::SeqCst);
        // 重置半开状态的请求限流计数
        self.half_open_requests.store(0, Ordering::SeqCst);
    }

    /// 转换到关闭状态
    async fn transition_to_closed(&self) {
        *self.state.write().await = CircuitState::Closed;
        self.consecutive_failures.store(0, Ordering::SeqCst);
        self.consecutive_successes.store(0, Ordering::SeqCst);
        // 重置计数器
        self.total_requests.store(0, Ordering::SeqCst);
        self.failed_requests.store(0, Ordering::SeqCst);
    }
}

/// 熔断器统计信息
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CircuitBreakerStats {
    pub state: CircuitState,
    pub consecutive_failures: u32,
    pub consecutive_successes: u32,
    pub total_requests: u32,
    pub failed_requests: u32,
    /// 当前占用中的 HalfOpen 探测名额（用于监控 permit 泄漏）
    pub half_open_requests: u32,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn test_circuit_breaker_closed_to_open() {
        let config = CircuitBreakerConfig {
            failure_threshold: 3,
            ..Default::default()
        };
        let breaker = std::sync::Arc::new(CircuitBreaker::new(config, "test:breaker", "Test Breaker"));

        // 初始状态应该是关闭
        assert_eq!(breaker.get_state().await, CircuitState::Closed);
        assert!(breaker.allow_request().await.allowed);

        // 记录 3 次失败
        for _ in 0..3 {
            breaker.record_failure().await;
        }

        // 应该转换到打开状态
        assert_eq!(breaker.get_state().await, CircuitState::Open);
        assert!(!breaker.allow_request().await.allowed);
    }

    #[tokio::test]
    async fn test_circuit_breaker_half_open_to_closed() {
        let config = CircuitBreakerConfig {
            failure_threshold: 2,
            success_threshold: 2,
            ..Default::default()
        };
        let breaker = std::sync::Arc::new(CircuitBreaker::new(config, "test:breaker", "Test Breaker"));

        // 打开熔断器
        breaker.record_failure().await;
        breaker.record_failure().await;
        assert_eq!(breaker.get_state().await, CircuitState::Open);

        // 手动转换到半开状态
        breaker.transition_to_half_open().await;
        assert_eq!(breaker.get_state().await, CircuitState::HalfOpen);

        // 记录 2 次成功
        breaker.record_success().await;
        breaker.record_success().await;

        // 应该转换到关闭状态
        assert_eq!(breaker.get_state().await, CircuitState::Closed);
    }

    #[tokio::test]
    async fn test_half_open_transition_does_not_reset_inflight_permit() {
        let config = CircuitBreakerConfig {
            timeout_seconds: 0,
            ..Default::default()
        };
        let breaker = std::sync::Arc::new(CircuitBreaker::new(config, "test:breaker", "Test Breaker"));

        // 进入 Open，然后由于 timeout_seconds=0，allow_request 会立即切换到 HalfOpen 并占用探测名额
        breaker.transition_to_open().await;
        let first = breaker.allow_request().await;
        assert!(first.allowed);
        assert!(first.permit.is_some());
        assert_eq!(breaker.get_state().await, CircuitState::HalfOpen);

        // 模拟并发下的“重复 HalfOpen 转换调用”，不应重置 in-flight 计数
        breaker.transition_to_half_open().await;

        // 由于名额仍被占用，第二次请求应被拒绝
        let second = breaker.allow_request().await;
        assert!(!second.allowed);
        assert!(second.permit.is_none());
    }

    #[tokio::test]
    async fn test_circuit_breaker_reset() {
        let config = CircuitBreakerConfig {
            failure_threshold: 2,
            ..Default::default()
        };
        let breaker = std::sync::Arc::new(CircuitBreaker::new(config, "test:breaker", "Test Breaker"));

        // 打开熔断器
        breaker.record_failure().await;
        breaker.record_failure().await;
        assert_eq!(breaker.get_state().await, CircuitState::Open);

        // 重置
        breaker.reset().await;
        assert_eq!(breaker.get_state().await, CircuitState::Closed);
        assert!(breaker.allow_request().await.allowed);
    }

    // ============================================================
    // RAII HalfOpen Permit Guard 单元测试
    // ============================================================
    // 这些测试验证 HalfOpenPermitGuard 的 Drop/disarm/forget 行为：
    // - Drop 时如果 armed=true → release_half_open_permit
    // - disarm() 后 Drop 是 no-op（armed=false）
    // - forget() 后 Drop 不会运行（mem::forget）
    // - panic unwinding 也会触发 Drop
    //
    // 这些测试在 RAII 实现之前会因为 `permit: Option<HalfOpenPermitGuard>`
    // 字段不存在而**编译失败**。RAII 实现后应全部通过。

    /// Test 1: Drop guard without disarm → permit 自动释放
    #[tokio::test]
    async fn test_drop_guard_releases_permit_on_drop() {
        let config = CircuitBreakerConfig {
            timeout_seconds: 0,
            ..Default::default()
        };
        let breaker = std::sync::Arc::new(CircuitBreaker::new(config, "test:raii_drop", "Test RAII Drop"));

        // 进入 Open → HalfOpen（timeout=0）
        breaker.transition_to_open().await;
        let result = breaker.allow_request().await;
        assert!(result.allowed);
        let permit = result.permit.expect("HalfOpen 必须返回 permit guard");

        // ⚠️ 故意 drop permit 不 disarm（模拟客户端断连 / future 被 cancel）
        drop(permit);

        // permit 必须自动归零
        assert_eq!(
            breaker.get_half_open_requests_for_test(),
            0,
            "Drop guard 必须自动释放 permit，不应卡在 1"
        );

        // 下次探测必须允许
        tokio::time::sleep(Duration::from_millis(10)).await;
        let result2 = breaker.allow_request().await;
        assert!(
            result2.allowed,
            "permit 释放后下次探测必须允许（当前实现会卡在 1 永远 false）"
        );
    }

    /// Test 2: disarm() 后 Drop 是 no-op（不双重释放）
    #[tokio::test]
    async fn test_drop_guard_no_op_after_disarm() {
        let config = CircuitBreakerConfig {
            timeout_seconds: 0,
            ..Default::default()
        };
        let breaker = std::sync::Arc::new(CircuitBreaker::new(config, "test:raii_disarm", "Test RAII Disarm"));

        breaker.transition_to_open().await;
        let result = breaker.allow_request().await;
        let permit = result.permit.expect("必须返回 permit guard");

        // 显式 disarm（标记"已处理"）——disarm 立即释放 permit + 让 Drop 变 no-op
        permit.disarm();

        // count 必须 = 0（disarm 已释放，由 disarm 负责，不依赖 record_success）
        let stats = breaker.get_stats().await;
        assert_eq!(
            stats.half_open_requests, 0,
            "disarm 应立即释放 permit，count 应归 0"
        );

        // permit 后续 drop 是 no-op（armed=false），不再二次释放
        // record_success 只更新 breaker 状态机，不动 permit 计数
        breaker.record_success().await;
        let stats = breaker.get_stats().await;
        assert_eq!(
            stats.half_open_requests, 0,
            "record_success 不应改动 permit 计数（disarm 已释放）"
        );
    }

    /// Test 3: panic unwinding 也会触发 Drop → permit 释放
    #[tokio::test]
    async fn test_drop_guard_releases_on_panic_unwind() {
        let config = CircuitBreakerConfig {
            timeout_seconds: 0,
            ..Default::default()
        };
        let breaker = std::sync::Arc::new(CircuitBreaker::new(config, "test:raii_panic", "Test RAII Panic"));

        breaker.transition_to_open().await;
        let result = breaker.allow_request().await;
        let permit = result.permit.expect("必须返回 permit guard");

        // 用 catch_unwind 包一个会 panic 的闭包，把 permit move 进去
        let unwind_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
            let _g = permit; // move permit 进闭包作用域
            panic!("simulated panic in forwarder future");
        }));

        // 确认 panic 真的发生了
        assert!(unwind_result.is_err(), "闭包必须 panic");

        // ⚠️ 即使 panic，Rust 的 unwinding 也会运行 guard 的 Drop
        // permit 必须自动归零
        assert_eq!(
            breaker.get_half_open_requests_for_test(),
            0,
            "panic unwinding 必须触发 Drop，permit 自动释放"
        );

        // 下次探测必须允许
        tokio::time::sleep(Duration::from_millis(10)).await;
        let result2 = breaker.allow_request().await;
        assert!(
            result2.allowed,
            "panic 释放 permit 后下次探测必须允许"
        );
    }
}
