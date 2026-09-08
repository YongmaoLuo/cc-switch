//! 供应商路由器模块
//!
//! 负责选择和管理代理目标供应商，实现智能故障转移

use crate::app_config::AppType;
use crate::database::Database;
use crate::error::AppError;
use crate::provider::Provider;
use crate::proxy::circuit_breaker::{AllowRequestResult, CircuitBreaker, CircuitBreakerConfig};
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Codex Official requests carry the selected account's native Authorization
/// header. Reusing that request against another account card would cross the
/// account boundary, so these cards must never participate in provider retry.
pub(crate) fn provider_supports_failover(app_type: &str, provider: &Provider) -> bool {
    app_type != AppType::Codex.as_str()
        || !crate::proxy::providers::is_codex_official_provider(provider)
}

/// 供应商路由器
pub struct ProviderRouter {
    /// 数据库连接
    db: Arc<Database>,
    /// 熔断器管理器 - key 格式: "app_type:provider_id"
    circuit_breakers: Arc<RwLock<HashMap<String, Arc<CircuitBreaker>>>>,
}

impl ProviderRouter {
    /// 创建新的供应商路由器
    pub fn new(db: Arc<Database>) -> Self {
        Self {
            db,
            circuit_breakers: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// 选择可用的供应商（支持故障转移）
    ///
    /// 返回按优先级排序的可用供应商列表：
    /// - 故障转移关闭时：仅返回当前供应商
    /// - 故障转移开启时：仅使用故障转移队列，按队列顺序依次尝试（P1 → P2 → ...）
    pub async fn select_providers(&self, app_type: &str) -> Result<Vec<Provider>, AppError> {
        let mut result = Vec::new();
        let mut total_providers = 0usize;
        let mut circuit_open_count = 0usize;
        let current_id = AppType::from_str(app_type)
            .ok()
            .and_then(|app_enum| {
                crate::settings::get_effective_current_provider(&self.db, &app_enum)
                    .ok()
                    .flatten()
            })
            .or_else(|| self.db.get_current_provider(app_type).ok().flatten());
        let current_provider = current_id
            .as_deref()
            .map(|id| self.db.get_provider_by_id(id, app_type))
            .transpose()?
            .flatten();

        // 检查该应用的自动故障转移开关是否开启（从 proxy_config 表读取）
        let auto_failover_enabled = match self.db.get_proxy_config_for_app(app_type).await {
            Ok(config) => config.auto_failover_enabled,
            Err(e) => {
                log::error!("[{app_type}] 读取 proxy_config 失败: {e}，默认禁用故障转移");
                false
            }
        };

        if auto_failover_enabled
            && current_provider
                .as_ref()
                .is_some_and(|provider| !provider_supports_failover(app_type, provider))
        {
            // A selected Codex Official account is an explicit account choice.
            // Keep it as a single route even if an old failover setting remains
            // enabled; retrying would reuse its inbound token for another card.
            total_providers = 1;
            result.push(current_provider.expect("checked above"));
        } else if auto_failover_enabled {
            // 故障转移开启：仅按队列顺序依次尝试（P1 → P2 → ...）
            let all_providers = self.db.get_all_providers(app_type)?;

            // 使用 DAO 返回的排序结果，确保和前端展示一致
            let ordered_ids: Vec<String> = self
                .db
                .get_failover_queue(app_type)?
                .into_iter()
                .map(|item| item.provider_id)
                .collect();

            for provider_id in ordered_ids {
                let Some(provider) = all_providers.get(&provider_id).cloned() else {
                    continue;
                };
                if !provider_supports_failover(app_type, &provider) {
                    continue;
                }
                total_providers += 1;

                let circuit_key = format!("{app_type}:{}", provider.id);
                let breaker = self.get_or_create_circuit_breaker(&circuit_key).await;

                if breaker.is_available().await {
                    result.push(provider);
                } else {
                    circuit_open_count += 1;
                }
            }
        } else {
            // 故障转移关闭：仅使用当前供应商，跳过熔断器检查
            if let Some(current) = current_provider {
                total_providers = 1;
                result.push(current);
            }
        }

        if result.is_empty() {
            if total_providers > 0 && circuit_open_count == total_providers {
                log::warn!("[{app_type}] [FO-004] 所有供应商均已熔断");
                return Err(AppError::AllProvidersCircuitOpen);
            } else {
                log::warn!("[{app_type}] [FO-005] 未配置供应商");
                return Err(AppError::NoProvidersConfigured);
            }
        }

        Ok(result)
    }

    /// 请求执行前获取熔断器“放行许可”
    ///
    /// - Closed：直接放行
    /// - Open：超时到达后切到 HalfOpen 并放行一次探测
    /// - HalfOpen：按限流规则放行探测
    ///
    /// 注意：调用方必须在请求结束后通过 `record_result()` 释放 HalfOpen 名额，
    /// 否则会导致该 Provider 长时间无法进入探测状态。
    pub async fn allow_provider_request(
        &self,
        provider_id: &str,
        app_type: &str,
    ) -> AllowRequestResult {
        let circuit_key = format!("{app_type}:{provider_id}");
        let breaker = self.get_or_create_circuit_breaker(&circuit_key).await;
        breaker.allow_request().await
    }

    /// 记录供应商请求结果
    ///
    /// **变更**：`used_half_open_permit: bool` 参数已删除（之前总是被忽略，
    /// CircuitBreaker::record_success/failure 内部无条件 release）。
    /// 调用方不需要关心 permit 是否在握——permit 生命周期由 RAII guard 管理。
    pub async fn record_result(
        &self,
        provider_id: &str,
        app_type: &str,
        success: bool,
        error_msg: Option<String>,
    ) -> Result<(), AppError> {
        // 1. 按应用独立获取熔断器配置
        let failure_threshold = match self.db.get_proxy_config_for_app(app_type).await {
            Ok(app_config) => app_config.circuit_failure_threshold,
            Err(_) => 5, // 默认值
        };

        // 2. 更新熔断器状态
        let circuit_key = format!("{app_type}:{provider_id}");
        let breaker = self.get_or_create_circuit_breaker(&circuit_key).await;

        if success {
            breaker.record_success().await;
        } else {
            breaker.record_failure().await;
        }

        // 3. 更新数据库健康状态（使用配置的阈值）
        self.db
            .update_provider_health_with_threshold(
                provider_id,
                app_type,
                success,
                error_msg.clone(),
                failure_threshold,
            )
            .await?;

        Ok(())
    }

    /// 重置熔断器（手动恢复）
    pub async fn reset_circuit_breaker(&self, circuit_key: &str) {
        let breakers = self.circuit_breakers.read().await;
        if let Some(breaker) = breakers.get(circuit_key) {
            breaker.reset().await;
        }
    }

    /// 重置指定供应商的熔断器
    pub async fn reset_provider_breaker(&self, provider_id: &str, app_type: &str) {
        let circuit_key = format!("{app_type}:{provider_id}");
        self.reset_circuit_breaker(&circuit_key).await;
    }

    /// 仅释放 HalfOpen permit，不影响健康统计（neutral 接口）
    ///
    /// 用于整流器等场景：请求结果不应计入 Provider 健康度，
    /// 但仍需释放占用的探测名额，避免 HalfOpen 状态卡死
    ///
    /// **变更**：`used_half_open_permit: bool` 参数已删除（之前仅做 if-true 短路，
    /// 现在调用 breaker.release_half_open_permit() 总是安全的——count=0 时是 no-op）。
    pub async fn release_permit_neutral(&self, provider_id: &str, app_type: &str) {
        let circuit_key = format!("{app_type}:{provider_id}");
        let breaker = self.get_or_create_circuit_breaker(&circuit_key).await;
        breaker.release_half_open_permit();
    }

    /// 更新所有熔断器的配置（热更新）
    pub async fn update_all_configs(&self, config: CircuitBreakerConfig) {
        let breakers = self.circuit_breakers.read().await;
        for breaker in breakers.values() {
            breaker.update_config(config.clone()).await;
        }
    }

    /// 更新指定应用已创建熔断器的配置（热更新）
    pub async fn update_app_configs(&self, app_type: &str, config: CircuitBreakerConfig) {
        let prefix = format!("{app_type}:");
        let breakers = self.circuit_breakers.read().await;
        for (key, breaker) in breakers.iter() {
            if key.starts_with(&prefix) {
                breaker.update_config(config.clone()).await;
            }
        }
    }

    /// 获取熔断器状态
    #[allow(dead_code)]
    pub async fn get_circuit_breaker_stats(
        &self,
        provider_id: &str,
        app_type: &str,
    ) -> Option<crate::proxy::circuit_breaker::CircuitBreakerStats> {
        let circuit_key = format!("{app_type}:{provider_id}");
        let breakers = self.circuit_breakers.read().await;

        if let Some(breaker) = breakers.get(&circuit_key) {
            Some(breaker.get_stats().await)
        } else {
            None
        }
    }

    /// 获取或创建熔断器
    async fn get_or_create_circuit_breaker(&self, key: &str) -> Arc<CircuitBreaker> {
        // 先尝试读锁获取
        {
            let breakers = self.circuit_breakers.read().await;
            if let Some(breaker) = breakers.get(key) {
                return breaker.clone();
            }
        }

        // 如果不存在，获取写锁创建
        let mut breakers = self.circuit_breakers.write().await;

        // 双重检查，防止竞争条件
        if let Some(breaker) = breakers.get(key) {
            return breaker.clone();
        }

        // 从 key 中提取 app_type 和 provider_id (格式: "app_type:provider_id")
        let app_type = key.split(':').next().unwrap_or("claude");
        let provider_id = key.split(':').nth(1).unwrap_or("");

        // 按应用独立读取熔断器配置
        let config = match self.db.get_proxy_config_for_app(app_type).await {
            Ok(app_config) => crate::proxy::circuit_breaker::CircuitBreakerConfig {
                failure_threshold: app_config.circuit_failure_threshold,
                success_threshold: app_config.circuit_success_threshold,
                timeout_seconds: app_config.circuit_timeout_seconds as u64,
                error_rate_threshold: app_config.circuit_error_rate_threshold,
                min_requests: app_config.circuit_min_requests,
            },
            Err(_) => crate::proxy::circuit_breaker::CircuitBreakerConfig::default(),
        };

        // 查询 provider 显示名用于日志可读性。如果查不到（Provider 已被删除），
        // 回退到 provider_id，让日志至少能定位到具体哪个 provider 出问题。
        let provider_name = self
            .db
            .get_provider_by_id(provider_id, app_type)
            .ok()
            .flatten()
            .map(|p| p.name)
            .unwrap_or_else(|| provider_id.to_string());

        let breaker = Arc::new(CircuitBreaker::new(config, key.to_string(), provider_name));
        breakers.insert(key.to_string(), breaker.clone());

        breaker
    }

    /// 查找指定应用的 fallback provider（用于跨 provider 的模型级回退）。
    ///
    /// 按 `base_url_keyword` 在 provider 的 `ANTHROPIC_BASE_URL` 中匹配（大小写不敏感）。
    /// 返回第一个匹配的 provider（按数据库中的 sort_index 排序）。
    pub async fn find_fallback_provider(
        &self,
        app_type: &str,
        base_url_keyword: &str,
    ) -> Result<Option<Provider>, AppError> {
        let all_providers = self.db.get_all_providers(app_type)?;
        let keyword = base_url_keyword.to_lowercase();

        let mut candidates: Vec<Provider> = all_providers
            .into_values()
            .filter(|p| {
                p.settings_config
                    .pointer("/env/ANTHROPIC_BASE_URL")
                    .and_then(|v| v.as_str())
                    .map(|url| url.to_lowercase().contains(&keyword))
                    .unwrap_or(false)
            })
            .collect();

        // 按 sort_index 排序，取第一个
        candidates.sort_by_key(|p| p.sort_index.unwrap_or(usize::MAX));
        Ok(candidates.into_iter().next())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database::Database;
    use crate::provider::{AuthBinding, AuthBindingSource, ProviderMeta};
    use serde_json::json;
    use serial_test::serial;
    use std::env;
    use tempfile::TempDir;

    fn managed_codex_official(id: &str, account_id: &str) -> Provider {
        let mut provider = Provider::with_id(
            id.to_string(),
            "OpenAI Official".to_string(),
            json!({ "auth": {}, "config": "" }),
            None,
        );
        provider.category = Some("official".to_string());
        provider.meta = Some(ProviderMeta {
            provider_type: Some("codex_oauth".to_string()),
            auth_binding: Some(AuthBinding {
                source: AuthBindingSource::ManagedAccount,
                auth_provider: Some("codex_oauth".to_string()),
                account_id: Some(account_id.to_string()),
            }),
            ..Default::default()
        });
        provider
    }

    struct TempHome {
        #[allow(dead_code)]
        dir: TempDir,
        original_home: Option<String>,
        original_userprofile: Option<String>,
        original_test_home: Option<String>,
    }

    impl TempHome {
        fn new() -> Self {
            let dir = TempDir::new().expect("failed to create temp home");
            let original_home = env::var("HOME").ok();
            let original_userprofile = env::var("USERPROFILE").ok();
            let original_test_home = env::var("CC_SWITCH_TEST_HOME").ok();

            env::set_var("HOME", dir.path());
            env::set_var("USERPROFILE", dir.path());
            env::set_var("CC_SWITCH_TEST_HOME", dir.path());
            crate::settings::reload_settings().expect("reload settings");

            Self {
                dir,
                original_home,
                original_userprofile,
                original_test_home,
            }
        }
    }

    impl Drop for TempHome {
        fn drop(&mut self) {
            match &self.original_home {
                Some(value) => env::set_var("HOME", value),
                None => env::remove_var("HOME"),
            }

            match &self.original_userprofile {
                Some(value) => env::set_var("USERPROFILE", value),
                None => env::remove_var("USERPROFILE"),
            }

            match &self.original_test_home {
                Some(value) => env::set_var("CC_SWITCH_TEST_HOME", value),
                None => env::remove_var("CC_SWITCH_TEST_HOME"),
            }
        }
    }

    #[tokio::test]
    #[serial]
    async fn test_provider_router_creation() {
        let _home = TempHome::new();
        let db = Arc::new(Database::memory().unwrap());
        let router = ProviderRouter::new(db);

        let breaker = router.get_or_create_circuit_breaker("claude:test").await;
        assert!(breaker.allow_request().await.allowed);
    }

    #[tokio::test]
    #[serial]
    async fn test_failover_disabled_uses_current_provider() {
        let _home = TempHome::new();
        let db = Arc::new(Database::memory().unwrap());

        let provider_a =
            Provider::with_id("a".to_string(), "Provider A".to_string(), json!({}), None);
        let provider_b =
            Provider::with_id("b".to_string(), "Provider B".to_string(), json!({}), None);

        db.save_provider("claude", &provider_a).unwrap();
        db.save_provider("claude", &provider_b).unwrap();
        db.set_current_provider("claude", "a").unwrap();
        db.add_to_failover_queue("claude", "b").unwrap();

        let router = ProviderRouter::new(db.clone());
        let providers = router.select_providers("claude").await.unwrap();

        assert_eq!(providers.len(), 1);
        assert_eq!(providers[0].id, "a");
    }

    #[tokio::test]
    #[serial]
    async fn test_failover_enabled_uses_queue_order_ignoring_current() {
        let _home = TempHome::new();
        let db = Arc::new(Database::memory().unwrap());

        // 设置 sort_index 来控制顺序：b=1, a=2
        let mut provider_a =
            Provider::with_id("a".to_string(), "Provider A".to_string(), json!({}), None);
        provider_a.sort_index = Some(2);
        let mut provider_b =
            Provider::with_id("b".to_string(), "Provider B".to_string(), json!({}), None);
        provider_b.sort_index = Some(1);

        db.save_provider("claude", &provider_a).unwrap();
        db.save_provider("claude", &provider_b).unwrap();
        db.set_current_provider("claude", "a").unwrap();

        db.add_to_failover_queue("claude", "b").unwrap();
        db.add_to_failover_queue("claude", "a").unwrap();

        // 启用自动故障转移（使用新的 proxy_config API）
        let mut config = db.get_proxy_config_for_app("claude").await.unwrap();
        config.auto_failover_enabled = true;
        db.update_proxy_config_for_app(config).await.unwrap();

        let router = ProviderRouter::new(db.clone());
        let providers = router.select_providers("claude").await.unwrap();

        assert_eq!(providers.len(), 2);
        // 故障转移开启时：仅按队列顺序选择（忽略当前供应商）
        assert_eq!(providers[0].id, "b");
        assert_eq!(providers[1].id, "a");
    }

    #[tokio::test]
    #[serial]
    async fn test_failover_enabled_uses_queue_only_even_if_current_not_in_queue() {
        let _home = TempHome::new();
        let db = Arc::new(Database::memory().unwrap());

        let provider_a =
            Provider::with_id("a".to_string(), "Provider A".to_string(), json!({}), None);
        let mut provider_b =
            Provider::with_id("b".to_string(), "Provider B".to_string(), json!({}), None);
        provider_b.sort_index = Some(1);

        db.save_provider("claude", &provider_a).unwrap();
        db.save_provider("claude", &provider_b).unwrap();
        db.set_current_provider("claude", "a").unwrap();

        // 只把 b 加入故障转移队列（模拟“当前供应商不在队列里”的常见配置）
        db.add_to_failover_queue("claude", "b").unwrap();

        let mut config = db.get_proxy_config_for_app("claude").await.unwrap();
        config.auto_failover_enabled = true;
        db.update_proxy_config_for_app(config).await.unwrap();

        let router = ProviderRouter::new(db.clone());
        let providers = router.select_providers("claude").await.unwrap();

        assert_eq!(providers.len(), 1);
        assert_eq!(providers[0].id, "b");
    }

    #[tokio::test]
    #[serial]
    async fn codex_official_current_stays_single_route_when_failover_is_stale() {
        let _home = TempHome::new();
        let db = Arc::new(Database::memory().unwrap());
        let official = managed_codex_official("official-a", "account-a");
        let fallback = Provider::with_id(
            "fallback".to_string(),
            "Fallback".to_string(),
            json!({}),
            None,
        );
        db.save_provider("codex", &official).unwrap();
        db.save_provider("codex", &fallback).unwrap();
        db.set_current_provider("codex", &official.id).unwrap();
        db.add_to_failover_queue("codex", &fallback.id).unwrap();

        let mut config = db.get_proxy_config_for_app("codex").await.unwrap();
        config.auto_failover_enabled = true;
        db.update_proxy_config_for_app(config).await.unwrap();

        let providers = ProviderRouter::new(db)
            .select_providers("codex")
            .await
            .unwrap();
        assert_eq!(providers.len(), 1);
        assert_eq!(providers[0].id, official.id);
    }

    #[tokio::test]
    #[serial]
    async fn stale_codex_official_queue_entries_are_not_retry_targets() {
        let _home = TempHome::new();
        let db = Arc::new(Database::memory().unwrap());
        let current = Provider::with_id(
            "third-party".to_string(),
            "Third Party".to_string(),
            json!({}),
            None,
        );
        let official = managed_codex_official("official-a", "account-a");
        let fallback = Provider::with_id(
            "fallback".to_string(),
            "Fallback".to_string(),
            json!({}),
            None,
        );
        db.save_provider("codex", &current).unwrap();
        db.save_provider("codex", &official).unwrap();
        db.save_provider("codex", &fallback).unwrap();
        db.set_current_provider("codex", &current.id).unwrap();
        db.add_to_failover_queue("codex", &official.id).unwrap();
        db.add_to_failover_queue("codex", &fallback.id).unwrap();

        let mut config = db.get_proxy_config_for_app("codex").await.unwrap();
        config.auto_failover_enabled = true;
        db.update_proxy_config_for_app(config).await.unwrap();

        let providers = ProviderRouter::new(db)
            .select_providers("codex")
            .await
            .unwrap();
        assert_eq!(
            providers
                .iter()
                .map(|provider| provider.id.as_str())
                .collect::<Vec<_>>(),
            vec!["fallback"]
        );
    }

    #[tokio::test]
    #[serial]
    async fn test_select_providers_does_not_consume_half_open_permit() {
        let _home = TempHome::new();
        let db = Arc::new(Database::memory().unwrap());

        db.update_circuit_breaker_config(&CircuitBreakerConfig {
            failure_threshold: 1,
            timeout_seconds: 0,
            ..Default::default()
        })
        .await
        .unwrap();

        let provider_a =
            Provider::with_id("a".to_string(), "Provider A".to_string(), json!({}), None);
        let provider_b =
            Provider::with_id("b".to_string(), "Provider B".to_string(), json!({}), None);

        db.save_provider("claude", &provider_a).unwrap();
        db.save_provider("claude", &provider_b).unwrap();

        db.add_to_failover_queue("claude", "a").unwrap();
        db.add_to_failover_queue("claude", "b").unwrap();

        // 启用自动故障转移（使用新的 proxy_config API）
        let mut config = db.get_proxy_config_for_app("claude").await.unwrap();
        config.auto_failover_enabled = true;
        db.update_proxy_config_for_app(config).await.unwrap();

        let router = ProviderRouter::new(db.clone());

        router
            .record_result("b", "claude", false, Some("fail".to_string()))
            .await
            .unwrap();

        let providers = router.select_providers("claude").await.unwrap();
        assert_eq!(providers.len(), 2);

        assert!(router.allow_provider_request("b", "claude").await.allowed);
    }

    #[tokio::test]
    #[serial]
    async fn test_release_permit_neutral_frees_half_open_slot() {
        let _home = TempHome::new();
        let db = Arc::new(Database::memory().unwrap());

        // 配置熔断器：1 次失败即熔断，0 秒超时立即进入 HalfOpen
        db.update_circuit_breaker_config(&CircuitBreakerConfig {
            failure_threshold: 1,
            timeout_seconds: 0,
            ..Default::default()
        })
        .await
        .unwrap();

        let provider_a =
            Provider::with_id("a".to_string(), "Provider A".to_string(), json!({}), None);
        db.save_provider("claude", &provider_a).unwrap();
        db.add_to_failover_queue("claude", "a").unwrap();

        // 启用自动故障转移
        let mut config = db.get_proxy_config_for_app("claude").await.unwrap();
        config.auto_failover_enabled = true;
        db.update_proxy_config_for_app(config).await.unwrap();

        let router = ProviderRouter::new(db.clone());

        // 触发熔断：1 次失败
        router
            .record_result("a", "claude", false, Some("fail".to_string()))
            .await
            .unwrap();

        // 第一次请求：获取 HalfOpen 探测名额
        let first = router.allow_provider_request("a", "claude").await;
        assert!(first.allowed);
        assert!(first.permit.is_some());

        // 第二次请求应被拒绝（名额已被占用）
        let second = router.allow_provider_request("a", "claude").await;
        assert!(!second.allowed);

        // 使用 release_permit_neutral 释放名额（不影响健康统计）
        router.release_permit_neutral("a", "claude").await;

        // 第三次请求应被允许（名额已释放）
        let third = router.allow_provider_request("a", "claude").await;
        assert!(third.allowed);
        assert!(third.permit.is_some());
    }

    /// 复现 issue：5 小时 quota 用完后，P1 (P_top) 熔断。
    /// 期望：5 小时后（quota 重置），P1 应该能被 HalfOpen 探测并恢复。
    /// 实际：用户反馈 P1 一直 Open，5 小时后探测不到。
    #[tokio::test]
    #[serial]
    async fn test_p1_recovers_via_half_open_probe_after_quota_reset() {
        let _home = TempHome::new();
        let db = Arc::new(Database::memory().unwrap());

        // 配置熔断器：模拟用户的 8 次失败阈值，timeout 用 0 加速测试
        db.update_circuit_breaker_config(&CircuitBreakerConfig {
            failure_threshold: 8,
            timeout_seconds: 0, // timeout=0 等价于"5 小时已过"
            ..Default::default()
        })
        .await
        .unwrap();

        // P1 (top) 和 P2 (backup) — 完全照搬用户当前队列顺序
        let p1 = Provider::with_id("p1".to_string(), "P1 MiniMax".to_string(), json!({}), None);
        let p2 = Provider::with_id(
            "p2".to_string(),
            "P2 MiniMax Huawei".to_string(),
            json!({}),
            None,
        );
        db.save_provider("claude", &p1).unwrap();
        db.save_provider("claude", &p2).unwrap();
        db.add_to_failover_queue("claude", "p1").unwrap();
        db.add_to_failover_queue("claude", "p2").unwrap();

        let mut config = db.get_proxy_config_for_app("claude").await.unwrap();
        config.auto_failover_enabled = true;
        db.update_proxy_config_for_app(config).await.unwrap();

        let router = ProviderRouter::new(db.clone());

        // === 阶段 1: 模拟 5 小时 quota 用尽，P1 连续 8 次返回 429 ===
        for _ in 0..8 {
            router
                .record_result(
                    "p1",
                    "claude",
                    false,
                    Some("HTTP 429: quota exhausted".to_string()),
                )
                .await
                .unwrap();
        }

        // 验证 P1 现在处于 Open 状态
        let p1_stats = router
            .get_circuit_breaker_stats("p1", "claude")
            .await
            .unwrap();
        assert_eq!(
            format!("{:?}", p1_stats.state),
            "Open",
            "P1 应该因为 8 次 429 进入 Open 状态"
        );

        // === 阶段 2: 5 小时过去，quota 重置（timeout=0 立即到期）===
        // 等待一小段时间确保 Instant::elapsed() >= 0
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        // === 阶段 3: 验证 P1 可以被探测（这是用户报告失败的地方）===
        let providers = router.select_providers("claude").await.unwrap();
        assert_eq!(
            providers.len(),
            2,
            "P1 (HalfOpen) + P2 (Closed) 应该在候选列表里，实际只有 {} 个",
            providers.len()
        );
        assert_eq!(
            providers[0].id, "p1",
            "P1 必须是候选列表的第一个（按 sort_index 排序）"
        );

        // 关键断言：P1 应当能被允许探测
        let p1_probe = router.allow_provider_request("p1", "claude").await;
        assert!(
            p1_probe.allowed,
            "P1 处于 HalfOpen 状态，allow_provider_request 必须返回 allowed=true，\
             实际是 allowed={}（这是用户报告的 bug 核心）",
            p1_probe.allowed
        );
        assert!(
            p1_probe.permit.is_some(),
            "P1 应当占用一个 HalfOpen 探测名额"
        );

        // === 阶段 4: 模拟 P1 在 quota 重置后连续 3 次成功（success_threshold=2 默认）===
        router
            .record_result("p1", "claude", true, None)
            .await
            .unwrap();
        router
            .record_result("p1", "claude", true, None)
            .await
            .unwrap();

        // === 阶段 5: P1 现在应该完全恢复（Closed）===
        let p1_stats_after = router
            .get_circuit_breaker_stats("p1", "claude")
            .await
            .unwrap();
        assert_eq!(
            format!("{:?}", p1_stats_after.state),
            "Closed",
            "P1 连续 2 次成功后应该转为 Closed，\
             实际状态: {:?}（这是用户希望恢复后达到的状态）",
            p1_stats_after.state
        );

        // 最终：select_providers 应该把 P1 作为可用 provider 返回
        let final_providers = router.select_providers("claude").await.unwrap();
        assert_eq!(final_providers.len(), 2);
        assert_eq!(final_providers[0].id, "p1");
        assert_eq!(final_providers[1].id, "p2");
    }

    /// 复现用户日志里观察到的现象：P1 长时间没被探测。
    /// 模拟场景：P1 和 P2 都被熔断，P2 的 HalfOpen 探测一直失败，
    /// P1 永远轮不到被探测。
    #[tokio::test]
    #[serial]
    async fn test_p1_should_get_half_open_probe_even_when_p2_keeps_failing() {
        let _home = TempHome::new();
        let db = Arc::new(Database::memory().unwrap());

        // timeout=0：每次 fail 后立刻可以再次进 HalfOpen
        db.update_circuit_breaker_config(&CircuitBreakerConfig {
            failure_threshold: 2,
            success_threshold: 2,
            timeout_seconds: 0,
            ..Default::default()
        })
        .await
        .unwrap();

        let p1 = Provider::with_id("p1".to_string(), "P1".to_string(), json!({}), None);
        let p2 = Provider::with_id("p2".to_string(), "P2".to_string(), json!({}), None);
        db.save_provider("claude", &p1).unwrap();
        db.save_provider("claude", &p2).unwrap();
        db.add_to_failover_queue("claude", "p1").unwrap();
        db.add_to_failover_queue("claude", "p2").unwrap();

        let mut config = db.get_proxy_config_for_app("claude").await.unwrap();
        config.auto_failover_enabled = true;
        db.update_proxy_config_for_app(config).await.unwrap();

        let router = ProviderRouter::new(db.clone());

        // P1 和 P2 同时被 trip（连续 2 次失败）
        for _ in 0..2 {
            router
                .record_result("p1", "claude", false, Some("quota".to_string()))
                .await
                .unwrap();
            router
                .record_result("p2", "claude", false, Some("quota".to_string()))
                .await
                .unwrap();
        }

        // 等待 10ms 确保 Instant::elapsed() >= 0（timeout_seconds=0）
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        // 第一轮请求：P1 应该被允许探测（HalfOpen 转换 + permit 获取）
        // 模拟 forwarder 的行为：先 select_providers，然后逐个 try_provider_request
        let providers = router.select_providers("claude").await.unwrap();
        assert_eq!(providers.len(), 2, "P1 和 P2 都应该在候选列表里");

        let p1_probe = router.allow_provider_request("p1", "claude").await;
        assert!(
            p1_probe.allowed,
            "第一轮请求 P1 应当能拿到探测名额（这是用户日志里没发生的）"
        );

        // 模拟 P1 探测失败（quota 还没重置）
        if p1_probe.permit.is_some() {
            router
                .record_result("p1", "claude", false, Some("still 429".to_string()))
                .await
                .unwrap();
        }

        // 第二轮：再等 10ms，让 P1 的 last_opened_at 重新计时
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        // 关键断言：第二轮 P1 仍然必须能拿到探测名额
        // （如果当前实现有 bug，P1 永远在等一个被 P2 占用的 permit）
        let p1_probe_2 = router.allow_provider_request("p1", "claude").await;
        assert!(
            p1_probe_2.allowed,
            "第二轮请求 P1 仍应当能拿到探测名额，\
             实际是 allowed={}（如果这个失败，说明 P1 的探测名额被泄漏了）",
            p1_probe_2.allowed
        );
    }

    /// 【关键】复现"探测发出但永远没收到响应"的场景——HalfOpen permit 泄漏。
    /// 这是从用户日志里 09:36:33 那个 [CB-001] 之后没 [CB-002]/[CB-003] 的现象推断的：
    /// 探测发出去了，但 record_result 从未被调用，permit 永久卡在占用状态。
    /// 后果：同一个 provider 永远无法再发新探测 → 看起来"breaker 卡住了"。
    #[tokio::test]
    #[serial]
    async fn test_half_open_probe_response_lost_releases_permit_on_next_transition() {
        let _home = TempHome::new();
        let db = Arc::new(Database::memory().unwrap());

        // 与用户配置一致：failure_threshold=8, success_threshold=2, timeout=0（加速测试）
        db.update_circuit_breaker_config(&CircuitBreakerConfig {
            failure_threshold: 8,
            success_threshold: 2,
            timeout_seconds: 0,
            ..Default::default()
        })
        .await
        .unwrap();

        let p1 = Provider::with_id("p1".to_string(), "P1".to_string(), json!({}), None);
        db.save_provider("claude", &p1).unwrap();
        db.add_to_failover_queue("claude", "p1").unwrap();

        let mut config = db.get_proxy_config_for_app("claude").await.unwrap();
        config.auto_failover_enabled = true;
        db.update_proxy_config_for_app(config).await.unwrap();

        let router = ProviderRouter::new(db.clone());

        // Trip P1：8 次连续 429
        for _ in 0..8 {
            router
                .record_result("p1", "claude", false, Some("429 quota".to_string()))
                .await
                .unwrap();
        }

        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        // === 关键场景：探测发出去了，但响应永远丢失（不调 record_result）===
        // 这模拟用户日志里 09:36:33 的 [CB-001] 之后没 [CB-003] 的现象
        #[allow(deprecated)]
        let first_probe = router.allow_provider_request("p1", "claude").await;
        assert!(first_probe.allowed, "第一次探测必须允许");
        assert!(
            first_probe.permit.is_some(),
            "第一次探测必须返回 permit guard"
        );

        // ⚠️ 这里**故意**不调用 record_result —— 模拟"探测响应丢失"，
        // 真实场景里可能是：网络中断、客户端断连、forwarder 内部 panic。
        //
        // RAII 修复前：half_open_requests 计数卡在 1，第二次探测永远 false
        // RAII 修复后：guard 随第一次探测 result drop → Drop 自动 release_half_open_permit
        drop(first_probe);

        // 下一次请求来了（90s 后的真实场景；这里用 timeout=0 + sleep 10ms 模拟）
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        // 【这是 bug 的核心断言】
        // 修复前：返回 allowed=false（half_open_requests 计数卡在 1）
        // 修复后（RAII guard Drop 兜底）：allowed=true（permit 已释放）
        let second_probe = router.allow_provider_request("p1", "claude").await;
        assert!(
            second_probe.allowed,
            "上一次探测响应丢失后，下一次请求 P1 仍应当能拿到探测名额。\
             RAII guard 的 Drop 必须自动 release permit。"
        );
    }

    /// **回归测试 / API 契约**：forwarder.rs 在 HalfOpen 探测期间必须持有 permit guard。
    ///
    /// 这是 forwarder.rs:484-491 bug 的核心契约——只有当 `probe.permit` 被绑定到
    /// 一个跨过 `forward()` 调用的局部变量时，`max_half_open_requests = 1` 的
    /// 限流才会生效。如果 forwarder.rs 又退化回 `(permit.allowed, permit.used_half_open_permit)`
    /// 这种"读完字段就让 AllowRequestResult 立刻 drop"的写法，那么本测试虽然本身
    /// 还能通过（因为它在 API 层用对了模式），但 forwarder.rs 的源文件回归测试
    /// （`forwarder_does_not_release_permit_early`）会立刻失败。
    ///
    /// 本测试 PASSES today，作用是文档化契约：guard 必须显式绑定。
    #[tokio::test]
    #[serial]
    async fn test_holding_guard_enforces_max_one_in_flight() {
        let _home = TempHome::new();
        let db = Arc::new(Database::memory().unwrap());

        db.update_circuit_breaker_config(&CircuitBreakerConfig {
            failure_threshold: 1,
            timeout_seconds: 0,
            ..Default::default()
        })
        .await
        .unwrap();

        let p1 = Provider::with_id("p1".to_string(), "P1".to_string(), json!({}), None);
        db.save_provider("claude", &p1).unwrap();
        db.add_to_failover_queue("claude", "p1").unwrap();

        let mut config = db.get_proxy_config_for_app("claude").await.unwrap();
        config.auto_failover_enabled = true;
        db.update_proxy_config_for_app(config).await.unwrap();

        let router = ProviderRouter::new(db.clone());

        // Trip P1 → HalfOpen（timeout=0）
        router
            .record_result("p1", "claude", false, Some("429".to_string()))
            .await
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        // === 正确模式：把 permit 绑定到一个跨过"请求生命周期"的变量 ===
        #[allow(deprecated)]
        let probe = router.allow_provider_request("p1", "claude").await;
        assert!(probe.allowed, "第一次探测必须允许");
        assert!(probe.permit.is_some(), "第一次探测必须返回 permit guard");
        let guard = probe.permit; // ← 关键：绑定 guard，不让它随 probe drop
        let _used_half_open_permit = guard.is_some(); // 派生 bool（迁移后替代旧字段）

        // 关键断言：guard 持有期间，第二次并发探测必须被 REJECT
        let concurrent_probe = router.allow_provider_request("p1", "claude").await;
        assert!(
            !concurrent_probe.allowed,
            "max_half_open_requests = 1 必须在 guard 持有期间生效。\
             当前实现若退化回 destructure 写法，guard 会在探测前 drop，\
             permit 立即释放 → 这里会允许第二次探测 → 测试失败。"
        );

        // 释放 guard（模拟 forwarder.rs 在请求完成后调用 record_*_result 走 disarm / drop）
        drop(guard);

        // guard 释放后，下一次探测应当被允许
        let after_release = router.allow_provider_request("p1", "claude").await;
        assert!(after_release.allowed, "guard 释放后下一次探测必须被允许");
    }

    /// **回归测试 / 行为证明**：旧的 destructure 写法会导致并发探测通过（这就是 bug）。
    ///
    /// 本测试 PASSES today——它文档化了"destructure + drop"这种用法的实际行为：
    /// permit 会被立刻释放，第二次探测会通过。
    /// 它的存在是为了让任何人想"简化" forwarder.rs 时三思——
    /// 写成 `(probe.allowed, probe.permit.is_some())` 后再让 probe drop
    /// 是 P0 级别 bug 的根源。
    #[tokio::test]
    #[serial]
    async fn test_destructure_pattern_releases_permit_immediately() {
        let _home = TempHome::new();
        let db = Arc::new(Database::memory().unwrap());

        db.update_circuit_breaker_config(&CircuitBreakerConfig {
            failure_threshold: 1,
            timeout_seconds: 0,
            ..Default::default()
        })
        .await
        .unwrap();

        let p1 = Provider::with_id("p1".to_string(), "P1".to_string(), json!({}), None);
        db.save_provider("claude", &p1).unwrap();
        db.add_to_failover_queue("claude", "p1").unwrap();

        let mut config = db.get_proxy_config_for_app("claude").await.unwrap();
        config.auto_failover_enabled = true;
        db.update_proxy_config_for_app(config).await.unwrap();

        let router = ProviderRouter::new(db.clone());

        router
            .record_result("p1", "claude", false, Some("429".to_string()))
            .await
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        // === 模拟 forwarder.rs 的旧 buggy 写法 ===
        // 在表达式结束后，probe 整个被 drop，里面的 permit guard 也 drop，
        // release_half_open_permit 立刻执行。
        #[allow(deprecated)]
        let (allowed, _used_half_open_permit) = {
            let probe = router.allow_provider_request("p1", "claude").await;
            (probe.allowed, probe.permit.is_some())
            // ← probe 在这里出作用域 → permit guard drop → permit 立即释放
        };
        assert!(allowed, "第一次探测必须允许");

        // 行为断言：destructure 后 permit 已经释放，第二次探测被允许
        // （这正是 forwarder.rs 的 bug：探测还没发出去，permit 就还回去了）
        #[allow(deprecated)]
        let second_probe = router.allow_provider_request("p1", "claude").await;
        assert!(
            second_probe.allowed,
            "destructure 写法会立刻释放 permit，所以第二次探测会通过——\
             这正是 forwarder.rs 的 P0 bug。forwarder.rs 必须改用 `let guard = probe.permit;`。"
        );
    }
}
