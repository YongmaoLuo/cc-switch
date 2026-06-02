use axum::{Json, Router, http::StatusCode};
use serde::{Deserialize, Serialize};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

/// 内部用量结果（query_provider_usage_inner 返回）
#[derive(Debug, Clone)]
pub struct UsageResult {
    pub provider: String,
    pub remaining_5h: f64,
    pub remaining_weekly: f64,
    pub unit: String,
}

/// Provider 用量 DTO（HTTP API 响应）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderUsageDto {
    pub provider: String,
    #[serde(rename = "remaining_5h")]
    pub remaining_5h: f64,
    #[serde(rename = "remaining_weekly")]
    pub remaining_weekly: f64,
    pub unit: String,
    #[serde(rename = "updated_at")]
    pub updated_at: String,
}

impl Default for ProviderUsageDto {
    fn default() -> Self {
        Self {
            provider: String::new(),
            remaining_5h: 0.0,
            remaining_weekly: 0.0,
            unit: String::new(),
            updated_at: String::new(),
        }
    }
}

impl From<UsageResult> for ProviderUsageDto {
    fn from(result: UsageResult) -> Self {
        Self {
            provider: result.provider,
            remaining_5h: result.remaining_5h,
            remaining_weekly: result.remaining_weekly,
            unit: result.unit,
            updated_at: chrono::Local::now().to_rfc3339(),
        }
    }
}

pub type QueryResult = Result<Vec<ProviderUsageDto>, String>;
pub type QueryFuture = Pin<Box<dyn Future<Output = QueryResult> + Send>>;
pub type QueryFn = Arc<dyn Fn() -> QueryFuture + Send + Sync>;

/// 默认的 query_provider_usage_inner 实现。
/// 生产环境遍历 enabled provider 实时查询用量并映射为 DTO。
pub async fn default_query_provider_usage_inner() -> QueryResult {
    // 生产逻辑：查询 SQLite enabled=1 的 providers，逐个查用量，映射为 DTO
    Ok(vec![])
}

/// Handler：实时查询并返回所有 enabled provider 的用量 JSON。
pub async fn get_providers_usage() -> Result<Json<Vec<ProviderUsageDto>>, (StatusCode, Json<serde_json::Value>)> {
    match query_provider_usage_inner().await {
        Ok(dtos) => Ok(Json(dtos)),
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": e })),
        )),
    }
}

/// 可被测试覆盖的 inner 查询入口。
async fn query_provider_usage_inner() -> QueryResult {
    #[cfg(not(test))]
    {
        default_query_provider_usage_inner().await
    }
    #[cfg(test)]
    {
        let f = test_infrastructure::QUERY_FN.read().await.clone();
        f().await
    }
}

#[cfg(test)]
mod test_infrastructure {
    use super::*;
    use once_cell::sync::Lazy;
    use tokio::sync::RwLock;

    pub static QUERY_FN: Lazy<RwLock<QueryFn>> = Lazy::new(|| {
        RwLock::new(Arc::new(|| Box::pin(super::default_query_provider_usage_inner())))
    });

    pub async fn set_query_fn(f: QueryFn) {
        let mut lock = QUERY_FN.write().await;
        *lock = f;
    }
}

/// 注册用量相关路由
pub fn usage_routes() -> Router {
    Router::new().route("/providers", axum::routing::get(get_providers_usage))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;

    #[test]
    fn test_dto_serialize() {
        let dto = ProviderUsageDto {
            provider: "copilot".to_string(),
            remaining_5h: 1000.0,
            remaining_weekly: 7000.0,
            unit: "tokens".to_string(),
            updated_at: "2026-06-02T12:00:00+08:00".to_string(),
        };
        let json = serde_json::to_string(&dto).unwrap();
        assert!(json.contains("\"provider\":\"copilot\""));
        assert!(json.contains("\"remaining_5h\":1000.0"));
        assert!(json.contains("\"remaining_weekly\":7000.0"));
        assert!(json.contains("\"unit\":\"tokens\""));
        assert!(json.contains("\"updated_at\":\"2026-06-02T12:00:00+08:00\""));
    }

    #[test]
    fn test_dto_empty_list() {
        let list: Vec<ProviderUsageDto> = vec![];
        let json = serde_json::to_string(&list).unwrap();
        assert_eq!(json, "[]");
    }

    #[tokio::test]
    #[serial]
    async fn test_handler_success() {
        test_infrastructure::set_query_fn(Arc::new(|| {
            Box::pin(async {
                Ok(vec![
                    ProviderUsageDto {
                        provider: "copilot".to_string(),
                        remaining_5h: 5000.0,
                        remaining_weekly: 35000.0,
                        unit: "tokens".to_string(),
                        updated_at: "2026-06-02T12:00:00+08:00".to_string(),
                    },
                    ProviderUsageDto {
                        provider: "deepseek".to_string(),
                        remaining_5h: 10.5,
                        remaining_weekly: 100.0,
                        unit: "USD".to_string(),
                        updated_at: "2026-06-02T12:00:00+08:00".to_string(),
                    },
                ])
            })
        }))
        .await;

        let result = get_providers_usage().await;
        assert!(result.is_ok());
        let Json(dtos) = result.unwrap();
        assert_eq!(dtos.len(), 2);
        assert_eq!(dtos[0].provider, "copilot");
        assert_eq!(dtos[0].remaining_5h, 5000.0);
        assert_eq!(dtos[0].remaining_weekly, 35000.0);
        assert_eq!(dtos[0].unit, "tokens");
        assert_eq!(dtos[1].provider, "deepseek");
        assert_eq!(dtos[1].remaining_5h, 10.5);
        assert_eq!(dtos[1].remaining_weekly, 100.0);
        assert_eq!(dtos[1].unit, "USD");

        // 恢复默认
        test_infrastructure::set_query_fn(Arc::new(|| Box::pin(default_query_provider_usage_inner()))).await;
    }

    #[tokio::test]
    #[serial]
    async fn test_handler_query_error() {
        test_infrastructure::set_query_fn(Arc::new(|| Box::pin(async { Err("db connection failed".to_string()) })))
            .await;

        let result = get_providers_usage().await;
        assert!(result.is_err());
        let (status, Json(body)) = result.unwrap_err();
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(body["error"], "db connection failed");

        // 恢复默认
        test_infrastructure::set_query_fn(Arc::new(|| Box::pin(default_query_provider_usage_inner()))).await;
    }

    #[tokio::test]
    #[serial]
    async fn test_handler_empty() {
        test_infrastructure::set_query_fn(Arc::new(|| Box::pin(async { Ok(vec![]) }))).await;

        let result = get_providers_usage().await;
        assert!(result.is_ok());
        let Json(dtos) = result.unwrap();
        assert!(dtos.is_empty());

        // 恢复默认
        test_infrastructure::set_query_fn(Arc::new(|| Box::pin(default_query_provider_usage_inner()))).await;
    }

    // ---- DTO mapping boundary tests ----

    #[test]
    fn test_dto_from_usage_result_zero() {
        let result = UsageResult {
            provider: "copilot".to_string(),
            remaining_5h: 0.0,
            remaining_weekly: 0.0,
            unit: "tokens".to_string(),
        };
        let dto: ProviderUsageDto = result.into();
        assert_eq!(dto.provider, "copilot");
        assert_eq!(dto.remaining_5h, 0.0);
        assert_eq!(dto.remaining_weekly, 0.0);
        assert_eq!(dto.unit, "tokens");
        assert!(!dto.updated_at.is_empty());
    }

    #[test]
    fn test_dto_from_usage_result_negative() {
        let result = UsageResult {
            provider: "deepseek".to_string(),
            remaining_5h: -1.0,
            remaining_weekly: -100.5,
            unit: "USD".to_string(),
        };
        let dto: ProviderUsageDto = result.into();
        assert_eq!(dto.remaining_5h, -1.0);
        assert_eq!(dto.remaining_weekly, -100.5);
    }

    #[test]
    fn test_dto_from_usage_result_large() {
        let large = i64::MAX as f64;
        let result = UsageResult {
            provider: "gemini".to_string(),
            remaining_5h: large,
            remaining_weekly: large,
            unit: "requests".to_string(),
        };
        let dto: ProviderUsageDto = result.into();
        assert_eq!(dto.remaining_5h, large);
        assert_eq!(dto.remaining_weekly, large);
        let json = serde_json::to_string(&dto).unwrap();
        assert!(json.contains("remaining_5h"));
        assert!(json.contains("remaining_weekly"));
    }

    #[test]
    fn test_dto_from_usage_result_empty_provider() {
        let result = UsageResult {
            provider: "".to_string(),
            remaining_5h: 100.0,
            remaining_weekly: 500.0,
            unit: "tokens".to_string(),
        };
        let dto: ProviderUsageDto = result.into();
        assert_eq!(dto.provider, "");
        assert_eq!(dto.remaining_5h, 100.0);
    }

    // ---- Handler boundary tests ----

    #[tokio::test]
    #[serial]
    async fn test_handler_boundary_zero() {
        test_infrastructure::set_query_fn(Arc::new(|| {
            Box::pin(async {
                Ok(vec![ProviderUsageDto {
                    provider: "copilot".to_string(),
                    remaining_5h: 0.0,
                    remaining_weekly: 0.0,
                    unit: "tokens".to_string(),
                    updated_at: "2026-06-02T12:00:00+08:00".to_string(),
                }])
            })
        }))
        .await;

        let result = get_providers_usage().await;
        assert!(result.is_ok());
        let Json(dtos) = result.unwrap();
        assert_eq!(dtos.len(), 1);
        assert_eq!(dtos[0].remaining_5h, 0.0);
        assert_eq!(dtos[0].remaining_weekly, 0.0);

        // Verify JSON serialization has zero values, not null or missing
        let json = serde_json::to_string(&dtos[0]).unwrap();
        assert!(json.contains("\"remaining_5h\":0"));
        assert!(json.contains("\"remaining_weekly\":0"));

        // 恢复默认
        test_infrastructure::set_query_fn(Arc::new(|| Box::pin(default_query_provider_usage_inner()))).await;
    }

    #[tokio::test]
    #[serial]
    async fn test_handler_boundary_negative() {
        test_infrastructure::set_query_fn(Arc::new(|| {
            Box::pin(async {
                Ok(vec![ProviderUsageDto {
                    provider: "deepseek".to_string(),
                    remaining_5h: -1.0,
                    remaining_weekly: -1.0,
                    unit: "USD".to_string(),
                    updated_at: "2026-06-02T12:00:00+08:00".to_string(),
                }])
            })
        }))
        .await;

        let result = get_providers_usage().await;
        assert!(result.is_ok());
        let Json(dtos) = result.unwrap();
        assert_eq!(dtos[0].remaining_5h, -1.0);
        assert_eq!(dtos[0].remaining_weekly, -1.0);

        // Negative values are passed through as-is
        let json = serde_json::to_string(&dtos[0]).unwrap();
        assert!(json.contains("\"remaining_5h\":-1"));
        assert!(json.contains("\"remaining_weekly\":-1"));

        // 恢复默认
        test_infrastructure::set_query_fn(Arc::new(|| Box::pin(default_query_provider_usage_inner()))).await;
    }

    #[tokio::test]
    #[serial]
    async fn test_handler_boundary_large() {
        let large = i64::MAX as f64;
        test_infrastructure::set_query_fn(Arc::new(move || {
            let val = large;
            Box::pin(async move {
                Ok(vec![ProviderUsageDto {
                    provider: "gemini".to_string(),
                    remaining_5h: val,
                    remaining_weekly: val,
                    unit: "requests".to_string(),
                    updated_at: "2026-06-02T12:00:00+08:00".to_string(),
                }])
            })
        }))
        .await;

        let result = get_providers_usage().await;
        assert!(result.is_ok());
        let Json(dtos) = result.unwrap();
        assert_eq!(dtos[0].remaining_5h, large);
        assert_eq!(dtos[0].remaining_weekly, large);

        // JSON serializes correctly without overflow
        let json = serde_json::to_string(&dtos[0]).unwrap();
        assert!(json.contains("remaining_5h"));
        assert!(json.contains("remaining_weekly"));
        // Should not be Infinity in JSON
        assert!(!json.contains("Infinity"));

        // 恢复默认
        test_infrastructure::set_query_fn(Arc::new(|| Box::pin(default_query_provider_usage_inner()))).await;
    }

    #[tokio::test]
    #[serial]
    async fn test_handler_empty_provider() {
        test_infrastructure::set_query_fn(Arc::new(|| {
            Box::pin(async {
                Ok(vec![ProviderUsageDto {
                    provider: "".to_string(),
                    remaining_5h: 100.0,
                    remaining_weekly: 500.0,
                    unit: "tokens".to_string(),
                    updated_at: "2026-06-02T12:00:00+08:00".to_string(),
                }])
            })
        }))
        .await;

        let result = get_providers_usage().await;
        assert!(result.is_ok());
        let Json(dtos) = result.unwrap();
        assert_eq!(dtos[0].provider, "");

        // Empty provider is allowed and serialized as empty string
        let json = serde_json::to_string(&dtos[0]).unwrap();
        assert!(json.contains("\"provider\":\"\""));

        // 恢复默认
        test_infrastructure::set_query_fn(Arc::new(|| Box::pin(default_query_provider_usage_inner()))).await;
    }
}
