use axum::Router;
use serde::{Deserialize, Serialize};

/// 内部用量结果（query_provider_usage_inner 返回）
#[derive(Debug, Clone)]
pub struct UsageResult {
    pub provider: String,
    pub remaining: i64,
    pub unit: String,
}

/// Provider 用量 DTO（HTTP API 响应）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderUsageDto {
    pub provider: String,
    #[serde(rename = "remaining_5h")]
    pub remaining_5h: i64,
    #[serde(rename = "remaining_weekly")]
    pub remaining_weekly: i64,
    pub unit: String,
    #[serde(rename = "updated_at")]
    pub updated_at: String,
}

impl Default for ProviderUsageDto {
    fn default() -> Self {
        Self {
            provider: String::new(),
            remaining_5h: 0,
            remaining_weekly: 0,
            unit: String::new(),
            updated_at: String::new(),
        }
    }
}

impl From<UsageResult> for ProviderUsageDto {
    fn from(result: UsageResult) -> Self {
        Self {
            provider: result.provider,
            remaining_5h: result.remaining,
            remaining_weekly: result.remaining * 7,
            unit: result.unit,
            updated_at: chrono::Local::now().to_rfc3339(),
        }
    }
}

/// 注册用量相关路由
pub fn usage_routes() -> Router {
    Router::new()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dto_serialize() {
        let dto = ProviderUsageDto {
            provider: "copilot".to_string(),
            remaining_5h: 1000,
            remaining_weekly: 7000,
            unit: "tokens".to_string(),
            updated_at: "2026-06-02T12:00:00+08:00".to_string(),
        };
        let json = serde_json::to_string(&dto).unwrap();
        assert!(json.contains("\"provider\":\"copilot\""));
        assert!(json.contains("\"remaining_5h\":1000"));
        assert!(json.contains("\"remaining_weekly\":7000"));
        assert!(json.contains("\"unit\":\"tokens\""));
        assert!(json.contains("\"updated_at\":\"2026-06-02T12:00:00+08:00\""));
    }

    #[test]
    fn test_dto_empty_list() {
        let list: Vec<ProviderUsageDto> = vec![];
        let json = serde_json::to_string(&list).unwrap();
        assert_eq!(json, "[]");
    }
}