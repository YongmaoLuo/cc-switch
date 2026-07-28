//! Axum handlers for the remote management API

use super::html::REMOTE_HTML;
use super::RemoteState;
use crate::app_config::AppType;
use crate::commands::{self, CopilotAuthState};
use crate::services::{ProviderService, ProviderSortUpdate};
use crate::store::AppState;
use axum::extract::State as AxumState;
use axum::http::header;
use axum::response::sse::{Event, Sse};
use axum::response::{Html, IntoResponse, Json};
use futures::stream::Stream;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::convert::Infallible;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use tauri::{Emitter, Manager};

const REMOTE_APP: AppType = AppType::Claude;

#[derive(Deserialize)]
pub struct SwitchRequest {
    pub provider_id: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReorderRequest {
    /// 被拖拽移动的 provider ID
    pub moved_id: String,
    /// 目标位置的 provider ID（移动到这个 provider 之前）
    pub before_id: String,
}

fn app_state(state: &RemoteState) -> tauri::State<'_, AppState> {
    state.app_handle.state::<AppState>()
}

/// GET / — HTML 页面
pub async fn index(AxumState(state): AxumState<Arc<RemoteState>>) -> impl IntoResponse {
    if !state.running.load(Ordering::SeqCst) {
        return (
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            "Remote server is stopped",
        )
            .into_response();
    }
    Html(REMOTE_HTML).into_response()
}

/// GET /api/health
pub async fn health_check(AxumState(state): AxumState<Arc<RemoteState>>) -> impl IntoResponse {
    if !state.running.load(Ordering::SeqCst) {
        return (
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"status": "stopped"})),
        )
            .into_response();
    }
    Json(json!({
        "status": "ok",
        "version": "1.0.0"
    }))
    .into_response()
}

/// GET /api/providers
pub async fn get_providers(AxumState(state): AxumState<Arc<RemoteState>>) -> impl IntoResponse {
    if !state.running.load(Ordering::SeqCst) {
        return (
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "Server is stopped"})),
        )
            .into_response();
    }

    let app_state = app_state(&state);

    match ProviderService::list(app_state.inner(), REMOTE_APP) {
        Ok(providers) => {
            // 获取当前 provider ID
            let current_id =
                ProviderService::current(app_state.inner(), REMOTE_APP).unwrap_or_default();

            let provider_list: Vec<serde_json::Value> = providers
                .iter()
                .map(|(id, p)| {
                    json!({
                        "id": id,
                        "name": p.name,
                        "is_current": id == &current_id,
                        "category": p.category,
                        "icon": p.icon,
                        "icon_color": p.icon_color,
                    })
                })
                .collect();

            Json(json!({"providers": provider_list})).into_response()
        }
        Err(e) => Json(json!({"error": e.to_string()})).into_response(),
    }
}

/// GET /api/current
pub async fn get_current(AxumState(state): AxumState<Arc<RemoteState>>) -> impl IntoResponse {
    if !state.running.load(Ordering::SeqCst) {
        return (
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "Server is stopped"})),
        )
            .into_response();
    }

    let app_state = app_state(&state);

    match ProviderService::current(app_state.inner(), REMOTE_APP) {
        Ok(current_id) => {
            if current_id.is_empty() {
                return Json(json!({"current": null})).into_response();
            }
            let providers =
                ProviderService::list(app_state.inner(), REMOTE_APP).unwrap_or_default();
            match providers.get(&current_id) {
                Some(p) => {
                    Json(json!({"current": {"id": current_id, "name": p.name}})).into_response()
                }
                None => Json(json!({"current": null})).into_response(),
            }
        }
        Err(_) => Json(json!({"current": null})).into_response(),
    }
}

/// POST /api/switch
pub async fn switch_provider(
    AxumState(state): AxumState<Arc<RemoteState>>,
    Json(body): Json<SwitchRequest>,
) -> impl IntoResponse {
    if !state.running.load(Ordering::SeqCst) {
        return (
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "Server is stopped"})),
        )
            .into_response();
    }

    if body.provider_id.is_empty() {
        return Json(json!({"success": false, "error": "Missing provider_id"})).into_response();
    }

    let app_state = app_state(&state);

    // 核心：通过 ProviderService::switch() 切换，确保 backfill 正确
    match ProviderService::switch(app_state.inner(), REMOTE_APP, &body.provider_id) {
        Ok(result) => {
            // 获取 provider 名称
            let providers =
                ProviderService::list(app_state.inner(), REMOTE_APP).unwrap_or_default();
            let name = providers
                .get(&body.provider_id)
                .map(|p| p.name.clone())
                .unwrap_or_default();

            // 广播 SSE 给远程浏览器
            let sse_data = json!({
                "type": "switch",
                "provider_id": body.provider_id,
                "name": name
            });
            let _ = state.sse_tx.send(sse_data.to_string());

            // 通知 Tauri 前端刷新（使用与 tray 相同的事件名和格式）
            let _ = state.app_handle.emit(
                "provider-switched",
                json!({
                    "appType": REMOTE_APP.as_str(),
                    "providerId": body.provider_id
                }),
            );

            // 刷新托盘菜单
            if let Ok(new_menu) =
                crate::tray::create_tray_menu(&state.app_handle, app_state.inner())
            {
                if let Some(tray) = state.app_handle.tray_by_id("main") {
                    let _ = tray.set_menu(Some(new_menu));
                }
            }

            log::info!(
                "[Remote] Switched to provider '{}' ({})",
                name,
                body.provider_id
            );

            Json(json!({
                "success": true,
                "name": name,
                "warnings": result.warnings
            }))
            .into_response()
        }
        Err(e) => {
            log::error!("[Remote] Switch failed: {e}");
            Json(json!({"success": false, "error": e.to_string()})).into_response()
        }
    }
}

/// POST /api/reorder
///
/// 模拟拖拽排序：将 moved_id 移动到 before_id 之前。
/// 如果 before_id 为空，则移动到列表末尾。
pub async fn reorder_providers(
    AxumState(state): AxumState<Arc<RemoteState>>,
    Json(body): Json<ReorderRequest>,
) -> impl IntoResponse {
    if !state.running.load(Ordering::SeqCst) {
        return (
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "Server is stopped"})),
        )
            .into_response();
    }

    if body.moved_id.is_empty() {
        return Json(json!({"success": false, "error": "Missing moved_id"})).into_response();
    }

    let app_state = app_state(&state);

    // 1. 获取当前完整 provider 列表
    let providers = match ProviderService::list(app_state.inner(), REMOTE_APP) {
        Ok(p) => p,
        Err(e) => {
            return Json(json!({"success": false, "error": e.to_string()})).into_response();
        }
    };

    // 2. 按 sort_index 排序（与前端 sortedProviders 一致）
    let mut sorted: Vec<(&String, &crate::provider::Provider)> = providers.iter().collect();
    sorted.sort_by(|(_, a), (_, b)| match (a.sort_index, b.sort_index) {
        (Some(ai), Some(bi)) => ai.cmp(&bi),
        (Some(_), None) => std::cmp::Ordering::Less,
        (None, Some(_)) => std::cmp::Ordering::Greater,
        (None, None) => std::cmp::Ordering::Equal,
    });

    // 3. 找到 moved_id 和 before_id 的位置
    let moved_idx = sorted
        .iter()
        .position(|(id, _)| id.as_str() == body.moved_id);
    let before_idx = if body.before_id.is_empty() {
        None // 移动到末尾
    } else {
        sorted
            .iter()
            .position(|(id, _)| id.as_str() == body.before_id)
    };

    let moved_idx = match moved_idx {
        Some(idx) => idx,
        None => {
            return Json(
                json!({"success": false, "error": format!("Provider '{}' not found", body.moved_id)}),
            )
            .into_response();
        }
    };

    // 4. 模拟 arrayMove：移除 moved，插入到 before 之前
    let item = sorted.remove(moved_idx);
    let new_idx = match before_idx {
        Some(idx) => {
            // 移除 moved 后，若 moved 在 before 前面，before 的位置前移了一位
            if idx > moved_idx {
                idx - 1
            } else {
                idx
            }
        }
        None => sorted.len(), // 末尾
    };
    sorted.insert(new_idx, item);

    // 5. 重新分配 sort_index
    let updates: Vec<ProviderSortUpdate> = sorted
        .into_iter()
        .enumerate()
        .map(|(idx, (id, _))| ProviderSortUpdate {
            id: id.clone(),
            sort_index: idx,
        })
        .collect();

    // 6. 保存
    match ProviderService::update_sort_order(app_state.inner(), REMOTE_APP, updates) {
        Ok(_) => {
            log::info!(
                "[Remote] Moved provider '{}' to position {} in {}",
                body.moved_id,
                new_idx,
                REMOTE_APP.as_str()
            );
            Json(json!({"success": true})).into_response()
        }
        Err(e) => {
            log::error!("[Remote] Reorder failed: {e}");
            Json(json!({"success": false, "error": e.to_string()})).into_response()
        }
    }
}

/// GET /api/events — SSE endpoint
pub async fn sse_events(
    AxumState(state): AxumState<Arc<RemoteState>>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let mut rx = state.sse_tx.subscribe();

    let stream = async_stream::stream! {
        loop {
            if !state.running.load(Ordering::SeqCst) {
                // Server stopped: send shutdown event and close stream
                yield Ok(Event::default().data(r#"{"type":"shutdown"}"#));
                break;
            }
            match rx.recv().await {
                Ok(data) => {
                    yield Ok(Event::default().data(data));
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                    // Skip lagged messages, continue
                    continue;
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                    break;
                }
            }
        }
    };

    Sse::new(stream).keep_alive(
        axum::response::sse::KeepAlive::new()
            .interval(Duration::from_secs(30))
            .text("ping"),
    )
}

/// GET /api/icon — 返回 CC Switch 图标 (PNG)
pub async fn get_icon() -> impl IntoResponse {
    let icon_bytes = include_bytes!("../../icons/icon.png");
    (
        [
            (header::CONTENT_TYPE, "image/png"),
            (header::CACHE_CONTROL, "public, max-age=86400"),
        ],
        icon_bytes.as_slice(),
    )
}

#[derive(Deserialize)]
pub struct IconQuery {
    pub color: Option<String>,
}

/// GET /api/provider-icons/:name?color=<hex> — 返回 provider 图标 (SVG)
/// color 参数：将 SVG 中的 currentColor 替换为指定颜色，使 <img> 标签也能正确着色
pub async fn get_provider_icon(
    axum::extract::Path(name): axum::extract::Path<String>,
    axum::extract::Query(query): axum::extract::Query<IconQuery>,
) -> impl IntoResponse {
    // 在编译时嵌入全部图标
    let svg_template: &'static str = match name.as_str() {
        "aicodemirror" => include_str!("../../../src/icons/extracted/aicodemirror.svg"),
        "aicoding" => include_str!("../../../src/icons/extracted/aicoding.svg"),
        "aihubmix-color" => include_str!("../../../src/icons/extracted/aihubmix-color.svg"),
        "algocode" => include_str!("../../../src/icons/extracted/algocode.svg"),
        "alibaba" => include_str!("../../../src/icons/extracted/alibaba.svg"),
        "anthropic" => include_str!("../../../src/icons/extracted/anthropic.svg"),
        "aws" => include_str!("../../../src/icons/extracted/aws.svg"),
        "azure" => include_str!("../../../src/icons/extracted/azure.svg"),
        "baidu" => include_str!("../../../src/icons/extracted/baidu.svg"),
        "bailian" => include_str!("../../../src/icons/extracted/bailian.svg"),
        "bytedance" => include_str!("../../../src/icons/extracted/bytedance.svg"),
        "catcoder" => include_str!("../../../src/icons/extracted/catcoder.svg"),
        "chatglm" => include_str!("../../../src/icons/extracted/chatglm.svg"),
        "claude" => include_str!("../../../src/icons/extracted/claude.svg"),
        "claw" => include_str!("../../../src/icons/extracted/claw.svg"),
        "cloudflare" => include_str!("../../../src/icons/extracted/cloudflare.svg"),
        "cohere" => include_str!("../../../src/icons/extracted/cohere.svg"),
        "copilot" => include_str!("../../../src/icons/extracted/copilot.svg"),
        "crazyrouter" => include_str!("../../../src/icons/extracted/crazyrouter.svg"),
        "ctok" => include_str!("../../../src/icons/extracted/ctok.svg"),
        "cubence" => include_str!("../../../src/icons/extracted/cubence.svg"),
        "dds" => include_str!("../../../src/icons/extracted/dds.svg"),
        "deepseek" => include_str!("../../../src/icons/extracted/deepseek.svg"),
        "doubao" => include_str!("../../../src/icons/extracted/doubao.svg"),
        "gemini" => include_str!("../../../src/icons/extracted/gemini.svg"),
        "gemma" => include_str!("../../../src/icons/extracted/gemma.svg"),
        "github" => include_str!("../../../src/icons/extracted/github.svg"),
        "githubcopilot" => include_str!("../../../src/icons/extracted/githubcopilot.svg"),
        "google" => include_str!("../../../src/icons/extracted/google.svg"),
        "googlecloud" => include_str!("../../../src/icons/extracted/googlecloud.svg"),
        "grok" => include_str!("../../../src/icons/extracted/grok.svg"),
        "huawei" => include_str!("../../../src/icons/extracted/huawei.svg"),
        "huggingface" => include_str!("../../../src/icons/extracted/huggingface.svg"),
        "hunyuan" => include_str!("../../../src/icons/extracted/hunyuan.svg"),
        "kimi" => include_str!("../../../src/icons/extracted/kimi.svg"),
        "lioncc" => include_str!("../../../src/icons/extracted/lioncc.svg"),
        "longcat-color" => include_str!("../../../src/icons/extracted/longcat-color.svg"),
        "mcp" => include_str!("../../../src/icons/extracted/mcp.svg"),
        "meta" => include_str!("../../../src/icons/extracted/meta.svg"),
        "micu" => include_str!("../../../src/icons/extracted/micu.svg"),
        "midjourney" => include_str!("../../../src/icons/extracted/midjourney.svg"),
        "minimax" => include_str!("../../../src/icons/extracted/minimax.svg"),
        "mistral" => include_str!("../../../src/icons/extracted/mistral.svg"),
        "modelscope-color" => include_str!("../../../src/icons/extracted/modelscope-color.svg"),
        "newapi" => include_str!("../../../src/icons/extracted/newapi.svg"),
        "notion" => include_str!("../../../src/icons/extracted/notion.svg"),
        "novita" => include_str!("../../../src/icons/extracted/novita.svg"),
        "nvidia" => include_str!("../../../src/icons/extracted/nvidia.svg"),
        "ollama" => include_str!("../../../src/icons/extracted/ollama.svg"),
        "openai" => include_str!("../../../src/icons/extracted/openai.svg"),
        "opencode-logo-light" => {
            include_str!("../../../src/icons/extracted/opencode-logo-light.svg")
        }
        "openrouter" => include_str!("../../../src/icons/extracted/openrouter.svg"),
        "packycode" => include_str!("../../../src/icons/extracted/packycode.svg"),
        "palm" => include_str!("../../../src/icons/extracted/palm.svg"),
        "perplexity" => include_str!("../../../src/icons/extracted/perplexity.svg"),
        "qwen" => include_str!("../../../src/icons/extracted/qwen.svg"),
        "rc" => include_str!("../../../src/icons/extracted/rc.svg"),
        "shengsuanyun" => include_str!("../../../src/icons/extracted/shengsuanyun.svg"),
        "siliconflow" => include_str!("../../../src/icons/extracted/siliconflow.svg"),
        "sssaicode" => include_str!("../../../src/icons/extracted/sssaicode.svg"),
        "stability" => include_str!("../../../src/icons/extracted/stability.svg"),
        "stepfun" => include_str!("../../../src/icons/extracted/stepfun.svg"),
        "tencent" => include_str!("../../../src/icons/extracted/tencent.svg"),
        "ucloud" => include_str!("../../../src/icons/extracted/ucloud.svg"),
        "vercel" => include_str!("../../../src/icons/extracted/vercel.svg"),
        "wenxin" => include_str!("../../../src/icons/extracted/wenxin.svg"),
        "xai" => include_str!("../../../src/icons/extracted/xai.svg"),
        "xiaomimimo" => include_str!("../../../src/icons/extracted/xiaomimimo.svg"),
        "yi" => include_str!("../../../src/icons/extracted/yi.svg"),
        "zeroone" => include_str!("../../../src/icons/extracted/zeroone.svg"),
        "zhipu" => include_str!("../../../src/icons/extracted/zhipu.svg"),
        _ => {
            r#"<svg xmlns="http://www.w3.org/2000/svg" width="1em" height="1em" viewBox="0 0 24 24" fill="currentColor"><circle cx="12" cy="12" r="10"/></svg>"#
        }
    };

    // 用实际颜色替换 currentColor，使 <img> 标签也能正确着色
    let svg_content = match &query.color {
        Some(color) if !color.is_empty() && color != "currentColor" => {
            svg_template.replace("currentColor", color)
        }
        _ => svg_template.to_string(),
    };

    (
        [
            (header::CONTENT_TYPE, "image/svg+xml"),
            (header::CACHE_CONTROL, "public, max-age=3600"),
        ],
        svg_content,
    )
}

#[derive(Deserialize, Default)]
#[serde(rename_all = "camelCase", default)]
pub struct ProxyControlRequest {
    /// true  = 启动代理（带 Live 配置接管，覆盖所有 app）
    /// false = 停止代理（恢复 Live 配置）— 默认
    pub start: bool,
}

/// POST /api/proxy/control — 统一控制代理的停止与启动
///
/// Body（可选）：
///   - `{}` 或缺省                  → 停止代理并恢复 Live 配置
///   - `{"start": true}`            → 启动代理并接管所有 app 的 Live 配置
///   - `{"start": false}`           → 停止代理（与默认行为相同）
///
/// 远程调用前必须停止代理，否则开发版本启动后会因 single-instance 冲突
/// 导致生产进程被抢占，进而使正在运行的 Agent 无法调用模型。
pub async fn proxy_control(
    AxumState(state): AxumState<Arc<RemoteState>>,
    body: Option<Json<ProxyControlRequest>>,
) -> impl IntoResponse {
    if !state.running.load(Ordering::SeqCst) {
        return (
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "Server is stopped"})),
        )
            .into_response();
    }

    let app_state = app_state(&state);
    let start = body.map(|Json(b)| b.start).unwrap_or(false);

    if start {
        // 启动代理 + 接管所有 app 的 Live 配置。
        // 幂等：start_with_takeover() 内部会先 is_running() 检查；
        // 如果 Live config 已被接管且指向当前代理，set_takeover_for_app 也会早退。
        match app_state.proxy_service.start_with_takeover().await {
            Ok(info) => {
                log::info!(
                    "[Remote] Proxy started and live configs taken over via API: {}:{}",
                    info.address,
                    info.port
                );
                Json(json!({
                    "success": true,
                    "action": "start",
                    "address": info.address,
                    "port": info.port,
                }))
                .into_response()
            }
            Err(e) => {
                log::error!("[Remote] Failed to start proxy via API: {e}");
                Json(json!({"success": false, "action": "start", "error": e.to_string()}))
                    .into_response()
            }
        }
    } else {
        match app_state.proxy_service.stop_with_restore().await {
            Ok(()) => {
                log::info!("[Remote] Proxy stopped and live configs restored via API");
                Json(json!({"success": true, "action": "stop"})).into_response()
            }
            Err(e) => {
                log::error!("[Remote] Failed to stop proxy via API: {e}");
                Json(json!({"success": false, "action": "stop", "error": e.to_string()}))
                    .into_response()
            }
        }
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProxyTakeoverRequest {
    pub app_type: String,
    pub enabled: bool,
}

/// POST /api/proxy/takeover — 为指定应用开启/关闭代理接管
///
/// Body: {"appType": "claude", "enabled": true}
pub async fn proxy_takeover(
    AxumState(state): AxumState<Arc<RemoteState>>,
    Json(body): Json<ProxyTakeoverRequest>,
) -> impl IntoResponse {
    if !state.running.load(Ordering::SeqCst) {
        return (
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "Server is stopped"})),
        )
            .into_response();
    }

    let app_state = app_state(&state);

    match app_state
        .proxy_service
        .set_takeover_for_app(&body.app_type, body.enabled)
        .await
    {
        Ok(()) => {
            log::info!(
                "[Remote] Proxy takeover for {} set to {} via API",
                body.app_type,
                body.enabled
            );
            Json(json!({"success": true})).into_response()
        }
        Err(e) => {
            log::error!("[Remote] Failed to set proxy takeover via API: {e}");
            Json(json!({"success": false, "error": e.to_string()})).into_response()
        }
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FailoverToggleRequest {
    pub app_type: String,
    pub enabled: bool,
}

/// POST /api/proxy/failover — 开启/关闭指定应用的自动故障转移
///
/// Body: {"appType": "claude", "enabled": true}
///
/// 开启时会自动将故障转移队列的 P1 设为当前供应商。
pub async fn proxy_failover(
    AxumState(state): AxumState<Arc<RemoteState>>,
    Json(body): Json<FailoverToggleRequest>,
) -> impl IntoResponse {
    use std::str::FromStr;
    use tauri::Emitter;

    if !state.running.load(Ordering::SeqCst) {
        return (
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "Server is stopped"})),
        )
            .into_response();
    }

    let app_state = app_state(&state);
    let app = &state.app_handle;

    // 强一致语义：开启故障转移后立即切到队列 P1（并确保队列非空）
    let p1_provider_id = if body.enabled {
        let mut queue = match app_state.db.get_failover_queue(&body.app_type) {
            Ok(q) => q,
            Err(e) => {
                return Json(json!({"success": false, "error": e.to_string()})).into_response();
            }
        };

        if queue.is_empty() {
            let app_enum = match crate::app_config::AppType::from_str(&body.app_type) {
                Ok(a) => a,
                Err(_) => {
                    return Json(
                        json!({"success": false, "error": format!("无效的应用类型: {}", body.app_type)}),
                    )
                    .into_response();
                }
            };

            let current_id =
                match crate::settings::get_effective_current_provider(&app_state.db, &app_enum) {
                    Ok(id) => id,
                    Err(e) => {
                        return Json(json!({"success": false, "error": e.to_string()}))
                            .into_response();
                    }
                };

            let Some(current_id) = current_id else {
                return Json(
                    json!({"success": false, "error": "故障转移队列为空，且未设置当前供应商，无法开启故障转移"}),
                )
                .into_response();
            };

            if let Err(e) = app_state
                .db
                .add_to_failover_queue(&body.app_type, &current_id)
            {
                return Json(json!({"success": false, "error": e.to_string()})).into_response();
            }

            queue = match app_state.db.get_failover_queue(&body.app_type) {
                Ok(q) => q,
                Err(e) => {
                    return Json(json!({"success": false, "error": e.to_string()})).into_response();
                }
            };
        }

        match queue.first() {
            Some(item) => item.provider_id.clone(),
            None => {
                return Json(
                    json!({"success": false, "error": "故障转移队列为空，无法开启故障转移"}),
                )
                .into_response();
            }
        }
    } else {
        String::new()
    };

    // 读取并更新 proxy_config
    let mut config = match app_state.db.get_proxy_config_for_app(&body.app_type).await {
        Ok(c) => c,
        Err(e) => {
            return Json(json!({"success": false, "error": e.to_string()})).into_response();
        }
    };
    config.auto_failover_enabled = body.enabled;

    if let Err(e) = app_state.db.update_proxy_config_for_app(config).await {
        return Json(json!({"success": false, "error": e.to_string()})).into_response();
    }

    // 开启后立即切到 P1
    if body.enabled {
        if let Err(e) = app_state
            .proxy_service
            .switch_proxy_target(&body.app_type, &p1_provider_id)
            .await
        {
            return Json(json!({"success": false, "error": e.to_string()})).into_response();
        }

        // 发射 provider-switched 事件
        let event_data = serde_json::json!({
            "appType": body.app_type,
            "providerId": p1_provider_id,
            "source": "failoverEnabled"
        });
        let _ = app.emit("provider-switched", event_data);

        // 广播给远程浏览器（SSE）
        let app_clone = app.clone();
        let p1_id = p1_provider_id.clone();
        let app_type = body.app_type.clone();
        tauri::async_runtime::spawn(async move {
            crate::remote::broadcast_provider_switch(&app_clone, &app_type, &p1_id).await;
        });
    }

    // 刷新托盘菜单
    if let Ok(new_menu) = crate::tray::create_tray_menu(app, app_state.inner()) {
        if let Some(tray) = app.tray_by_id(crate::tray::TRAY_ID) {
            let _ = tray.set_menu(Some(new_menu));
        }
    }

    log::info!(
        "[Remote] Auto failover for {} set to {} via API",
        body.app_type,
        body.enabled
    );

    Json(json!({"success": true})).into_response()
}

/// 单个 tier 的用量 DTO
#[derive(Debug, Clone, Serialize)]
pub struct TierUsageDto {
    pub name: String,
    pub used_percent: f64,
    pub remaining: f64,
    pub total: f64,
    pub unit: String,
    pub resets_at: Option<String>,
}

/// Provider 用量 DTO
#[derive(Debug, Clone, Serialize)]
pub struct ProviderUsageDto {
    pub provider: String,
    pub app_type: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub tiers: Vec<TierUsageDto>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<String>,
    /// true = 查询成功, false = 查询失败
    pub success: bool,
    /// 查询失败时的错误信息
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// GET /api/usage — 实时查询所有 enabled provider 的官方用量
///
/// 与 /api/providers 不同：本接口不返回状态/配置，而是触发一次实时查询
/// 拉取每个 provider 官方的 quota 数据（5h/周限额/余额等）并合并为统一 DTO。
pub async fn get_usage(AxumState(state): AxumState<Arc<RemoteState>>) -> impl IntoResponse {
    if !state.running.load(Ordering::SeqCst) {
        return (
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "Server is stopped"})),
        )
            .into_response();
    }

    let app_state = state.app_handle.state::<AppState>();
    let copilot_state = state.app_handle.state::<CopilotAuthState>();

    let mut results: Vec<ProviderUsageDto> = Vec::new();

    for app_type in AppType::all() {
        let providers = match app_state.db.get_all_providers(app_type.as_str()) {
            Ok(p) => p,
            Err(e) => {
                log::warn!(
                    "[Remote] Failed to get providers for {}: {e}",
                    app_type.as_str()
                );
                continue;
            }
        };

        for (provider_id, provider) in providers {
            // 只查询启用了 usage_script 的 provider
            if !provider.has_usage_script_enabled() {
                continue;
            }

            let template_type = provider
                .meta
                .as_ref()
                .and_then(|m| m.usage_script.as_ref())
                .and_then(|s| s.template_type.as_deref())
                .unwrap_or("");

            // Copilot provider 必须先有 OAuth 登录；未登录时静默跳过
            if template_type == "github_copilot" {
                let auth = copilot_state.0.read().await;
                if !auth.is_authenticated().await {
                    log::debug!(
                        "[Remote] Skipping Copilot provider {}: not authenticated",
                        provider_id
                    );
                    continue;
                }
            }

            let query_result = commands::query_provider_usage_inner(
                app_state.inner(),
                copilot_state.inner(),
                app_type.clone(),
                &provider_id,
            )
            .await;

            let usage_result = match query_result {
                Ok(r) => r,
                Err(e) => {
                    log::warn!("[Remote] Usage query failed for {}: {e}", provider_id);
                    results.push(ProviderUsageDto {
                        provider: provider.name.clone(),
                        app_type: app_type.as_str().to_string(),
                        tiers: vec![],
                        updated_at: None,
                        success: false,
                        error: Some(format!("{e}")),
                    });
                    continue;
                }
            };

            if !usage_result.success {
                log::warn!(
                    "[Remote] Usage query returned failure for {}: {:?}",
                    provider_id,
                    usage_result.error
                );
                results.push(ProviderUsageDto {
                    provider: provider.name.clone(),
                    app_type: app_type.as_str().to_string(),
                    tiers: vec![],
                    updated_at: None,
                    success: false,
                    error: usage_result.error.clone(),
                });
                continue;
            }

            let data = match usage_result.data {
                Some(d) if !d.is_empty() => d,
                _ => continue,
            };

            let tiers: Vec<TierUsageDto> = data
                .iter()
                .map(|d| TierUsageDto {
                    name: d.plan_name.clone().unwrap_or_default(),
                    used_percent: d.used.unwrap_or(0.0),
                    remaining: d.remaining.unwrap_or(0.0),
                    total: d.total.unwrap_or(0.0),
                    unit: d.unit.clone().unwrap_or_default(),
                    resets_at: d.extra.clone(),
                })
                .collect();

            results.push(ProviderUsageDto {
                provider: provider.name.clone(),
                app_type: app_type.as_str().to_string(),
                tiers,
                updated_at: Some(chrono::Local::now().to_rfc3339()),
                success: true,
                error: None,
            });
        }
    }

    Json(json!({ "providers": results })).into_response()
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TestProviderRequest {
    /// Provider ID
    pub provider_id: String,
    /// 应用类型，如 "claude" / "codex" / "gemini"
    pub app_type: String,
}

/// POST /api/provider/test — 对单个供应商执行模型连通性测试
///
/// 与 UI 点击 "测试模型" 按钮调用 `stream_check_provider` Tauri 命令完全一致：
/// 复用同一组 helper（resolve_copilot_auth_override / base_url / claude_api_format）
/// 和 `StreamCheckService::check_with_retry`。返回值也是同一个 `StreamCheckResult`
/// 结构，便于自动化运维脚本（例如先测一下 P1 可用性再决定是否排到队列头部）。
///
/// Body: {"providerId": "p1", "appType": "claude"}
///
/// Response: 成功时直接返回 `StreamCheckResult` JSON 字段；
///           失败时返回 {"success": false, "error": "..."}。
pub async fn test_provider(
    AxumState(state): AxumState<Arc<RemoteState>>,
    Json(body): Json<TestProviderRequest>,
) -> impl IntoResponse {
    use std::str::FromStr;

    if !state.running.load(Ordering::SeqCst) {
        return (
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"success": false, "error": "Server is stopped"})),
        )
            .into_response();
    }

    if body.provider_id.is_empty() {
        return Json(json!({"success": false, "error": "Missing provider_id"})).into_response();
    }

    let app_type = match AppType::from_str(&body.app_type) {
        Ok(t) => t,
        Err(_) => {
            return Json(
                json!({"success": false, "error": format!("无效的应用类型: {}", body.app_type)}),
            )
            .into_response();
        }
    };

    let app_state = app_state(&state);
    let copilot_state = state.app_handle.state::<CopilotAuthState>();

    // 与 Tauri 命令 stream_check_provider 完全相同的逻辑路径
    let config = match app_state.db.get_stream_check_config() {
        Ok(c) => c,
        Err(e) => {
            return Json(json!({"success": false, "error": e.to_string()})).into_response();
        }
    };

    let providers = match app_state.db.get_all_providers(app_type.as_str()) {
        Ok(p) => p,
        Err(e) => {
            return Json(json!({"success": false, "error": e.to_string()})).into_response();
        }
    };

    let provider = match providers.get(&body.provider_id) {
        Some(p) => p,
        None => {
            return Json(
                json!({"success": false, "error": format!("供应商 {} 不存在", body.provider_id)}),
            )
            .into_response();
        }
    };

    let auth_override =
        match commands::resolve_copilot_auth_override(provider, copilot_state.inner()).await {
            Ok(a) => a,
            Err(e) => {
                return Json(json!({"success": false, "error": e.to_string()})).into_response();
            }
        };

    let base_url_override =
        match commands::resolve_copilot_base_url_override(provider, copilot_state.inner()).await {
            Ok(b) => b,
            Err(e) => {
                return Json(json!({"success": false, "error": e.to_string()})).into_response();
            }
        };

    let claude_api_format_override = match commands::resolve_claude_api_format_override(
        &app_type,
        provider,
        &config,
        copilot_state.inner(),
        auth_override.as_ref(),
    )
    .await
    {
        Ok(c) => c,
        Err(e) => {
            return Json(json!({"success": false, "error": e.to_string()})).into_response();
        }
    };

    let result = match crate::services::stream_check::StreamCheckService::check_with_retry(
        &app_type,
        provider,
        &config,
        auth_override,
        base_url_override,
        claude_api_format_override,
    )
    .await
    {
        Ok(r) => r,
        Err(e) => {
            return Json(json!({"success": false, "error": e.to_string()})).into_response();
        }
    };

    // 复用 Tauri 命令的 stream_check_log 记录
    let _ = app_state.db.save_stream_check_log(
        &body.provider_id,
        &provider.name,
        app_type.as_str(),
        &result,
    );

    let status_str = serde_json::to_string(&result.status)
        .unwrap_or_else(|_| "\"unknown\"".to_string())
        .trim_matches('"')
        .to_string();

    log::info!(
        "[Remote] /api/provider/test {} {} → {} ({}ms)",
        app_type.as_str(),
        body.provider_id,
        status_str,
        result.response_time_ms.unwrap_or(0)
    );

    Json(json!({
        "success": true,
        "status": status_str,
        "operational": result.success,
        "message": result.message,
        "response_time_ms": result.response_time_ms,
        "http_status": result.http_status,
        "model_used": result.model_used,
        "tested_at": result.tested_at,
        "retry_count": result.retry_count,
        "error_category": result.error_category,
    }))
    .into_response()
}
