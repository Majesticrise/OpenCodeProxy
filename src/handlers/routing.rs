use crate::{config::{RoutingConfig, ROUTING_FILE}, state::AppState};
use axum::{extract::State, response::IntoResponse, Json};
use serde_json::json;
use tracing::{info, warn};

pub async fn api_get_routing(State(state): State<AppState>) -> impl IntoResponse {
    let routing = state.routing.read().unwrap().clone();
    Json(json!(routing))
}

pub async fn api_set_routing(
    State(state): State<AppState>,
    Json(new_routing): Json<RoutingConfig>,
) -> impl IntoResponse {
    // 写内存
    let (snapshot, before_map_len, before_fb) = {
        let mut guard = state.routing.write().unwrap();
        let before = (guard.mapping.len(), guard.enable_fallback_chain);
        *guard = new_routing;
        (guard.clone(), before.0, before.1)
    };

    // 异步落盘：失败只 warn，不影响响应
    let path = ROUTING_FILE.clone();
    tokio::spawn(async move {
        match serde_json::to_string_pretty(&snapshot) {
            Ok(json_str) => {
                if let Err(e) = tokio::fs::write(&path, json_str).await {
                    warn!("Failed to persist routing config: {}", e);
                }
            }
            Err(e) => warn!("Failed to serialize routing config: {}", e),
        }
    });

    info!(
        "Routing config updated: {} entries (fallback={}), was {} entries (fallback={})",
        snapshot.mapping.len(),
        snapshot.enable_fallback_chain,
        before_map_len,
        before_fb,
    );
    Json(json!({ "ok": true, "mapping": snapshot.mapping }))
}