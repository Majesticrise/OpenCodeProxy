use crate::{config::RoutingConfig, state::AppState};
use axum::{extract::State, response::IntoResponse, Json};
use serde_json::json;
use tracing::info;

pub async fn api_get_routing(State(state): State<AppState>) -> impl IntoResponse {
    let routing = state.routing.read().unwrap().clone();
    Json(json!(routing))
}

pub async fn api_set_routing(
    State(state): State<AppState>,
    Json(new_routing): Json<RoutingConfig>,
) -> impl IntoResponse {
    let mut guard = state.routing.write().unwrap();
    let before_map = guard.mapping.clone();
    let before_fb = guard.enable_fallback_chain;
    *guard = new_routing;
    info!(
        "Routing config updated: {} entries (fallback={}), was {} entries (fallback={})",
        guard.mapping.len(), guard.enable_fallback_chain, before_map.len(), before_fb,
    );
    Json(json!({ "ok": true, "mapping": guard.mapping }))
}