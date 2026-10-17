use crate::{config::RoutingConfig, state::AppState};
use axum::{extract::State, response::IntoResponse, Json};
use serde_json::json;

pub async fn list_models(State(state): State<AppState>) -> impl IntoResponse {
    let routing = state.routing.read().unwrap().clone();
    if !routing.mapping.is_empty() {
        let mut models: Vec<_> = routing.mapping.keys()
            .filter(|k| k.as_str() != "*")
            .map(|id| {
                let candidates = routing.mapping.get(id).cloned().unwrap_or_default();
                json!({ "id": id, "object": "model", "owned_by": "zen-proxy",
                        "upstream_candidates": candidates })
            })
            .collect();
        if !models.is_empty() {
            if routing.mapping.contains_key("*") {
                models.push(json!({
                    "id": "*", "object": "model", "owned_by": "zen-proxy",
                    "upstream_candidates": routing.mapping.get("*").cloned().unwrap_or_default(),
                }));
            }
            return Json(json!({ "object": "list", "data": models }));
        }
    }

    let models = vec![
        json!({"id": "deepseek-v4-flash-free", "object": "model", "owned_by": "deepseek"}),
        json!({"id": "big-pickle", "object": "model", "owned_by": "opencode"}),
        json!({"id": "mimo-v2.5-free", "object": "model", "owned_by": "xiaomi"}),
        json!({"id": "nemotron-3-super-free", "object": "model", "owned_by": "nvidia"}),
        json!({"id": "minimax-m3-free", "object": "model", "owned_by": "minimax"}),
    ];
    Json(json!({ "object": "list", "data": models }))
}