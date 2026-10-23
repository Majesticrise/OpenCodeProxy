use crate::{settings::Mode, state::AppState};
use axum::{extract::State, response::IntoResponse, Json};
use serde::Deserialize;
use serde_json::json;
use tracing::info;

pub async fn api_get_settings(State(state): State<AppState>) -> impl IntoResponse {
    let s = state.settings.read().unwrap().clone();
    Json(json!({
        "mode": s.mode,
        "has_api_key": s.api_key.is_some(),
        "api_key_masked": s.masked_api_key(),
    }))
}

#[derive(Deserialize)]
pub struct SetModeBody {
    pub mode: Mode,
}

pub async fn api_set_mode(
    State(state): State<AppState>,
    Json(body): Json<SetModeBody>,
) -> impl IntoResponse {
    let snapshot = {
        let mut s = state.settings.write().unwrap();
        let before = s.mode.clone();
        s.mode = body.mode.clone();
        let snapshot = s.clone();
        drop(s);
        info!("Mode changed: {:?} -> {:?}", before, body.mode);
        snapshot
    };

    if let Err(e) = snapshot.save() {
        tracing::warn!("Failed to persist settings: {}", e);
        return Json(json!({ "ok": false, "error": e.to_string() }));
    }
    Json(json!({ "ok": true, "mode": body.mode }))
}

#[derive(Deserialize)]
pub struct SetApiKeyBody {
    /// `Some("sk-...")` 设置 / 覆盖；`Some("")` 或 `None` 清空
    pub api_key: Option<String>,
}

pub async fn api_set_api_key(
    State(state): State<AppState>,
    Json(body): Json<SetApiKeyBody>,
) -> impl IntoResponse {
    let snapshot = {
        let mut s = state.settings.write().unwrap();
        s.api_key = body.api_key.filter(|k| !k.trim().is_empty());
        s.clone()
    };

    if let Err(e) = snapshot.save() {
        tracing::warn!("Failed to persist settings: {}", e);
        return Json(json!({ "ok": false, "error": e.to_string() }));
    }
    info!(
        "API key updated: {}",
        if snapshot.api_key.is_some() { "set" } else { "cleared" }
    );
    Json(json!({ "ok": true, "has_api_key": snapshot.api_key.is_some() }))
}