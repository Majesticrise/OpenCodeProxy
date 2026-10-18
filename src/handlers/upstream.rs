use crate::{
    config::{OPENCODE_UA, VALIDATE_TARGET},
    state::{AppState, UpstreamCache},
};
use axum::{extract::State, response::IntoResponse, Json};
use serde_json::json;
use std::time::{Duration, Instant};
use tracing::{info, warn};

fn impersonate() -> wreq_util::Profile {
    wreq_util::Profile::Chrome131
}

async fn probe_upstream(state: &AppState) -> Result<(Vec<String>, String), String> {
    let url = VALIDATE_TARGET.clone();
    let ua = OPENCODE_UA.as_str();

    let direct = wreq::Client::builder()
        .emulation(impersonate())
        .timeout(Duration::from_secs(10))
        .build()
        .map_err(|e| e.to_string())?;
    match direct.get(&url).header("User-Agent", ua).send().await {
        Ok(r) if r.status().is_success() => {
            if let Ok(v) = r.json::<serde_json::Value>().await {
                if let Some(models) = parse_models(&v) {
                    return Ok((models, "direct".to_string()));
                }
            }
        }
        Ok(r) => warn!("Direct probe returned {}; trying proxies", r.status()),
        Err(e) => warn!("Direct probe failed: {}", e),
    }

    // 代理兜底：加 impersonate
    for _ in 0..3 {
        let Some(proxy) = state.pool.get_proxy().await else {
            break;
        };
        let mut b = wreq::Client::builder()
            .emulation(impersonate())
            .timeout(Duration::from_secs(8));
        if let Ok(rp) = wreq::Proxy::all(&proxy) {
            b = b.proxy(rp);
        }
        let Ok(c) = b.build() else {
            continue;
        };
        match c.get(&url).header("User-Agent", ua).send().await {
            Ok(r) if r.status().is_success() => {
                if let Ok(v) = r.json::<serde_json::Value>().await {
                    if let Some(models) = parse_models(&v) {
                        return Ok((models, format!("proxy:{}", proxy)));
                    }
                }
            }
            Ok(_) | Err(_) => continue,
        }
    }

    Err("all probes failed".to_string())
}

fn parse_models(v: &serde_json::Value) -> Option<Vec<String>> {
    let data = v.get("data")?.as_array()?;
    let models: Vec<String> = data
        .iter()
        .filter_map(|m| m.get("id").and_then(|i| i.as_str()).map(String::from))
        .collect();
    if models.is_empty() {
        None
    } else {
        Some(models)
    }
}

pub async fn api_upstream_models(State(state): State<AppState>) -> impl IntoResponse {
    let cache = state.upstream_models.read().unwrap().clone();
    Json(json!({
        "models": cache.models,
        "source": cache.source,
        "last_error": cache.last_error,
        "age_secs": cache.fetched_at.map(|t| t.elapsed().as_secs()),
    }))
}

pub async fn api_upstream_refresh(State(state): State<AppState>) -> impl IntoResponse {
    let result = probe_upstream(&state).await;
    let mut cache = state.upstream_models.write().unwrap();
    match result {
        Ok((models, source)) => {
            let count = models.len();
            *cache = UpstreamCache {
                models,
                fetched_at: Some(Instant::now()),
                last_error: None,
                source: Some(source.clone()),
            };
            info!("Upstream model probe ok: {} models via {}", count, source);
            Json(json!({ "ok": true, "models": cache.models, "source": source }))
        }
        Err(e) => {
            cache.last_error = Some(e.clone());
            warn!("Upstream model probe failed: {}", e);
            Json(json!({ "ok": false, "error": e }))
        }
    }
}