use crate::{db::now_secs, state::AppState};
use axum::{
    extract::{Query, State},
    response::IntoResponse,
    Json,
};
use serde::Deserialize;
use serde_json::json;
use std::time::Instant;

/// 代理池详情：信誉（成功/失败/风控次数）、IP 情报（国家/ISP/类型/评分）、冷却状态。
pub async fn api_proxies(State(state): State<AppState>) -> impl IntoResponse {
    let inner = state.pool.inner.read().await;
    let now = Instant::now();
    let proxies: Vec<_> = inner
        .proxies
        .iter()
        .map(|p| {
            json!({
                "url": p.url,
                "latency": p.latency,
                "country": p.info.country,
                "country_code": p.info.country_code,
                "isp": p.info.isp,
                "asn": p.info.asn,
                "ip_type": p.info.ip_type,
                "risk": p.info.risk,
                "hosting": p.info.hosting,
                "success_count": p.success_count,
                "fail_count": p.fail_count,
                "ban_count": p.ban_count,
                "available": p.available(now),
                "cooldown_remaining_secs": p
                    .cooldown_until
                    .map(|t| t.saturating_duration_since(now).as_secs()),
                "last_used": p.last_used,
            })
        })
        .collect();
    Json(json!({ "count": proxies.len(), "proxies": proxies }))
}

#[derive(Deserialize)]
pub struct UsageQuery {
    #[serde(default = "default_days")]
    pub days: i64,
}

fn default_days() -> i64 {
    14
}

/// 用量报表：按天 / 按模型聚合（token 计量、成功率、平均延迟与 TTFT）。
pub async fn api_usage(
    State(state): State<AppState>,
    Query(q): Query<UsageQuery>,
) -> impl IntoResponse {
    let days = q.days.clamp(1, 365);
    let since = now_secs() - days * 86400;

    let daily = state.db.usage_daily(since);
    let models: Vec<_> = state
        .db
        .usage_models(since)
        .iter()
        .map(|m| {
            json!({
                "model": m.model,
                "requests": m.requests,
                "success": m.success,
                "prompt_tokens": m.prompt_tokens,
                "completion_tokens": m.completion_tokens,
                "total_tokens": m.total_tokens,
                "avg_latency_ms": m.avg_latency_ms,
                "avg_ttft_ms": m.avg_ttft_ms,
            })
        })
        .collect();

    Json(json!({ "days": days, "daily": daily, "models": models }))
}
