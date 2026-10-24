use crate::{db::now_secs, proxy::ProxyFilters, state::AppState};
use axum::{
    extract::{Query, State},
    response::IntoResponse,
    Json,
};
use serde::Deserialize;
use serde_json::json;
use std::time::Instant;
use tracing::info;

/// 代理池详情：信誉（成功/失败/风控次数）、IP 情报（国家/ISP/类型/评分）、冷却状态。
pub async fn api_proxies(State(state): State<AppState>) -> impl IntoResponse {
    let inner = state.pool.inner.read().await;
    let now = Instant::now();
    let filters = state.pool.filters.read().unwrap().clone();
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
    Json(json!({
        "count": proxies.len(),
        "proxies": proxies,
        "filters": {
            "countries": filters.country_whitelist,
            "max_risk": filters.max_risk,
        }
    }))
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

    let by_proxy: Vec<_> = state
        .db
        .usage_proxies(since)
        .iter()
        .map(|p| {
            json!({
                "proxy": p.proxy_url,
                "requests": p.requests,
                "success": p.success,
                "total_tokens": p.total_tokens,
                "avg_latency_ms": p.avg_latency_ms,
                "avg_ttft_ms": p.avg_ttft_ms,
            })
        })
        .collect();

    Json(json!({ "days": days, "daily": daily, "models": models, "by_proxy": by_proxy }))
}

/// 上游健康探测汇总（近 1h / 24h 的 ok / restricted / down 分布）。
pub async fn api_upstream_health(State(state): State<AppState>) -> impl IntoResponse {
    let now = now_secs();
    let (t1, ok1, r1, d1) = state.db.probe_summary(now - 3600);
    let (t24, ok24, r24, d24) = state.db.probe_summary(now - 86400);
    Json(json!({
        "last_1h":  { "total": t1,  "ok": ok1,  "restricted": r1,  "down": d1  },
        "last_24h": { "total": t24, "ok": ok24, "restricted": r24, "down": d24 },
    }))
}

/// 代理手动操作：ban（拉黑 24h）/ unban / remove / enrich（立即富化）。
#[derive(Deserialize)]
pub struct ProxyActionBody {
    pub action: String,
    pub url: Option<String>,
}

pub async fn api_proxy_action(
    State(state): State<AppState>,
    Json(body): Json<ProxyActionBody>,
) -> impl IntoResponse {
    match body.action.as_str() {
        "enrich" => {
            let pool = state.pool.clone();
            tokio::spawn(async move {
                pool.enrich_pending().await;
            });
            Json(json!({ "ok": true, "action": "enrich" }))
        }
        "ban" | "unban" | "remove" => {
            let Some(url) = body.url.filter(|u| !u.trim().is_empty()) else {
                return Json(json!({ "ok": false, "error": "url required" }));
            };
            let url = url.trim().to_string();
            match body.action.as_str() {
                "ban" => state.pool.manual_ban(&url).await,
                "unban" => state.pool.manual_unban(&url).await,
                _ => state.pool.manual_remove(&url).await,
            }
            Json(json!({ "ok": true, "action": body.action, "url": url }))
        }
        other => Json(json!({ "ok": false, "error": format!("unknown action: {}", other) })),
    }
}

/// 读取代理过滤规则。
pub async fn api_get_filters(State(state): State<AppState>) -> impl IntoResponse {
    let f = state.pool.filters.read().unwrap().clone();
    Json(json!({ "countries": f.country_whitelist, "max_risk": f.max_risk }))
}

/// 保存代理过滤规则：立即生效并持久化到 settings.json。
#[derive(Deserialize)]
pub struct FiltersBody {
    #[serde(default)]
    pub countries: Vec<String>,
    pub max_risk: Option<u32>,
}

pub async fn api_set_filters(
    State(state): State<AppState>,
    Json(body): Json<FiltersBody>,
) -> impl IntoResponse {
    // 规范化：去空格、转大写，只保留两位字母的合法国家码
    let countries: Vec<String> = body
        .countries
        .iter()
        .map(|c| c.trim().to_uppercase())
        .filter(|c| c.len() == 2 && c.chars().all(|ch| ch.is_ascii_alphabetic()))
        .collect();

    state.pool.set_filters(ProxyFilters {
        country_whitelist: countries.clone(),
        max_risk: body.max_risk,
    });

    // 持久化到 settings.json（与其他运行时设置同文件）
    let snapshot = {
        let mut s = state.settings.write().unwrap();
        s.proxy_country_whitelist = countries;
        s.proxy_max_risk = body.max_risk;
        s.clone()
    };
    if let Err(e) = snapshot.save() {
        return Json(json!({ "ok": false, "error": e.to_string() }));
    }
    info!(
        "Proxy filters updated: countries={:?} max_risk={:?}",
        snapshot.proxy_country_whitelist, snapshot.proxy_max_risk
    );
    Json(json!({
        "ok": true,
        "countries": snapshot.proxy_country_whitelist,
        "max_risk": snapshot.proxy_max_risk,
    }))
}
