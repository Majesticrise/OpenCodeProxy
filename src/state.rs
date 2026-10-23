use crate::{
    config::RoutingConfig,
    logger::LogBuffer,
    metrics::MetricsStore,
    proxy::ProxyPool,
    settings::RuntimeSettings,
};
use dashmap::DashMap;
use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
    time::Instant,
};

#[derive(Clone, Debug, Default)]
pub struct UpstreamCache {
    pub models: Vec<String>,
    pub fetched_at: Option<Instant>,
    pub last_error: Option<String>,
    pub source: Option<String>,
}

#[derive(Clone)]
pub struct AppState {
    pub pool: ProxyPool,
    pub log_buffer: Arc<LogBuffer>,
    pub metrics: Arc<MetricsStore>,
    pub client_cache: Arc<DashMap<Option<String>, wreq::Client>>,
    pub routing: Arc<RwLock<RoutingConfig>>,
    pub upstream_models: Arc<RwLock<UpstreamCache>>,
    /// session → 代理绑定。
    /// - `Some(Some(url))`：该 session 上次走 url 成功
    /// - `Some(None)`：该 session 上次走直连成功
    /// - 未插入：未绑定，走随机选代理
    pub session_proxy_binding: Arc<RwLock<HashMap<String, Option<String>>>>,
    /// 运行时设置（匿名/实名模式 + API Key）
    pub settings: Arc<RwLock<RuntimeSettings>>,
}