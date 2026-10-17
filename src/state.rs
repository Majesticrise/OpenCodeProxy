use crate::{
    config::RoutingConfig,
    logger::LogBuffer,
    metrics::MetricsStore,
    proxy::ProxyPool,
};
use dashmap::DashMap;
use std::{
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
    pub client_cache: Arc<DashMap<Option<String>, reqwest::Client>>,
    /// 运行时路由配置（热更新）
    pub routing: Arc<RwLock<RoutingConfig>>,
    /// 上游模型缓存
    pub upstream_models: Arc<RwLock<UpstreamCache>>,
}