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
    /// Client 缓存：key 为代理 URL（None 表示直连），value 为带 impersonate 的 rquest Client
    pub client_cache: Arc<DashMap<Option<String>, rquest::Client>>,
    pub routing: Arc<RwLock<RoutingConfig>>,
    pub upstream_models: Arc<RwLock<UpstreamCache>>,
}