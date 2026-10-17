use crate::{logger::LogBuffer, metrics::MetricsStore, proxy::ProxyPool};
use dashmap::DashMap;
use std::sync::Arc;

#[derive(Clone)]
pub struct AppState {
    pub pool: ProxyPool,
    pub log_buffer: Arc<LogBuffer>,
    pub metrics: Arc<MetricsStore>,
    /// 按代理 URL 缓存 reqwest::Client（None 表示直连 client）
    /// 复用连接池，避免每次请求重复 TLS 握手
    pub client_cache: Arc<DashMap<Option<String>, reqwest::Client>>,
}