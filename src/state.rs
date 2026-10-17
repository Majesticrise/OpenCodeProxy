use crate::{logger::LogBuffer, metrics::MetricsStore, proxy::ProxyPool};
use std::sync::Arc;

#[derive(Clone)]
pub struct AppState {
    pub pool: ProxyPool,
    pub log_buffer: Arc<LogBuffer>,
    pub metrics: Arc<MetricsStore>,
}