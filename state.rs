use crate::{logger::LogBuffer, proxy::ProxyPool};
use std::sync::Arc;

#[derive(Clone)]
pub struct AppState {
    pub pool: ProxyPool,
    pub log_buffer: Arc<LogBuffer>,
}