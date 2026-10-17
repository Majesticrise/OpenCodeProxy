use std::env;

lazy_static::lazy_static! {
    pub static ref ZEN_UPSTREAM: String = env::var("ZEN_UPSTREAM").unwrap_or_else(|_| "https://opencode.ai/zen/v1".to_string());
    pub static ref LISTEN_HOST: String = env::var("LISTEN_HOST").unwrap_or_else(|_| "0.0.0.0".to_string());
    pub static ref LISTEN_PORT: u16 = env::var("LISTEN_PORT").unwrap_or_else(|_| "8910".to_string()).parse().unwrap_or(8910);
    pub static ref LOG_LEVEL: String = env::var("LOG_LEVEL").unwrap_or_else(|_| "info".to_string());
    pub static ref PROXY_FILE: String = env::var("PROXY_FILE").unwrap_or_else(|_| "proxies.txt".to_string());
}