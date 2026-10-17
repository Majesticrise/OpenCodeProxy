use axum::{
    body::Body,
    extract::{Extension, Request, State},
    http::{HeaderMap, HeaderValue, StatusCode, Uri},
    middleware::{self, Next},
    response::{Html, IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use futures_util::StreamExt;
use rand::seq::SliceRandom;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::{
    collections::{HashSet, VecDeque},
    convert::Infallible,
    env,
    io::Write,
    path::Path,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};
use tokio::sync::RwLock;
use tracing::{error, info, warn};
use tracing_subscriber::fmt::MakeWriter;

// ── 配置文件与环境变量 ────────────────────────────────────────────────
lazy_static::lazy_static! {
    static ref ZEN_UPSTREAM: String = env::var("ZEN_UPSTREAM").unwrap_or_else(|_| "https://opencode.ai/zen/v1".to_string());
    static ref LISTEN_HOST: String = env::var("LISTEN_HOST").unwrap_or_else(|_| "0.0.0.0".to_string());
    static ref LISTEN_PORT: u16 = env::var("LISTEN_PORT").unwrap_or_else(|_| "8910".to_string()).parse().unwrap_or(8910);
    static ref LOG_LEVEL: String = env::var("LOG_LEVEL").unwrap_or_else(|_| "info".to_string());
    static ref PROXY_FILE: String = env::var("PROXY_FILE").unwrap_or_else(|_| "proxies.txt".to_string());
}

#[derive(Serialize, Deserialize, Clone)]
struct ModelInfo {
    id: String,
    name: String,
    owned_by: String,
}

// ── 日志缓冲区 ────────────────────────────────────────────────────────
struct LogBuffer {
    logs: Mutex<VecDeque<String>>,
}

impl LogBuffer {
    fn new() -> Self {
        Self { logs: Mutex::new(VecDeque::with_capacity(500)) }
    }
    fn push(&self, msg: String) {
        let mut logs = self.logs.lock().unwrap();
        if logs.len() >= 500 {
            logs.pop_front();
        }
        logs.push_back(msg);
    }
    fn get_all(&self) -> Vec<String> {
        self.logs.lock().unwrap().iter().cloned().collect()
    }
}

struct WebLogWriter {
    buffer: Arc<LogBuffer>,
}

impl<'a> MakeWriter<'a> for WebLogWriter {
    type Writer = WebLogWriterGuard;
    fn make_writer(&'a self) -> Self::Writer {
        WebLogWriterGuard { buffer: self.buffer.clone() }
    }
}

struct WebLogWriterGuard {
    buffer: Arc<LogBuffer>,
}

impl Write for WebLogWriterGuard {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let msg = String::from_utf8_lossy(buf).trim_end().to_string();
        if !msg.is_empty() {
            println!("{}", msg);
            self.buffer.push(msg);
        }
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

// ── 代理池实现 ────────────────────────────────────────────────────────
struct ProxyPoolInner {
    proxies: Vec<(String, f64)>,
    is_fetching: bool,
    file_path: String,
    is_running: AtomicBool,
    total_requests: AtomicU64,
}

#[derive(Clone)]
struct ProxyPool {
    inner: Arc<RwLock<ProxyPoolInner>>,
}

impl ProxyPool {
    fn new(file_path: &str) -> Self {
        Self {
            inner: Arc::new(RwLock::new(ProxyPoolInner {
                proxies: Vec::new(),
                is_fetching: false,
                file_path: file_path.to_string(),
                is_running: AtomicBool::new(true),
                total_requests: AtomicU64::new(0),
            })),
        }
    }

    async fn load_proxies(&self) {
        let file_path = { self.inner.read().await.file_path.clone() };
        if Path::new(&file_path).exists() {
            match tokio::fs::read_to_string(&file_path).await {
                Ok(content) => {
                    let mut file_proxies = Vec::new();
                    for line in content.lines() {
                        let line = line.trim();
                        if line.is_empty() || line.starts_with('#') {
                            continue;
                        }
                        let proxy_url = if !(line.starts_with("http://")
                            || line.starts_with("https://")
                            || line.starts_with("socks5://"))
                        {
                            format!("http://{}", line)
                        } else {
                            line.to_string()
                        };
                        file_proxies.push((proxy_url, 0.0));
                    }
                    if !file_proxies.is_empty() {
                        let mut inner = self.inner.write().await;
                        inner.proxies = file_proxies;
                        info!("Loaded {} proxies from local file {}", inner.proxies.len(), file_path);
                        return;
                    }
                }
                Err(e) => error!("Failed to read local proxies file: {}", e),
            }
        }
        self.fetch_free_proxies().await;
    }

    async fn fetch_free_proxies(&self) {
        if self.inner.read().await.is_fetching {
            return;
        }
        self.inner.write().await.is_fetching = true;

        let self_clone = self.clone();
        tokio::spawn(async move {
            info!("Fetching free proxy list from multiple public APIs...");
            let sources = vec![
                // ── HTTP/HTTPS 代理 ──
                "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/http.txt",
                "https://raw.githubusercontent.com/TheSpeedX/SOCKS-List/master/http.txt",
                "https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/http.txt",
                "https://raw.githubusercontent.com/clarketm/proxy-list/master/proxy-list-raw.txt",
                "https://raw.githubusercontent.com/proxifly/free-proxy-list/main/proxies/protocols/http/data.txt",
                // ── SOCKS4 代理 ──
                "https://raw.githubusercontent.com/TheSpeedX/SOCKS-List/master/socks4.txt",
                "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/socks4.txt",
                // ── SOCKS5 代理（推荐，延迟更低） ──
                "https://raw.githubusercontent.com/TheSpeedX/SOCKS-List/master/socks5.txt",
                "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/socks5.txt",
                "https://raw.githubusercontent.com/hookzof/socks5_list/master/proxy.txt",
                // ── ProxyScrape 接口 ──
                "https://api.proxyscrape.com/v2/?request=displayproxies&protocol=http&timeout=5000&country=all&ssl=all&anonymity=all",
                "https://api.proxyscrape.com/v2/?request=displayproxies&protocol=socks5&timeout=5000&country=all&ssl=all&anonymity=all",
            ];

            let mut raw_ips = HashSet::new();
            let client = reqwest::Client::builder()
                .timeout(Duration::from_secs(8))
                .build()
                .unwrap();

            for url in sources {
                match client.get(url).send().await {
                    Ok(resp) => {
                        if resp.status() == 200 {
                            if let Ok(text) = resp.text().await {
                                for line in text.lines() {
                                    let line = line.trim();
                                    if !line.is_empty() {
                                        raw_ips.insert(line.to_string());
                                    }
                                }
                            }
                        }
                    }
                    Err(e) => warn!("Failed to fetch from source {}: {}", url, e),
                }
            }

            if raw_ips.is_empty() {
                error!("All public proxy sources failed or returned empty list.");
                self_clone.inner.write().await.is_fetching = false;
                return;
            }

            let raw_proxies: Vec<String> = raw_ips
                .into_iter()
                .map(|ip| format!("http://{}", ip))
                .collect();
            info!(
                "Fetched {} unique raw proxies. Starting background validation...",
                raw_proxies.len()
            );
            self_clone.validate_proxies(raw_proxies).await;
        });
    }

    async fn validate_proxies(&self, raw_proxies: Vec<String>) {
        let sample_size = std::cmp::min(3000, raw_proxies.len());
        let mut test_targets = raw_proxies;
        test_targets.shuffle(&mut rand::thread_rng());
        test_targets.truncate(sample_size);

        let mut tasks = Vec::new();
        let sem = Arc::new(tokio::sync::Semaphore::new(200));

        for proxy in test_targets {
            let sem_clone = sem.clone();
            tasks.push(tokio::spawn(async move {
                let _permit = sem_clone.acquire().await.unwrap();
                let start = Instant::now();
                let client = reqwest::Client::builder()
                    .proxy(reqwest::Proxy::all(&proxy).ok()?)
                    .timeout(Duration::from_millis(2500))
                    .build()
                    .ok()?;
                match client.get("https://opencode.ai").send().await {
                    Ok(resp) if resp.status() == 200 => {
                        let elapsed = start.elapsed().as_secs_f64();
                        Some((proxy, elapsed))
                    }
                    _ => None,
                }
            }));
        }

        let mut validated = Vec::new();
        for task in tasks {
            if let Ok(Some((proxy, elapsed))) = task.await {
                validated.push((proxy, elapsed));
            }
        }
        validated.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
        validated.truncate(100);

        let mut inner = self.inner.write().await;
        inner.proxies = validated.clone();
        inner.is_fetching = false;

        if !validated.is_empty() {
            info!(
                "Proxy pool updated: {} high-speed verified proxies ready. (Fastest: {:.2}s)",
                validated.len(),
                validated[0].1
            );
        } else {
            warn!("No proxies passed the high-speed check (under 2.5s). Pool empty.");
        }
    }

    async fn get_proxy(&self) -> Option<String> {
        let inner = self.inner.read().await;
        if inner.proxies.is_empty() {
            return None;
        }
        let limit = std::cmp::min(15, inner.proxies.len());
        let slice = &inner.proxies[..limit];
        slice.choose(&mut rand::thread_rng()).map(|x| x.0.clone())
    }

    async fn report_failure(&self, proxy_url: &str) {
        let mut inner = self.inner.write().await;
        let before_len = inner.proxies.len();
        inner.proxies.retain(|x| x.0 != proxy_url);
        let after_len = inner.proxies.len();
        if before_len != after_len {
            warn!(
                "Removed failed proxy: {}. Active pool size: {}",
                proxy_url, after_len
            );
        }
        if after_len < 10 && !inner.is_fetching {
            let self_clone = self.clone();
            tokio::spawn(async move {
                self_clone.fetch_free_proxies().await;
            });
        }
    }
}

// ── Web 路由处理 ──────────────────────────────────────────────────────
async fn root() -> impl IntoResponse {
    Html(include_str!("../web/index.html"))
}

async fn static_handler(uri: Uri) -> impl IntoResponse {
    let path = uri.path().trim_start_matches("/static/");
    match path {
        "style.css" => (
            [(axum::http::header::CONTENT_TYPE, "text/css")],
            include_str!("../web/style.css"),
        )
            .into_response(),
        "app.js" => (
            [(axum::http::header::CONTENT_TYPE, "application/javascript")],
            include_str!("../web/app.js"),
        )
            .into_response(),
        _ => (StatusCode::NOT_FOUND, "Not Found").into_response(),
    }
}

async fn api_status(State(pool): State<ProxyPool>) -> impl IntoResponse {
    let inner = pool.inner.read().await;
    let pool_size = inner.proxies.len();
    let running = inner.is_running.load(Ordering::Relaxed);
    let avg_latency = if pool_size > 0 {
        inner.proxies.iter().map(|p| p.1).sum::<f64>() / pool_size as f64
    } else {
        0.0
    };
    Json(json!({
        "running": running,
        "pool_size": pool_size,
        "fetching": inner.is_fetching,
        "total_requests": inner.total_requests.load(Ordering::Relaxed),
        "avg_latency": avg_latency
    }))
}

async fn api_logs(Extension(log_buf): Extension<Arc<LogBuffer>>) -> impl IntoResponse {
    Json(json!(log_buf.get_all()))
}

async fn api_toggle(State(pool): State<ProxyPool>) -> impl IntoResponse {
    let inner = pool.inner.read().await;
    let current = inner.is_running.load(Ordering::Relaxed);
    inner.is_running.store(!current, Ordering::Relaxed);
    info!(
        "Proxy service toggled: {}",
        if !current { "RUNNING" } else { "STOPPED" }
    );
    Json(json!({ "running": !current }))
}

async fn health(State(pool): State<ProxyPool>) -> impl IntoResponse {
    let size = pool.inner.read().await.proxies.len();
    Json(json!({ "status": "ok", "proxies": size }))
}

async fn list_models() -> impl IntoResponse {
    let models = vec![
        json!({"id": "deepseek-v4-flash-free", "object": "model", "owned_by": "deepseek"}),
        json!({"id": "big-pickle", "object": "model", "owned_by": "opencode"}),
        json!({"id": "mimo-v2.5-free", "object": "model", "owned_by": "xiaomi"}),
        json!({"id": "nemotron-3-super-free", "object": "model", "owned_by": "nvidia"}),
        json!({"id": "minimax-m3-free", "object": "model", "owned_by": "minimax"}),
    ];
    Json(json!({
        "object": "list",
        "data": models
    }))
}

// ── 核心转发逻辑 ──────────────────────────────────────────────────────
async fn chat_completions(
    State(pool): State<ProxyPool>,
    body_bytes: bytes::Bytes,
) -> impl IntoResponse {
    if !pool.inner.read().await.is_running.load(Ordering::Relaxed) {
        return (StatusCode::SERVICE_UNAVAILABLE, "Proxy service is stopped by user.")
            .into_response();
    }

    let body_json: serde_json::Value = match serde_json::from_slice(&body_bytes) {
        Ok(json) => json,
        Err(e) => {
            return (StatusCode::BAD_REQUEST, format!("Invalid JSON: {}", e)).into_response()
        }
    };

    let model = body_json
        .get("model")
        .and_then(|m| m.as_str())
        .unwrap_or("deepseek-v4-flash-free");
    let is_stream = body_json
        .get("stream")
        .and_then(|s| s.as_bool())
        .unwrap_or(false);
    info!(
        "→ {} model={} messages",
        if is_stream { "STREAM" } else { "SYNC  " },
        model
    );

    pool.inner
        .read()
        .await
        .total_requests
        .fetch_add(1, Ordering::Relaxed);

    let mut forward_body = body_json.clone();
    if let Some(obj) = forward_body.as_object_mut() {
        obj.remove("variant");
    }

    let max_retries = 5;
    let mut target_resp: Option<reqwest::Response> = None;
    let mut last_proxy: Option<String> = None;

    for attempt in 0..max_retries {
        let proxy = pool.get_proxy().await;
        last_proxy = proxy.clone();
        info!(
            "Chat Completion attempt {}/{} using proxy: {:?}",
            attempt + 1,
            max_retries,
            proxy
        );

        let mut client_builder = reqwest::Client::builder()
            .timeout(Duration::from_secs(300))
            .connect_timeout(Duration::from_millis(2500));
        if let Some(ref p) = proxy {
            if let Ok(rp) = reqwest::Proxy::all(p) {
                client_builder = client_builder.proxy(rp);
            }
        }

        let client = match client_builder.build() {
            Ok(c) => c,
            Err(e) => {
                error!("Failed to build reqwest client: {}", e);
                continue;
            }
        };

        let target_url = format!("{}/chat/completions", *ZEN_UPSTREAM);
        match client
            .post(&target_url)
            .header("Content-Type", "application/json")
            .json(&forward_body)
            .send()
            .await
        {
            Ok(resp) if resp.status().as_u16() == 429 => {
                warn!("Proxy {:?} hit 429. Retrying...", proxy);
                if let Some(ref p) = proxy {
                    pool.report_failure(p).await;
                }
                continue;
            }
            Ok(resp) => {
                target_resp = Some(resp);
                break;
            }
            Err(e) => {
                warn!("Proxy {:?} connection failed: {}. Retrying...", proxy, e);
                if let Some(ref p) = proxy {
                    pool.report_failure(p).await;
                }
                continue;
            }
        }
    }

    let resp = match target_resp {
        Some(r) => r,
        None => {
            warn!("All proxies failed. Falling back to direct connection.");
            let client = match reqwest::Client::builder()
                .connect_timeout(Duration::from_millis(4000))
                .timeout(Duration::from_secs(300))
                .build()
            {
                Ok(c) => c,
                Err(e) => {
                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("Failed to create client: {}", e),
                    )
                        .into_response()
                }
            };

            let target_url = format!("{}/chat/completions", *ZEN_UPSTREAM);
            match client
                .post(&target_url)
                .header("Content-Type", "application/json")
                .json(&forward_body)
                .send()
                .await
            {
                Ok(r) => r,
                Err(e) => {
                    return (
                        StatusCode::BAD_GATEWAY,
                        format!("Upstream direct connection failed: {}", e),
                    )
                        .into_response()
                }
            }
        }
    };

    let status = StatusCode::from_u16(resp.status().as_u16()).unwrap_or(StatusCode::OK);
    let mut headers = HeaderMap::new();
    for (key, value) in resp.headers().iter() {
        let name = key.as_str().to_lowercase();
        if name == "content-encoding" || name == "content-length" || name == "transfer-encoding" {
            continue;
        }
        if let Ok(val) = HeaderValue::from_bytes(value.as_bytes()) {
            if let Ok(header_name) =
                axum::http::header::HeaderName::from_bytes(key.as_str().as_bytes())
            {
                headers.insert(header_name, val);
            }
        }
    }

    info!("  ← status={} (routed via: {:?})", status, last_proxy);
    let stream = resp
        .bytes_stream()
        .map(|result| result.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e)));
    let mut response = Response::new(Body::from_stream(stream));
    *response.status_mut() = status;
    *response.headers_mut() = headers;
    response
}

async fn strip_auth_middleware(mut request: Request, next: Next) -> Result<Response, Infallible> {
    request.headers_mut().remove("authorization");
    Ok(next.run(request).await)
}

// ── 主入口 ──────────────────────────────────────────────────────────
#[tokio::main]
async fn main() {
    let log_buffer = Arc::new(LogBuffer::new());
    let filter = env::var("RUST_LOG").unwrap_or_else(|_| LOG_LEVEL.to_string());

    // 初始化 tracing，将日志同时输出到终端和 Web 端
    let subscriber = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::new(filter))
        .with_writer(WebLogWriter {
            buffer: log_buffer.clone(),
        })
        .finish();
    tracing::subscriber::set_global_default(subscriber).unwrap();

    info!("============================================================");
    info!("Zen Proxy (Rust version v1.2.6 - WebUI Integrated)");
    info!("============================================================");

    let proxy_pool = ProxyPool::new(&PROXY_FILE);
    let pool_clone = proxy_pool.clone();
    tokio::spawn(async move {
        pool_clone.load_proxies().await;
    });

    let pool_refresh = proxy_pool.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(3600)).await;
            info!("Periodic proxy pool refresh triggered (1-hour interval)...");
            if !Path::new(&*PROXY_FILE).exists() {
                pool_refresh.fetch_free_proxies().await;
            } else {
                info!("Local proxies file is active. Skipping auto public refresh.");
            }
        }
    });

    let app = Router::new()
        .route("/", get(root))
        .route("/static/*path", get(static_handler))
        .route("/health", get(health))
        .route("/api/status", get(api_status))
        .route("/api/logs", get(api_logs))
        .route("/api/toggle", post(api_toggle))
        .route("/v1/models", get(list_models))
        .route("/v1/chat/completions", post(chat_completions))
        .layer(middleware::from_fn(strip_auth_middleware))
        .with_state(proxy_pool) // 状态1：代理池 (State)
        .layer(axum::Extension(log_buffer)); // 状态2：日志缓冲区 (Extension)

    let addr = format!("{}:{}", *LISTEN_HOST, *LISTEN_PORT);
    info!("Listen:    http://{}", addr);
    info!("WebUI:     http://{}/", addr);
    info!("Upstream:  {}", *ZEN_UPSTREAM);
    info!("============================================================");

    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
