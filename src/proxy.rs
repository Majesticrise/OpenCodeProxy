use crate::config::{MIN_POOL_SIZE, VALIDATE_TARGET};
use futures_util::stream::{self, StreamExt};
use rand::seq::SliceRandom;
use std::{
    collections::{HashMap, HashSet},
    path::Path,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

const COOLDOWN_RATE_LIMIT: Duration = Duration::from_secs(30);
const COOLDOWN_CONNECTION: Duration = Duration::from_secs(300);
const COOLDOWN_BANNED: Duration = Duration::from_secs(900);
const MAX_FAILS_BEFORE_REMOVE: u32 = 5;
/// Banned 阈值单独设更高：403 FreeTierError 有时是 session 层面的临时拒绝，
/// 换一次 session 可能就过；攒 10 次才移除，避免代理池被瞬时误判掏空。
const MAX_BANS_BEFORE_REMOVE: u32 = 10;

/// 每验证成功多少个代理就 flush 一批到池子
const VALIDATION_BATCH_FLUSH: usize = 5;
/// 池子上限（按 latency 排序后保留前 N 个）
const MAX_POOL_SIZE: usize = 120;

fn impersonate() -> wreq_util::Profile {
    wreq_util::Profile::Chrome131
}

#[derive(Clone)]
pub struct ProxyEntry {
    pub url: String,
    pub latency: f64,
    pub cooldown_until: Option<Instant>,
    pub fail_count: u32,
    pub last_success: Option<Instant>,
    /// 累计请求成功次数。优先复用该值高的代理，避免每次盲选反复重试。
    pub success_count: u32,
}

impl ProxyEntry {
    fn new(url: String, latency: f64) -> Self {
        Self {
            url,
            latency,
            cooldown_until: None,
            fail_count: 0,
            last_success: None,
            success_count: 0,
        }
    }

    pub fn available(&self, now: Instant) -> bool {
        self.cooldown_until.map_or(true, |t| now >= t)
    }
}

#[derive(Clone, Copy)]
pub enum FailureKind {
    RateLimit,
    Connection,
    Banned,
}

impl FailureKind {
    fn cooldown(self) -> Duration {
        match self {
            FailureKind::RateLimit => COOLDOWN_RATE_LIMIT,
            FailureKind::Connection => COOLDOWN_CONNECTION,
            FailureKind::Banned => COOLDOWN_BANNED,
        }
    }

    /// 该失败类型累计 fail_count 并在达到阈值时被移除；RateLimit 不计入。
    fn max_fails_before_removal(self) -> Option<u32> {
        match self {
            FailureKind::Connection => Some(MAX_FAILS_BEFORE_REMOVE),
            FailureKind::Banned => Some(MAX_BANS_BEFORE_REMOVE),
            FailureKind::RateLimit => None,
        }
    }

    fn label(self) -> &'static str {
        match self {
            FailureKind::RateLimit => "rate-limited",
            FailureKind::Connection => "connection-error",
            FailureKind::Banned => "banned",
        }
    }
}

pub struct ProxyPoolInner {
    pub proxies: Vec<ProxyEntry>,
    pub file_path: String,
    pub is_running: AtomicBool,
    pub total_requests: AtomicU64,
}

#[derive(Clone)]
pub struct ProxyPool {
    pub inner: Arc<RwLock<ProxyPoolInner>>,
    pub is_fetching: Arc<AtomicBool>,
}

struct FetchingReset(Arc<AtomicBool>);
impl Drop for FetchingReset {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

impl ProxyPool {
    pub fn new(file_path: &str) -> Self {
        Self {
            inner: Arc::new(RwLock::new(ProxyPoolInner {
                proxies: Vec::new(),
                file_path: file_path.to_string(),
                is_running: AtomicBool::new(true),
                total_requests: AtomicU64::new(0),
            })),
            is_fetching: Arc::new(AtomicBool::new(false)),
        }
    }

    pub async fn load_proxies(&self) {
        let file_path = { self.inner.read().await.file_path.clone() };

        if Path::new(&file_path).exists() {
            if let Ok(content) = tokio::fs::read_to_string(&file_path).await {
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
                    file_proxies.push(ProxyEntry::new(proxy_url, 0.0));
                }
                if !file_proxies.is_empty() {
                    let mut inner = self.inner.write().await;
                    let n = file_proxies.len();
                    inner.proxies = file_proxies;
                    info!("Loaded {} proxies from local file {}", n, file_path);
                    return;
                }
            }
        }

        if let Ok(content) = tokio::fs::read_to_string("verified_proxies.json").await {
            if let Ok(validated) = serde_json::from_str::<Vec<(String, f64)>>(&content) {
                if !validated.is_empty() {
                    let mut inner = self.inner.write().await;
                    let n = validated.len();
                    inner.proxies = validated
                        .into_iter()
                        .map(|(url, lat)| ProxyEntry::new(url, lat))
                        .collect();
                    info!("Loaded {} proxies from verified_proxies.json", n);
                    return;
                }
            }
        }

        self.fetch_free_proxies().await;
    }

    pub async fn fetch_free_proxies(&self) {
        if self
            .is_fetching
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }

        let self_clone = self.clone();
        tokio::spawn(async move {
            let _reset = FetchingReset(self_clone.is_fetching.clone());

            info!("Fetching free proxy list from multiple public APIs...");

            let sources = vec![
                "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/http.txt",
                "https://raw.githubusercontent.com/TheSpeedX/SOCKS-List/master/http.txt",
                "https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/http.txt",
                "https://raw.githubusercontent.com/clarketm/proxy-list/master/proxy-list-raw.txt",
                "https://raw.githubusercontent.com/proxifly/free-proxy-list/main/proxies/protocols/http/data.txt",
                "https://raw.githubusercontent.com/TheSpeedX/SOCKS-List/master/socks4.txt",
                "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/socks4.txt",
                "https://raw.githubusercontent.com/TheSpeedX/SOCKS-List/master/socks5.txt",
                "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/socks5.txt",
                "https://raw.githubusercontent.com/hookzof/socks5_list/master/proxy.txt",
            ];

            let mut raw_ips = HashSet::new();
            let client = wreq::Client::builder()
                .emulation(impersonate())
                .timeout(Duration::from_secs(8))
                .build()
                .unwrap();

            for url in sources {
                match client.get(url).send().await {
                    Ok(resp) if resp.status() == 200 => {
                        if let Ok(text) = resp.text().await {
                            for line in text.lines() {
                                let line = line.trim();
                                if !line.is_empty() {
                                    raw_ips.insert(line.to_string());
                                }
                            }
                        }
                    }
                    Err(e) => warn!("Failed to fetch from source {}: {}", url, e),
                    _ => {}
                }
            }

            if raw_ips.is_empty() {
                error!("All public proxy sources failed or returned empty list.");
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
        let sample_size = std::cmp::min(7000, raw_proxies.len());
        let mut test_targets = raw_proxies;
        test_targets.shuffle(&mut rand::thread_rng());
        test_targets.truncate(sample_size);
        let total_targets = test_targets.len();

        let existing_entries: HashMap<String, ProxyEntry> = {
            let inner = self.inner.read().await;
            inner
                .proxies
                .iter()
                .cloned()
                .map(|p| (p.url.clone(), p))
                .collect()
        };

        let sem = Arc::new(tokio::sync::Semaphore::new(200));
        let target = VALIDATE_TARGET.clone();

        let mut batch: Vec<(String, f64)> = Vec::with_capacity(VALIDATION_BATCH_FLUSH);
        let mut checked = 0usize;
        let mut total_validated = 0usize;
        let mut flush_batches = 0usize;

        let mut stream = stream::iter(test_targets.into_iter().map(|proxy| {
            let sem = sem.clone();
            let target = target.clone();
            async move {
                let _permit = sem.acquire().await.unwrap();
                let start = Instant::now();
                let client = wreq::Client::builder()
                    .emulation(impersonate())
                    .proxy(wreq::Proxy::all(&proxy).ok()?)
                    .timeout(Duration::from_millis(5000))
                    .build()
                    .ok()?;
                match client.get(&target).send().await {
                    Ok(resp) if resp.status().is_success() => {
                        Some((proxy, start.elapsed().as_secs_f64()))
                    }
                    _ => None,
                }
            }
        }))
        .buffer_unordered(200);

        while let Some(result) = stream.next().await {
            checked += 1;
            if let Some(item) = result {
                total_validated += 1;
                batch.push(item);

                if batch.len() >= VALIDATION_BATCH_FLUSH {
                    let flushed =
                        self.flush_validation_batch(&mut batch, &existing_entries).await;
                    flush_batches += 1;

                    let pool_size = self.inner.read().await.proxies.len();
                    info!(
                        "Validating: {}/{} checked, {} passed, flushed {} (batch #{}, pool now {})",
                        checked,
                        total_targets,
                        total_validated,
                        flushed,
                        flush_batches,
                        pool_size
                    );
                }
            }
        }

        if !batch.is_empty() {
            let flushed = self
                .flush_validation_batch(&mut batch, &existing_entries)
                .await;
            if flushed > 0 {
                let pool_size = self.inner.read().await.proxies.len();
                info!(
                    "Validating: final partial batch flushed {} (pool now {})",
                    flushed, pool_size
                );
            }
        }

        let (pool_size, available_count, fastest) = {
            let inner = self.inner.read().await;
            let now = Instant::now();
            let avail = inner.proxies.iter().filter(|p| p.available(now)).count();
            let fastest = inner
                .proxies
                .iter()
                .filter(|p| p.latency > 0.0)
                .map(|p| p.latency)
                .fold(f64::INFINITY, f64::min);
            (inner.proxies.len(), avail, fastest)
        };

        if total_validated == 0 {
            warn!(
                "Proxy validation complete: 0/{} passed. Pool preserved ({} nodes).",
                checked, pool_size
            );
        } else {
            info!(
                "Proxy validation complete: {}/{} passed ({} flush batches), pool={} ({} available), fastest={:.2}s",
                total_validated,
                checked,
                flush_batches,
                pool_size,
                available_count,
                if fastest.is_finite() { fastest } else { 0.0 }
            );
        }

        self.persist_verified_proxies().await;
    }

    async fn flush_validation_batch(
        &self,
        batch: &mut Vec<(String, f64)>,
        existing: &HashMap<String, ProxyEntry>,
    ) -> usize {
        if batch.is_empty() {
            return 0;
        }
        let count = batch.len();

        let mut inner = self.inner.write().await;
        for (url, lat) in batch.drain(..) {
            let entry = match existing.get(&url).cloned() {
                Some(mut old) => {
                    old.latency = lat;
                    old
                }
                None => ProxyEntry::new(url, lat),
            };

            if let Some(pos) = inner.proxies.iter().position(|p| p.url == entry.url) {
                inner.proxies[pos] = entry;
            } else {
                inner.proxies.push(entry);
            }
        }

        inner.proxies.sort_by(|a, b| {
            let la = if a.latency <= 0.0 { -1.0 } else { a.latency };
            let lb = if b.latency <= 0.0 { -1.0 } else { b.latency };
            la.partial_cmp(&lb).unwrap_or(std::cmp::Ordering::Equal)
        });
        if inner.proxies.len() > MAX_POOL_SIZE {
            inner.proxies.truncate(MAX_POOL_SIZE);
        }

        count
    }

    async fn persist_verified_proxies(&self) {
        let now = Instant::now();
        let to_save: Vec<(String, f64)> = {
            let inner = self.inner.read().await;
            inner
                .proxies
                .iter()
                .filter(|p| p.available(now))
                .map(|p| (p.url.clone(), p.latency))
                .collect()
        };

        if to_save.is_empty() {
            return;
        }

        match serde_json::to_string_pretty(&to_save) {
            Ok(json_str) => {
                if let Err(e) = tokio::fs::write("verified_proxies.json", json_str).await {
                    warn!("Failed to persist proxy pool: {}", e);
                } else {
                    debug!(
                        "Proxy pool persisted to verified_proxies.json ({} nodes)",
                        to_save.len()
                    );
                }
            }
            Err(e) => warn!("Failed to serialize proxy pool: {}", e),
        }
    }

    pub async fn get_proxy(&self) -> Option<String> {
        let inner = self.inner.read().await;
        let now = Instant::now();
        let mut rng = rand::thread_rng();

        let available: Vec<&ProxyEntry> = inner
            .proxies
            .iter()
            .filter(|p| p.available(now))
            .collect();

        if available.is_empty() {
            return None;
        }

        // 「成功数优先」：只在历史成功过的节点里选；没有成功记录时，
        // 在最快的前 15 个节点中冷启动盲选。
        let proven: Vec<&ProxyEntry> = available
            .iter()
            .copied()
            .filter(|p| p.success_count > 0)
            .collect();

        if !proven.is_empty() {
            // proven 是 Vec<&ProxyEntry>，choose 返回 Option<&&ProxyEntry>，
            // p.url.clone() 通过 auto-deref 得到 String。
            return proven.choose(&mut rng).map(|p| p.url.clone());
        }

        let limit = std::cmp::min(15, available.len());
        available[..limit].choose(&mut rng).map(|p| p.url.clone())
    }
    /// 检查某个 URL 对应的代理当前是否可用（存在于池中且未处于冷却）。
    /// 用于 session → 代理绑定的可用性判定。
    pub async fn is_available(&self, proxy_url: &str) -> bool {
        let inner = self.inner.read().await;
        let now = Instant::now();
        inner
            .proxies
            .iter()
            .find(|p| p.url == proxy_url)
            .map(|p| p.available(now))
            .unwrap_or(false)
    }

    pub async fn report_failure(&self, proxy_url: &str) {
        self.report_kind(proxy_url, FailureKind::Connection).await;
    }

    pub async fn report_rate_limit(&self, proxy_url: &str) {
        self.report_kind(proxy_url, FailureKind::RateLimit).await;
    }

    pub async fn report_banned(&self, proxy_url: &str) {
        self.report_kind(proxy_url, FailureKind::Banned).await;
    }

    pub async fn report_success(&self, proxy_url: &str) {
        let mut inner = self.inner.write().await;
        if let Some(entry) = inner.proxies.iter_mut().find(|p| p.url == proxy_url) {
            entry.fail_count = 0;
            entry.cooldown_until = None;
            entry.success_count = entry.success_count.saturating_add(1);
            entry.last_success = Some(Instant::now());
        }
    }

    async fn report_kind(&self, proxy_url: &str, kind: FailureKind) {
        let cooldown = kind.cooldown();
        let max_fails = kind.max_fails_before_removal();
        let label = kind.label();

        let (after_len, fetching) = {
            let mut inner = self.inner.write().await;
            let now = Instant::now();

            let mut should_remove = false;
            if let Some(entry) = inner.proxies.iter_mut().find(|p| p.url == proxy_url) {
                if max_fails.is_some() {
                    entry.fail_count += 1;
                }
                entry.cooldown_until = Some(now + cooldown);
                should_remove = max_fails
                    .map(|max| entry.fail_count >= max)
                    .unwrap_or(false);
            }

            if should_remove {
                let before = inner.proxies.len();
                inner.proxies.retain(|p| p.url != proxy_url);
                warn!(
                    "Removed repeatedly-failing proxy: {}. Pool size: {} -> {}",
                    proxy_url,
                    before,
                    inner.proxies.len()
                );
            } else {
                debug!(
                    "Proxy {} marked as {}, cooldown {:?}",
                    proxy_url, label, cooldown
                );
            }

            (
                inner.proxies.len(),
                self.is_fetching.load(Ordering::Relaxed),
            )
        };

        if after_len < *MIN_POOL_SIZE && !fetching {
            let self_clone = self.clone();
            tokio::spawn(async move {
                self_clone.fetch_free_proxies().await;
            });
        }
    }
}