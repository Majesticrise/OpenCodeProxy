use crate::config::{MIN_POOL_SIZE, VALIDATE_TARGET};
use futures_util::stream::{self, StreamExt};
use rand::seq::SliceRandom;
use std::{
    collections::HashSet,
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
/// 403 FreeTierError 冷却（比连接错误略短，给 IP 信誉恢复留窗口）
const COOLDOWN_BANNED: Duration = Duration::from_secs(900);
const MAX_FAILS_BEFORE_REMOVE: u32 = 3;

#[derive(Clone)]
pub struct ProxyEntry {
    pub url: String,
    pub latency: f64,
    pub cooldown_until: Option<Instant>,
    pub fail_count: u32,
    pub last_success: Option<Instant>,
}

impl ProxyEntry {
    fn new(url: String, latency: f64) -> Self {
        Self {
            url,
            latency,
            cooldown_until: None,
            fail_count: 0,
            last_success: None,
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

    /// Banned 不累加 fail_count，避免误删有希望的代理
    fn counts_toward_removal(self) -> bool {
        matches!(self, FailureKind::Connection)
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

        let sem = Arc::new(tokio::sync::Semaphore::new(200));
        let mut validated = Vec::new();
        let target = VALIDATE_TARGET.clone();

        let mut stream = stream::iter(test_targets.into_iter().map(|proxy| {
            let sem = sem.clone();
            let target = target.clone();
            async move {
                let _permit = sem.acquire().await.unwrap();
                let start = Instant::now();
                let client = reqwest::Client::builder()
                    .proxy(reqwest::Proxy::all(&proxy).ok()?)
                    .timeout(Duration::from_millis(5000))
                    .build()
                    .ok()?;
                match client.get(&target).send().await {
                    Ok(resp) if resp.status() == 200 => {
                        Some((proxy, start.elapsed().as_secs_f64()))
                    }
                    _ => None,
                }
            }
        }))
        .buffer_unordered(200);

        while let Some(result) = stream.next().await {
            if let Some(item) = result {
                validated.push(item);
            }
        }

        validated.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
        validated.truncate(100);

        if !validated.is_empty() {
            info!(
                "Proxy pool updated: {} high-speed verified proxies ready. (Fastest: {:.2}s)",
                validated.len(),
                validated[0].1
            );

            let to_save: Vec<(String, f64)> =
                validated.iter().map(|(u, l)| (u.clone(), *l)).collect();
            match serde_json::to_string_pretty(&to_save) {
                Ok(json_str) => {
                    if let Err(e) = tokio::fs::write("verified_proxies.json", json_str).await {
                        warn!("Failed to persist proxy pool: {}", e);
                    } else {
                        info!("Proxy pool persisted to verified_proxies.json");
                    }
                }
                Err(e) => warn!("Failed to serialize proxy pool: {}", e),
            }

            let mut inner = self.inner.write().await;
            inner.proxies = validated
                .into_iter()
                .map(|(url, lat)| ProxyEntry::new(url, lat))
                .collect();
        } else {
            warn!("No proxies passed the high-speed check (under 5s). Pool empty.");
            let mut inner = self.inner.write().await;
            inner.proxies.clear();
        }
    }

    pub async fn get_proxy(&self) -> Option<String> {
        let inner = self.inner.read().await;
        let now = Instant::now();

        let available: Vec<&ProxyEntry> = inner
            .proxies
            .iter()
            .filter(|p| p.available(now))
            .collect();

        if available.is_empty() {
            return None;
        }

        let limit = std::cmp::min(15, available.len());
        let slice = &available[..limit];
        slice.choose(&mut rand::thread_rng()).map(|x| x.url.clone())
    }

    pub async fn report_failure(&self, proxy_url: &str) {
        self.report_kind(proxy_url, FailureKind::Connection).await;
    }

    pub async fn report_rate_limit(&self, proxy_url: &str) {
        self.report_kind(proxy_url, FailureKind::RateLimit).await;
    }

    /// 403 FreeTierError：长冷却但不累计 fail_count
    pub async fn report_banned(&self, proxy_url: &str) {
        self.report_kind(proxy_url, FailureKind::Banned).await;
    }

    pub async fn report_success(&self, proxy_url: &str) {
        let mut inner = self.inner.write().await;
        if let Some(entry) = inner.proxies.iter_mut().find(|p| p.url == proxy_url) {
            entry.fail_count = 0;
            entry.cooldown_until = None;
            entry.last_success = Some(Instant::now());
        }
    }

    async fn report_kind(&self, proxy_url: &str, kind: FailureKind) {
        let cooldown = kind.cooldown();
        let counts = kind.counts_toward_removal();
        let label = kind.label();

        let (after_len, fetching) = {
            let mut inner = self.inner.write().await;
            let now = Instant::now();

            let mut should_remove = false;
            if let Some(entry) = inner.proxies.iter_mut().find(|p| p.url == proxy_url) {
                if counts {
                    entry.fail_count += 1;
                }
                entry.cooldown_until = Some(now + cooldown);
                should_remove = counts && entry.fail_count >= MAX_FAILS_BEFORE_REMOVE;
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