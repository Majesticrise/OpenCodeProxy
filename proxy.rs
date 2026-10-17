use crate::config::PROXY_FILE;
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
use tracing::{error, info, warn};

pub struct ProxyPoolInner {
    pub proxies: Vec<(String, f64)>,
    pub is_fetching: bool,
    pub file_path: String,
    pub is_running: AtomicBool,
    pub total_requests: AtomicU64,
}

#[derive(Clone)]
pub struct ProxyPool {
    pub inner: Arc<RwLock<ProxyPoolInner>>,
}

impl ProxyPool {
    pub fn new(file_path: &str) -> Self {
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

    pub async fn load_proxies(&self) {
        let file_path = { self.inner.read().await.file_path.clone() };

        // 1. 优先读取本地 proxies.txt
        if Path::new(&file_path).exists() {
            if let Ok(content) = tokio::fs::read_to_string(&file_path).await {
                let mut file_proxies = Vec::new();
                for line in content.lines() {
                    let line = line.trim();
                    if line.is_empty() || line.starts_with('#') { continue; }
                    let proxy_url = if !(line.starts_with("http://") || line.starts_with("https://") || line.starts_with("socks5://")) {
                        format!("http://{}", line)
                    } else { line.to_string() };
                    file_proxies.push((proxy_url, 0.0));
                }
                if !file_proxies.is_empty() {
                    let mut inner = self.inner.write().await;
                    inner.proxies = file_proxies;
                    info!("Loaded {} proxies from local file {}", inner.proxies.len(), file_path);
                    return;
                }
            }
        }

        // 2. 其次读取持久化的 verified_proxies.json
        if let Ok(content) = tokio::fs::read_to_string("verified_proxies.json").await {
            if let Ok(validated) = serde_json::from_str::<Vec<(String, f64)>>(&content) {
                if !validated.is_empty() {
                    let mut inner = self.inner.write().await;
                    inner.proxies = validated;
                    info!("Loaded {} proxies from verified_proxies.json", inner.proxies.len());
                    return;
                }
            }
        }

        // 3. 最后才触发网络抓取
        self.fetch_free_proxies().await;
    }

    pub async fn fetch_free_proxies(&self) {
        if self.inner.read().await.is_fetching { return; }
        self.inner.write().await.is_fetching = true;

        let self_clone = self.clone();
        tokio::spawn(async move {
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
            let client = reqwest::Client::builder().timeout(Duration::from_secs(8)).build().unwrap();

            for url in sources {
                match client.get(url).send().await {
                    Ok(resp) if resp.status() == 200 => {
                        if let Ok(text) = resp.text().await {
                            for line in text.lines() {
                                let line = line.trim();
                                if !line.is_empty() { raw_ips.insert(line.to_string()); }
                            }
                        }
                    }
                    Err(e) => warn!("Failed to fetch from source {}: {}", url, e),
                    _ => {}
                }
            }

            if raw_ips.is_empty() {
                error!("All public proxy sources failed or returned empty list.");
                self_clone.inner.write().await.is_fetching = false;
                return;
            }

            let raw_proxies: Vec<String> = raw_ips.into_iter().map(|ip| format!("http://{}", ip)).collect();
            info!("Fetched {} unique raw proxies. Starting background validation...", raw_proxies.len());
            self_clone.validate_proxies(raw_proxies).await;
        });
    }

    async fn validate_proxies(&self, raw_proxies: Vec<String>) {
        let sample_size = std::cmp::min(3000, raw_proxies.len());
        let mut test_targets = raw_proxies;
        test_targets.shuffle(&mut rand::thread_rng());
        test_targets.truncate(sample_size);

        let sem = Arc::new(tokio::sync::Semaphore::new(200));
        let mut validated = Vec::new();

        // 分块流式验证，内存友好
        let mut stream = stream::iter(test_targets.into_iter().map(|proxy| {
            let sem = sem.clone();
            async move {
                let _permit = sem.acquire().await.unwrap();
                let start = Instant::now();
                let client = reqwest::Client::builder()
                    .proxy(reqwest::Proxy::all(&proxy).ok()?)
                    .timeout(Duration::from_millis(5000))
                    .build().ok()?;
                match client.get("https://opencode.ai").send().await {
                    Ok(resp) if resp.status() == 200 => Some((proxy, start.elapsed().as_secs_f64())),
                    _ => None,
                }
            }
        })).buffer_unordered(200);

        while let Some(result) = stream.next().await {
            if let Some(item) = result {
                validated.push(item);
            }
        }

        validated.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
        validated.truncate(100);

        let mut inner = self.inner.write().await;
        inner.proxies = validated.clone();
        inner.is_fetching = false;

        if !validated.is_empty() {
            info!("Proxy pool updated: {} high-speed verified proxies ready. (Fastest: {:.2}s)", validated.len(), validated[0].1);
            // 持久化
            let json_str = serde_json::to_string_pretty(&validated).unwrap_or_default();
            if let Err(e) = tokio::fs::write("verified_proxies.json", json_str).await {
                warn!("Failed to persist proxy pool: {}", e);
            } else {
                info!("Proxy pool persisted to verified_proxies.json");
            }
        } else {
            warn!("No proxies passed the high-speed check (under 5s). Pool empty.");
        }
    }

    pub async fn get_proxy(&self) -> Option<String> {
        let inner = self.inner.read().await;
        if inner.proxies.is_empty() { return None; }
        let limit = std::cmp::min(15, inner.proxies.len());
        let slice = &inner.proxies[..limit];
        slice.choose(&mut rand::thread_rng()).map(|x| x.0.clone())
    }

    pub async fn report_failure(&self, proxy_url: &str) {
        let mut inner = self.inner.write().await;
        let before_len = inner.proxies.len();
        inner.proxies.retain(|x| x.0 != proxy_url);
        let after_len = inner.proxies.len();
        if before_len != after_len {
            warn!("Removed failed proxy: {}. Active pool size: {}", proxy_url, after_len);
        }
        if after_len < 10 && !inner.is_fetching {
            let self_clone = self.clone();
            tokio::spawn(async move { self_clone.fetch_free_proxies().await; });
        }
    }
}