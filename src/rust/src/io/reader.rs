use bytes::Bytes;
use crate::io::cache::BlockCache;
use crate::io::source::DataSource;
use futures::stream::{self, StreamExt};
use std::collections::HashMap;
use std::ops::Range;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use thiserror::Error;
use tokio::sync::Notify;

/// One-shot signal used for per-block single-flight coordination.
/// The fetcher calls [`BlockSignal::signal`] after the fetch attempt
/// completes (regardless of outcome); waiters call
/// [`BlockSignal::wait`] and, after returning, consult the cache to
/// determine whether the fetch succeeded (hit) or failed (miss).
///
/// The AtomicBool + Notify combination avoids the classic
/// miss-notification race of a bare `Notify`: `wait` registers a
/// `Notified` future *before* checking `done`, so either the flag is
/// already set (we return immediately) or the pending `notify_waiters`
/// call will deliver to our registered future.
struct BlockSignal {
    done: AtomicBool,
    notify: Notify,
}

impl BlockSignal {
    fn new() -> Self {
        Self {
            done: AtomicBool::new(false),
            notify: Notify::new(),
        }
    }

    fn signal(&self) {
        self.done.store(true, Ordering::Release);
        self.notify.notify_waiters();
    }

    async fn wait(&self) {
        loop {
            let notified = self.notify.notified();
            tokio::pin!(notified);
            // Register the future with Notify before the flag check.
            // Any subsequent signal() is guaranteed to wake us.
            notified.as_mut().enable();
            if self.done.load(Ordering::Acquire) {
                return;
            }
            notified.await;
            if self.done.load(Ordering::Acquire) {
                return;
            }
            // Spurious wake-up (unlikely); loop and re-register.
        }
    }
}

type BlockSignalHandle = Arc<BlockSignal>;

/// Process-wide HTTP range-request counter. Used by diagnostic
/// instrumentation to report how many distinct HTTP calls a given
/// `sl_read` incurred. Reset with [`reset_request_counter`] at the
/// start of an operation; query with [`request_counter`] at the end.
static TOTAL_REQUESTS: AtomicU64 = AtomicU64::new(0);
static TOTAL_BYTES: AtomicU64 = AtomicU64::new(0);

pub fn reset_request_counter() {
    TOTAL_REQUESTS.store(0, Ordering::Relaxed);
    TOTAL_BYTES.store(0, Ordering::Relaxed);
}

pub fn request_counter() -> (u64, u64) {
    (
        TOTAL_REQUESTS.load(Ordering::Relaxed),
        TOTAL_BYTES.load(Ordering::Relaxed),
    )
}

#[derive(Error, Debug)]
pub enum IoError {
    #[error("HTTP request failed: {0}")]
    Http(#[from] reqwest::Error),

    #[error("HTTP {status}: {url}")]
    HttpStatus { status: u16, url: String },

    #[error("Local file I/O error: {0}")]
    LocalIo(#[from] std::io::Error),

    #[error("Server does not support range requests")]
    RangeNotSupported,
}

/// Maximum number of retry attempts for transient HTTP errors.
const MAX_RETRIES: usize = 3;

/// Base delay between retries (doubled each attempt).
const RETRY_BASE_DELAY: Duration = Duration::from_millis(500);

/// Maximum number of redirects to follow manually.
const MAX_REDIRECTS: usize = 10;

/// HTTP status codes that warrant a retry.
fn is_retriable(status: u16) -> bool {
    matches!(status, 429 | 500 | 502 | 503 | 504)
}

/// Identify URLs in the Earthdata OAuth redirect chain that must NOT
/// receive a Range header. These are session-establishment endpoints
/// (URS OAuth authorize/login callbacks) that either ignore Range or
/// — in ORNL-DAAC's case — hang indefinitely when given one.
fn is_auth_callback_url(url: &str) -> bool {
    url.contains("urs.earthdata.nasa.gov")
        || url.contains("/oauth/")
        || url.contains("/login?code=")
}

/// Configuration for the I/O reader.
#[derive(Debug, Clone)]
pub struct ReaderConfig {
    /// Block size for cache alignment (default: 256 KiB).
    pub block_size: usize,
    /// Maximum number of blocks to keep in the LRU cache (default: 512 = 128 MiB).
    pub max_cache_blocks: usize,
    /// Maximum gap (in blocks) between missing blocks that will be merged into
    /// a single HTTP request. Higher values trade bandwidth for fewer round trips.
    /// Default: 4 (= 1 MiB gap with 256 KiB blocks).
    pub coalesce_gap_blocks: u64,
    /// Number of blocks to prefetch on the first read (when cache is cold).
    /// This captures superblock + root group + initial B-tree in one request.
    /// Default: 4 (= 1 MiB with 256 KiB blocks).
    pub initial_prefetch_blocks: u64,
    /// Maximum number of coalesced ranges to fetch in parallel.
    /// Default: 8.
    pub max_concurrent_fetches: usize,
}

impl Default for ReaderConfig {
    fn default() -> Self {
        Self {
            block_size: 256 * 1024,         // 256 KiB
            max_cache_blocks: 512,           // 128 MiB total cache
            coalesce_gap_blocks: 4,          // bridge gaps up to 1 MiB
            initial_prefetch_blocks: 4,      // fetch 1 MiB on cold start
            max_concurrent_fetches: 32,      // parallel HTTP requests
        }
    }
}

/// The main I/O reader that handles byte-range fetches with caching.
///
/// All reads are aligned to block boundaries and cached in an LRU cache.
/// Adjacent block fetches are coalesced into single HTTP requests, and
/// multiple coalesced ranges are fetched in parallel.
///
/// For NASA Earthdata URLs, the reader handles the multi-step OAuth
/// redirect flow:
///   1. GET data URL → 302 to `urs.earthdata.nasa.gov/oauth/authorize`
///   2. GET URS with Basic auth → 302 back to data URL (sets cookies)
///   3. GET data URL with cookies → 302 to CloudFront presigned URL
///   4. GET CloudFront URL → 206 data
///
/// The final CloudFront URL is cached for subsequent Range requests.
pub struct Reader {
    source: DataSource,
    client: reqwest::Client,
    cache: Mutex<BlockCache>,
    config: ReaderConfig,
    /// Cached resolved URL after following the OAuth redirect chain.
    /// Typically a CloudFront presigned URL that can be used directly
    /// for subsequent Range requests without re-authenticating.
    resolved_url: Mutex<Option<String>>,
    /// Serializes OAuth-chain resolution so a fleet of concurrent
    /// readers doesn't fire N parallel redirect chains for the same
    /// granule. Held only across the resolve operation, never across
    /// the main Range-request path.
    resolve_guard: tokio::sync::Mutex<()>,
    /// Per-block single-flight map. A block index appears here while
    /// one task is fetching it; other concurrent readers wait on the
    /// signal instead of issuing a duplicate HTTP request.
    in_flight: Mutex<HashMap<u64, BlockSignalHandle>>,
}

impl Reader {
    /// Create a new reader for the given data source.
    pub fn new(source: DataSource, config: ReaderConfig) -> Self {
        let cache = BlockCache::new(config.block_size, config.max_cache_blocks);
        let client = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(60))
            // HTTP/1.1 only. H/2 via ALPN was retested after the
            // Reader gained per-block single-flight and science-phase
            // byte-range coalescing: on the Mondah Forest workload
            // H/2 produced the same request pattern (130 req / 57MB
            // per granule) but ran ~7% slower than H/1.1 across
            // three runs each. The multiplexing win has nothing to
            // amortise once duplicate fetches are already eliminated,
            // and the previously-noted truncation risk with the
            // manual redirect chain remains a concern. Stay on H/1.1
            // until a workload actually benefits.
            .http1_only()
            .no_gzip()
            .no_brotli()
            .no_deflate()
            // Disable automatic redirects — we follow them manually to inject
            // Basic auth at the URS OAuth step while keeping cookies flowing.
            .redirect(reqwest::redirect::Policy::none())
            // Enable the cookie store so session cookies from URS are
            // automatically sent on subsequent requests in the redirect chain.
            .cookie_store(true)
            .build()
            .expect("failed to build HTTP client");
        Self {
            source,
            client,
            cache: Mutex::new(cache),
            config,
            resolved_url: Mutex::new(None),
            resolve_guard: tokio::sync::Mutex::new(()),
            in_flight: Mutex::new(HashMap::new()),
        }
    }

    /// Read `length` bytes starting at `offset` from the data source.
    ///
    /// Uses block-aligned caching: checks cache first, fetches missing blocks,
    /// coalesces adjacent block fetches into single HTTP range requests.
    pub async fn read(&self, offset: u64, length: usize) -> Result<Vec<u8>, IoError> {
        if length == 0 {
            return Ok(Vec::new());
        }

        match &self.source {
            DataSource::Local { path } => {
                // Offload blocking file I/O so we don't stall the
                // current-thread runtime. HDF5 navigation fires many
                // small reads concurrently; blocking them serially
                // would defeat the concurrency model.
                let path = path.clone();
                tokio::task::spawn_blocking(move || -> Result<Vec<u8>, IoError> {
                    use std::io::{Read, Seek, SeekFrom};
                    let mut file = std::fs::File::open(&path)?;
                    let file_len = file.metadata()?.len();
                    if offset >= file_len {
                        return Ok(Vec::new());
                    }
                    // Clamp read length to EOF so speculative oversized
                    // reads (common during HDF5 navigation) succeed the
                    // same way they do for HTTP — the HDF5 parser
                    // handles short reads itself.
                    let available = (file_len - offset) as usize;
                    let read_len = length.min(available);
                    file.seek(SeekFrom::Start(offset))?;
                    let mut buf = vec![0u8; read_len];
                    file.read_exact(&mut buf)?;
                    Ok(buf)
                })
                .await
                .map_err(|e| {
                    IoError::LocalIo(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        e.to_string(),
                    ))
                })?
            }
            DataSource::Http { .. } => self.read_cached(offset, length).await,
        }
    }

    /// Read with block-level caching for HTTP sources.
    ///
    /// Uses per-block single-flight: if another task is already
    /// fetching a block we need, we wait on its signal cell instead
    /// of issuing a duplicate HTTP request. An RAII guard
    /// ([`InFlightGuard`]) ensures our registered cells are always
    /// released on success, error, or panic, so waiters can't hang.
    async fn read_cached(&self, offset: u64, length: usize) -> Result<Vec<u8>, IoError> {
        let block_size = self.config.block_size as u64;

        // Determine which blocks we need and capture the cached ones'
        // bytes now. Holding the Bytes locally prevents a concurrent
        // reader from evicting those blocks before we extract below.
        let (cache_is_cold, mut cached_blocks, mut missing_blocks) = {
            let mut cache = self.cache.lock().unwrap_or_else(|e| e.into_inner());
            let is_cold = cache.len() == 0;
            let (cached, missing) = cache.query(offset, length);
            (is_cold, cached, missing)
        };

        // On cold start, prefetch extra blocks to capture metadata in one request
        if cache_is_cold && !missing_blocks.is_empty() {
            let first_block = missing_blocks[0];
            for i in 0..self.config.initial_prefetch_blocks {
                let block = first_block + i;
                if !missing_blocks.contains(&block) {
                    missing_blocks.push(block);
                }
            }
            missing_blocks.sort_unstable();
        }

        // Partition missing blocks into (we_fetch, we_wait). Blocks
        // already registered by another task become waiters; new
        // blocks we register and fetch ourselves. Then — in the same
        // lock scope — coalesce our blocks into byte ranges and
        // claim any *bridge* blocks (blocks that our coalesced
        // fetch will incidentally cover because `coalesce_gap_blocks`
        // lets us merge across gaps). Registering bridges closes a
        // single-flight hole: without it, a peer waiting on a bridge
        // block could fetch it redundantly, and — worse — the
        // bridge would end up pushed into cached_blocks from both
        // our fetch path and the peer's wait-then-get path,
        // producing a duplicate block in the extract. The sort+dedup
        // at the end is the backstop; this is the proper fix.
        let mut our_blocks: Vec<u64> = Vec::new();
        let mut wait_cells: Vec<(u64, BlockSignalHandle)> = Vec::new();
        let mut owned_cells: Vec<(u64, BlockSignalHandle)> = Vec::new();
        let ranges;
        {
            let mut in_flight = self.in_flight.lock().unwrap_or_else(|e| e.into_inner());
            for idx in missing_blocks {
                if let Some(cell) = in_flight.get(&idx) {
                    wait_cells.push((idx, cell.clone()));
                } else {
                    let cell: BlockSignalHandle = Arc::new(BlockSignal::new());
                    in_flight.insert(idx, cell.clone());
                    our_blocks.push(idx);
                    owned_cells.push((idx, cell));
                }
            }

            // Compute coalesced ranges over blocks we own. A short
            // borrow of the cache lock inside the outer in_flight
            // scope is safe because neither is held across .await.
            ranges = {
                let cache = self.cache.lock().unwrap_or_else(|e| e.into_inner());
                cache.coalesce_ranges(our_blocks.clone(), self.config.coalesce_gap_blocks)
            };

            // Claim bridge blocks: every block inside a coalesced
            // range that isn't already accounted for by our primary
            // partition. If it's already in_flight (another task is
            // fetching it right now), don't register — just wait.
            // Otherwise register as ours.
            let mut seen: std::collections::HashSet<u64> =
                our_blocks.iter().copied().collect();
            for (idx, _) in &wait_cells {
                seen.insert(*idx);
            }
            for r in &ranges {
                let start_blk = r.start / block_size;
                let end_blk = r.end / block_size; // exclusive, block-aligned
                for blk in start_blk..end_blk {
                    if !seen.insert(blk) {
                        continue;
                    }
                    if let Some(cell) = in_flight.get(&blk) {
                        wait_cells.push((blk, cell.clone()));
                    } else {
                        let cell: BlockSignalHandle = Arc::new(BlockSignal::new());
                        in_flight.insert(blk, cell.clone());
                        owned_cells.push((blk, cell));
                        // Note: we don't add to `our_blocks` — the
                        // coalesced `ranges` already covers this
                        // block, and we don't want to re-coalesce.
                    }
                }
            }
        }

        // Release our cells when this scope exits (success, error, or
        // panic). Drop unregisters from the in_flight map and signals
        // each block, so waiters never hang.
        let _guard = InFlightGuard {
            reader: self,
            cells: &owned_cells,
        };

        // Fetch the coalesced ranges. Blocks another task is fetching
        // are NOT in these ranges except where they overlap a bridge
        // we already moved to wait_cells above; in that rare case the
        // peer's data wins in cache and the dedup backstop at the end
        // guarantees we emit each block once.
        if !ranges.is_empty() {

            let results: Vec<Result<(Range<u64>, Bytes), IoError>> = stream::iter(ranges)
                .map(|range| async move {
                    let data = self.fetch_range(range.clone()).await?;
                    Ok::<_, IoError>((range, data))
                })
                .buffered(self.config.max_concurrent_fetches)
                .collect()
                .await;

            // Insert fetched data into cache AND capture locally so
            // extract_range sees a complete view regardless of later
            // LRU eviction. Errors are collected and the first one is
            // returned after the guard runs.
            let mut fetch_err: Option<IoError> = None;
            {
                let mut cache = self.cache.lock().unwrap_or_else(|e| e.into_inner());
                for result in results {
                    match result {
                        Ok((range, data)) => {
                            let range_start_block = range.start / block_size;
                            let mut pos = 0usize;
                            let mut block_idx = range_start_block;
                            while pos < data.len() {
                                let chunk_len = (block_size as usize).min(data.len() - pos);
                                let block = data.slice(pos..pos + chunk_len);
                                cache.insert(block_idx, block.clone());
                                cached_blocks.push((block_idx, block));
                                pos += chunk_len;
                                block_idx += 1;
                            }
                        }
                        Err(e) => {
                            if fetch_err.is_none() {
                                fetch_err = Some(e);
                            }
                        }
                    }
                }
            }

            if let Some(e) = fetch_err {
                return Err(e);
            }
        }

        // Wait for blocks other tasks are fetching, then collect them
        // from cache. A miss after the signal fires means that
        // fetcher failed; propagate as an error.
        for (idx, signal) in &wait_cells {
            signal.wait().await;
            let data = {
                let mut cache = self.cache.lock().unwrap_or_else(|e| e.into_inner());
                cache.get(idx)
            };
            match data {
                Some(b) => cached_blocks.push((*idx, b)),
                None => {
                    return Err(IoError::HttpStatus {
                        status: 503,
                        url: format!(
                            "single-flight peer failed to fetch block {}",
                            idx
                        ),
                    });
                }
            }
        }

        // Extract from the collected blocks. extract_range walks them
        // in order and emits each once, so sort by index and dedup
        // before calling it. Dedup matters when a block is populated
        // by more than one code path in the same read — notably:
        // our coalesced fetch bridges a block that's already being
        // fetched by another task, so the wait-loop also pushes it
        // into cached_blocks. Without dedup the bytes would be
        // emitted twice, producing a corrupt over-long output.
        cached_blocks.sort_by_key(|(idx, _)| *idx);
        cached_blocks.dedup_by_key(|(idx, _)| *idx);
        let cache = self.cache.lock().unwrap_or_else(|e| e.into_inner());
        Ok(cache.extract_range(offset, length, &cached_blocks))
    }

    /// Send a GET request following redirects manually.
    ///
    /// Handles the NASA Earthdata OAuth redirect flow:
    ///   - At `urs.earthdata.nasa.gov`: sends Basic auth credentials
    ///   - At other `.nasa.gov` domains: sends request with cookies (no auth)
    ///   - At non-NASA domains (S3/CloudFront): no auth, no cookies needed
    ///
    /// The reqwest cookie store maintains session cookies across the chain.
    ///
    /// Returns the response and the final URL it was fetched from.
    async fn send_with_redirects(
        &self,
        start_url: &str,
        range_header: Option<&str>,
    ) -> Result<(reqwest::Response, String), IoError> {
        let auth = match &self.source {
            DataSource::Http { auth, .. } => auth.as_ref(),
            DataSource::Local { .. } => None,
        };

        let mut current_url = start_url.to_string();

        for _ in 0..MAX_REDIRECTS {
            let mut request = self.client.get(&current_url);

            // Send Range only on data hops, not on auth-callback hops.
            // Auth callbacks (URS OAuth, DAAC /login, /oauth/...) are
            // session-establishment endpoints that don't serve file
            // bytes; ORNL-DAAC's /login callback hangs indefinitely
            // when a Range header is present. LP.DAAC's chain
            // terminates at a CloudFront presigned URL which handles
            // Range fine, so keeping Range on data hops preserves the
            // 206 response the caller depends on for partial content.
            if let Some(range) = range_header {
                if !is_auth_callback_url(&current_url) {
                    request = request.header("Range", range);
                }
            }

            // Send Basic auth only at the URS OAuth endpoint.
            if let Some(creds) = auth {
                if current_url.contains("urs.earthdata.nasa.gov") {
                    request = request.basic_auth(&creds.username, Some(&creds.password));
                }
            }

            let response = request.send().await.map_err(IoError::Http)?;

            if response.status().is_redirection() {
                if let Some(location) = response.headers().get("location") {
                    let loc = location.to_str().unwrap_or("");
                    current_url = if loc.starts_with("http://") || loc.starts_with("https://") {
                        loc.to_string()
                    } else {
                        // Relative URL: resolve against current
                        reqwest::Url::parse(&current_url)
                            .and_then(|base| base.join(loc))
                            .map(|u| u.to_string())
                            .unwrap_or_else(|_| loc.to_string())
                    };
                    continue;
                }
            }

            return Ok((response, current_url));
        }

        Err(IoError::HttpStatus {
            status: 302,
            url: format!("Too many redirects: {}", start_url),
        })
    }

    /// Resolve the data URL by following the OAuth redirect chain once.
    ///
    /// Returns the cached resolved URL if available, otherwise follows
    /// redirects and caches the result (typically a CloudFront presigned URL).
    async fn get_resolved_url(&self) -> Result<String, IoError> {
        // Fast path: return cached URL
        {
            let resolved = self.resolved_url.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(url) = resolved.as_ref() {
                return Ok(url.clone());
            }
        }

        let (url, has_auth) = match &self.source {
            DataSource::Http { url, auth } => (url.as_str(), auth.is_some()),
            DataSource::Local { .. } => unreachable!(),
        };

        // If no auth credentials, no redirect dance needed
        if !has_auth {
            return Ok(url.to_string());
        }

        // Serialize concurrent resolutions. The first task runs the
        // redirect chain and populates the cache; any task that
        // acquires the guard afterwards returns from the re-check
        // below without a second network chain.
        let _resolve = self.resolve_guard.lock().await;
        {
            let resolved = self.resolved_url.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(url) = resolved.as_ref() {
                return Ok(url.clone());
            }
        }

        // Follow the full OAuth redirect chain with a small Range request
        let (response, final_url) = self
            .send_with_redirects(url, Some("bytes=0-0"))
            .await?;

        let status = response.status();
        if !status.is_success() && status != reqwest::StatusCode::PARTIAL_CONTENT {
            return Err(IoError::HttpStatus {
                status: status.as_u16(),
                url: url.to_string(),
            });
        }

        // Cache the resolved URL (CloudFront presigned)
        {
            let mut resolved = self.resolved_url.lock().unwrap_or_else(|e| e.into_inner());
            *resolved = Some(final_url.clone());
        }

        Ok(final_url)
    }

    /// Clear the cached resolved URL (e.g., if the presigned URL expired).
    fn invalidate_resolved_url(&self) {
        let mut resolved = self.resolved_url.lock().unwrap_or_else(|e| e.into_inner());
        *resolved = None;
    }

    /// Fetch a raw byte range from the HTTP source with retry logic.
    ///
    /// Retries up to [`MAX_RETRIES`] times with exponential backoff for
    /// transient server errors (429, 500, 502, 503, 504).
    async fn fetch_range(&self, range: Range<u64>) -> Result<Bytes, IoError> {
        let orig_url = match &self.source {
            DataSource::Http { url, .. } => url.as_str(),
            DataSource::Local { .. } => unreachable!("fetch_range called on local source"),
        };

        let range_header = format!("bytes={}-{}", range.start, range.end.saturating_sub(1));

        let mut last_error: Option<IoError> = None;

        for attempt in 0..=MAX_RETRIES {
            if attempt > 0 {
                let delay = RETRY_BASE_DELAY * 2u32.pow(attempt as u32 - 1);
                log::debug!(
                    "Retry {}/{} after {:?} for {} Range: {}",
                    attempt,
                    MAX_RETRIES,
                    delay,
                    orig_url,
                    range_header
                );
                tokio::time::sleep(delay).await;
            }

            // Re-resolve each iteration. get_resolved_url has a fast
            // path that returns the cached URL immediately; after a
            // 403/401 the cache is cleared via invalidate_resolved_url
            // so the next iteration follows the OAuth chain afresh.
            let resolved = match self.get_resolved_url().await {
                Ok(u) => u,
                Err(IoError::Http(e)) if attempt < MAX_RETRIES => {
                    last_error = Some(IoError::Http(e));
                    continue;
                }
                Err(e) => return Err(e),
            };

            if attempt == 0 {
                log::debug!("HTTP GET {} Range: {}", resolved, range_header);
            }

            // If using a resolved URL (e.g., CloudFront presigned), request
            // directly — the presigned URL has its own auth in query params.
            // If it's the same as the original, use the redirect-following path.
            let (response, _) = if resolved != orig_url {
                // Direct request to resolved (presigned) URL
                let request = self
                    .client
                    .get(&resolved)
                    .header("Range", &range_header);
                match request.send().await {
                    Ok(r) => (r, resolved.clone()),
                    Err(e) if attempt < MAX_RETRIES => {
                        last_error = Some(IoError::Http(e));
                        continue;
                    }
                    Err(e) => return Err(IoError::Http(e)),
                }
            } else {
                // No cached resolution; use redirect-following path
                match self
                    .send_with_redirects(orig_url, Some(&range_header))
                    .await
                {
                    Ok(r) => r,
                    Err(IoError::Http(e)) if attempt < MAX_RETRIES => {
                        last_error = Some(IoError::Http(e));
                        continue;
                    }
                    Err(e) => return Err(e),
                }
            };

            let status = response.status();

            if status == reqwest::StatusCode::RANGE_NOT_SATISFIABLE {
                return Err(IoError::RangeNotSupported);
            }

            // If the presigned URL expired (403) or auth failed (401),
            // invalidate the cache and retry with a fresh redirect resolution.
            if (status == reqwest::StatusCode::FORBIDDEN
                || status == reqwest::StatusCode::UNAUTHORIZED)
                && attempt < MAX_RETRIES
            {
                self.invalidate_resolved_url();
                last_error = Some(IoError::HttpStatus {
                    status: status.as_u16(),
                    url: orig_url.to_string(),
                });
                continue;
            }

            if is_retriable(status.as_u16()) && attempt < MAX_RETRIES {
                last_error = Some(IoError::HttpStatus {
                    status: status.as_u16(),
                    url: orig_url.to_string(),
                });
                continue;
            }

            if !status.is_success() && status != reqwest::StatusCode::PARTIAL_CONTENT {
                return Err(IoError::HttpStatus {
                    status: status.as_u16(),
                    url: orig_url.to_string(),
                });
            }

            // Body download can also fail (connection reset, timeout).
            // Retry these as well.
            match response.bytes().await {
                Ok(bytes) => {
                    TOTAL_REQUESTS.fetch_add(1, Ordering::Relaxed);
                    TOTAL_BYTES.fetch_add(bytes.len() as u64, Ordering::Relaxed);
                    return Ok(bytes);
                }
                Err(e) if attempt < MAX_RETRIES => {
                    log::debug!("Body read failed (attempt {}): {}", attempt + 1, e);
                    last_error = Some(IoError::Http(e));
                    continue;
                }
                Err(e) => return Err(IoError::Http(e)),
            }
        }

        Err(last_error.unwrap_or(IoError::HttpStatus {
            status: 500,
            url: orig_url.to_string(),
        }))
    }

    /// Get the total file size via a HEAD request (for HTTP) or file metadata (for local).
    pub async fn file_size(&self) -> Result<u64, IoError> {
        match &self.source {
            DataSource::Local { path } => {
                let metadata = std::fs::metadata(path)?;
                Ok(metadata.len())
            }
            DataSource::Http { .. } => {
                let (response, _) = self.send_with_redirects(
                    match &self.source {
                        DataSource::Http { url, .. } => url.as_str(),
                        _ => unreachable!(),
                    },
                    None,
                ).await?;
                let len = response
                    .headers()
                    .get("content-length")
                    .and_then(|v| v.to_str().ok())
                    .and_then(|v| v.parse::<u64>().ok())
                    .unwrap_or(0);
                Ok(len)
            }
        }
    }
}

/// RAII guard that unregisters in-flight block entries and signals
/// waiters when the read scope ends. Running on Drop is what makes
/// single-flight robust against errors and panics: even if the
/// fetcher aborts mid-flight, every waiter gets unblocked and can
/// decide for itself (by consulting the block cache) whether the
/// fetch succeeded.
struct InFlightGuard<'a> {
    reader: &'a Reader,
    cells: &'a [(u64, BlockSignalHandle)],
}

impl<'a> Drop for InFlightGuard<'a> {
    fn drop(&mut self) {
        let mut in_flight = self
            .reader
            .in_flight
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        for (idx, cell) in self.cells {
            in_flight.remove(idx);
            cell.signal();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Covers the core single-flight semantics: a concurrent waiter
    /// that arrives after the fetcher but before it signals must
    /// return promptly when the signal fires. Uses a plain
    /// BlockSignal (no HTTP) to isolate the coordination primitive.
    #[tokio::test(flavor = "current_thread")]
    async fn block_signal_wakes_waiter() {
        let signal = Arc::new(BlockSignal::new());
        let s2 = signal.clone();
        let waiter = tokio::spawn(async move { s2.wait().await });
        // Yield so the waiter registers its Notified future before
        // the signal fires.
        tokio::task::yield_now().await;
        signal.signal();
        waiter.await.unwrap();
    }

    /// A signal that has already been fired before the waiter arrives
    /// must short-circuit through the `done` flag rather than hang on
    /// the missed notify_waiters call.
    #[tokio::test(flavor = "current_thread")]
    async fn block_signal_handles_pre_fired() {
        let signal = Arc::new(BlockSignal::new());
        signal.signal();
        signal.wait().await; // returns immediately
    }
}
