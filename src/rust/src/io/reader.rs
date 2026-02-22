use crate::io::cache::BlockCache;
use crate::io::source::DataSource;
use std::ops::Range;
use std::sync::Mutex;
use std::time::Duration;
use thiserror::Error;

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

/// HTTP status codes that warrant a retry.
fn is_retriable(status: u16) -> bool {
    matches!(status, 429 | 500 | 502 | 503 | 504)
}

/// Configuration for the I/O reader.
#[derive(Debug, Clone)]
pub struct ReaderConfig {
    /// Block size for cache alignment (default: 256 KiB).
    pub block_size: usize,
    /// Maximum number of blocks to keep in the LRU cache (default: 512 = 128 MiB).
    pub max_cache_blocks: usize,
}

impl Default for ReaderConfig {
    fn default() -> Self {
        Self {
            block_size: 256 * 1024,    // 256 KiB
            max_cache_blocks: 512,      // 128 MiB total cache
        }
    }
}

/// The main I/O reader that handles byte-range fetches with caching.
///
/// All reads are aligned to block boundaries and cached in an LRU cache.
/// Adjacent block fetches are coalesced into single HTTP requests.
pub struct Reader {
    source: DataSource,
    client: reqwest::Client,
    cache: Mutex<BlockCache>,
    config: ReaderConfig,
}

impl Reader {
    /// Create a new reader for the given data source.
    pub fn new(source: DataSource, config: ReaderConfig) -> Self {
        let cache = BlockCache::new(config.block_size, config.max_cache_blocks);
        Self {
            source,
            client: reqwest::Client::new(),
            cache: Mutex::new(cache),
            config,
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
                use std::io::{Read, Seek, SeekFrom};
                let mut file = std::fs::File::open(path)?;
                file.seek(SeekFrom::Start(offset))?;
                let mut buf = vec![0u8; length];
                file.read_exact(&mut buf)?;
                Ok(buf)
            }
            DataSource::Http { .. } => self.read_cached(offset, length).await,
        }
    }

    /// Read with block-level caching for HTTP sources.
    async fn read_cached(&self, offset: u64, length: usize) -> Result<Vec<u8>, IoError> {
        let block_size = self.config.block_size;

        // Determine which blocks we need and which are already cached
        let missing_blocks = {
            let mut cache = self.cache.lock().unwrap();
            let (_cached, missing) = cache.query(offset, length);
            missing
        };

        // Fetch missing blocks (coalesced into minimal range requests)
        if !missing_blocks.is_empty() {
            let ranges = {
                let cache = self.cache.lock().unwrap();
                cache.coalesce_ranges(missing_blocks.clone())
            };

            for range in ranges {
                let data = self.fetch_range(range.clone()).await?;

                // Split the fetched data back into individual blocks for caching
                let range_start_block = range.start / block_size as u64;
                let mut cache = self.cache.lock().unwrap();

                let mut pos = 0;
                let mut block_idx = range_start_block;
                while pos < data.len() {
                    let chunk_len = block_size.min(data.len() - pos);
                    cache.insert(block_idx, data[pos..pos + chunk_len].to_vec());
                    pos += chunk_len;
                    block_idx += 1;
                }
            }
        }

        // Now all blocks should be in cache -- extract the requested range
        let mut cache = self.cache.lock().unwrap();
        let start_block = offset / block_size as u64;
        let end_block = (offset + length as u64).saturating_sub(1) / block_size as u64;

        let mut all_blocks: Vec<(u64, Vec<u8>)> = Vec::new();
        for idx in start_block..=end_block {
            if let Some(data) = cache.get(&idx) {
                all_blocks.push((idx, data.clone()));
            }
        }

        let block_refs: Vec<(u64, &[u8])> = all_blocks
            .iter()
            .map(|(idx, data)| (*idx, data.as_slice()))
            .collect();

        Ok(cache.extract_range(offset, length, &block_refs))
    }

    /// Fetch a raw byte range from the HTTP source with retry logic.
    ///
    /// Retries up to [`MAX_RETRIES`] times with exponential backoff for
    /// transient server errors (429, 500, 502, 503, 504).
    async fn fetch_range(&self, range: Range<u64>) -> Result<Vec<u8>, IoError> {
        let (url, bearer_token) = match &self.source {
            DataSource::Http { url, bearer_token } => (url.as_str(), bearer_token.as_deref()),
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
                    url,
                    range_header
                );
                tokio::time::sleep(delay).await;
            } else {
                log::debug!("HTTP GET {} Range: {}", url, range_header);
            }

            let mut request = self.client.get(url).header("Range", &range_header);

            if let Some(token) = bearer_token {
                request = request.bearer_auth(token);
            }

            let response = match request.send().await {
                Ok(r) => r,
                Err(e) if attempt < MAX_RETRIES => {
                    last_error = Some(IoError::Http(e));
                    continue;
                }
                Err(e) => return Err(IoError::Http(e)),
            };

            let status = response.status();

            if status == reqwest::StatusCode::RANGE_NOT_SATISFIABLE {
                return Err(IoError::RangeNotSupported);
            }

            if is_retriable(status.as_u16()) && attempt < MAX_RETRIES {
                last_error = Some(IoError::HttpStatus {
                    status: status.as_u16(),
                    url: url.to_string(),
                });
                continue;
            }

            if !status.is_success() && status != reqwest::StatusCode::PARTIAL_CONTENT {
                return Err(IoError::HttpStatus {
                    status: status.as_u16(),
                    url: url.to_string(),
                });
            }

            return Ok(response.bytes().await?.to_vec());
        }

        Err(last_error.unwrap_or(IoError::HttpStatus {
            status: 500,
            url: url.to_string(),
        }))
    }

    /// Read bytes and return as a fixed-size array (convenience for parsing headers).
    pub async fn read_exact<const N: usize>(&self, offset: u64) -> Result<[u8; N], IoError> {
        let data = self.read(offset, N).await?;
        let mut arr = [0u8; N];
        arr.copy_from_slice(&data[..N]);
        Ok(arr)
    }

    /// Get the total file size via a HEAD request (for HTTP) or file metadata (for local).
    pub async fn file_size(&self) -> Result<u64, IoError> {
        match &self.source {
            DataSource::Local { path } => {
                let metadata = std::fs::metadata(path)?;
                Ok(metadata.len())
            }
            DataSource::Http { url, bearer_token } => {
                let mut request = self.client.head(url);
                if let Some(token) = bearer_token {
                    request = request.bearer_auth(token);
                }
                let response = request.send().await?;
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
