//! Guards and retries for reads made from an R call.

use crate::products::common::GroupData;

/// Ignores SIGPIPE while Rust code runs inside an R call, restoring the
/// previous disposition on drop.
///
/// R installs a SIGPIPE handler that raises an R error ("ignoring SIGPIPE
/// signal"), which longjmps out of the signal handler. A vectored write
/// (`writev`) to a socket the server has closed raises SIGPIPE, so a
/// dropped connection mid-read would unwind straight through the tokio
/// runtime: the whole read is lost instead of the request being retried,
/// and the runtime's thread-local context guard never runs, so every
/// later call in the session fails with "Cannot start a runtime from
/// within a runtime". With SIGPIPE ignored the write returns EPIPE, which
/// surfaces as an ordinary HTTP error and goes through the retry path.
pub struct SigpipeGuard {
    #[cfg(unix)]
    previous: libc::sigaction,
}

impl SigpipeGuard {
    pub fn new() -> Self {
        #[cfg(unix)]
        unsafe {
            let mut ignore: libc::sigaction = std::mem::zeroed();
            ignore.sa_sigaction = libc::SIG_IGN;
            libc::sigemptyset(&mut ignore.sa_mask);
            let mut previous: libc::sigaction = std::mem::zeroed();
            libc::sigaction(libc::SIGPIPE, &ignore, &mut previous);
            SigpipeGuard { previous }
        }
        #[cfg(not(unix))]
        SigpipeGuard {}
    }
}

impl Drop for SigpipeGuard {
    fn drop(&mut self) {
        #[cfg(unix)]
        unsafe {
            libc::sigaction(libc::SIGPIPE, &self.previous, std::ptr::null_mut());
        }
    }
}

/// Error text of a granule read that retrying cannot fix.
pub fn is_permanent_failure(e: &str) -> bool {
    e.contains("rejected the bearer token") || e.contains("Local file I/O")
}

/// Number of further passes over the granules that failed.
pub const GRANULE_RETRY_PASSES: u32 = 2;

/// Read many granules with bounded concurrency, then retry the ones that
/// failed (two at a time, after a pause) before giving up on them.
/// Returns the groups read and, for each granule that still failed, its
/// URL and last error.
pub async fn read_granules<F, Fut>(urls: &[String], read: F) -> (Vec<GroupData>, Vec<(String, String)>)
where
    F: Fn(String) -> Fut,
    Fut: std::future::Future<Output = Result<Vec<GroupData>, String>>,
{
    use futures::stream::{self, StreamExt};

    let mut all_groups = Vec::new();
    let mut pending: Vec<String> = urls.to_vec();
    let mut failed: Vec<(String, String)> = Vec::new();

    for pass in 0..=GRANULE_RETRY_PASSES {
        if pending.is_empty() {
            break;
        }
        if pass > 0 {
            log::warn!("Retrying {} granule(s) that failed (pass {})", pending.len(), pass);
            tokio::time::sleep(std::time::Duration::from_secs(2u64.pow(pass))).await;
        }
        // Up from 4. Multi-granule reads are dominated by HTTP round-trips;
        // bumping concurrency lets us saturate NASA DAAC endpoints more
        // completely on fast connections. Retries run two at a time.
        let width = if pass == 0 { 8 } else { 2 };
        let batch: Vec<String> = pending.drain(..).collect();
        let results: Vec<(String, Result<Vec<GroupData>, String>)> = stream::iter(batch)
            .map(|url| {
                let fut = read(url.clone());
                async move { (url, fut.await) }
            })
            .buffer_unordered(width)
            .collect()
            .await;

        // A granule that failed before and is not retried again keeps its
        // entry; those retried are re-entered below if they fail again.
        failed.retain(|(_, e)| is_permanent_failure(e));
        for (url, result) in results {
            match result {
                Ok(groups) => all_groups.extend(groups),
                Err(e) => {
                    log::warn!("File read failed: {}", e);
                    if !is_permanent_failure(&e) {
                        pending.push(url.clone());
                    }
                    failed.push((url, e));
                }
            }
        }
    }
    (all_groups, failed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Mutex;

    fn group(name: &str) -> GroupData {
        GroupData {
            group_name: name.to_string(),
            columns: HashMap::new(),
            selected_indices: Vec::new(),
            pool_columns: HashMap::new(),
        }
    }

    /// SIGPIPE is ignored while the guard lives and the previous handler
    /// is back once it drops.
    #[cfg(unix)]
    #[test]
    fn sigpipe_guard_ignores_then_restores() {
        unsafe {
            let current = || {
                let mut a: libc::sigaction = std::mem::zeroed();
                libc::sigaction(libc::SIGPIPE, std::ptr::null(), &mut a);
                a.sa_sigaction
            };
            let before = current();
            {
                let _g = SigpipeGuard::new();
                assert_eq!(current(), libc::SIG_IGN);
            }
            assert_eq!(current(), before);
        }
    }

    /// A granule that fails once is read on the retry; one that keeps
    /// failing is reported with its last error; a permanent failure is
    /// tried once only.
    #[tokio::test(flavor = "current_thread")]
    async fn read_granules_retries_transient_failures() {
        let calls: Mutex<HashMap<String, usize>> = Mutex::new(HashMap::new());
        let urls = vec!["ok".to_string(), "flaky".to_string(), "down".to_string(), "auth".to_string()];
        let (groups, failed) = read_granules(&urls, |url| {
            let n = {
                let mut c = calls.lock().unwrap();
                let e = c.entry(url.clone()).or_insert(0);
                *e += 1;
                *e
            };
            async move {
                match (url.as_str(), n) {
                    ("ok", _) | ("flaky", 2) => Ok(vec![group(&url)]),
                    ("auth", _) => Err("NASA Earthdata rejected the bearer token (HTTP 401)".to_string()),
                    _ => Err(format!("HTTP request failed: attempt {}", n)),
                }
            }
        })
        .await;
        let mut names: Vec<_> = groups.iter().map(|g| g.group_name.clone()).collect();
        names.sort();
        assert_eq!(names, vec!["flaky", "ok"]);
        let mut failed_urls: Vec<_> = failed.iter().map(|(u, _)| u.clone()).collect();
        failed_urls.sort();
        assert_eq!(failed_urls, vec!["auth", "down"]);
        let down = failed.iter().find(|(u, _)| u == "down").unwrap();
        assert!(down.1.ends_with(&format!("attempt {}", GRANULE_RETRY_PASSES + 1)));
        let c = calls.lock().unwrap();
        assert_eq!(c["auth"], 1);
        assert_eq!(c["down"], GRANULE_RETRY_PASSES as usize + 1);
    }
}
