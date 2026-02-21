use std::path::PathBuf;

/// Describes where an HDF5 file is located.
#[derive(Debug, Clone)]
pub enum DataSource {
    /// Remote file accessible via HTTPS with Range request support.
    Http {
        url: String,
        /// Optional bearer token for NASA Earthdata authentication.
        bearer_token: Option<String>,
    },
    /// Local file on disk (for testing and local fallback).
    Local { path: PathBuf },
}

impl DataSource {
    /// Create an HTTP source from a URL.
    pub fn http(url: impl Into<String>) -> Self {
        DataSource::Http {
            url: url.into(),
            bearer_token: None,
        }
    }

    /// Create an HTTP source with a bearer token for authentication.
    pub fn http_with_token(url: impl Into<String>, token: impl Into<String>) -> Self {
        DataSource::Http {
            url: url.into(),
            bearer_token: Some(token.into()),
        }
    }

    /// Create a local file source.
    pub fn local(path: impl Into<PathBuf>) -> Self {
        DataSource::Local { path: path.into() }
    }
}
