use std::path::PathBuf;

/// NASA Earthdata credentials for Basic auth in the OAuth redirect flow.
#[derive(Debug, Clone)]
pub struct EarthdataAuth {
    pub username: String,
    pub password: String,
}

/// Describes where an HDF5 file is located.
#[derive(Debug, Clone)]
pub enum DataSource {
    /// Remote file accessible via HTTPS with Range request support.
    Http {
        url: String,
        /// Optional Earthdata credentials for the OAuth redirect flow.
        auth: Option<EarthdataAuth>,
    },
    /// Local file on disk (for testing and local fallback).
    Local { path: PathBuf },
}

impl DataSource {
    /// Create an HTTP source from a URL.
    pub fn http(url: impl Into<String>) -> Self {
        DataSource::Http {
            url: url.into(),
            auth: None,
        }
    }

    /// Create an HTTP source with Earthdata credentials.
    pub fn http_with_auth(
        url: impl Into<String>,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        DataSource::Http {
            url: url.into(),
            auth: Some(EarthdataAuth {
                username: username.into(),
                password: password.into(),
            }),
        }
    }

    /// Create a local file source.
    pub fn local(path: impl Into<PathBuf>) -> Self {
        DataSource::Local { path: path.into() }
    }
}
