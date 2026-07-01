use std::path::PathBuf;

/// NASA Earthdata bearer-token credential for authenticating protected DAAC
/// reads.
///
/// The `token` (from `EARTHDATA_TOKEN`, resolved on the R side) is sent as
/// `Authorization: Bearer` to the NASA DAAC host. The DAAC validates it
/// server-side and 303-redirects straight to a presigned CloudFront/S3 URL;
/// the token is never forwarded to that presigned target. Tokens expire (NASA
/// EDL tokens last 60 days); an invalid or expired token yields a 401 at the
/// DAAC, surfaced as [`super::reader::IoError::AuthExpired`].
#[derive(Debug, Clone)]
pub struct EarthdataAuth {
    pub token: String,
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

    /// Create an HTTP source authenticated with a NASA Earthdata bearer token.
    ///
    /// The token is resolved from `EARTHDATA_TOKEN` on the R side and passed in
    /// explicitly, so the R session's view of the environment is the single
    /// source of truth (no independent env read here that could diverge from
    /// it).
    pub fn http_with_token(url: impl Into<String>, token: impl Into<String>) -> Self {
        DataSource::Http {
            url: url.into(),
            auth: Some(EarthdataAuth {
                token: token.into(),
            }),
        }
    }

    /// Create a local file source.
    pub fn local(path: impl Into<PathBuf>) -> Self {
        DataSource::Local { path: path.into() }
    }
}
