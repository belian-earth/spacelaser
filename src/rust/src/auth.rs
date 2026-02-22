//! NASA Earthdata Login authentication.
//!
//! Exchanges username/password for a bearer token via the EDL token API.
//! Tokens are valid for 60 days; each user can have at most 2 active tokens.

use reqwest::Client;

const TOKEN_CREATE_URL: &str = "https://urs.earthdata.nasa.gov/api/users/token";
const TOKEN_LIST_URL: &str = "https://urs.earthdata.nasa.gov/api/users/tokens";

/// Obtain a bearer token from the Earthdata Login token API.
///
/// Tries to create a new token first. If that fails (e.g. the user already has
/// 2 active tokens), falls back to listing existing tokens and returning the
/// first one.
pub async fn fetch_earthdata_token(username: &str, password: &str) -> Result<String, String> {
    let client = Client::builder()
        .build()
        .map_err(|e| format!("HTTP client error: {e}"))?;

    // Try to create a new token (POST /api/users/token)
    let resp = client
        .post(TOKEN_CREATE_URL)
        .basic_auth(username, Some(password))
        .send()
        .await
        .map_err(|e| format!("Token request failed: {e}"))?;

    if resp.status().is_success() {
        let text = resp
            .text()
            .await
            .map_err(|e| format!("Failed to read token response: {e}"))?;
        let body: serde_json::Value = serde_json::from_str(&text)
            .map_err(|e| format!("Failed to parse token response: {e}"))?;
        if let Some(token) = body.get("access_token").and_then(|v| v.as_str()) {
            return Ok(token.to_string());
        }
    } else if resp.status() == reqwest::StatusCode::UNAUTHORIZED {
        return Err("Invalid Earthdata username or password.".to_string());
    }

    // Creation may have failed because the user already has 2 tokens.
    // List existing tokens (GET /api/users/tokens) and return the first.
    let resp = client
        .get(TOKEN_LIST_URL)
        .basic_auth(username, Some(password))
        .send()
        .await
        .map_err(|e| format!("Token list request failed: {e}"))?;

    if resp.status() == reqwest::StatusCode::UNAUTHORIZED {
        return Err("Invalid Earthdata username or password.".to_string());
    }

    if !resp.status().is_success() {
        return Err(format!(
            "Earthdata token API returned HTTP {}.",
            resp.status().as_u16()
        ));
    }

    let text = resp
        .text()
        .await
        .map_err(|e| format!("Failed to read token list: {e}"))?;
    let body: serde_json::Value = serde_json::from_str(&text)
        .map_err(|e| format!("Failed to parse token list: {e}"))?;

    if let Some(arr) = body.as_array() {
        if let Some(first) = arr.first() {
            if let Some(token) = first.get("access_token").and_then(|v| v.as_str()) {
                return Ok(token.to_string());
            }
        }
    }

    Err(
        "No active tokens found and could not create one. Check your Earthdata credentials."
            .to_string(),
    )
}
