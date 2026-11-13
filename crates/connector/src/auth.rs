use base64::Engine;
use serde::Deserialize;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Default)]
pub struct Credentials {
    /// e.g. "https://sts.test.nordpoolgroup.com/connect/token" — get from Nord Pool docs.
    pub token_url: String,
    /// client_id for the binary intraday PMD API (from "Client Strings and Scopes").
    pub client_id: String,
    /// client_secret paired with the client_id.
    pub client_secret: String,
    /// Scope for the binary intraday PMD API.
    pub scope: String,
    /// Service-provider-issued username.
    pub username: String,
    /// Service-provider-issued password.
    pub password: String,
}

impl Credentials {
    fn basic_auth_header(&self) -> String {
        let raw = format!("{}:{}", self.client_id, self.client_secret);
        let encoded = base64::engine::general_purpose::STANDARD.encode(raw);
        format!("Basic {encoded}")
    }
}

#[derive(Debug, Deserialize)]
struct TokenResponse {
    access_token: String,
    expires_in: u64,
    // Always "Bearer" per the OAuth2 spec; we never branch on it, but keep it modeled to
    // document the response shape. allow(dead_code) silences the never-read warning.
    #[allow(dead_code)]
    token_type: String,
}

#[derive(Debug, Clone)]
pub struct Token {
    pub access_token: String,
    pub expires_at: Instant,
}

impl Token {
    pub fn needs_refresh(&self) -> bool {
        let margin = crate::config::get().connector.token_refresh_margin;
        Instant::now() + margin >= self.expires_at
    }
}

/// Source of bearer tokens. `Client` is generic over this so production uses real
/// `Credentials` (which hits Nord Pool's SSO) while tests can supply a fake.
pub trait TokenSource {
    fn fetch(&self) -> Result<Token, AuthError>;
}

impl TokenSource for Credentials {
    fn fetch(&self) -> Result<Token, AuthError> {
        fetch_token(self)
    }
}

/// Fetch a new bearer token using the OAuth2 password grant.
pub fn fetch_token(creds: &Credentials) -> Result<Token, AuthError> {
    tracing::debug!(url = %creds.token_url, scope = %creds.scope, "requesting token");

    let form = [
        ("grant_type", "password"),
        ("scope", creds.scope.as_str()),
        ("username", creds.username.as_str()),
        ("password", creds.password.as_str()),
    ];

    let response = ureq::post(&creds.token_url)
        .timeout(crate::config::get().connector.token_http_timeout)
        .set("Authorization", &creds.basic_auth_header())
        .set("Content-Type", "application/x-www-form-urlencoded")
        .send_form(&form)
        .map_err(|e| AuthError::Http(e.to_string()))?;

    let parsed: TokenResponse = response
        .into_json()
        .map_err(|e| AuthError::Body(e.to_string()))?;
    tracing::info!(expires_in_s = parsed.expires_in, "token acquired");
    Ok(Token {
        access_token: parsed.access_token,
        expires_at: Instant::now() + Duration::from_secs(parsed.expires_in),
    })
}

#[derive(Debug, thiserror::Error)]
pub enum AuthError {
    #[error("token request failed: {0}")]
    Http(String),
    #[error("token response decode failed: {0}")]
    Body(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};

    fn sample_creds() -> Credentials {
        Credentials {
            token_url: "https://example.test/token".into(),
            client_id: "client_intraday_api".into(),
            client_secret: "1xB9Ik1xsEu2nbwVa1BR".into(), // Credentials from nordpool documentation.
            scope: "intraday_api".into(),
            username: "user".into(),
            password: "pass".into(),
        }
    }

    #[test]
    fn token_needs_refresh_when_close_to_expiry() {
        let token = Token {
            access_token: "tok".into(),
            // 10 seconds from now — well inside the 60s safety margin.
            expires_at: Instant::now() + Duration::from_secs(10),
        };
        assert!(token.needs_refresh());
    }

    #[test]
    fn token_does_not_need_refresh_when_far_from_expiry() {
        let token = Token {
            access_token: "tok".into(),
            // Comfortably outside the 60s safety margin.
            expires_at: Instant::now() + Duration::from_secs(3600),
        };
        assert!(!token.needs_refresh());
    }

    #[test]
    fn basic_auth_header_matches_documented_base64_for_intraday_client() {
        // From Nord Pool's "Client Authorization Strings and Scopes" page.
        let expected = "Basic Y2xpZW50X2ludHJhZGF5X2FwaToxeEI5SWsxeHNFdTJuYndWYTFCUg==";
        assert_eq!(sample_creds().basic_auth_header(), expected);
    }
}
