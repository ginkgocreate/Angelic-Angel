use thiserror::Error;

#[derive(Error, Debug)]
pub enum AngelicAngelError {
    #[error("config error: {0}")]
    Config(String),

    #[error("AutoPush error: {0}")]
    AutoPush(String),

    /// The AutoPush server asked us to back off (WebSocket close code 4774).
    #[error("AutoPush server requested backoff (close code 4774)")]
    Backoff,

    #[error("Twitter API error: {0}")]
    TwitterApi(String),

    /// Twitter rejected our credentials (HTTP 401/403). Retrying won't help;
    /// the user needs to refresh `auth_token` / `ct0`.
    #[error("Twitter authentication failed: {0}")]
    TwitterAuth(String),

    /// The server invalidated our UAID again right after re-registration.
    #[error("re-registration failed: {0} (run `angelic-angel register` again)")]
    Reregistration(String),

    #[error("webhook error: {0}")]
    Webhook(String),

    /// Boxed: `tungstenite::Error` is large and would bloat every `Result`.
    #[error("WebSocket error: {0}")]
    WebSocket(Box<tokio_tungstenite::tungstenite::Error>),

    #[error("decryption error: {0}")]
    Decryption(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("TOML error: {0}")]
    Toml(#[from] toml::de::Error),

    #[error("base64 decode error: {0}")]
    Base64Decode(#[from] base64::DecodeError),
}

impl From<tokio_tungstenite::tungstenite::Error> for AngelicAngelError {
    fn from(e: tokio_tungstenite::tungstenite::Error) -> Self {
        AngelicAngelError::WebSocket(Box::new(e))
    }
}

impl AngelicAngelError {
    /// Errors that won't go away by reconnecting; the listener should stop.
    pub fn is_fatal(&self) -> bool {
        matches!(
            self,
            AngelicAngelError::TwitterAuth(_) | AngelicAngelError::Reregistration(_)
        )
    }
}

pub type Result<T> = std::result::Result<T, AngelicAngelError>;
