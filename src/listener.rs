use crate::autopush::{self, ConnectResult, ReregistrationInfo};
use crate::config::{self, Registration};
use crate::error::{AngelicAngelError, Result};
use crate::twitter;
use crate::webhook::{self, WebhookConfig, WebhookSender};
use std::path::Path;
use std::time::Duration;

/// How long to wait after the server sends close code 4774 (Firefox-compatible).
const SERVER_BACKOFF_SECS: u64 = 30 * 60;

/// Outcome of a single listen session.
///
/// Follows the design of Firefox's PushServiceWebSocket.sys.mjs, explicitly
/// distinguishing whether the connection was ever established. Firefox resets
/// _retryFailCount to 0 on any message receipt (including the hello response),
/// so we need to know if hello succeeded to mirror that behavior.
enum SessionOutcome {
    /// WebSocket closed normally (connection was established).
    NormalClose,
    /// Disconnected after a successful handshake (hello succeeded, then a WebSocket error).
    DisconnectedAfterConnect(AngelicAngelError),
    /// Failed before establishing a connection (hello never completed).
    ConnectionFailed(AngelicAngelError),
    /// Unrecoverable error (expired cookies, repeated UAID invalidation, broken config).
    Fatal(AngelicAngelError),
}

/// Computes an exponential backoff delay compatible with Firefox's implementation.
///
/// Firefox (PushServiceWebSocket.sys.mjs L408-431):
///   retryTimeout = retryBaseInterval * 2^retryFailCount
///   retryTimeout = min(retryTimeout, pingInterval)
///
/// - retryBaseInterval = 5s  (dom.push.retryBaseInterval = 5000)
/// - pingInterval      = 5m  (capped; Firefox uses 30m but we use a shorter ping interval)
fn calc_backoff(retry_count: u32) -> u64 {
    const RETRY_BASE_INTERVAL_SECS: u64 = 5;
    const PING_INTERVAL_SECS: u64 = 5 * 60;
    std::cmp::min(
        RETRY_BASE_INTERVAL_SECS.saturating_mul(2u64.saturating_pow(retry_count.saturating_sub(1))),
        PING_INTERVAL_SECS,
    )
}

struct ListenState {
    retry_count: u32,
    /// A fresh AutoPush registration whose Twitter registration hasn't succeeded yet.
    /// Kept across retries so a transient Twitter error doesn't create a new AutoPush
    /// subscription on every attempt.
    pending_reregistration: Option<ReregistrationInfo>,
}

/// Main listen loop with automatic reconnection.
///
/// Reconnection strategy mirrors Firefox (PushServiceWebSocket.sys.mjs):
/// - Reset retry counter on any successful message receipt (connection established).
/// - No upper limit on retry attempts (infinite retries), except for fatal errors.
/// - Exponential backoff: 5s * 2^n, capped at 5 minutes.
/// - Server-initiated backoff via close code 4774 delays reconnection for 30 minutes.
pub async fn listen(
    mut registration: Registration,
    config_path: &Path,
    webhook_config: WebhookConfig,
) -> Result<()> {
    let (sender, worker) = webhook::spawn(webhook_config)?;
    let mut state = ListenState {
        retry_count: 0,
        pending_reregistration: None,
    };

    let result = loop {
        match listen_once(&mut registration, config_path, &sender, &mut state).await {
            SessionOutcome::NormalClose => {
                state.retry_count = 0;
                tracing::info!("WebSocket connection closed, reconnecting");
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            SessionOutcome::DisconnectedAfterConnect(AngelicAngelError::Backoff)
            | SessionOutcome::ConnectionFailed(AngelicAngelError::Backoff) => {
                tracing::warn!("server requested backoff, delaying reconnect for 30 minutes");
                tokio::time::sleep(Duration::from_secs(SERVER_BACKOFF_SECS)).await;
            }
            SessionOutcome::DisconnectedAfterConnect(e) => {
                state.retry_count = 0;
                tracing::info!(error = %e, "disconnected after connect, reconnecting");
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            SessionOutcome::ConnectionFailed(e) => {
                state.retry_count += 1;
                let delay = calc_backoff(state.retry_count);
                tracing::warn!(
                    retry_count = state.retry_count,
                    delay_secs = delay,
                    error = %e,
                    "WebSocket connection failed, retrying"
                );
                tokio::time::sleep(Duration::from_secs(delay)).await;
            }
            SessionOutcome::Fatal(e) => {
                tracing::error!(error = %e, "fatal error, stopping listener");
                break Err(e);
            }
        }
    };

    // Let queued webhook deliveries finish (or reach the dead-letter file) before exiting.
    drop(sender);
    let _ = worker.await;
    result
}

/// Runs a single listen session: connect, receive notifications, return outcome.
async fn listen_once(
    registration: &mut Registration,
    config_path: &Path,
    sender: &WebhookSender,
    state: &mut ListenState,
) -> SessionOutcome {
    let mut client = match try_connect(registration, config_path, state).await {
        Ok(client) => client,
        Err(e) if e.is_fatal() => return SessionOutcome::Fatal(e),
        Err(e) => return SessionOutcome::ConnectionFailed(e),
    };

    tracing::info!("WebSocket connection established, listening for notifications");

    match run_notification_loop(&mut client, registration, sender).await {
        Ok(()) => SessionOutcome::NormalClose,
        Err(e) if e.is_fatal() => SessionOutcome::Fatal(e),
        Err(e) => SessionOutcome::DisconnectedAfterConnect(e),
    }
}

/// Establishes a connection to AutoPush, handling UAID invalidation transparently.
async fn try_connect(
    registration: &mut Registration,
    config_path: &Path,
    state: &mut ListenState,
) -> Result<autopush::AutoPushClient> {
    let reregistration = match state.pending_reregistration.take() {
        Some(pending) => {
            tracing::info!("retrying Twitter registration for pending AutoPush subscription");
            pending
        }
        None => match autopush::connect_and_listen(&registration.autopush).await? {
            ConnectResult::Connected(client) => {
                tracing::info!("connected with existing session");
                return Ok(client);
            }
            ConnectResult::NeedsReregistration(info) => info,
        },
    };

    let new_reg = &reregistration.registration;
    tracing::warn!(
        new_uaid = %new_reg.uaid,
        new_channel_id = %new_reg.channel_id,
        "UAID invalidated (pushsubscriptionchange), re-registering"
    );

    let subscription = crate::push::PushSubscription {
        endpoint: new_reg.endpoint.clone(),
        autopush: config::AutoPushSession {
            uaid: new_reg.uaid.clone(),
            channel_id: new_reg.channel_id.clone(),
        },
        keys: reregistration.keys.clone(),
    };

    let mut full_config = config::Config::load(config_path)?;

    tracing::info!("re-registering with Twitter API");
    if let Err(e) = twitter::register(&full_config.twitter, &subscription).await {
        if !e.is_fatal() {
            state.pending_reregistration = Some(reregistration);
        }
        return Err(e);
    }
    tracing::info!("Twitter API re-registration complete");

    registration.endpoint = subscription.endpoint;
    registration.autopush = subscription.autopush;
    registration.keys = subscription.keys;
    full_config.registration = Some(registration.clone());
    full_config.save(config_path)?;
    tracing::info!("saved updated registration");

    match autopush::connect_and_listen(&registration.autopush).await? {
        ConnectResult::Connected(client) => Ok(client),
        ConnectResult::NeedsReregistration(_) => Err(AngelicAngelError::Reregistration(
            "UAID was invalidated again right after re-registration".to_string(),
        )),
    }
}

/// Receives and processes notifications in a loop until the connection drops.
///
/// A notification is ACKed once it has been decrypted and handed to the webhook
/// worker (or written to the dead-letter file); webhook retries happen in the background.
async fn run_notification_loop(
    client: &mut autopush::AutoPushClient,
    registration: &Registration,
    sender: &WebhookSender,
) -> Result<()> {
    while let Some(notification) = client.next_notification().await? {
        let ack_code = match notification.data {
            Some(ref data) => {
                match decrypt_notification(data, &notification.headers, &registration.keys) {
                    Ok(payload) => {
                        sender.enqueue(payload).await?;
                        autopush::AckCode::Delivered
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "decryption error, sending ACK with decryption_error");
                        autopush::AckCode::DecryptionError
                    }
                }
            }
            None => {
                tracing::info!("empty notification (no data)");
                autopush::AckCode::Delivered
            }
        };

        client
            .ack_notification(
                notification.channel_id.clone(),
                notification.version.clone(),
                ack_code,
            )
            .await?;
        tracing::debug!(
            channel_id = %notification.channel_id,
            ack_code = ?ack_code,
            "ACK sent"
        );
    }

    Ok(())
}

/// Decrypts a notification payload and parses it as JSON (non-JSON text is wrapped
/// as `{"raw": "..."}`).
fn decrypt_notification(
    data: &str,
    headers: &Option<std::collections::HashMap<String, String>>,
    keys: &crate::config::WebPushKeys,
) -> Result<serde_json::Value> {
    let encrypted = base64::Engine::decode(&base64::engine::general_purpose::URL_SAFE_NO_PAD, data)
        .map_err(|e| AngelicAngelError::Decryption(format!("base64 decode failed: {}", e)))?;

    tracing::debug!(encrypted_size = encrypted.len(), "decoding encrypted message");

    if let Some(hdrs) = headers {
        tracing::debug!(?hdrs, "notification headers");
    }

    let decrypted = match decrypt_ece(&encrypted, headers, keys) {
        Ok(decrypted) => {
            tracing::debug!(decrypted_size = decrypted.len(), "ECE decryption succeeded");
            decrypted
        }
        Err(e) => {
            tracing::error!(
                error = %e,
                encrypted_head = ?&encrypted[..encrypted.len().min(16)],
                "ECE decryption failed"
            );
            return Err(e);
        }
    };

    let text = String::from_utf8(decrypted)
        .map_err(|e| AngelicAngelError::Decryption(format!("UTF-8 conversion failed: {}", e)))?;

    let payload: serde_json::Value =
        serde_json::from_str(&text).unwrap_or_else(|_| serde_json::json!({ "raw": text }));

    tracing::info!(payload = %payload, "notification decrypted");
    Ok(payload)
}

fn decrypt_ece(
    encrypted: &[u8],
    headers: &Option<std::collections::HashMap<String, String>>,
    keys: &crate::config::WebPushKeys,
) -> Result<Vec<u8>> {
    tracing::debug!(
        private_key_len = keys.private_key.len(),
        public_key_len = keys.public_key.len(),
        auth_secret_len = keys.auth_secret.len(),
        encrypted_len = encrypted.len(),
        "starting ECE decryption"
    );

    let key_pair = ece::EcKeyComponents::new(keys.private_key.clone(), keys.public_key.clone());

    let encoding = headers
        .as_ref()
        .and_then(|h| h.get("encoding"))
        .map(|s| s.as_str());

    match encoding {
        Some("aesgcm") => {
            tracing::debug!("decrypting with aesgcm encoding");
            decrypt_aesgcm(encrypted, headers, &key_pair, &keys.auth_secret)
        }
        Some("aes128gcm") | None => {
            tracing::debug!("decrypting with aes128gcm encoding");
            let decrypted = ece::decrypt(&key_pair, &keys.auth_secret, encrypted).map_err(|e| {
                tracing::debug!(error = %e, "aes128gcm decryption error");
                AngelicAngelError::Decryption(format!("aes128gcm decryption failed: {}", e))
            })?;

            tracing::debug!(decrypted_len = decrypted.len(), "aes128gcm decryption succeeded");
            Ok(decrypted)
        }
        Some(other) => Err(AngelicAngelError::Decryption(format!(
            "unsupported encoding: {}",
            other
        ))),
    }
}

fn decrypt_aesgcm(
    encrypted: &[u8],
    headers: &Option<std::collections::HashMap<String, String>>,
    key_pair: &ece::EcKeyComponents,
    auth_secret: &[u8],
) -> Result<Vec<u8>> {
    let headers = headers.as_ref().ok_or_else(|| {
        AngelicAngelError::Decryption("aesgcm encoding requires headers but none were provided".to_string())
    })?;

    let dh_b64 = parse_header_param(headers.get("crypto_key"), "dh")?;
    let sender_public_key =
        base64::Engine::decode(&base64::engine::general_purpose::URL_SAFE_NO_PAD, &dh_b64)
            .map_err(|e| {
                AngelicAngelError::Decryption(format!("failed to base64-decode sender public key: {}", e))
            })?;

    tracing::debug!(sender_public_key_len = sender_public_key.len(), "parsed sender public key");

    let salt_b64 = parse_header_param(headers.get("encryption"), "salt")?;
    let salt = base64::Engine::decode(&base64::engine::general_purpose::URL_SAFE_NO_PAD, &salt_b64)
        .map_err(|e| AngelicAngelError::Decryption(format!("failed to base64-decode salt: {}", e)))?;

    tracing::debug!(salt_len = salt.len(), "parsed salt");

    let block = ece::legacy::AesGcmEncryptedBlock::new(
        &sender_public_key,
        &salt,
        4096,
        encrypted.to_vec(),
    )
    .map_err(|e| AngelicAngelError::Decryption(format!("failed to construct AesGcmEncryptedBlock: {}", e)))?;

    let decrypted = ece::legacy::decrypt_aesgcm(key_pair, auth_secret, &block).map_err(|e| {
        tracing::debug!(error = %e, "aesgcm decryption error");
        AngelicAngelError::Decryption(format!("aesgcm decryption failed: {}", e))
    })?;

    tracing::debug!(decrypted_len = decrypted.len(), "aesgcm decryption succeeded");
    Ok(decrypted)
}

/// Extracts a named parameter from a semicolon-delimited header value.
///
/// Example: given `"dh=abc123;p256ecdsa=xyz"` and param `"dh"`, returns `"abc123"`.
fn parse_header_param(header_value: Option<&String>, param_name: &str) -> Result<String> {
    let header = header_value.ok_or_else(|| {
        AngelicAngelError::Decryption(format!("missing required header for param '{}'", param_name))
    })?;

    for part in header.split(';') {
        let part = part.trim();
        if let Some(value) = part.strip_prefix(&format!("{}=", param_name)) {
            return Ok(value.to_string());
        }
    }

    Err(AngelicAngelError::Decryption(format!(
        "param '{}' not found in header: {}",
        param_name, header
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine;
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;

    #[test]
    fn backoff_matches_firefox_schedule() {
        assert_eq!(calc_backoff(1), 5);
        assert_eq!(calc_backoff(2), 10);
        assert_eq!(calc_backoff(3), 20);
        assert_eq!(calc_backoff(7), 300);
        assert_eq!(calc_backoff(100), 300);
    }

    #[test]
    fn header_param_parsing() {
        let h = "dh=abc123;p256ecdsa=xyz".to_string();
        assert_eq!(parse_header_param(Some(&h), "dh").unwrap(), "abc123");
        assert_eq!(parse_header_param(Some(&h), "p256ecdsa").unwrap(), "xyz");
        assert!(parse_header_param(Some(&h), "salt").is_err());
        assert!(parse_header_param(None, "dh").is_err());
    }

    fn encrypt_for(keys: &crate::config::WebPushKeys, message: &[u8]) -> String {
        let encrypted = ece::encrypt(&keys.public_key, &keys.auth_secret, message).unwrap();
        URL_SAFE_NO_PAD.encode(encrypted)
    }

    #[test]
    fn decrypts_aes128gcm_with_generated_keys() {
        let keys = crate::push::generate_keys();
        let data = encrypt_for(&keys, br#"{"title":"hello","body":"world"}"#);

        let payload = decrypt_notification(&data, &None, &keys).unwrap();
        assert_eq!(payload, serde_json::json!({"title": "hello", "body": "world"}));
    }

    #[test]
    fn non_json_payload_is_wrapped() {
        let keys = crate::push::generate_keys();
        let data = encrypt_for(&keys, b"plain text");

        let payload = decrypt_notification(&data, &None, &keys).unwrap();
        assert_eq!(payload, serde_json::json!({"raw": "plain text"}));
    }

    #[test]
    fn wrong_keys_fail_as_decryption_error() {
        let keys = crate::push::generate_keys();
        let other = crate::push::generate_keys();
        let data = encrypt_for(&keys, b"secret");

        assert!(matches!(
            decrypt_notification(&data, &None, &other),
            Err(AngelicAngelError::Decryption(_))
        ));
    }
}
