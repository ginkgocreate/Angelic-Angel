//! Webhook delivery with retries and a dead-letter file.
//!
//! The listener hands decrypted payloads to a background worker through a bounded
//! queue, so a slow or failing webhook never blocks the WebSocket (which must keep
//! answering pings). Payloads that still fail after all retries are appended to a
//! JSON Lines dead-letter file and can be re-sent later with `angelic-angel replay`.
//!
//! Notifications are ACKed to AutoPush once queued, so on shutdown the worker writes
//! whatever is still queued (and the in-flight payload) to the dead-letter file.
//! Only a hard kill (SIGKILL, power loss) can lose queued payloads.

use crate::error::{AngelicAngelError, Result};
use reqwest::{Client, StatusCode};
use serde_json::{Value, json};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::AsyncWriteExt;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;

/// Maximum payloads waiting for delivery before new ones go straight to the dead-letter file.
const QUEUE_CAPACITY: usize = 1000;
const REQUEST_TIMEOUT_SECS: u64 = 10;
const DEFAULT_MAX_ATTEMPTS: u32 = 8;
const RETRY_BASE_MS: u64 = 1000;
const RETRY_CAP_MS: u64 = 60_000;
/// On shutdown, how long the worker may keep delivering normally before the rest of
/// the queue is written to the dead-letter file.
const SHUTDOWN_GRACE_SECS: u64 = 5;

#[derive(Debug, Clone)]
pub struct WebhookConfig {
    pub url: String,
    /// Sent as `Authorization: Bearer <token>` when set (WEBHOOK_BEARER_TOKEN).
    pub bearer_token: Option<String>,
    pub dead_letter_path: PathBuf,
    pub max_attempts: u32,
    pub retry_base: Duration,
}

impl WebhookConfig {
    /// Reads settings from the environment:
    /// - `WEBHOOK_ENDPOINT` (required)
    /// - `WEBHOOK_BEARER_TOKEN` (optional)
    /// - `WEBHOOK_DEAD_LETTER` (optional, default `<config>.failed.jsonl`)
    /// - `WEBHOOK_MAX_ATTEMPTS` (optional, default 8)
    pub fn from_env(config_path: &Path) -> Result<Self> {
        let url = std::env::var("WEBHOOK_ENDPOINT").map_err(|_| {
            AngelicAngelError::Config(
                "WEBHOOK_ENDPOINT environment variable is not set".to_string(),
            )
        })?;
        let bearer_token = std::env::var("WEBHOOK_BEARER_TOKEN")
            .ok()
            .filter(|s| !s.is_empty());
        let dead_letter_path = std::env::var_os("WEBHOOK_DEAD_LETTER")
            .map(PathBuf::from)
            .unwrap_or_else(|| default_dead_letter_path(config_path));
        let max_attempts = match std::env::var("WEBHOOK_MAX_ATTEMPTS") {
            Ok(v) => v.parse::<u32>().ok().filter(|n| *n >= 1).ok_or_else(|| {
                AngelicAngelError::Config(format!("invalid WEBHOOK_MAX_ATTEMPTS: {}", v))
            })?,
            Err(_) => DEFAULT_MAX_ATTEMPTS,
        };

        Ok(Self {
            url,
            bearer_token,
            dead_letter_path,
            max_attempts,
            retry_base: Duration::from_millis(RETRY_BASE_MS),
        })
    }
}

pub fn default_dead_letter_path(config_path: &Path) -> PathBuf {
    with_suffix(config_path, ".failed.jsonl")
}

fn with_suffix(path: &Path, suffix: &str) -> PathBuf {
    let mut p = path.as_os_str().to_owned();
    p.push(suffix);
    PathBuf::from(p)
}

/// Handle for queueing payloads to the background delivery worker.
#[derive(Clone)]
pub struct WebhookSender {
    tx: mpsc::Sender<Value>,
    dead_letter_path: PathBuf,
}

impl WebhookSender {
    /// Queues a payload without blocking. If the queue is full (webhook down for a long
    /// time), the payload is written to the dead-letter file instead of being dropped.
    pub async fn enqueue(&self, payload: Value) -> Result<()> {
        match self.tx.try_send(payload) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Full(payload)) => {
                tracing::warn!("webhook queue full, writing payload to dead-letter file");
                append_dead_letter(&self.dead_letter_path, &payload, "webhook queue full").await
            }
            Err(mpsc::error::TrySendError::Closed(payload)) => {
                append_dead_letter(&self.dead_letter_path, &payload, "webhook worker stopped").await
            }
        }
    }
}

/// The background delivery task.
pub struct WebhookWorker {
    handle: JoinHandle<()>,
    stop: watch::Sender<bool>,
}

impl WebhookWorker {
    /// Waits until every `WebhookSender` is dropped and the queue has been delivered
    /// (or dead-lettered after retries).
    #[allow(dead_code)]
    pub async fn finish(self) {
        let WebhookWorker { handle, stop } = self;
        let _ = handle.await;
        drop(stop);
    }

    /// Shuts the worker down after the senders are dropped: delivery continues for a
    /// short grace period, then the in-flight payload and the rest of the queue are
    /// written to the dead-letter file without further retries.
    pub async fn shutdown(self) {
        let WebhookWorker { mut handle, stop } = self;
        if tokio::time::timeout(Duration::from_secs(SHUTDOWN_GRACE_SECS), &mut handle)
            .await
            .is_err()
        {
            let _ = stop.send(true);
            let _ = handle.await;
        }
    }
}

/// Resolves once a stop has been requested (never, if the worker handle was dropped).
async fn stop_requested(stop: &mut watch::Receiver<bool>) {
    if stop.wait_for(|s| *s).await.is_err() {
        std::future::pending::<()>().await;
    }
}

/// Starts the delivery worker.
pub fn spawn(config: WebhookConfig) -> Result<(WebhookSender, WebhookWorker)> {
    let client = build_client()?;
    let (tx, mut rx) = mpsc::channel::<Value>(QUEUE_CAPACITY);
    let (stop_tx, mut stop_rx) = watch::channel(false);
    let sender = WebhookSender {
        tx,
        dead_letter_path: config.dead_letter_path.clone(),
    };

    let handle = tokio::spawn(async move {
        loop {
            let payload = tokio::select! {
                biased;
                _ = stop_requested(&mut stop_rx) => break,
                p = rx.recv() => match p {
                    Some(p) => p,
                    None => break,
                },
            };

            let result = tokio::select! {
                biased;
                _ = stop_requested(&mut stop_rx) => Err(AngelicAngelError::Webhook(
                    "shut down before delivery completed".to_string(),
                )),
                r = deliver_with_retry(&client, &config, &payload) => r,
            };
            if let Err(e) = result {
                tracing::error!(error = %e, path = %config.dead_letter_path.display(), "webhook delivery failed, saving to dead-letter file");
                save_dead_letter(&config.dead_letter_path, &payload, &e.to_string()).await;
            }
        }

        // Stopped: persist anything still queued.
        rx.close();
        while let Ok(payload) = rx.try_recv() {
            save_dead_letter(
                &config.dead_letter_path,
                &payload,
                "shut down before delivery",
            )
            .await;
        }
    });

    Ok((
        sender,
        WebhookWorker {
            handle,
            stop: stop_tx,
        },
    ))
}

async fn save_dead_letter(path: &Path, payload: &Value, error: &str) {
    if let Err(e) = append_dead_letter(path, payload, error).await {
        // Last resort: keep the payload in the log so it's not silently lost.
        tracing::error!(error = %e, payload = %payload, "failed to write dead-letter file");
    }
}

fn build_client() -> Result<Client> {
    Ok(Client::builder()
        .timeout(Duration::from_secs(REQUEST_TIMEOUT_SECS))
        .build()?)
}

/// Whether a failed status is worth retrying (server-side or rate limiting).
fn is_retryable(status: StatusCode) -> bool {
    status.is_server_error()
        || status == StatusCode::TOO_MANY_REQUESTS
        || status == StatusCode::REQUEST_TIMEOUT
}

/// Exponential backoff for the n-th retry (1-based): base * 2^(n-1), capped at 60s.
fn retry_delay(base: Duration, retry: u32) -> Duration {
    let ms = (base.as_millis() as u64)
        .saturating_mul(2u64.saturating_pow(retry.saturating_sub(1)))
        .min(RETRY_CAP_MS);
    Duration::from_millis(ms)
}

/// Parses `Retry-After` given in seconds (HTTP-date form is ignored).
fn retry_after(response: &reqwest::Response) -> Option<Duration> {
    response
        .headers()
        .get(reqwest::header::RETRY_AFTER)?
        .to_str()
        .ok()?
        .trim()
        .parse::<u64>()
        .ok()
        .map(|s| Duration::from_millis(s.saturating_mul(1000).min(RETRY_CAP_MS)))
}

async fn deliver_with_retry(
    client: &Client,
    config: &WebhookConfig,
    payload: &Value,
) -> Result<()> {
    let mut attempt = 0;
    loop {
        attempt += 1;

        let mut request = client.post(&config.url).json(payload);
        if let Some(token) = &config.bearer_token {
            request = request.bearer_auth(token);
        }

        let (error, server_delay) = match request.send().await {
            Ok(response) if response.status().is_success() => {
                tracing::info!(status = %response.status(), attempt, "webhook request succeeded");
                return Ok(());
            }
            Ok(response) => {
                let status = response.status();
                let delay = retry_after(&response);
                let body = response.text().await.unwrap_or_default();
                let error = AngelicAngelError::Webhook(format!("HTTP {}: {}", status, body));
                if !is_retryable(status) {
                    return Err(error);
                }
                (error, delay)
            }
            Err(e) => (AngelicAngelError::Http(e), None),
        };

        if attempt >= config.max_attempts {
            return Err(AngelicAngelError::Webhook(format!(
                "giving up after {} attempts: {}",
                attempt, error
            )));
        }

        let delay = server_delay.unwrap_or_else(|| retry_delay(config.retry_base, attempt));
        tracing::warn!(attempt, delay_ms = delay.as_millis() as u64, error = %error, "webhook request failed, retrying");
        tokio::time::sleep(delay).await;
    }
}

async fn append_dead_letter(path: &Path, payload: &Value, error: &str) -> Result<()> {
    let failed_at = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let line = serde_json::to_string(&json!({
        "failed_at": failed_at,
        "error": error,
        "payload": payload,
    }))?;
    append_line(path, &line).await
}

async fn append_line(path: &Path, line: &str) -> Result<()> {
    let mut file = tokio::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .await?;
    file.write_all(format!("{}\n", line).as_bytes()).await?;
    file.flush().await?;
    Ok(())
}

/// Removes the replay lock file when dropped.
struct ReplayLock(PathBuf);

impl Drop for ReplayLock {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

/// Re-sends every entry in the dead-letter file. Returns (sent, still_failed).
///
/// Safe to run while `listen` is running: the file is first moved aside to
/// `<file>.replaying`, so new failures keep going to a fresh dead-letter file, and
/// entries that fail again are appended back to it. A lock file prevents two replays
/// from sending the same entries twice. If a previous replay was interrupted, its
/// `.replaying` file is processed first (run `replay` again for the rest).
pub async fn replay(config: &WebhookConfig) -> Result<(usize, usize)> {
    let path = &config.dead_letter_path;
    let lock_path = with_suffix(path, ".replay.lock");
    let replaying = with_suffix(path, ".replaying");

    match std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&lock_path)
    {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
            return Err(AngelicAngelError::Config(format!(
                "another replay is running (delete {} if it was interrupted)",
                lock_path.display()
            )));
        }
        Err(e) => return Err(e.into()),
    }
    let _lock = ReplayLock(lock_path);

    if !replaying.exists() {
        match tokio::fs::rename(path, &replaying).await {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok((0, 0)),
            Err(e) => return Err(e.into()),
        }
    }

    let content = tokio::fs::read_to_string(&replaying).await?;
    let client = build_client()?;
    let (mut sent, mut failed) = (0, 0);

    for line in content.lines().filter(|l| !l.trim().is_empty()) {
        let entry: Value = match serde_json::from_str(line) {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(error = %e, "keeping unreadable dead-letter line");
                append_line(path, line).await?;
                failed += 1;
                continue;
            }
        };
        let payload = entry.get("payload").cloned().unwrap_or(Value::Null);

        match deliver_with_retry(&client, config, &payload).await {
            Ok(()) => sent += 1,
            Err(e) => {
                let mut entry = entry;
                entry["error"] = Value::String(e.to_string());
                append_line(path, &serde_json::to_string(&entry)?).await?;
                failed += 1;
            }
        }
    }

    tokio::fs::remove_file(&replaying).await?;
    Ok((sent, failed))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::io::AsyncReadExt;
    use tokio::net::TcpListener;

    /// Minimal HTTP server: answers the n-th request with `statuses[n]` (last one repeats).
    /// Returns the URL, the request counter, and the captured raw requests.
    async fn fake_server(
        statuses: Vec<u16>,
    ) -> (
        String,
        Arc<AtomicUsize>,
        Arc<tokio::sync::Mutex<Vec<String>>>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let count = Arc::new(AtomicUsize::new(0));
        let requests = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let (c, r) = (count.clone(), requests.clone());

        tokio::spawn(async move {
            loop {
                let (mut sock, _) = listener.accept().await.unwrap();
                let n = c.fetch_add(1, Ordering::SeqCst);
                let status = *statuses.get(n).unwrap_or(statuses.last().unwrap());
                let r = r.clone();
                tokio::spawn(async move {
                    let mut buf = vec![0u8; 8192];
                    let mut req = Vec::new();
                    // Read headers, then the body by Content-Length.
                    loop {
                        let k = sock.read(&mut buf).await.unwrap();
                        if k == 0 {
                            break;
                        }
                        req.extend_from_slice(&buf[..k]);
                        let text = String::from_utf8_lossy(&req).to_string();
                        if let Some(idx) = text.find("\r\n\r\n") {
                            let len = text
                                .lines()
                                .find_map(|l| {
                                    l.to_ascii_lowercase()
                                        .strip_prefix("content-length:")
                                        .map(|v| v.trim().parse::<usize>().unwrap())
                                })
                                .unwrap_or(0);
                            if req.len() >= idx + 4 + len {
                                break;
                            }
                        }
                    }
                    r.lock()
                        .await
                        .push(String::from_utf8_lossy(&req).to_string());
                    let resp = format!(
                        "HTTP/1.1 {} X\r\ncontent-length: 0\r\nconnection: close\r\n\r\n",
                        status
                    );
                    let _ = sock.write_all(resp.as_bytes()).await;
                });
            }
        });

        (format!("http://{}/hook", addr), count, requests)
    }

    fn test_config(url: String, dead_letter: PathBuf, max_attempts: u32) -> WebhookConfig {
        WebhookConfig {
            url,
            bearer_token: Some("secret-token".into()),
            dead_letter_path: dead_letter,
            max_attempts,
            retry_base: Duration::from_millis(10),
        }
    }

    fn temp_path(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("aa-{}-{}.jsonl", name, uuid::Uuid::new_v4()))
    }

    #[test]
    fn backoff_grows_and_caps() {
        let base = Duration::from_secs(1);
        assert_eq!(retry_delay(base, 1), Duration::from_secs(1));
        assert_eq!(retry_delay(base, 2), Duration::from_secs(2));
        assert_eq!(retry_delay(base, 4), Duration::from_secs(8));
        assert_eq!(retry_delay(base, 30), Duration::from_secs(60));
    }

    #[test]
    fn retryable_statuses() {
        assert!(is_retryable(StatusCode::INTERNAL_SERVER_ERROR));
        assert!(is_retryable(StatusCode::TOO_MANY_REQUESTS));
        assert!(!is_retryable(StatusCode::BAD_REQUEST));
        assert!(!is_retryable(StatusCode::UNAUTHORIZED));
    }

    #[tokio::test]
    async fn retries_server_errors_then_succeeds() {
        let (url, count, requests) = fake_server(vec![500, 503, 200]).await;
        let cfg = test_config(url, temp_path("unused"), 5);
        let client = build_client().unwrap();

        deliver_with_retry(&client, &cfg, &json!({"hello": "world"}))
            .await
            .unwrap();
        assert_eq!(count.load(Ordering::SeqCst), 3);

        let reqs = requests.lock().await;
        let last = reqs.last().unwrap().to_ascii_lowercase();
        assert!(last.contains("authorization: bearer secret-token"));
        assert!(last.contains(r#"{"hello":"world"}"#));
    }

    #[tokio::test]
    async fn client_errors_are_not_retried() {
        let (url, count, _) = fake_server(vec![400]).await;
        let cfg = test_config(url, temp_path("unused"), 5);
        let client = build_client().unwrap();

        assert!(deliver_with_retry(&client, &cfg, &json!({})).await.is_err());
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn failed_payloads_go_to_dead_letter_and_replay() {
        let dead = temp_path("dead");
        let (url, count, _) = fake_server(vec![500, 500, 200]).await;
        let cfg = test_config(url, dead.clone(), 2);

        // Worker gives up after 2 attempts (both 500) and writes the dead letter.
        let (sender, worker) = spawn(cfg.clone()).unwrap();
        sender.enqueue(json!({"id": 1})).await.unwrap();
        drop(sender);
        worker.finish().await;
        assert_eq!(count.load(Ordering::SeqCst), 2);

        let content = std::fs::read_to_string(&dead).unwrap();
        let entry: Value = serde_json::from_str(content.lines().next().unwrap()).unwrap();
        assert_eq!(entry["payload"], json!({"id": 1}));
        assert!(entry["error"].as_str().unwrap().contains("giving up"));

        // Replay: the server now answers 200, so nothing is left.
        assert_eq!(replay(&cfg).await.unwrap(), (1, 0));
        assert!(!dead.exists());
        assert!(!with_suffix(&dead, ".replaying").exists());
        assert!(!with_suffix(&dead, ".replay.lock").exists());
    }

    #[tokio::test]
    async fn replay_keeps_failures_and_new_entries() {
        let dead = temp_path("replay-fail");
        let (url, _, _) = fake_server(vec![400]).await;
        let cfg = test_config(url, dead.clone(), 1);

        append_dead_letter(&dead, &json!({"id": "old"}), "x")
            .await
            .unwrap();
        // Simulates an interrupted replay that left entries behind.
        append_line(&with_suffix(&dead, ".replaying"), "not json")
            .await
            .unwrap();

        // The interrupted `.replaying` file is processed first; the main file is untouched.
        assert_eq!(replay(&cfg).await.unwrap(), (0, 1));
        let content = std::fs::read_to_string(&dead).unwrap();
        assert!(content.contains(r#""id":"old""#));
        assert!(content.contains("not json"));

        // Next run: both entries fail again (400) and are appended back.
        assert_eq!(replay(&cfg).await.unwrap(), (0, 2));
        assert_eq!(std::fs::read_to_string(&dead).unwrap().lines().count(), 2);
        std::fs::remove_file(&dead).unwrap();
    }

    #[tokio::test]
    async fn concurrent_replay_is_refused() {
        let dead = temp_path("replay-lock");
        let lock = with_suffix(&dead, ".replay.lock");
        std::fs::write(&lock, "").unwrap();
        let cfg = test_config("http://127.0.0.1:1/".into(), dead, 1);

        assert!(replay(&cfg).await.is_err());
        // The lock belongs to the other replay and must not be removed.
        assert!(lock.exists());
        std::fs::remove_file(&lock).unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn shutdown_dead_letters_queued_and_in_flight_payloads() {
        let dead = temp_path("shutdown");
        // Always 503 with a long backoff, so payloads stay queued.
        let (url, _, _) = fake_server(vec![503]).await;
        let mut cfg = test_config(url, dead.clone(), 100);
        cfg.retry_base = Duration::from_secs(30);

        let (sender, worker) = spawn(cfg).unwrap();
        for id in 0..3 {
            sender.enqueue(json!({ "id": id })).await.unwrap();
        }
        drop(sender);
        worker.shutdown().await;

        let content = std::fs::read_to_string(&dead).unwrap();
        assert_eq!(content.lines().count(), 3);
        for id in 0..3 {
            assert!(content.contains(&format!(r#""id":{}"#, id)));
        }
        std::fs::remove_file(&dead).unwrap();
    }

    #[tokio::test]
    async fn unreachable_webhook_is_dead_lettered() {
        // Bind and drop to get a port with nothing listening.
        let port = TcpListener::bind("127.0.0.1:0")
            .await
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let dead = temp_path("unreachable");
        let cfg = test_config(format!("http://127.0.0.1:{}/", port), dead.clone(), 2);

        let (sender, worker) = spawn(cfg).unwrap();
        sender.enqueue(json!({"id": 2})).await.unwrap();
        drop(sender);
        worker.finish().await;

        assert!(
            std::fs::read_to_string(&dead)
                .unwrap()
                .contains(r#""id":2"#)
        );
        std::fs::remove_file(&dead).unwrap();
    }
}
