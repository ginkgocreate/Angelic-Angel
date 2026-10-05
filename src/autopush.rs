use crate::config::{AutoPushSession, WebPushKeys};
use crate::error::{Result, AngelicAngelError};
use crate::push;
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use socket2::{SockRef, TcpKeepalive};
use std::collections::{HashMap, HashSet, VecDeque};
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::time::Instant;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, client_async_tls_with_config};
use tracing::{debug, info, warn};
use uuid::Uuid;

const AUTOPUSH_WS_URL: &str = "wss://push.services.mozilla.com/";

/// Twitter/X VAPID public key (applicationServerKey) used by PushManager.subscribe().
const TWITTER_VAPID_PUBLIC_KEY: &[u8] = &[
    4, 94, 104, 18, 141, 49, 13, 74, 96, 202, 82, 131, 78, 91, 29, 242, 150, 102, 197, 0, 53, 149,
    230, 8, 54, 38, 62, 173, 43, 28, 89, 130, 191, 222, 213, 128, 147, 62, 21, 49, 187, 95, 212,
    194, 196, 253, 140, 157, 234, 34, 8, 234, 143, 158, 221, 15, 83, 8, 222, 111, 100, 204, 213,
    48, 75,
];

/// Application-level ping interval in seconds.
/// Firefox uses 30 minutes (services.push.pingInterval = 1800000ms), but we use 5 minutes
/// to stay within typical infrastructure idle-connection timeouts (~20 minutes).
/// Well above autopush-rs's ExcessivePing threshold (45 seconds).
const PING_INTERVAL_SECS: u64 = 5 * 60;

/// Pong wait timeout in seconds. Matches Firefox's requestTimeout (10000ms).
const PONG_TIMEOUT_SECS: u64 = 10;

/// WebSocket close code for server-initiated backoff (Firefox: kBACKOFF_WS_STATUS_CODE = 4774).
const BACKOFF_WS_STATUS_CODE: u16 = 4774;

/// TCP keepalive interval in seconds.
/// Prevents intermediate devices (NAT/LB) from dropping idle TCP connections.
/// Typical NAT idle timeouts are 15-30 minutes; 60s keepalive comfortably avoids them.
const TCP_KEEPALIVE_SECS: u64 = 60;

/// WebSocket Ping frame interval in seconds (RFC 6455 Section 5.5.2).
/// Prevents L7 load balancers/CDNs from considering the WebSocket idle at the frame level.
/// Sent independently of the application-level ping (5 minutes).
const WS_PING_INTERVAL_SECS: u64 = 150;

type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// Result of a new AutoPush registration.
#[derive(Debug, Clone)]
pub struct AutoPushRegistration {
    pub uaid: String,
    pub channel_id: String,
    pub endpoint: String,
}

/// Information returned when re-registration is needed (includes freshly generated keys).
#[derive(Debug, Clone)]
pub struct ReregistrationInfo {
    pub registration: AutoPushRegistration,
    /// Freshly generated keys (Firefox: ensureCrypto() always creates new keys on re-subscribe).
    pub keys: WebPushKeys,
}

/// A received push notification.
#[derive(Debug, Clone)]
pub struct Notification {
    pub channel_id: String,
    pub version: String,
    pub data: Option<String>,
    pub headers: Option<HashMap<String, String>>,
}

/// Maximum number of recent message IDs to track for duplicate detection (Firefox-compatible).
const MAX_RECENT_MESSAGE_IDS: usize = 100;

/// Bounded set of recently seen message IDs (insertion-ordered for eviction).
#[derive(Default)]
struct RecentIds {
    order: VecDeque<String>,
    set: HashSet<String>,
}

impl RecentIds {
    /// Records `id` and returns `true` if it was not seen before.
    fn insert(&mut self, id: &str) -> bool {
        if self.set.contains(id) {
            return false;
        }
        if self.order.len() >= MAX_RECENT_MESSAGE_IDS
            && let Some(old) = self.order.pop_front()
        {
            self.set.remove(&old);
        }
        self.order.push_back(id.to_string());
        self.set.insert(id.to_string());
        true
    }
}

pub struct AutoPushClient {
    ws: WsStream,
    #[allow(dead_code)]
    uaid: String,
    #[allow(dead_code)]
    channel_id: String,
    recent_ids: RecentIds,
}

// --- AutoPush protocol message types ---

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "messageType")]
enum AutoPushMessage {
    #[serde(rename = "hello")]
    Hello {
        #[serde(skip_serializing_if = "Option::is_none")]
        uaid: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        #[serde(rename = "channelIDs")]
        channel_ids: Option<Vec<String>>,
        use_webpush: bool,
        /// Firefox-compatible: broadcast listeners (always empty `{}`).
        broadcasts: HashMap<String, String>,
    },
    #[serde(rename = "register")]
    Register {
        #[serde(rename = "channelID")]
        channel_id: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        key: Option<String>,
    },
    #[serde(rename = "unregister")]
    Unregister {
        #[serde(rename = "channelID")]
        channel_id: String,
        code: u16,
    },
    #[serde(rename = "ack")]
    Ack { updates: Vec<AckUpdate> },
}

/// ACK status codes (Firefox PushServiceWebSocket.sys.mjs compatible).
#[derive(Debug, Clone, Copy)]
pub enum AckCode {
    /// Successfully delivered.
    Delivered = 100,
    /// Decryption failed.
    DecryptionError = 101,
    /// Delivery failed for other reasons.
    #[allow(dead_code)]
    NotDelivered = 102,
}

#[derive(Serialize, Deserialize, Debug)]
struct AckUpdate {
    #[serde(rename = "channelID")]
    channel_id: String,
    version: String,
    /// ACK status code (100=delivered, 101=decryption_error, 102=not_delivered).
    code: u16,
}

#[derive(Deserialize, Debug)]
#[serde(tag = "messageType")]
enum AutoPushResponse {
    #[serde(rename = "hello")]
    Hello {
        status: u16,
        uaid: String,
        #[serde(default)]
        use_webpush: Option<bool>,
        #[serde(default)]
        broadcasts: Option<HashMap<String, String>>,
    },
    #[serde(rename = "register")]
    Register {
        status: u16,
        #[serde(rename = "pushEndpoint")]
        push_endpoint: String,
        #[serde(rename = "channelID")]
        #[allow(dead_code)]
        channel_id: String,
    },
    #[serde(rename = "notification")]
    Notification {
        #[serde(rename = "channelID")]
        channel_id: String,
        version: String,
        #[serde(default)]
        data: Option<String>,
        #[serde(default)]
        headers: Option<HashMap<String, String>>,
    },
}

/// What was received while waiting for a pong.
enum PongWaitResult {
    PongReceived,
    NotificationReceived(Notification),
    ConnectionClosed,
    Timeout,
}

/// Determines if a text message is an application-level pong.
///
/// Firefox treats an empty JSON object `{}` or any JSON without a `messageType` field as a pong.
fn is_pong(text: &str) -> bool {
    if let Ok(value) = serde_json::from_str::<serde_json::Value>(text) {
        if let Some(obj) = value.as_object() {
            return obj.is_empty() || !obj.contains_key("messageType");
        }
    }
    false
}

/// Checks if a WebSocket close frame indicates a server backoff request (close code 4774).
fn is_backoff_close(frame: &Option<tokio_tungstenite::tungstenite::protocol::CloseFrame>) -> bool {
    match frame {
        Some(cf) => {
            cf.code
                == tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode::from(
                    BACKOFF_WS_STATUS_CODE,
                )
        }
        None => false,
    }
}

/// Opens a WebSocket connection with TCP keepalive enabled.
///
/// Sets TCP keepalive on the underlying socket to prevent intermediate network devices
/// (NAT tables, load balancers) from dropping idle connections. Sets a Firefox-like
/// User-Agent header to mimic a browser connection.
async fn connect_ws(url: &str) -> Result<WsStream> {
    let mut request = url
        .into_client_request()
        .map_err(|e| AngelicAngelError::AutoPush(format!("failed to build request: {}", e)))?;
    request.headers_mut().insert(
        "User-Agent",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:134.0) Gecko/20100101 Firefox/134.0"
            .parse()
            .unwrap(),
    );
    // Origin header for proxy compatibility.
    request
        .headers_mut()
        .insert("Origin", "https://x.com".parse().unwrap());

    let uri = request.uri().clone();
    let host = uri
        .host()
        .ok_or_else(|| AngelicAngelError::AutoPush("URL has no host".to_string()))?;
    let port = uri.port_u16().unwrap_or(443);
    let addr = format!("{}:{}", host, port);

    let tcp_stream = TcpStream::connect(&addr)
        .await
        .map_err(|e| AngelicAngelError::AutoPush(format!("TCP connect failed ({}): {}", addr, e)))?;

    let sock_ref = SockRef::from(&tcp_stream);
    let keepalive = TcpKeepalive::new().with_time(Duration::from_secs(TCP_KEEPALIVE_SECS));
    sock_ref
        .set_tcp_keepalive(&keepalive)
        .map_err(|e| AngelicAngelError::AutoPush(format!("failed to set TCP keepalive: {}", e)))?;
    tcp_stream.set_nodelay(true)?;

    debug!(
        "TCP connection established: {} (keepalive={}s, nodelay=true)",
        addr, TCP_KEEPALIVE_SECS
    );

    // TLS + WebSocket handshake (connector: None = rustls via rustls-tls-webpki-roots feature).
    let (ws, _response) = client_async_tls_with_config(request, tcp_stream, None, None)
        .await
        .map_err(|e| AngelicAngelError::AutoPush(format!("WebSocket connect failed: {}", e)))?;

    debug!("WebSocket connection established (TLS + keepalive)");
    Ok(ws)
}

/// Waits for the next text frame during a handshake step (timeout: 10s, matching Firefox
/// requestTimeout) and parses it. A close frame with code 4774 maps to `Backoff`.
async fn recv_response(ws: &mut WsStream, step: &str) -> Result<AutoPushResponse> {
    match tokio::time::timeout(Duration::from_secs(PONG_TIMEOUT_SECS), ws.next()).await {
        Ok(Some(Ok(Message::Text(text)))) => Ok(serde_json::from_str(&text)?),
        Ok(Some(Ok(Message::Close(frame)))) if is_backoff_close(&frame) => {
            warn!(step, "server backoff request received during handshake");
            Err(AngelicAngelError::Backoff)
        }
        Ok(Some(Ok(msg))) => Err(AngelicAngelError::AutoPush(format!(
            "unexpected message type ({}): {:?}",
            step, msg
        ))),
        Ok(Some(Err(e))) => Err(e.into()),
        Ok(None) => Err(AngelicAngelError::AutoPush(format!(
            "connection closed by server ({})",
            step
        ))),
        Err(_) => Err(AngelicAngelError::AutoPush(format!(
            "{} response timed out ({}s)",
            step, PONG_TIMEOUT_SECS
        ))),
    }
}

/// Sends hello and returns the UAID assigned by the server.
///
/// Firefox does not send channelIDs in hello; an empty UAID requests a new one.
async fn hello(ws: &mut WsStream, uaid: &str) -> Result<String> {
    let hello_msg = AutoPushMessage::Hello {
        uaid: Some(uaid.to_string()),
        channel_ids: None,
        use_webpush: true,
        broadcasts: HashMap::new(),
    };
    ws.send(Message::Text(serde_json::to_string(&hello_msg)?.into()))
        .await?;
    debug!("hello sent");

    match recv_response(ws, "hello").await? {
        AutoPushResponse::Hello {
            status,
            uaid,
            use_webpush,
            broadcasts,
        } => {
            if status != 200 {
                return Err(AngelicAngelError::AutoPush(format!(
                    "hello failed: status={}",
                    status
                )));
            }
            debug!(?use_webpush, ?broadcasts, "hello response extra fields");
            info!("hello succeeded: uaid={}", uaid);
            Ok(uaid)
        }
        _ => Err(AngelicAngelError::AutoPush(
            "unexpected response: expected hello".to_string(),
        )),
    }
}

/// Performs a fresh registration: hello -> register -> receive endpoint.
pub async fn register_new() -> Result<AutoPushRegistration> {
    info!("starting new AutoPush registration");

    let mut ws = connect_ws(AUTOPUSH_WS_URL).await?;
    let uaid = hello(&mut ws, "").await?;

    let channel_id = Uuid::new_v4().to_string();
    let register_msg = AutoPushMessage::Register {
        channel_id: channel_id.clone(),
        key: Some(URL_SAFE_NO_PAD.encode(TWITTER_VAPID_PUBLIC_KEY)),
    };
    ws.send(Message::Text(serde_json::to_string(&register_msg)?.into()))
        .await?;
    debug!("register sent: channel_id={}", channel_id);

    let endpoint = match recv_response(&mut ws, "register").await? {
        AutoPushResponse::Register {
            status,
            push_endpoint,
            ..
        } => {
            if status != 200 {
                return Err(AngelicAngelError::AutoPush(format!(
                    "register failed: status={}",
                    status
                )));
            }
            info!("register succeeded: endpoint={}", push_endpoint);
            push_endpoint
        }
        _ => {
            return Err(AngelicAngelError::AutoPush(
                "unexpected response: expected register".to_string(),
            ));
        }
    };

    ws.close(None).await?;

    Ok(AutoPushRegistration {
        uaid,
        channel_id,
        endpoint,
    })
}

/// Result of a reconnect attempt.
pub enum ConnectResult {
    /// Connected successfully with the existing session.
    Connected(AutoPushClient),
    /// UAID was invalidated; includes fresh keys and a new registration for the caller
    /// to propagate to Twitter API and persist.
    NeedsReregistration(ReregistrationInfo),
}

/// Reconnects to AutoPush with an existing session.
///
/// If the server assigns a different UAID (pushsubscriptionchange equivalent):
/// 1. Generates a fresh P-256 key pair and auth secret (Firefox: ensureCrypto()).
/// 2. Registers a new channel with the new UAID.
/// 3. Returns `NeedsReregistration` so the caller can update Twitter API and save.
pub async fn connect_and_listen(session: &AutoPushSession) -> Result<ConnectResult> {
    info!("reconnecting to AutoPush: uaid={}", session.uaid);

    let mut ws = connect_ws(AUTOPUSH_WS_URL).await?;
    let uaid = hello(&mut ws, &session.uaid).await?;

    if uaid != session.uaid {
        warn!(
            old_uaid = %session.uaid,
            new_uaid = %uaid,
            "UAID invalidated, generating new keys and re-registering"
        );

        let _ = ws.close(None).await;

        // Generate fresh keys (Firefox: ensureCrypto() on re-subscribe).
        let new_keys = push::generate_keys();
        info!("generated new encryption keys");

        let registration = register_new().await?;

        info!(new_uaid = %registration.uaid, "re-registration complete");

        return Ok(ConnectResult::NeedsReregistration(ReregistrationInfo {
            registration,
            keys: new_keys,
        }));
    }

    Ok(ConnectResult::Connected(AutoPushClient {
        ws,
        uaid: session.uaid.clone(),
        channel_id: session.channel_id.clone(),
        recent_ids: RecentIds::default(),
    }))
}

impl AutoPushClient {
    /// Waits for the next notification, automatically handling ping/pong and keepalive.
    ///
    /// Implements a three-tier timer scheme matching Firefox (PushServiceWebSocket.sys.mjs):
    /// - Application ping timer: sends `{}` every 5 minutes to verify server liveness.
    /// - Request timeout: if no pong arrives within 10 seconds of a ping, forces reconnect.
    /// - Backoff: exponential backoff on connection errors (managed by the caller in listener.rs).
    pub async fn next_notification(&mut self) -> Result<Option<Notification>> {
        let mut next_app_ping = Instant::now() + Duration::from_secs(PING_INTERVAL_SECS);
        let mut next_ws_ping = Instant::now() + Duration::from_secs(WS_PING_INTERVAL_SECS);

        loop {
            let now = Instant::now();
            let app_remaining = next_app_ping.saturating_duration_since(now);
            let ws_remaining = next_ws_ping.saturating_duration_since(now);
            let remaining = app_remaining.min(ws_remaining);

            match tokio::time::timeout(remaining, self.ws.next()).await {
                // Message received: reset ping timers (Firefox resets on any message receipt).
                Ok(msg) => {
                    next_app_ping = Instant::now() + Duration::from_secs(PING_INTERVAL_SECS);
                    next_ws_ping = Instant::now() + Duration::from_secs(WS_PING_INTERVAL_SECS);

                    match msg {
                        Some(Ok(Message::Text(text))) => {
                            if is_pong(&text) {
                                debug!("application-level pong received");
                                continue;
                            }

                            let resp: AutoPushResponse = serde_json::from_str(&text)?;
                            match resp {
                                AutoPushResponse::Notification {
                                    channel_id,
                                    version,
                                    data,
                                    headers,
                                } => {
                                    let notification = Notification {
                                        channel_id,
                                        version,
                                        data,
                                        headers,
                                    };
                                    if let Some(n) = self.accept(notification).await? {
                                        return Ok(Some(n));
                                    }
                                }
                                other => {
                                    debug!(?other, "non-notification message received");
                                    continue;
                                }
                            }
                        }
                        Some(Ok(Message::Ping(data))) => {
                            debug!("WebSocket ping received, sending pong");
                            self.ws.send(Message::Pong(data)).await?;
                            continue;
                        }
                        Some(Ok(Message::Pong(_))) => {
                            debug!("WebSocket pong received");
                            continue;
                        }
                        Some(Ok(Message::Close(frame))) => {
                            if is_backoff_close(&frame) {
                                warn!("server backoff request received (close code 4774)");
                                return Err(AngelicAngelError::Backoff);
                            }
                            info!("WebSocket closed by server");
                            return Ok(None);
                        }
                        Some(Ok(msg)) => {
                            debug!(?msg, "unexpected message type");
                            continue;
                        }
                        Some(Err(e)) => {
                            return Err(e.into());
                        }
                        None => {
                            info!("WebSocket stream ended");
                            return Ok(None);
                        }
                    }
                }
                // Timer expired: determine which ping timer fired.
                Err(_) => {
                    let now = Instant::now();

                    if now >= next_app_ping {
                        debug!(
                            idle_secs = PING_INTERVAL_SECS,
                            "sending application-level ping"
                        );
                        self.ws.send(Message::Text("{}".into())).await?;

                        match self.wait_for_pong_or_notification().await? {
                            PongWaitResult::PongReceived => {
                                debug!("pong received, connection healthy");
                                next_app_ping =
                                    Instant::now() + Duration::from_secs(PING_INTERVAL_SECS);
                                next_ws_ping =
                                    Instant::now() + Duration::from_secs(WS_PING_INTERVAL_SECS);
                            }
                            PongWaitResult::NotificationReceived(notification) => {
                                return Ok(Some(notification));
                            }
                            PongWaitResult::ConnectionClosed => {
                                info!("connection closed while waiting for pong");
                                return Ok(None);
                            }
                            PongWaitResult::Timeout => {
                                warn!(
                                    timeout_secs = PONG_TIMEOUT_SECS,
                                    "pong timed out, reconnection needed"
                                );
                                return Err(AngelicAngelError::AutoPush(
                                    "pong timed out, reconnection needed".to_string(),
                                ));
                            }
                        }
                    } else {
                        // WebSocket Ping frame timer (RFC 6455 Section 5.5.2).
                        debug!(interval_secs = WS_PING_INTERVAL_SECS, "sending WebSocket ping frame");
                        self.ws.send(Message::Ping(vec![].into())).await?;
                        next_ws_ping = Instant::now() + Duration::from_secs(WS_PING_INTERVAL_SECS);
                    }
                }
            }
        }
    }

    /// Duplicate detection (track last 100 message IDs). Returns `None` for a duplicate,
    /// which is still ACKed so the server stops retrying.
    async fn accept(&mut self, notification: Notification) -> Result<Option<Notification>> {
        if !self.recent_ids.insert(&notification.version) {
            warn!(version = %notification.version, "duplicate notification detected, skipping");
            self.ack_notification(notification.channel_id, notification.version, AckCode::Delivered)
                .await?;
            return Ok(None);
        }
        info!(
            channel_id = %notification.channel_id,
            version = %notification.version,
            "notification received"
        );
        Ok(Some(notification))
    }

    /// Waits for a pong or notification with a timeout.
    ///
    /// Implements Firefox's request timeout timer (requestTimeout: 10s).
    /// Returns whichever arrives first: pong, notification, connection close, or timeout.
    async fn wait_for_pong_or_notification(&mut self) -> Result<PongWaitResult> {
        let deadline = Instant::now() + Duration::from_secs(PONG_TIMEOUT_SECS);

        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Ok(PongWaitResult::Timeout);
            }

            match tokio::time::timeout(remaining, self.ws.next()).await {
                Ok(Some(Ok(Message::Text(text)))) => {
                    if is_pong(&text) {
                        return Ok(PongWaitResult::PongReceived);
                    }
                    match serde_json::from_str::<AutoPushResponse>(&text) {
                        Ok(AutoPushResponse::Notification {
                            channel_id,
                            version,
                            data,
                            headers,
                        }) => {
                            let notification = Notification {
                                channel_id,
                                version,
                                data,
                                headers,
                            };
                            if let Some(n) = self.accept(notification).await? {
                                return Ok(PongWaitResult::NotificationReceived(n));
                            }
                        }
                        Ok(other) => {
                            debug!(?other, "non-notification message while waiting for pong");
                            continue;
                        }
                        Err(e) => return Err(e.into()),
                    }
                }
                Ok(Some(Ok(Message::Ping(data)))) => {
                    debug!("WebSocket ping received while waiting for pong, sending pong");
                    self.ws.send(Message::Pong(data)).await?;
                    continue;
                }
                Ok(Some(Ok(Message::Close(frame)))) => {
                    if is_backoff_close(&frame) {
                        warn!("server backoff request received while waiting for pong (close code 4774)");
                        return Err(AngelicAngelError::Backoff);
                    }
                    return Ok(PongWaitResult::ConnectionClosed);
                }
                Ok(None) => {
                    return Ok(PongWaitResult::ConnectionClosed);
                }
                Ok(Some(Ok(_))) => continue,
                Ok(Some(Err(e))) => return Err(e.into()),
                Err(_) => return Ok(PongWaitResult::Timeout),
            }
        }
    }

    /// Sends an ACK for a received notification.
    pub async fn ack_notification(
        &mut self,
        channel_id: String,
        version: String,
        code: AckCode,
    ) -> Result<()> {
        let ack_msg = AutoPushMessage::Ack {
            updates: vec![AckUpdate {
                channel_id,
                version,
                code: code as u16,
            }],
        };
        let ack_json = serde_json::to_string(&ack_msg)?;
        self.ws.send(Message::Text(ack_json.into())).await?;
        debug!(code = code as u16, "ACK sent");
        Ok(())
    }

    #[allow(dead_code)]
    pub fn uaid(&self) -> &str {
        &self.uaid
    }

    #[allow(dead_code)]
    pub fn channel_id(&self) -> &str {
        &self.channel_id
    }
}

/// Unregisters a channel from AutoPush.
pub async fn unregister(session: &AutoPushSession) -> Result<()> {
    info!(
        uaid = %session.uaid,
        channel_id = %session.channel_id,
        "starting AutoPush unregistration"
    );

    let mut ws = connect_ws(AUTOPUSH_WS_URL).await?;
    hello(&mut ws, &session.uaid).await?;

    let unregister_msg = AutoPushMessage::Unregister {
        channel_id: session.channel_id.clone(),
        code: 200,
    };
    let unregister_json = serde_json::to_string(&unregister_msg)?;
    ws.send(Message::Text(unregister_json.into())).await?;

    debug!(channel_id = %session.channel_id, "unregister sent");

    ws.close(None).await?;

    info!("AutoPush unregistration complete");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pong_detection() {
        assert!(is_pong("{}"));
        assert!(is_pong(r#"{"status":200}"#));
        assert!(!is_pong(r#"{"messageType":"notification"}"#));
        assert!(!is_pong("not json"));
        assert!(!is_pong("[]"));
    }

    #[test]
    fn recent_ids_detects_duplicates_and_evicts_oldest() {
        let mut ids = RecentIds::default();
        assert!(ids.insert("a"));
        assert!(!ids.insert("a"));

        for i in 0..MAX_RECENT_MESSAGE_IDS {
            assert!(ids.insert(&format!("x{}", i)));
        }
        assert_eq!(ids.order.len(), MAX_RECENT_MESSAGE_IDS);
        assert_eq!(ids.set.len(), MAX_RECENT_MESSAGE_IDS);
        // "a" was evicted, so it is accepted again.
        assert!(ids.insert("a"));
        assert!(!ids.insert(&format!("x{}", MAX_RECENT_MESSAGE_IDS - 1)));
    }

    #[test]
    fn notification_response_parses() {
        let text = r#"{"messageType":"notification","channelID":"c1","version":"v1","data":"abc","headers":{"encoding":"aes128gcm"}}"#;
        match serde_json::from_str::<AutoPushResponse>(text).unwrap() {
            AutoPushResponse::Notification {
                channel_id,
                version,
                data,
                headers,
            } => {
                assert_eq!(channel_id, "c1");
                assert_eq!(version, "v1");
                assert_eq!(data.as_deref(), Some("abc"));
                assert_eq!(headers.unwrap()["encoding"], "aes128gcm");
            }
            other => panic!("unexpected: {:?}", other),
        }
    }
}
