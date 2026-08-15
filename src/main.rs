//! # Rust WebSocket Chat Server
//!
//! A minimal chat server built with `axum` + `tokio` to demonstrate how
//! WebSockets work. Every connected client can broadcast text messages to
//! all other connected clients.
//!
//! ## How it works
//!
//! 1. A single [`tokio::sync::broadcast`] channel is shared across all
//!    connections. Anyone who sends a message publishes it to the channel.
//! 2. Each WebSocket connection subscribes to that channel. A background
//!    task forwards every message received on the subscriber into the
//!    client's socket.
//! 3. Another background task reads incoming messages from the socket and
//!    publishes them to the channel.
//!
//! Open `static/index.html` in a browser (or visit `http://localhost:3000`)
//! after starting the server to chat.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::SystemTime;

use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        ConnectInfo, State,
    },
    response::IntoResponse,
    routing::get,
    Router,
};
use futures_util::{SinkExt, StreamExt};
use std::net::SocketAddr;
use tokio::sync::{broadcast, Mutex};
use tower_http::services::ServeDir;
use tracing::{info, warn};

/// Shared application state, cloned into every handler.
///
/// `broadcast::Sender` is the publish end of a fan-out channel. Every
/// connected client also holds a `broadcast::Receiver` (created per
/// connection) to read the messages other people send.
///
/// `Mutex<HashSet<String>>` tracks currently online usernames so we can
/// announce joins/parts and show a live roster in the UI.
#[derive(Clone)]
struct AppState {
    /// Publish end: `tx.send(msg)` pushes `msg` to every active receiver.
    tx: broadcast::Sender<String>,
    /// Set of currently connected usernames.
    online: Arc<Mutex<HashSet<String>>>,
}

#[tokio::main]
async fn main() {
    // Initialise structured logging. `RUST_LOG=debug` increases verbosity.
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    // Capacity = 1024 in-flight messages per subscriber. If a slow client
    // falls behind more than this, older messages are dropped (the `recv`
    // call returns `Err(Lagged(...))` which we handle gracefully below).
    let (tx, _rx) = broadcast::channel::<String>(1024);

    let state = AppState {
        tx,
        online: Arc::new(Mutex::new(HashSet::new())),
    };

    // Build the router:
    //   GET /        -> static frontend (index.html, app.js, style.css)
    //   GET /ws      -> WebSocket upgrade handler
    let app = Router::new()
        .route("/ws", get(ws_handler))
        .fallback_service(ServeDir::new("static"))
        .with_state(state);

    let addr = SocketAddr::from(([0, 0, 0, 0], 3000));
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    info!("chat server listening on http://{addr}");
    info!("open the app in a browser and join the room.");

    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await
    .unwrap();
}

/// HTTP handler that upgrades the request to a WebSocket connection.
///
/// The browser's `new WebSocket("ws://localhost:3000/ws")` lands here. We
/// return a 101 Switching Protocols response via `on_upgrade`, then run
/// [`handle_socket`] for the lifetime of the connection.
async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
) -> impl IntoResponse {
    info!("new WebSocket connection from {addr}");
    ws.on_upgrade(move |socket| handle_socket(socket, state, addr))
}

/// Drives a single WebSocket connection.
///
/// Two concurrent tasks:
/// - `receive_task`: reads messages the client sends us and broadcasts them.
/// - `send_task`: reads messages from the broadcast channel and writes
///   them out to this client.
///
/// The first one to finish (usually because the client disconnected) causes
/// the other to be aborted.
async fn handle_socket(socket: WebSocket, state: AppState, addr: SocketAddr) {
    // Split the socket into a writer (sink) and a reader (stream).
    let (mut sender, mut receiver) = socket.split();

    // Per-connection subscription to the shared broadcast channel.
    let mut rx = state.tx.subscribe();

    // The username arrives as the very first message from the client.
    // Wait for it; if the client disconnects first, bail out.
    let username = match receiver.next().await {
        Some(Ok(Message::Text(name))) => name.to_string(),
        _ => {
            info!("client {addr} disconnected before sending a username");
            return;
        }
    };

    // Record the user as online.
    {
        let mut online = state.online.lock().await;
        online.insert(username.clone());
    }

    // Announce the join to everyone (including the newcomer).
    let join_msg = format!("[SYSTEM] {} joined the chat", username);
    let _ = state.tx.send(join_msg);
    broadcast_roster(&state).await;

    // Spawn the writer task: forward broadcast messages -> client socket.
    let send_tx = state.tx.clone();
    let send_task = tokio::spawn(async move {
        loop {
            match rx.recv().await {
                Ok(msg) => {
                    // Sending can fail if the client hung up; then we stop.
                    if sender.send(Message::Text(msg.into())).await.is_err() {
                        break;
                    }
                }
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    warn!("client {addr} lagged, skipped {n} messages");
                    let _ = sender
                        .send(Message::Text(
                            format!("[SYSTEM] you missed {n} messages (slow connection)").into(),
                        ))
                        .await;
                }
                Err(broadcast::error::RecvError::Closed) => break,
            }
        }
    });

    // Spawn the reader task: forward client messages -> broadcast channel.
    let recv_username = username.clone();
    let recv_state = state.clone();
    let recv_task = tokio::spawn(async move {
        while let Some(Ok(msg)) = receiver.next().await {
            let text = match msg {
                Message::Text(t) => t.to_string(),
                Message::Binary(b) => match String::from_utf8(b.to_vec()) {
                    Ok(s) => s,
                    Err(_) => continue,
                },
                Message::Close(_) => break,
                // Ping/Pong are handled automatically by axum; ignore here.
                _ => continue,
            };

            // Skip empty messages.
            if text.trim().is_empty() {
                continue;
            }

            let stamp = current_time();
            let line = format!("[{stamp}] {recv_username}: {text}");
            // `_` discards the receiver count; we don't need it.
            let _ = recv_state.tx.send(line);
        }
        // Silence the unused warning for the cloned sender handle.
        let _ = &send_tx;
    });

    // Wait for either task to finish, then cancel the other.
    tokio::select! {
        _ = send_task => {}
        _ = recv_task => {}
    }

    // Cleanup: remove the user and announce the part.
    {
        let mut online = state.online.lock().await;
        online.remove(&username);
    }
    let part_msg = format!("[SYSTEM] {} left the chat", username);
    let _ = state.tx.send(part_msg);
    broadcast_roster(&state).await;
    info!("client {addr} ({username}) disconnected");
}

/// Broadcast the current list of online users as a special `[ROSTER]` line
/// that the frontend parses to render the sidebar.
async fn broadcast_roster(state: &AppState) {
    let online = state.online.lock().await;
    let names: Vec<String> = online.iter().cloned().collect();
    let roster = format!("[ROSTER] {}", names.join(", "));
    let _ = state.tx.send(roster);
}

/// Returns the current wall-clock time as `HH:MM:SS` for message timestamps.
fn current_time() -> String {
    let secs = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    // IST offset (UTC+5:30) in seconds — matches the server's timezone.
    let ist_offset = 5 * 3600 + 30 * 60;
    let local = secs + ist_offset;
    let h = (local / 3600) % 24;
    let m = (local / 60) % 60;
    let s = local % 60;
    format!("{h:02}:{m:02}:{s:02}")
}
