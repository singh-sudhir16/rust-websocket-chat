//! # Rust WebSocket Chat Server v0.2
//!
//! A chat server built with `axum` + `tokio` that demonstrates:
//!
//! - **Multiple rooms** — each room has its own `broadcast::Sender`.
//! - **Direct messages** — a `clients` registry maps usernames to per-client
//!   mpsc channels for targeted delivery.
//! - **Typing indicators** — ephemeral frames broadcast to the room.
//! - **Reconnection with replay** — a ring-buffer history per room; clients
//!   reconnect with their `last_seen_id` and receive missed messages.
//!
//! ## Wire protocol
//!
//! All frames are JSON. See [`protocol`] for the full type list.
//!
//! ## Layout
//!
//! - [`protocol`] — JSON frame types (ClientFrame / ServerFrame).
//! - [`state`] — shared state (room registry, client registry).
//! - [`handler`] — WebSocket connection lifecycle.

mod handler;
mod protocol;
mod state;

use std::net::SocketAddr;

use tower_http::services::ServeDir;
use tracing::info;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let state = state::AppState::new();

    let app = axum::Router::new()
        .route("/ws", axum::routing::get(handler::ws_handler))
        .fallback_service(ServeDir::new("static"))
        .with_state(state);

    let port: u16 = std::env::var("PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(3000);

    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    info!("chat server listening on http://{addr}");

    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await
    .unwrap();
}
