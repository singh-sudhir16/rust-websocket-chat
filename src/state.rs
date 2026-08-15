//! Shared application state.
//!
//! The server holds two registries:
//!
//! - [`Rooms`]: maps room names to [`RoomState`] (a broadcast channel +
//!   ring-buffer history for reconnection replay).
//! - [`Clients`]: maps usernames to [`ClientHandle`] (a direct mpsc sender
//!   for DM routing + which room they're in).
//!
//! Both are wrapped in `Arc<RwLock<...>>` and cloned into every handler.

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::{broadcast, mpsc, RwLock};

use crate::protocol::{ServerFrame, HISTORY_SIZE};

/// The default rooms that exist on server start.
pub const DEFAULT_ROOMS: &[&str] = &["general", "random", "tech"];

/// Per-room state: a broadcast channel for fan-out + a history buffer.
pub struct RoomState {
    /// Publish end. Every subscriber gets their own `Receiver` from this.
    pub tx: broadcast::Sender<ServerFrame>,
    /// Ring buffer of recent messages (for reconnection replay).
    pub history: Vec<ServerFrame>,
    /// Monotonic message counter — gives each message a unique ID.
    pub next_msg_id: u64,
}

impl RoomState {
    pub fn new() -> Self {
        let (tx, _rx) = broadcast::channel(256);
        Self {
            tx,
            history: Vec::new(),
            next_msg_id: 1,
        }
    }

    /// Publish a frame to all subscribers AND store it in history.
    pub fn publish(&mut self, frame: ServerFrame) {
        if let ServerFrame::Chat { .. } = &frame {
            self.history.push(frame.clone());
            if self.history.len() > HISTORY_SIZE {
                self.history.remove(0);
            }
        }
        let _ = self.tx.send(frame);
    }

    /// Returns messages with IDs strictly greater than `last_seen_id`.
    pub fn history_since(&self, last_seen_id: u64) -> Vec<ServerFrame> {
        self.history
            .iter()
            .filter(|f| f.message_id().is_some_and(|id| id > last_seen_id))
            .cloned()
            .collect()
    }
}

/// Handle to a connected client, stored in the clients registry.
/// Used for routing direct messages.
pub struct ClientHandle {
    /// Direct channel to the client's writer task.
    pub tx: mpsc::UnboundedSender<ServerFrame>,
    /// Which room the client is currently in.
    pub room: String,
}

/// The top-level shared state, cloned into every WebSocket handler.
#[derive(Clone)]
pub struct AppState {
    /// Room registry: room name → room state.
    pub rooms: Arc<RwLock<HashMap<String, RoomState>>>,
    /// Client registry: username → client handle (for DM routing).
    pub clients: Arc<RwLock<HashMap<String, ClientHandle>>>,
}

impl AppState {
    pub fn new() -> Self {
        let mut rooms = HashMap::new();
        for &name in DEFAULT_ROOMS {
            rooms.insert(name.to_string(), RoomState::new());
        }
        Self {
            rooms: Arc::new(RwLock::new(rooms)),
            clients: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Returns the list of all room names.
    pub async fn room_names(&self) -> Vec<String> {
        self.rooms.read().await.keys().cloned().collect()
    }
}
