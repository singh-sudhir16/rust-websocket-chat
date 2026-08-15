//! Wire protocol for the chat server.
//!
//! Every WebSocket frame is a JSON object with a `type` field. The server
//! and client both use [`serde`] to (de)serialise these.
//!
//! ## Client → Server frames
//!
//! - [`ClientFrame::Join`] — first frame; registers a username.
//! - [`ClientFrame::Chat`] — broadcast a message to the current room.
//! - [`ClientFrame::SwitchRoom`] — leave the current room and join another.
//! - [`ClientFrame::Dm`] — send a direct message to a specific user.
//! - [`ClientFrame::Typing`] — broadcast a typing indicator to the room.
//! - [`ClientFrame::Reconnect`] — rejoin with a `last_seen_id` to replay
//!   missed messages.
//!
//! ## Server → Client frames
//!
//! - [`ServerFrame::Chat`] — a chat message in a room.
//! - [`ServerFrame::System`] — a system notice (join/leave).
//! - [`ServerFrame::Roster`] — list of online users in a room.
//! - [`ServerFrame::RoomList`] — all available rooms.
//! - [`ServerFrame::Typing`] — someone is/isn't typing.
//! - [`ServerFrame::Dm`] — a direct message from another user.
//! - [`ServerFrame::DmSent`] — confirmation that a DM was delivered.
//! - [`ServerFrame::History`] — replayed messages for a reconnecting client.
//! - [`ServerFrame::Error`] — error message.

use serde::{Deserialize, Serialize};

/// Maximum number of messages kept in a room's history buffer for
/// reconnection replay.
pub const HISTORY_SIZE: usize = 200;

// ─────────────────────────── Client → Server ───────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ClientFrame {
    /// First frame after connecting. Registers the username.
    Join { username: String },

    /// Broadcast a chat message to the current room.
    Chat { text: String },

    /// Switch to a different room.
    SwitchRoom { room: String },

    /// Send a direct message to a specific user.
    Dm { to: String, text: String },

    /// Typing indicator. `is_typing: false` cancels.
    Typing { is_typing: bool },

    /// Reconnect as a known user and replay missed messages.
    Reconnect { username: String, last_seen_id: u64 },
}

// ─────────────────────────── Server → Client ───────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ServerFrame {
    /// A chat message in a room.
    Chat {
        id: u64,
        timestamp: String,
        username: String,
        text: String,
        room: String,
    },

    /// A system notice (join/leave/switch).
    System { text: String, room: String },

    /// List of online users in a room.
    Roster { users: Vec<String>, room: String },

    /// All available rooms.
    RoomList { rooms: Vec<String> },

    /// Typing indicator from a user in a room.
    Typing {
        username: String,
        is_typing: bool,
        room: String,
    },

    /// A direct message from another user.
    Dm {
        from: String,
        text: String,
        timestamp: String,
    },

    /// Confirmation that a DM was sent.
    DmSent {
        to: String,
        text: String,
        timestamp: String,
    },

    /// Replayed messages for a reconnecting client.
    History {
        messages: Vec<ServerFrame>,
        room: String,
    },

    /// Error message.
    Error { text: String },
}

impl ServerFrame {
    /// Returns the message ID if this is a [`ServerFrame::Chat`].
    pub fn message_id(&self) -> Option<u64> {
        match self {
            ServerFrame::Chat { id, .. } => Some(*id),
            _ => None,
        }
    }

    /// Returns the room this frame belongs to, if applicable.
    #[allow(dead_code)]
    pub fn room(&self) -> Option<&str> {
        match self {
            ServerFrame::Chat { room, .. }
            | ServerFrame::System { room, .. }
            | ServerFrame::Roster { room, .. }
            | ServerFrame::Typing { room, .. }
            | ServerFrame::History { room, .. } => Some(room.as_str()),
            _ => None,
        }
    }

    /// Serialise to a JSON string for sending over WebSocket.
    pub fn to_json(&self) -> String {
        serde_json::to_string(self)
            .unwrap_or_else(|_| r#"{"type":"error","text":"serialize failed"}"#.to_string())
    }
}
