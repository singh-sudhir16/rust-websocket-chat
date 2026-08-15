//! WebSocket connection handler.
//!
//! Each connection runs three concurrent tasks:
//!
//! 1. **Reader** — reads JSON frames from the WebSocket, routes them by type.
//! 2. **Room forwarder** — reads from the current room's broadcast channel
//!    and forwards into the client's mpsc.
//! 3. **Writer** — reads from the client's mpsc and sends WebSocket frames.
//!
//! The room forwarder is restartable: when the client switches rooms, we
//! abort the old forwarder and spawn a new one subscribed to the new room.

use std::net::SocketAddr;
use std::time::{Duration, SystemTime};

use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        ConnectInfo, State,
    },
    response::IntoResponse,
};
use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{info, warn};

use crate::protocol::{ClientFrame, ServerFrame};
use crate::state::{AppState, ClientHandle};

/// HTTP handler that upgrades to a WebSocket connection.
pub async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
) -> impl IntoResponse {
    info!("new WebSocket connection from {addr}");
    ws.on_upgrade(move |socket| handle_socket(socket, state, addr))
}

/// Drives a single WebSocket connection. See module docs.
async fn handle_socket(socket: WebSocket, state: AppState, addr: SocketAddr) {
    let (mut ws_sender, mut ws_receiver) = socket.split();

    // mpsc channel: anything sent here is written out to the WebSocket.
    let (client_tx, mut client_rx) = mpsc::unbounded_channel::<ServerFrame>();

    // Wait for the first frame — must be Join or Reconnect.
    let hello = match wait_for_hello(&mut ws_receiver).await {
        Some(h) => h,
        None => {
            info!("client {addr} disconnected before joining");
            return;
        }
    };
    let username = hello.username;
    let initial_room = hello.room;
    let last_seen_id = hello.last_seen_id;

    // Register the client.
    {
        let mut clients = state.clients.write().await;
        clients.insert(
            username.clone(),
            ClientHandle {
                tx: client_tx.clone(),
                room: "general".to_string(),
            },
        );
    }

    // Send the room list immediately.
    let rooms = state.room_names().await;
    let _ = client_tx.send(ServerFrame::RoomList { rooms });

    // Update the client's room in the registry.
    {
        let mut clients = state.clients.write().await;
        if let Some(handle) = clients.get_mut(&username) {
            handle.room = initial_room.clone();
        }
    }

    // -- Spawn the writer task --
    let writer_task = tokio::spawn(async move {
        while let Some(frame) = client_rx.recv().await {
            let json = frame.to_json();
            if ws_sender.send(Message::Text(json.into())).await.is_err() {
                break;
            }
        }
    });

    // -- Spawn the room forwarder BEFORE announcing join --
    // The forwarder subscribes to the room's broadcast channel. If we
    // announce before spawning it, the client misses the system/roster
    // frames that announce_join publishes.
    let forwarder_state = state.clone();
    let forwarder_username = username.clone();
    let forwarder_client_tx = client_tx.clone();

    let forwarder_handle = spawn_room_forwarder(
        forwarder_state.clone(),
        forwarder_username.clone(),
        initial_room.clone(),
        forwarder_client_tx.clone(),
    );

    // Yield once to let the forwarder task subscribe to the broadcast
    // channel before we publish the join announcement.
    tokio::task::yield_now().await;

    // Replay history if reconnecting (after forwarder is subscribed).
    if let Some(last_id) = last_seen_id {
        let missed = {
            let rooms = state.rooms.read().await;
            rooms
                .get(&initial_room)
                .map(|rs| rs.history_since(last_id))
                .unwrap_or_default()
        };
        if !missed.is_empty() {
            let _ = client_tx.send(ServerFrame::History {
                messages: missed,
                room: initial_room.clone(),
            });
        }
    }

    // Announce the join — now the forwarder is listening and will deliver
    // the system and roster frames to this client.
    announce_join(&state, &username, &initial_room).await;

    // -- Reader task: reads from WebSocket, routes frames --
    let reader_state = state.clone();
    let reader_username = username.clone();
    let reader_client_tx = client_tx.clone();
    let reader_addr = addr;

    let mut current_room = initial_room.clone();
    let mut current_forwarder = forwarder_handle;

    loop {
        tokio::select! {
            // Read from WebSocket
            msg = ws_receiver.next() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        let frame = match serde_json::from_str::<ClientFrame>(&text) {
                            Ok(f) => f,
                            Err(e) => {
                                let _ = reader_client_tx.send(ServerFrame::Error {
                                    text: format!("invalid JSON: {e}"),
                                });
                                continue;
                            }
                        };
                        match handle_client_frame(
                            frame,
                            &reader_username,
                            &mut current_room,
                            &reader_state,
                            &reader_client_tx,
                            &mut current_forwarder,
                        )
                        .await
                        {
                            FrameAction::Continue => {}
                            FrameAction::Disconnect => break,
                        }
                    }
                    Some(Ok(Message::Binary(data))) => {
                        // Try to parse binary as UTF-8 JSON.
                        if let Ok(text) = String::from_utf8(data.to_vec()) {
                            let frame = match serde_json::from_str::<ClientFrame>(&text) {
                                Ok(f) => f,
                                Err(_) => continue,
                            };
                            match handle_client_frame(
                                frame,
                                &reader_username,
                                &mut current_room,
                                &reader_state,
                                &reader_client_tx,
                                &mut current_forwarder,
                            )
                            .await
                            {
                                FrameAction::Continue => {}
                                FrameAction::Disconnect => break,
                            }
                        }
                    }
                    Some(Ok(Message::Close(_))) | None => break,
                    _ => {} // Ping/Pong handled by axum
                }
            }
        }
    }

    // Cleanup
    current_forwarder.abort();
    cleanup_client(&state, &username, &current_room).await;
    writer_task.abort();
    info!("client {reader_addr} ({username}) disconnected");
}

/// What the reader should do after handling a frame.
#[allow(dead_code)]
enum FrameAction {
    Continue,
    Disconnect,
}

/// The result of waiting for the initial Join/Reconnect frame.
struct Hello {
    username: String,
    room: String,
    last_seen_id: Option<u64>,
}

/// Waits for the first frame, which must be Join or Reconnect.
async fn wait_for_hello(
    receiver: &mut futures_util::stream::SplitStream<WebSocket>,
) -> Option<Hello> {
    let msg = receiver.next().await?;
    let text = match msg.ok()? {
        Message::Text(t) => t.to_string(),
        Message::Binary(b) => String::from_utf8(b.to_vec()).ok()?,
        _ => return None,
    };

    let frame = serde_json::from_str::<ClientFrame>(&text).ok()?;

    match frame {
        ClientFrame::Join { username } => Some(Hello {
            username,
            room: "general".to_string(),
            last_seen_id: None,
        }),
        ClientFrame::Reconnect {
            username,
            last_seen_id,
        } => Some(Hello {
            username,
            room: "general".to_string(),
            last_seen_id: Some(last_seen_id),
        }),
        _ => None,
    }
}

/// Routes a client frame to the appropriate action.
async fn handle_client_frame(
    frame: ClientFrame,
    username: &str,
    current_room: &mut String,
    state: &AppState,
    client_tx: &mpsc::UnboundedSender<ServerFrame>,
    forwarder: &mut JoinHandle<()>,
) -> FrameAction {
    match frame {
        ClientFrame::Chat { text } => {
            if text.trim().is_empty() {
                return FrameAction::Continue;
            }
            let timestamp = current_time();
            let (id, room) = {
                let mut rooms = state.rooms.write().await;
                let room_state = rooms.get_mut(current_room);
                match room_state {
                    Some(rs) => {
                        let id = rs.next_msg_id;
                        rs.next_msg_id += 1;
                        rs.publish(ServerFrame::Chat {
                            id,
                            timestamp,
                            username: username.to_string(),
                            text,
                            room: current_room.clone(),
                        });
                        (id, current_room.clone())
                    }
                    None => {
                        let _ = client_tx.send(ServerFrame::Error {
                            text: format!("room '{}' does not exist", current_room),
                        });
                        return FrameAction::Continue;
                    }
                }
            };
            info!("[{room}] {username} sent message {id}");
        }

        ClientFrame::SwitchRoom { room } => {
            if room == *current_room {
                return FrameAction::Continue;
            }
            // Announce leave from old room
            let old_room = current_room.clone();
            announce_leave(state, username, &old_room).await;

            // Subscribe to new room
            let _new_rx = {
                let mut rooms = state.rooms.write().await;
                rooms.entry(room.clone()).or_insert_with(|| {
                    info!("creating room '{room}' on demand");
                    crate::state::RoomState::new()
                });
                rooms.get(&room).unwrap().tx.subscribe()
            };

            // Restart the forwarder
            forwarder.abort();
            *forwarder = spawn_room_forwarder(
                state.clone(),
                username.to_string(),
                room.clone(),
                client_tx.clone(),
            );

            *current_room = room.clone();

            // Update client's room in the registry
            {
                let mut clients = state.clients.write().await;
                if let Some(handle) = clients.get_mut(username) {
                    handle.room = room.clone();
                }
            }

            // Announce join in new room
            announce_join(state, username, &room).await;

            info!("[{old_room}→{room}] {username} switched rooms");
        }

        ClientFrame::Dm { to, text } => {
            let timestamp = current_time();

            // Look up the recipient
            let recipient_tx = {
                let clients = state.clients.read().await;
                clients.get(&to).map(|h| h.tx.clone())
            };

            match recipient_tx {
                Some(tx) => {
                    // Send to recipient
                    let _ = tx.send(ServerFrame::Dm {
                        from: username.to_string(),
                        text: text.clone(),
                        timestamp: timestamp.clone(),
                    });
                    // Confirm to sender
                    let _ = client_tx.send(ServerFrame::DmSent {
                        to: to.clone(),
                        text,
                        timestamp,
                    });
                }
                None => {
                    let _ = client_tx.send(ServerFrame::Error {
                        text: format!("user '{to}' is not online"),
                    });
                }
            }
        }

        ClientFrame::Typing { is_typing } => {
            let room = current_room.clone();
            let frame = ServerFrame::Typing {
                username: username.to_string(),
                is_typing,
                room: room.clone(),
            };
            let mut rooms = state.rooms.write().await;
            if let Some(rs) = rooms.get_mut(&room) {
                let _ = rs.tx.send(frame);
            }
        }

        ClientFrame::Join { .. } | ClientFrame::Reconnect { .. } => {
            // Already handled in wait_for_hello. Ignore duplicate.
        }
    }
    FrameAction::Continue
}

/// Spawns a task that forwards messages from a room's broadcast channel
/// to the client's mpsc.
fn spawn_room_forwarder(
    state: AppState,
    _username: String,
    room: String,
    client_tx: mpsc::UnboundedSender<ServerFrame>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut rx = {
            let rooms = state.rooms.read().await;
            match rooms.get(&room) {
                Some(rs) => rs.tx.subscribe(),
                None => return,
            }
        };

        loop {
            match rx.recv().await {
                Ok(frame) => {
                    if client_tx.send(frame).is_err() {
                        break;
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                    warn!("client lagged in '{room}', skipped {n} messages");
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
            }
        }
    })
}

/// Announces a user joining a room and sends an updated roster.
async fn announce_join(state: &AppState, username: &str, room: &str) {
    let frame = ServerFrame::System {
        text: format!("{username} joined the room"),
        room: room.to_string(),
    };
    let mut rooms = state.rooms.write().await;
    if let Some(rs) = rooms.get_mut(room) {
        rs.publish(frame);
    }
    drop(rooms);

    broadcast_roster(state, room).await;
}

/// Announces a user leaving a room and sends an updated roster.
async fn announce_leave(state: &AppState, username: &str, room: &str) {
    let frame = ServerFrame::System {
        text: format!("{username} left the room"),
        room: room.to_string(),
    };
    let mut rooms = state.rooms.write().await;
    if let Some(rs) = rooms.get_mut(room) {
        rs.publish(frame);
    }
    drop(rooms);

    broadcast_roster(state, room).await;
}

/// Broadcasts the current roster of online users in a room.
async fn broadcast_roster(state: &AppState, room: &str) {
    let users: Vec<String> = {
        let clients = state.clients.read().await;
        clients
            .iter()
            .filter(|(_, h)| h.room == room)
            .map(|(name, _)| name.clone())
            .collect()
    };

    let frame = ServerFrame::Roster {
        users,
        room: room.to_string(),
    };

    let mut rooms = state.rooms.write().await;
    if let Some(rs) = rooms.get_mut(room) {
        let _ = rs.tx.send(frame);
    }
}

/// Removes a client from the registries and announces their departure.
async fn cleanup_client(state: &AppState, username: &str, room: &str) {
    {
        let mut clients = state.clients.write().await;
        clients.remove(username);
    }
    announce_leave(state, username, room).await;
}

/// Returns the current wall-clock time as `HH:MM:SS` (IST).
fn current_time() -> String {
    let secs = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let ist_offset = 5 * 3600 + 30 * 60;
    let local = secs + ist_offset;
    let h = (local / 3600) % 24;
    let m = (local / 60) % 60;
    let s = local % 60;
    format!("{h:02}:{m:02}:{s:02}")
}

/// Spawn a heartbeat task that sends periodic pings.
/// (Not currently used — axum handles pings internally — but shown here
/// for learning purposes. In a production server you'd want application-
/// level heartbeats to detect dead connections behind proxies.)
#[allow(dead_code)]
fn spawn_heartbeat(_client_tx: mpsc::UnboundedSender<ServerFrame>) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(30));
        loop {
            interval.tick().await;
            // In a real app you'd send a ping frame here and expect a pong.
            // axum handles protocol-level ping/pong automatically, so this
            // is just a placeholder for application-level heartbeats.
        }
    })
}
