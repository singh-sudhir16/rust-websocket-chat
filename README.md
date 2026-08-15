# 💬 Rust WebSocket Chat

A minimal but complete chat application built in **Rust** to learn how
**WebSockets** work end-to-end. It ships with a server (axum + tokio) and a
dependency-free vanilla JS frontend served from the same process.

> Built for demo and learning. Open two browser tabs and chat with yourself.

---

## What you'll learn

- How an HTTP request is **upgraded** to a persistent WebSocket connection.
- How a server **fans out** one message to many connected clients using a
  `tokio::sync::broadcast` channel.
- How to **split** a WebSocket into a reader and a writer and run them
  concurrently.
- How the browser's `WebSocket` API sends and receives text frames.

---

## Architecture

```
┌─────────────┐      HTTP GET /ws (Upgrade)      ┌──────────────────┐
│  Browser    │ ───────────────────────────────▶ │  axum server     │
│  (app.js)   │ ◀─────────────────────────────── │  (src/main.rs)   │
│             │      bidirectional frames        │                  │
│  tab 1      │                                  │  broadcast       │
│  tab 2      │ ──send("hi")──▶                  │  channel ──┐     │
│             │                  ◀──recv("hi")── │            │     │
└─────────────┘                                  │  ┌─────────┴───┐ │
                                                 │  │ every client│ │
                                                 │  │ subscriber  │ │
                                                 │  └─────────────┘ │
                                                 └──────────────────┘
```

### The key idea: a broadcast channel

```
            ┌─────────────┐
client A ──▶│             │──▶ subscriber A ──▶ client A's socket
client B ──▶│  broadcast  │──▶ subscriber B ──▶ client B's socket
client C ──▶│   channel   │──▶ subscriber C ──▶ client C's socket
            └─────────────┘
```

Every connection:

1. Calls `state.tx.subscribe()` to get its **own receiver**.
2. Spawns a **reader task** that publishes incoming frames via `tx.send(...)`.
3. Spawns a **writer task** that forwards received broadcasts to its socket.

When any client sends a message, **every** subscriber (including the sender)
gets a copy. That's the whole chat server.

---

## Project layout

```
rust-websocket-chat/
├── Cargo.toml          # dependencies: axum, tokio, tower-http, futures-util
├── src/
│   └── main.rs         # the entire server (~240 lines, heavily commented)
└── static/             # frontend, served by tower-http::ServeDir
    ├── index.html      # join screen + chat screen
    ├── style.css       # dark theme, no frameworks
    └── app.js          # WebSocket client logic
```

---

## Run it

### Prerequisites

- [Rust](https://rustup.rs) (stable, 1.75+)

### Start the server

```sh
cargo run
```

The server listens on `http://localhost:3000`.

### Chat

1. Open `http://localhost:3000` in your browser.
2. Enter a username and click **Join Chat**.
3. Open a **second** tab (or a different browser) and join with another name.
4. Type messages — everyone in the room sees them instantly.

### Try the slow-client behavior

Set `RUST_LOG=debug` to see lag warnings when a subscriber falls behind:

```sh
RUST_LOG=debug cargo run
```

---

## How the WebSocket upgrade works

A WebSocket connection starts life as an ordinary HTTP request:

```
GET /ws HTTP/1.1
Host: localhost:3000
Upgrade: websocket
Connection: Upgrade
Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==
Sec-WebSocket-Version: 13
```

The server responds with `101 Switching Protocols` and from that point the
**same TCP socket** carries WebSocket frames in both directions — no more
HTTP. In axum this is handled by the `WebSocketUpgrade` extractor:

```rust
async fn ws_handler(ws: WebSocketUpgrade, State(state): State<AppState>) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, state, addr))
}
```

`on_upgrade` sends the `101` response and hands you a `WebSocket` you can split
into a sink (writer) and stream (reader).

---

## Message protocol

The app uses a tiny line-based text protocol (no JSON needed for a demo):

| Frame                                | Meaning                          |
| ------------------------------------ | -------------------------------- |
| _(first frame from client)_          | The client's username            |
| `[HH:MM:SS] alice: hello`            | A chat message from a user       |
| `[SYSTEM] alice joined the chat`     | A join/leave notice              |
| `[ROSTER] alice, bob, carol`         | The current list of online users |

The frontend (`app.js`) parses these prefixes and routes each line to the
right renderer.

---

## Dependencies

| crate                | why                                           |
| -------------------- | --------------------------------------------- |
| `axum` (`ws` feature)| HTTP server + first-class WebSocket support   |
| `tokio` (`full`)     | Async runtime, tasks, `broadcast` channel     |
| `tower-http` (`fs`)  | `ServeDir` for serving the static frontend    |
| `futures-util`       | `SinkExt`/`StreamExt` to split the socket     |
| `tracing`            | Structured logging                            |

---

## Things to try next (learning extensions)

- [ ] Send messages as JSON instead of line-based text.
- [ ] Add per-room channels (multiple broadcast senders keyed by room id).
- [ ] Persist messages to SQLite so history survives restarts.
- [ ] Add a `/health` REST endpoint alongside `/ws`.
- [ ] Deploy behind a reverse proxy with TLS (`wss://`).

---

## License

MIT
