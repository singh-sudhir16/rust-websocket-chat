# 💬 Rust WebSocket Chat

A multi-room chat application built in **Rust** to learn how **WebSockets**
work end-to-end. Built with axum + tokio on the backend and vanilla JS on the
frontend — no frameworks, no build step.

> Built for demo and learning. Open two browser tabs and chat with yourself.

---

## ✨ Features (v0.2)

- **Multi-room chat** — switch between `#general`, `#random`, `#tech` or any
  room you type. Each room has its own broadcast channel.
- **Direct messages (DMs)** — click any online user to open a private
  conversation. DMs are routed directly via a per-client mpsc channel.
- **Typing indicators** — see when someone is typing in real time, with
  debounce and auto-timeout.
- **Reconnection with replay** — if your connection drops, the client
  reconnects with exponential backoff and the server replays all messages
  you missed using a per-room history buffer + `last_seen_id`.
- **Live roster** — see who's online in each room, updated in real time.

---

## What you'll learn

- How an HTTP request is **upgraded** to a persistent WebSocket connection.
- How to **fan out** messages using `tokio::sync::broadcast` channels.
- How to **route** messages: broadcast (rooms) vs targeted (DMs).
- How to **split** a WebSocket into a reader and a writer and run them
  concurrently.
- How to handle **reconnection** with message replay using monotonic IDs.
- How to design a **JSON wire protocol** with tagged enums.

---

## Architecture

```
┌─────────────┐      HTTP GET /ws (Upgrade)      ┌──────────────────┐
│  Browser    │ ───────────────────────────────▶ │  axum server     │
│  (app.js)   │ ◀─────────────────────────────── │  (src/)          │
│             │      bidirectional JSON frames   │                  │
└─────────────┘                                  │  ┌────────────┐  │
                                                 │  │ Room: gen  │  │
                                                 │  │ broadcast  │  │
                                                 │  │ + history  │  │
                                                 │  ├────────────┤  │
                                                 │  │ Room: rand │  │
                                                 │  │ broadcast  │  │
                                                 │  │ + history  │  │
                                                 │  ├────────────┤  │
                                                 │  │ Clients    │  │
                                                 │  │ (DM routing)│ │
                                                 │  └────────────┘  │
                                                 └──────────────────┘
```

### Key concepts

| Concept | How it's implemented |
|---|---|
| **Room broadcast** | Each room has a `broadcast::Sender<ServerFrame>`. Subscribers get their own receiver. |
| **DM routing** | A `HashMap<String, ClientHandle>` maps usernames → per-client `mpsc::UnboundedSender`. |
| **Reconnection replay** | Each room keeps a ring buffer of the last 200 messages. Reconnecting clients send `last_seen_id` and receive `History` with missed messages. |
| **Typing indicators** | Ephemeral `Typing` frames broadcast to the room (not stored in history). |
| **Wire protocol** | Tagged JSON enums: `{"type":"chat",...}`, `{"type":"dm",...}`, etc. |

---

## Project layout

```
rust-websocket-chat/
├── Cargo.toml              # deps: axum, tokio, serde, tower-http
├── Dockerfile              # multi-stage build for deployment
├── fly.toml                # Fly.io config (if using Fly)
├── render.yaml             # Render blueprint (if using Render)
├── src/
│   ├── main.rs             # entry point, router, static serving
│   ├── protocol.rs         # JSON frame types (ClientFrame / ServerFrame)
│   ├── state.rs            # shared state (room registry, client registry)
│   └── handler.rs          # WebSocket lifecycle: reader, writer, forwarder
└── static/                 # frontend, served by tower-http::ServeDir
    ├── index.html          # join screen + chat screen
    ├── style.css           # dark theme, no frameworks
    └── app.js              # WebSocket client with reconnection
```

---

## Run locally

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
3. Open a **second** tab and join with another name.
4. Try these:
   - **Switch rooms** — click a room in the left sidebar.
   - **Send a DM** — click an online user in the sidebar.
   - **Typing indicator** — start typing and watch the other tab.
   - **Test reconnection** — kill the server (`Ctrl+C`), restart it, and
     watch the client reconnect and replay missed messages.

---

## Wire protocol

All frames are JSON with a `type` discriminator.

### Client → Server

```json
{"type": "join", "username": "alice"}
{"type": "chat", "text": "hello!"}
{"type": "switch_room", "room": "random"}
{"type": "dm", "to": "bob", "text": "hey bob"}
{"type": "typing", "is_typing": true}
{"type": "reconnect", "username": "alice", "last_seen_id": 42}
```

### Server → Client

```json
{"type": "chat", "id": 1, "timestamp": "16:30:00", "username": "alice", "text": "hello!", "room": "general"}
{"type": "system", "text": "alice joined the room", "room": "general"}
{"type": "roster", "users": ["alice", "bob"], "room": "general"}
{"type": "room_list", "rooms": ["general", "random", "tech"]}
{"type": "typing", "username": "alice", "is_typing": true, "room": "general"}
{"type": "dm", "from": "alice", "text": "hey bob", "timestamp": "16:30:00"}
{"type": "dm_sent", "to": "bob", "text": "hey bob", "timestamp": "16:30:00"}
{"type": "history", "messages": [...], "room": "general"}
{"type": "error", "text": "user 'bob' is not online"}
```

---

## Deploy

### Render (recommended — free tier, no CC required)

1. Push this repo to GitHub.
2. Go to [render.com](https://dashboard.render.com/blueprints).
3. Click **New Blueprint** and select this repo.
4. Render detects `render.yaml` and creates the service automatically.
5. Your app will be live at `https://rust-websocket-chat.onrender.com`.

### Fly.io (requires credit card)

```sh
flyctl deploy
```

The `fly.toml` and `Dockerfile` are already configured.

---

## Dependencies

| crate | why |
|---|---|
| `axum` (`ws`) | HTTP server + WebSocket upgrade |
| `tokio` (`full`) | Async runtime, `broadcast` + `mpsc` channels |
| `tower-http` (`fs`) | `ServeDir` for static frontend |
| `futures-util` | `SinkExt`/`StreamExt` to split the socket |
| `serde` / `serde_json` | JSON wire protocol |
| `uuid` | Unique message IDs |
| `tracing` | Structured logging |

---

## License

MIT
