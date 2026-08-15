// Rust WebSocket Chat v0.2 — Frontend Logic
//
// Features:
//  - JSON wire protocol (matches src/protocol.rs)
//  - Multi-room switching with live roster per room
//  - Direct messages (DMs) via user list clicks
//  - Typing indicators with debounce
//  - Reconnection with exponential backoff + missed-message replay
//  - Message history persisted in localStorage (last_seen_id)

(() => {
  "use strict";

  // ===== DOM =====
  const joinScreen = document.getElementById("join-screen");
  const chatScreen = document.getElementById("chat-screen");
  const joinForm = document.getElementById("join-form");
  const usernameInput = document.getElementById("username-input");
  const messagesEl = document.getElementById("messages");
  const msgForm = document.getElementById("msg-form");
  const msgInput = document.getElementById("msg-input");
  const sendBtn = document.getElementById("send-btn");
  const leaveBtn = document.getElementById("leave-btn");
  const roomListEl = document.getElementById("room-list");
  const onlineListEl = document.getElementById("online-list");
  const onlineCountEl = document.getElementById("online-count");
  const currentRoomNameEl = document.getElementById("current-room-name");
  const roomHeaderEl = document.getElementById("room-header");
  const dmBarEl = document.getElementById("dm-bar");
  const dmPeerNameEl = document.getElementById("dm-peer-name");
  const dmBackBtn = document.getElementById("dm-back-btn");
  const connDot = document.getElementById("conn-dot");
  const connText = document.getElementById("conn-text");
  const typingBar = document.getElementById("typing-bar");
  const typingText = document.getElementById("typing-text");
  const reconnectBanner = document.getElementById("reconnect-banner");
  const reconnectText = document.getElementById("reconnect-text");
  const roomTypingEl = document.getElementById("room-typing");

  // ===== State =====
  let username = "";
  let ws = null;
  let currentRoom = "general";
  let viewMode = "room"; // "room" or "dm"
  let dmPeer = null;
  let rooms = [];
  let rosterByRoom = {}; // { "general": ["alice", "bob"], ... }
  let typingUsers = new Set(); // users currently typing in current room
  let typingDebounceTimer = null;
  let lastSeenId = 0;
  let reconnectAttempts = 0;
  let reconnectTimer = null;
  let intentionallyClosed = false;
  // Message store: { room: [msgs], "__dm__:alice": [msgs] }
  let messageStore = {};

  // ===== WebSocket URL =====
  function wsUrl() {
    const proto = location.protocol === "https:" ? "wss:" : "ws:";
    return `${proto}//${location.host}/ws`;
  }

  // ===== LocalStorage helpers =====
  function loadLastSeenId() {
    const val = localStorage.getItem("rwc:last_seen_id");
    return val ? parseInt(val, 10) : 0;
  }
  function saveLastSeenId(id) {
    if (id > lastSeenId) {
      lastSeenId = id;
      localStorage.setItem("rwc:last_seen_id", String(id));
    }
  }

  // ===== Join flow =====
  joinForm.addEventListener("submit", (e) => {
    e.preventDefault();
    const name = usernameInput.value.trim();
    if (!name) return;
    username = name;
    lastSeenId = loadLastSeenId();
    intentionallyClosed = false;
    connect();
  });

  function connect() {
    ws = new WebSocket(wsUrl());
    updateConnStatus("connecting");

    ws.onopen = () => {
      reconnectAttempts = 0;
      updateConnStatus("online");
      hideReconnectBanner();

      // Send Join or Reconnect frame
      if (reconnectAttempts === 0 && lastSeenId > 0) {
        sendFrame({ type: "reconnect", username, last_seen_id: lastSeenId });
      } else {
        sendFrame({ type: "join", username });
      }

      // Switch screens
      joinScreen.classList.add("hidden");
      chatScreen.classList.remove("hidden");
      msgInput.disabled = false;
      sendBtn.disabled = false;
      msgInput.focus();
    };

    ws.onmessage = (e) => {
      let frame;
      try {
        frame = JSON.parse(e.data);
      } catch {
        return;
      }
      handleServerFrame(frame);
    };

    ws.onclose = () => {
      updateConnStatus("offline");
      msgInput.disabled = true;
      sendBtn.disabled = true;

      if (!intentionallyClosed) {
        showReconnectBanner();
        scheduleReconnect();
      }
    };

    ws.onerror = () => {};
  }

  function scheduleReconnect() {
    const delay = Math.min(1000 * 2 ** reconnectAttempts, 30000);
    reconnectAttempts++;
    reconnectText.textContent = `Reconnecting… (attempt ${reconnectAttempts}, ${Math.round(delay / 1000)}s)`;
    reconnectTimer = setTimeout(() => {
      connect();
    }, delay);
  }

  function showReconnectBanner() {
    reconnectBanner.classList.remove("hidden");
  }

  function hideReconnectBanner() {
    reconnectBanner.classList.add("hidden");
  }

  // ===== Send helpers =====
  function sendFrame(obj) {
    if (ws && ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify(obj));
    }
  }

  // ===== Server frame handler =====
  function handleServerFrame(frame) {
    switch (frame.type) {
      case "chat":
        addMessage(frame.room, {
          kind: frame.username === username ? "self" : "other",
          author: frame.username,
          text: frame.text,
          timestamp: frame.timestamp,
        });
        saveLastSeenId(frame.id);
        if (frame.room === currentRoom && viewMode === "room") {
          renderMessages();
        }
        break;

      case "system":
        addMessage(frame.room, {
          kind: "system",
          text: frame.text,
        });
        if (frame.room === currentRoom && viewMode === "room") {
          renderMessages();
        }
        break;

      case "roster":
        rosterByRoom[frame.room] = frame.users;
        if (frame.room === currentRoom) {
          renderOnlineList();
        }
        break;

      case "room_list":
        rooms = frame.rooms;
        renderRoomList();
        break;

      case "typing":
        if (frame.username === username) break;
        if (frame.room !== currentRoom) break;
        if (frame.is_typing) {
          typingUsers.add(frame.username);
        } else {
          typingUsers.delete(frame.username);
        }
        renderTyping();
        break;

      case "dm":
        // Incoming DM from another user
        {
          const key = `__dm__:${frame.from}`;
          addMessage(key, {
            kind: "other",
            author: frame.from,
            text: frame.text,
            timestamp: frame.timestamp,
          });
          if (viewMode === "dm" && dmPeer === frame.from) {
            renderMessages();
          } else {
            // Flash the user in the online list
            flashUser(frame.from);
          }
        }
        break;

      case "dm_sent":
        // Confirmation of a DM we sent
        {
          const key = `__dm__:${frame.to}`;
          addMessage(key, {
            kind: "self",
            author: username,
            text: frame.text,
            timestamp: frame.timestamp,
          });
          if (viewMode === "dm" && dmPeer === frame.to) {
            renderMessages();
          }
        }
        break;

      case "history":
        // Replayed messages from reconnect
        if (frame.messages.length > 0) {
          for (const msg of frame.messages) {
            if (msg.type === "chat") {
              addMessage(frame.room, {
                kind: msg.username === username ? "self" : "other",
                author: msg.username,
                text: msg.text,
                timestamp: msg.timestamp,
              });
              saveLastSeenId(msg.id);
            }
          }
          if (frame.room === currentRoom && viewMode === "room") {
            renderMessages();
          }
        }
        break;

      case "error":
        flashError(frame.text);
        break;
    }
  }

  // ===== Message store =====
  function addMessage(key, msg) {
    if (!messageStore[key]) messageStore[key] = [];
    messageStore[key].push(msg);
    // Cap at 500 messages per key
    if (messageStore[key].length > 500) {
      messageStore[key].shift();
    }
  }

  function getCurrentKey() {
    if (viewMode === "dm" && dmPeer) {
      return `__dm__:${dmPeer}`;
    }
    return currentRoom;
  }

  // ===== Rendering =====

  function renderMessages() {
    const key = getCurrentKey();
    const msgs = messageStore[key] || [];
    messagesEl.innerHTML = "";
    for (const m of msgs) {
      const el = document.createElement("div");
      el.className = `msg msg-${m.kind}`;
      if (m.kind === "system") {
        el.textContent = m.text;
      } else {
        const meta = document.createElement("span");
        meta.className = "meta";
        meta.textContent = `${m.timestamp || ""} · ${m.author}`;
        el.appendChild(meta);
        const body = document.createElement("span");
        body.textContent = m.text;
        el.appendChild(body);
      }
      messagesEl.appendChild(el);
    }
    scrollToBottom();
  }

  function renderRoomList() {
    roomListEl.innerHTML = "";
    for (const room of rooms) {
      const li = document.createElement("li");
      li.textContent = room;
      if (room === currentRoom && viewMode === "room") {
        li.classList.add("active");
      }
      li.addEventListener("click", () => switchRoom(room));
      roomListEl.appendChild(li);
    }
  }

  function renderOnlineList() {
    const users = rosterByRoom[currentRoom] || [];
    onlineCountEl.textContent = String(users.length);
    onlineListEl.innerHTML = "";
    for (const name of users) {
      const li = document.createElement("li");
      const dot = document.createElement("span");
      dot.className = "dot";
      li.appendChild(dot);
      const span = document.createElement("span");
      span.textContent = name;
      li.appendChild(span);
      if (name === username) {
        li.classList.add("me");
      } else {
        li.addEventListener("click", () => openDm(name));
      }
      onlineListEl.appendChild(li);
    }
  }

  function renderTyping() {
    if (viewMode !== "room") {
      typingBar.classList.add("hidden");
      roomTypingEl.textContent = "";
      return;
    }

    const users = Array.from(typingUsers);
    if (users.length === 0) {
      typingBar.classList.add("hidden");
      roomTypingEl.textContent = "";
    } else if (users.length === 1) {
      typingBar.classList.remove("hidden");
      typingText.textContent = `${users[0]} is typing…`;
      roomTypingEl.textContent = `${users[0]} is typing…`;
    } else {
      typingBar.classList.remove("hidden");
      typingText.textContent = `${users.length} people are typing…`;
      roomTypingEl.textContent = `${users.length} people are typing…`;
    }
  }

  function scrollToBottom() {
    messagesEl.scrollTop = messagesEl.scrollHeight;
  }

  // ===== Actions =====

  function switchRoom(room) {
    if (room === currentRoom && viewMode === "room") return;
    viewMode = "room";
    currentRoom = room;
    sendFrame({ type: "switch_room", room });
    currentRoomNameEl.textContent = `#${room}`;
    roomHeaderEl.classList.remove("hidden");
    dmBarEl.classList.add("hidden");
    typingUsers.clear();
    renderTyping();
    renderRoomList();
    renderMessages();
    renderOnlineList();
    msgInput.focus();
  }

  function openDm(peer) {
    if (peer === username) return;
    viewMode = "dm";
    dmPeer = peer;
    dmPeerNameEl.textContent = peer;
    roomHeaderEl.classList.add("hidden");
    dmBarEl.classList.remove("hidden");
    typingBar.classList.add("hidden");
    typingUsers.clear();
    renderTyping();
    renderRoomList();
    renderMessages();
    msgInput.focus();
  }

  function closeDm() {
    viewMode = "room";
    dmPeer = null;
    roomHeaderEl.classList.remove("hidden");
    dmBarEl.classList.add("hidden");
    renderMessages();
    renderRoomList();
    msgInput.focus();
  }

  // ===== Sending messages =====
  msgForm.addEventListener("submit", (e) => {
    e.preventDefault();
    const text = msgInput.value.trim();
    if (!text || !ws || ws.readyState !== WebSocket.OPEN) return;

    if (viewMode === "dm" && dmPeer) {
      sendFrame({ type: "dm", to: dmPeer, text });
    } else {
      sendFrame({ type: "chat", text });
    }

    msgInput.value = "";
    // Stop typing indicator
    sendTyping(false);
    msgInput.focus();
  });

  // ===== Typing indicator =====
  msgInput.addEventListener("input", () => {
    if (viewMode !== "room") return;
    sendTyping(true);
    clearTimeout(typingDebounceTimer);
    typingDebounceTimer = setTimeout(() => sendTyping(false), 2000);
  });

  function sendTyping(isTyping) {
    sendFrame({ type: "typing", is_typing: isTyping });
  }

  // ===== Leave =====
  leaveBtn.addEventListener("click", () => {
    intentionallyClosed = true;
    clearTimeout(reconnectTimer);
    if (ws) ws.close();
    chatScreen.classList.add("hidden");
    joinScreen.classList.remove("hidden");
    usernameInput.value = "";
    usernameInput.focus();
    messageStore = {};
    rosterByRoom = {};
    typingUsers.clear();
    currentRoom = "general";
    viewMode = "room";
  });

  dmBackBtn.addEventListener("click", closeDm);

  // ===== Connection status =====
  function updateConnStatus(status) {
    switch (status) {
      case "online":
        connDot.className = "dot dot-online";
        connText.textContent = "connected";
        break;
      case "connecting":
        connDot.className = "dot dot-offline";
        connText.textContent = "connecting…";
        break;
      case "offline":
        connDot.className = "dot dot-offline";
        connText.textContent = "disconnected";
        break;
    }
  }

  // ===== Flash helpers =====
  function flashUser(name) {
    const items = onlineListEl.querySelectorAll("li");
    for (const li of items) {
      if (li.querySelector("span:last-child")?.textContent === name) {
        li.style.background = "var(--accent-dim)";
        setTimeout(() => (li.style.background = ""), 1500);
      }
    }
  }

  function flashError(text) {
    const el = document.createElement("div");
    el.className = "msg msg-system";
    el.style.color = "var(--red)";
    el.textContent = `⚠ ${text}`;
    messagesEl.appendChild(el);
    scrollToBottom();
    setTimeout(() => el.remove(), 4000);
  }

  // ===== Init =====
  usernameInput.focus();
})();
