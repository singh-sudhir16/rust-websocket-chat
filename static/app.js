// Rust WebSocket Chat — frontend logic
//
// Walkthrough:
//  1. User enters a username on the join screen.
//  2. We open a WebSocket to /ws and immediately send the username as the
//     first text frame. The server uses that to register the user.
//  3. Every message we receive is either:
//       - a normal chat line   "[HH:MM:SS] alice: hello"
//       - a system notice       "[SYSTEM] alice joined the chat"
//       - a roster update       "[ROSTER] alice, bob, carol"
//  4. Typing + Enter sends a text frame; the server broadcasts it.

(() => {
  "use strict";

  // ---- DOM references ----
  const joinScreen = document.getElementById("join-screen");
  const chatScreen = document.getElementById("chat-screen");
  const joinForm = document.getElementById("join-form");
  const usernameInput = document.getElementById("username-input");
  const messagesEl = document.getElementById("messages");
  const userCountEl = document.getElementById("user-count");
  const userListEl = document.getElementById("user-list");
  const msgForm = document.getElementById("msg-form");
  const msgInput = document.getElementById("msg-input");
  const sendBtn = document.getElementById("send-btn");
  const leaveBtn = document.getElementById("leave-btn");
  const connDot = document.getElementById("conn-dot");
  const connText = document.getElementById("conn-text");
  const wsStateEl = document.getElementById("ws-state");

  // ---- State ----
  let username = "";
  let ws = null;

  // Derive the WebSocket URL from the current page location so it works
  // whether served from localhost:3000 or any other host/port.
  function wsUrl() {
    const proto = location.protocol === "https:" ? "wss:" : "ws:";
    return `${proto}//${location.host}/ws`;
  }

  // ---- Join flow ----
  joinForm.addEventListener("submit", (e) => {
    e.preventDefault();
    const name = usernameInput.value.trim();
    if (!name) return;
    username = name;
    connect();
  });

  function connect() {
    ws = new WebSocket(wsUrl());

    wsStateEl.textContent = ws.readyState; // 0 = CONNECTING

    // Connection is open: send the username as the first frame, switch screens.
    ws.onopen = () => {
      wsStateEl.textContent = ws.readyState; // 1 = OPEN
      setOnline(true);
      ws.send(username);
      joinScreen.classList.add("hidden");
      chatScreen.classList.remove("hidden");
      msgInput.disabled = false;
      sendBtn.disabled = false;
      msgInput.focus();
    };

    // A message arrived: route it to the right renderer.
    ws.onmessage = (e) => {
      const data = String(e.data);
      if (data.startsWith("[ROSTER]")) {
        renderRoster(data.slice("[ROSTER]".length).trim());
      } else if (data.startsWith("[SYSTEM]")) {
        renderSystem(data.slice("[SYSTEM]".length).trim());
      } else {
        renderChat(data);
      }
    };

    ws.onclose = () => {
      wsStateEl.textContent = 3; // 3 = CLOSED
      setOnline(false);
      msgInput.disabled = true;
      sendBtn.disabled = true;
    };

    ws.onerror = () => {
      // The browser will fire onclose after this; nothing extra to do.
      console.error("WebSocket error");
    };
  }

  // ---- Sending messages ----
  msgForm.addEventListener("submit", (e) => {
    e.preventDefault();
    const text = msgInput.value.trim();
    if (!text || !ws || ws.readyState !== WebSocket.OPEN) return;
    ws.send(text);
    msgInput.value = "";
    msgInput.focus();
  });

  // ---- Leaving ----
  leaveBtn.addEventListener("click", () => {
    if (ws) ws.close();
    chatScreen.classList.add("hidden");
    joinScreen.classList.remove("hidden");
    usernameInput.value = "";
    usernameInput.focus();
    // Reset message history for a clean rejoin.
    messagesEl.innerHTML = "";
    userListEl.innerHTML = "";
    userCountEl.textContent = "0";
  });

  // ---- Renderers ----

  function renderChat(line) {
    // Line shape: "[HH:MM:SS] alice: hello there"
    const match = line.match(/^\[(\d{2}:\d{2}:\d{2})\] (.*?): (.*)$/s);
    const el = document.createElement("div");

    if (match) {
      const [, time, author, body] = match;
      const isSelf = author === username;
      el.className = `msg ${isSelf ? "msg-self" : "msg-other"}`;
      el.innerHTML = `<span class="meta">${escapeHtml(time)} · ${escapeHtml(
        author
      )}</span>${escapeHtml(body)}`;
    } else {
      // Unrecognised format: show it verbatim.
      el.className = "msg msg-other";
      el.textContent = line;
    }

    messagesEl.appendChild(el);
    scrollToBottom();
  }

  function renderSystem(text) {
    const el = document.createElement("div");
    el.className = "msg msg-system";
    el.textContent = text;
    messagesEl.appendChild(el);
    scrollToBottom();
  }

  function renderRoster(csv) {
    const names = csv ? csv.split(",").map((s) => s.trim()).filter(Boolean) : [];
    userListEl.innerHTML = "";
    for (const name of names) {
      const li = document.createElement("li");
      if (name === username) li.classList.add("me");
      li.innerHTML = `<span class="dot dot-online"></span>${escapeHtml(name)}`;
      userListEl.appendChild(li);
    }
    userCountEl.textContent = String(names.length);
  }

  // ---- Helpers ----

  function setOnline(isOnline) {
    connDot.className = `dot ${isOnline ? "dot-online" : "dot-offline"}`;
    connText.textContent = isOnline ? "connected" : "disconnected";
  }

  function scrollToBottom() {
    messagesEl.scrollTop = messagesEl.scrollHeight;
  }

  // Escape user-supplied text before injecting into innerHTML.
  function escapeHtml(s) {
    return s
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#39;");
  }

  // Focus the username field on load.
  usernameInput.focus();
})();
