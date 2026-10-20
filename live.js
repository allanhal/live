const http = require("http");
const crypto = require("crypto");
const express = require("express");
const { WebSocketServer } = require("ws");

const PORT = parseInt(process.env.PORT || "3000", 10);
const PUBLIC_ORIGIN = process.env.PUBLIC_ORIGIN || "https://live.allanpinheiro.com";

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

// rooms: Map<roomId, { broadcaster: ws | null, viewers: Map<viewerId, ws> }>
// Viewers are keyed by id because every broadcaster-originated message has to
// find one: as a Set that meant rebuilding an array of all viewers per message,
// which is O(N^2) allocations across an ICE flood.
const rooms = new Map();

function getRoom(id) {
  if (!rooms.has(id)) rooms.set(id, { broadcaster: null, viewers: new Map() });
  return rooms.get(id);
}

function cleanRoom(id) {
  const room = rooms.get(id);
  if (room && !room.broadcaster && room.viewers.size === 0) rooms.delete(id);
}

// ws.send() on an already-closed socket emits "error", and an "error" with no
// listener throws. Both matter once we start terminating dead sockets.
function send(ws, payload) {
  if (ws && ws.readyState === ws.OPEN) ws.send(JSON.stringify(payload));
}

// --- Reaping idle rooms ---

// A client that vanishes without a TCP FIN (laptop sleeps, wifi drops) never
// fires "close", so its room stays keyed to a broadcaster who is not coming
// back and the next person to open the URL joins as a viewer of nothing.
// Ping every round and drop anything that missed the previous one; the close
// handler then cleans the room normally. The sweep is a backstop for rooms
// created by getRoom() that never reached a close.
const HEARTBEAT_MS = parseInt(process.env.HEARTBEAT_MS || "30000", 10);

const heartbeat = setInterval(() => {
  for (const ws of wss.clients) {
    if (ws.isAlive === false) {
      ws.terminate();
      continue;
    }
    ws.isAlive = false;
    ws.ping();
  }

  for (const [id, room] of rooms) {
    if (!room.broadcaster && room.viewers.size === 0) {
      rooms.delete(id);
      console.log(`[${id}] room reaped (empty)`);
    }
  }
}, HEARTBEAT_MS);

heartbeat.unref();
wss.on("close", () => clearInterval(heartbeat));

// --- WebSocket signaling ---

wss.on("connection", (ws, req) => {
  ws.isAlive = true;
  ws.on("pong", () => { ws.isAlive = true; });
  ws.on("error", () => ws.terminate());

  const url = new URL(req.url, "http://localhost");
  const roomId = url.searchParams.get("room");
  if (!roomId) return ws.close(4000, "missing room");

  // Resolved on join rather than on connect. A socket that never joins should
  // not conjure a room, and a room created here could be swept before the join
  // arrived, leaving this socket holding an object no longer in `rooms` while
  // the next person to join the same URL got a different one.
  let room = null;
  let role = null;

  ws.on("message", (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }

    switch (msg.type) {
      case "join": {
        if (room) break;
        room = getRoom(roomId);
        if (!room.broadcaster) {
          role = "broadcaster";
          room.broadcaster = ws;
          send(ws, { type: "role", role: "broadcaster" });
          // Viewers already waiting in this room would otherwise never be
          // offered a stream: "viewer-joined" only fired at their join, which
          // happened before there was a broadcaster to hear it.
          for (const viewerId of room.viewers.keys()) {
            send(ws, { type: "viewer-joined", viewerId });
          }
          console.log(`[${roomId}] broadcaster connected (${room.viewers.size} viewers waiting)`);
        } else {
          role = "viewer";
          room.viewers.set(idFor(ws), ws);
          send(ws, { type: "role", role: "viewer" });
          send(room.broadcaster, { type: "viewer-joined", viewerId: idFor(ws) });
          console.log(`[${roomId}] viewer connected (${room.viewers.size} total)`);
        }
        break;
      }

      case "offer":
      case "answer":
      case "ice-candidate": {
        if (!room) break;
        if (role === "broadcaster" && msg.viewerId) {
          const target = room.viewers.get(msg.viewerId);
          if (target) send(target, { type: msg.type, data: msg.data });
        } else if (role === "viewer" && room.broadcaster) {
          send(room.broadcaster, { type: msg.type, data: msg.data, viewerId: idFor(ws) });
        }
        break;
      }
    }
  });

  ws.on("close", () => {
    if (!room) return;
    if (role === "broadcaster") {
      room.broadcaster = null;
      for (const v of room.viewers.values()) send(v, { type: "broadcaster-left" });
      // Keep the viewers: they are still connected and waiting, and clearing
      // them here deleted the room out from under them, so a returning
      // broadcaster got a fresh room object the old viewers were not in.
      console.log(`[${roomId}] broadcaster disconnected (${room.viewers.size} viewers waiting)`);
    } else if (role === "viewer") {
      room.viewers.delete(idFor(ws));
      if (room.broadcaster) {
        send(room.broadcaster, { type: "viewer-left", viewerId: idFor(ws) });
      }
      console.log(`[${roomId}] viewer disconnected (${room.viewers.size} remaining)`);
    }
    cleanRoom(roomId);
  });
});

const wsIds = new WeakMap();
let nextId = 1;
function idFor(ws) {
  if (!wsIds.has(ws)) wsIds.set(ws, String(nextId++));
  return wsIds.get(ws);
}

// --- Room IDs ---

// Short, speakable slugs like "swift-otter-24" so a room can be read out loud
// or typed from memory instead of copied character by character.
const ADJECTIVES = [
  "amber", "brave", "bright", "calm", "clever", "crisp", "daring", "eager",
  "fair", "fancy", "gentle", "glad", "golden", "happy", "jolly", "keen",
  "kind", "lively", "lucky", "merry", "mighty", "neat", "noble", "plucky",
  "proud", "quick", "quiet", "rapid", "royal", "sharp", "shiny", "silver",
  "smart", "snappy", "solid", "spry", "sunny", "swift", "tidy", "tough",
  "vivid", "warm", "wise", "witty", "zesty",
];

const NOUNS = [
  "otter", "falcon", "tiger", "panda", "koala", "heron", "lynx", "raven",
  "bison", "cobra", "dingo", "eagle", "ferret", "gecko", "hawk", "ibis",
  "jaguar", "kiwi", "lemur", "moose", "newt", "orca", "puffin", "quail",
  "rabbit", "salmon", "shark", "sloth", "stork", "swan", "tapir", "toucan",
  "turtle", "viper", "walrus", "weasel", "wombat", "yak", "zebra", "badger",
  "beaver", "camel", "crane", "donkey", "marmot",
];

function newRoomId() {
  const pick = (arr) => arr[crypto.randomInt(arr.length)];
  return pick(ADJECTIVES) + "-" + pick(NOUNS) + "-" + crypto.randomInt(10, 100);
}

// Any room id reaching :roomId is interpolated into the page, so keep it to a
// charset that cannot break out of the HTML or the JS string literal. Staying
// permissive lets people pick their own room, e.g. /standup.
const ROOM_ID_RE = /^[a-z0-9][a-z0-9-]{0,39}$/;

// --- HTTP routes ---

app.get("/", (_req, res) => {
  res.redirect(`/${newRoomId()}`);
});

// Registered before :roomId, which would otherwise match "healthz" as a room.
app.get("/healthz", (_req, res) => {
  res.json({
    ok: true,
    rooms: rooms.size,
    clients: wss.clients.size,
    uptime: Math.round(process.uptime()),
  });
});

app.get("/:roomId", (req, res) => {
  const roomId = req.params.roomId.toLowerCase();
  if (!ROOM_ID_RE.test(roomId)) return res.status(404).type("text").send("Not found");
  // Canonicalise so the URL people share is the one the room is keyed on.
  if (roomId !== req.params.roomId) return res.redirect(`/${roomId}`);
  res.type("html").send(PAGE_HTML.replace(/__ROOM_ID__/g, roomId));
});

// --- Embedded page ---

const PAGE_HTML = /* html */ `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Live Stream</title>
<style>
  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
  body {
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
    background: #0a0a0a; color: #e0e0e0;
    display: flex; flex-direction: column; align-items: center; justify-content: center;
    min-height: 100vh; padding: 16px;
  }
  #status {
    position: fixed; top: 0; left: 0; right: 0;
    padding: 10px 16px; text-align: center;
    background: #121212;
    font-size: 14px; z-index: 10;
  }
  .dot { display: inline-block; width: 8px; height: 8px; border-radius: 50%; margin-right: 6px; vertical-align: middle; }
  .dot-red { background: #e53e3e; }
  .dot-green { background: #38a169; animation: pulse 2s infinite; will-change: opacity; }
  .dot-yellow { background: #d69e2e; }
  @keyframes pulse { 0%,100% { opacity: 1; } 50% { opacity: 0.5; } }
  #video-container {
    position: relative; width: 100%; max-width: 1280px;
    contain: paint;
    background: #111; border-radius: 8px; overflow: hidden;
    aspect-ratio: 16/9;
  }
  video {
    width: 100%; height: 100%; object-fit: contain; background: #000;
  }
  #start-btn {
    position: absolute; top: 50%; left: 50%; transform: translate(-50%, -50%);
    padding: 14px 32px; font-size: 18px; font-weight: 600;
    background: #e53e3e; color: white; border: none; border-radius: 8px;
    cursor: pointer; transition: background 0.2s; z-index: 2;
  }
  #start-btn:hover { background: #c53030; }
  #volume {
    position: absolute; bottom: 16px; right: 16px;
    display: none; align-items: center; gap: 10px;
    padding: 8px 14px; z-index: 3;
    background: rgba(18, 18, 18, 0.82); border-radius: 6px;
    /* The control sits over the video, so it has to stay readable on a white
       frame as well as a dark one. */
    backdrop-filter: blur(4px);
  }
  #mute-btn {
    padding: 0; font-size: 18px; line-height: 1;
    background: none; color: #e0e0e0; border: none;
    cursor: pointer;
  }
  /* The slider is the hint to unmute while the stream is muted, so it is worth
     more than the 2px the UA default gives it. */
  #volume-slider {
    -webkit-appearance: none; appearance: none;
    width: 90px; height: 4px; border-radius: 2px;
    background: #444; cursor: pointer;
  }
  #volume-slider::-webkit-slider-thumb {
    -webkit-appearance: none; appearance: none;
    width: 13px; height: 13px; border-radius: 50%;
    background: #e0e0e0; border: none; cursor: pointer;
  }
  #volume-slider::-moz-range-thumb {
    width: 13px; height: 13px; border-radius: 50%;
    background: #e0e0e0; border: none; cursor: pointer;
  }
  #volume-slider:focus-visible { outline: 2px solid #4a90d9; outline-offset: 4px; }
  #volume.muted #volume-slider { background: #333; }
  #volume.muted #volume-slider::-webkit-slider-thumb { background: #777; }
  #volume.muted #volume-slider::-moz-range-thumb { background: #777; }
  #waiting {
    position: absolute; top: 50%; left: 50%; transform: translate(-50%, -50%);
    color: #888; font-size: 16px;
  }
  #info { margin-top: 12px; font-size: 13px; color: #666; text-align: center; }
  #viewers-count { position: fixed; top: 0; right: 16px; padding: 10px; font-size: 13px; color: #999; z-index: 11; }
</style>
</head>
<body>

<div id="status"><span class="dot dot-yellow"></span> Connecting...</div>
<div id="viewers-count"></div>

<div id="video-container">
  <button id="start-btn" style="display:none">Share Screen</button>
  <video id="video" autoplay playsinline></video>
  <div id="waiting" style="display:none">Waiting for stream...</div>
  <div id="volume" class="muted">
    <button id="mute-btn" title="Unmute" aria-label="Unmute">🔇</button>
    <input id="volume-slider" type="range" min="0" max="100" value="100"
           aria-label="Volume" title="Volume">
  </div>
</div>

<div id="info"></div>

<script>
const ROOM_ID = "__ROOM_ID__";
const video = document.getElementById("video");
const startBtn = document.getElementById("start-btn");
const statusEl = document.getElementById("status");
const waitingEl = document.getElementById("waiting");
const infoEl = document.getElementById("info");
const viewersEl = document.getElementById("viewers-count");
const volumeEl = document.getElementById("volume");
const muteBtn = document.getElementById("mute-btn");
const volumeSlider = document.getElementById("volume-slider");

let role = null;
let ws = null;
let localStream = null;
const peerConnections = new Map(); // viewerId -> RTCPeerConnection
const waitingViewers = new Set();  // viewerIds announced before we had a stream

const ICE_SERVERS = [
  { urls: "stun:stun.l.google.com:19302" },
  { urls: "stun:stun1.l.google.com:19302" },
];

function connect() {
  const proto = location.protocol === "https:" ? "wss:" : "ws:";
  ws = new WebSocket(proto + "//" + location.host + "/?room=" + ROOM_ID);

  ws.onopen = () => {
    ws.send(JSON.stringify({ type: "join" }));
    setStatus("yellow", "Joined room, determining role...");
  };

  ws.onmessage = async (e) => {
    const msg = JSON.parse(e.data);

    switch (msg.type) {
      case "role":
        role = msg.role;
        if (role === "broadcaster") {
          waitingEl.style.display = "none";
          if (localStream) {
            startBtn.style.display = "none";
            setStatus("green", "Broadcasting");
          } else {
            startBtn.style.display = "block";
            setStatus("yellow", "Ready to broadcast — click Share Screen");
          }
          infoEl.textContent = "Share this URL with viewers: " + location.href;
        } else {
          startBtn.style.display = "none";
          waitingEl.style.display = "block";
          setStatus("yellow", "Waiting for broadcaster to start...");
        }
        break;

      // --- Broadcaster events ---
      case "viewer-joined":
        if (role === "broadcaster") {
          // Before Share Screen there is nothing to offer, so remember them and
          // connect once the stream exists. Without this a viewer who opened
          // the URL first waited forever.
          if (localStream) await createPeerForViewer(msg.viewerId);
          else waitingViewers.add(msg.viewerId);
        }
        updateViewerCount();
        break;

      case "viewer-left":
        waitingViewers.delete(msg.viewerId);
        if (peerConnections.has(msg.viewerId)) {
          peerConnections.get(msg.viewerId).close();
          peerConnections.delete(msg.viewerId);
        }
        if (peerConnections.size === 0) stopQualityMonitor();
        updateViewerCount();
        break;

      case "answer":
        if (role === "broadcaster" && msg.viewerId) {
          const pc = peerConnections.get(msg.viewerId);
          if (pc) await pc.setRemoteDescription(msg.data);
        }
        break;

      // --- Viewer events ---
      case "offer":
        if (role === "viewer") await handleOffer(msg.data);
        break;

      case "broadcaster-left":
        video.srcObject = null;
        waitingEl.style.display = "block";
        setStatus("red", "Broadcaster disconnected");
        break;

      case "ice-candidate":
        if (role === "broadcaster" && msg.viewerId) {
          const pc = peerConnections.get(msg.viewerId);
          if (pc && msg.data) await pc.addIceCandidate(msg.data);
        } else if (role === "viewer" && viewerPC) {
          if (msg.data) await viewerPC.addIceCandidate(msg.data);
        }
        break;
    }
  };

  ws.onclose = () => {
    setStatus("red", "Disconnected — reconnecting in 3s...");
    setTimeout(connect, 3000);
  };
}

// --- Broadcaster logic ---

startBtn.addEventListener("click", async () => {
  try {
    localStream = await navigator.mediaDevices.getDisplayMedia({
      video: { cursor: "always", width: { ideal: 1920 }, height: { ideal: 1080 }, frameRate: { ideal: 60 } },
      audio: { echoCancellation: false, noiseSuppression: false, autoGainControl: false, sampleRate: 48000 },
    });

    video.srcObject = localStream;
    video.muted = true;
    startBtn.style.display = "none";
    setStatus("green", "Broadcasting");

    // Connect anyone who was already in the room before we started sharing.
    for (const viewerId of waitingViewers) await createPeerForViewer(viewerId);
    waitingViewers.clear();
    updateViewerCount();

    localStream.getVideoTracks()[0].onended = () => {
      setStatus("red", "Screen share stopped");
      for (const [, pc] of peerConnections) pc.close();
      peerConnections.clear();
      stopQualityMonitor();
      currentTierIndex = 0;
      localStream = null;
      video.srcObject = null;
      startBtn.style.display = "block";
      updateViewerCount();
    };
  } catch (err) {
    setStatus("red", "Screen share cancelled or failed");
  }
});

async function createPeerForViewer(viewerId) {
  const pc = new RTCPeerConnection({ iceServers: ICE_SERVERS });
  peerConnections.set(viewerId, pc);

  for (const track of localStream.getTracks()) {
    pc.addTrack(track, localStream);
  }

  // Aim the encoder at the current tier before negotiating. Without this the
  // sender runs at the browser default and the monitor is comparing throughput
  // against a target nobody set.
  await applyQualityTier(pc, currentTierIndex);

  pc.onicecandidate = (e) => {
    if (e.candidate) {
      ws.send(JSON.stringify({ type: "ice-candidate", viewerId, data: e.candidate }));
    }
  };

  const offer = await pc.createOffer();
  await pc.setLocalDescription(offer);
  ws.send(JSON.stringify({ type: "offer", viewerId, data: pc.localDescription }));

  startQualityMonitor();
}

// --- Adaptive bitrate ---

const QUALITY_TIERS = [
  { label: "high",   maxBitrate: 8_000_000, scaleDown: 1 },
  { label: "medium", maxBitrate: 3_000_000, scaleDown: 1.5 },
  { label: "low",    maxBitrate: 1_000_000, scaleDown: 2 },
  { label: "potato", maxBitrate: 500_000,   scaleDown: 3 },
];

let currentTierIndex = 0;

async function applyQualityTier(pc, tierIndex) {
  const tier = QUALITY_TIERS[tierIndex];
  const sender = pc.getSenders().find(s => s.track && s.track.kind === "video");
  if (!sender) return;
  const params = sender.getParameters();
  if (!params.encodings || params.encodings.length === 0) params.encodings = [{}];
  params.encodings[0].maxBitrate = tier.maxBitrate;
  params.encodings[0].scaleResolutionDownBy = tier.scaleDown;
  await sender.setParameters(params);
}

// One monitor for the whole broadcast, not one per viewer. The tier is a
// single shared decision (every viewer is fed from one uplink and one CPU), so
// a timer per viewer meant N of them racing to mutate currentTierIndex and
// falling through the tiers N times too fast.
//
// The old test compared measured throughput against the tier's maxBitrate.
// That could only ever ratchet down: nothing applied the tier to the sender at
// peer creation, so the encoder targeted the browser default of roughly 2 Mbps
// and "below 40% of 8 Mbps" was true from the first sample on an idle network.
// Three samples later it had dropped a tier, and recovery needed throughput
// above 85% of a target that was never set, which a static screen never
// reaches. Every session ended at 500 kbps and stayed there.
//
// qualityLimitationReason is the signal that actually answers the question:
// the encoder reports whether it is being held back, and by what.
const MONITOR_MS = 2000;
const DOWNGRADE_AFTER = 3;   // ~6s limited before dropping a tier
const UPGRADE_AFTER = 8;     // ~16s clear before climbing back

let qualityMonitor = null;
let limitedRounds = 0;
let clearRounds = 0;

function startQualityMonitor() {
  if (qualityMonitor) return;
  limitedRounds = 0;
  clearRounds = 0;

  qualityMonitor = setInterval(async () => {
    if (peerConnections.size === 0) return;

    let sawVideo = false;
    let limitedBy = null;

    for (const pc of peerConnections.values()) {
      if (pc.connectionState !== "connected") continue;
      const stats = await pc.getStats();
      for (const report of stats.values()) {
        if (report.type === "outbound-rtp" && report.kind === "video") {
          sawVideo = true;
          const reason = report.qualityLimitationReason;
          if (reason && reason !== "none") limitedBy = reason;
        }
      }
    }

    if (!sawVideo) return;

    if (limitedBy) {
      limitedRounds++;
      clearRounds = 0;
    } else {
      clearRounds++;
      limitedRounds = 0;
    }

    if (limitedRounds >= DOWNGRADE_AFTER && currentTierIndex < QUALITY_TIERS.length - 1) {
      currentTierIndex++;
      limitedRounds = 0;
      console.log("[quality] limited by " + limitedBy + ", dropping to " + QUALITY_TIERS[currentTierIndex].label);
      await retuneAllPeers();
    } else if (clearRounds >= UPGRADE_AFTER && currentTierIndex > 0) {
      currentTierIndex--;
      clearRounds = 0;
      console.log("[quality] unconstrained, raising to " + QUALITY_TIERS[currentTierIndex].label);
      await retuneAllPeers();
    }
  }, MONITOR_MS);
}

function stopQualityMonitor() {
  clearInterval(qualityMonitor);
  qualityMonitor = null;
}

async function retuneAllPeers() {
  for (const pc of peerConnections.values()) await applyQualityTier(pc, currentTierIndex);
  const tier = QUALITY_TIERS[currentTierIndex];
  setStatus("green", currentTierIndex === 0
    ? "Broadcasting"
    : "Broadcasting (" + tier.label + " quality)");
}

// --- Viewer logic ---

let viewerPC = null;

async function handleOffer(offer) {
  viewerPC = new RTCPeerConnection({ iceServers: ICE_SERVERS });

  viewerPC.ontrack = (e) => {
    video.srcObject = e.streams[0] || new MediaStream([e.track]);
    // A new stream has to start muted to autoplay, so put the control back in
    // its muted state rather than leaving it showing sound that is not playing.
    applyVolume(lastVolume, true);
    waitingEl.style.display = "none";
    setStatus("green", "Watching stream");
    video.play().then(() => {
      if (role === "viewer") volumeEl.style.display = "flex";
    }).catch(() => {});
  };

  viewerPC.onicecandidate = (e) => {
    if (e.candidate) {
      ws.send(JSON.stringify({ type: "ice-candidate", data: e.candidate }));
    }
  };

  await viewerPC.setRemoteDescription(offer);
  const answer = await viewerPC.createAnswer();
  await viewerPC.setLocalDescription(answer);
  ws.send(JSON.stringify({ type: "answer", data: viewerPC.localDescription }));
}

// --- Volume ---

// Autoplay only survives if the video starts muted, so the control opens muted
// and the first interaction with either half of it is the unmute.
const VOLUME_KEY = "live:volume";

let lastVolume = 1;
try {
  const stored = parseFloat(localStorage.getItem(VOLUME_KEY));
  if (stored > 0 && stored <= 1) lastVolume = stored;
} catch {}

volumeSlider.value = String(Math.round(lastVolume * 100));

function applyVolume(level, muted) {
  video.muted = muted || level === 0;
  video.volume = level;
  volumeSlider.value = String(Math.round(level * 100));
  volumeEl.classList.toggle("muted", video.muted);
  muteBtn.textContent = video.muted ? "🔇" : level > 0.5 ? "🔊" : "🔈";
  const action = video.muted ? "Unmute" : "Mute";
  muteBtn.title = action;
  muteBtn.setAttribute("aria-label", action);
  // Only a level worth returning to is remembered: storing 0 would make the
  // next visit mute itself with no way to tell that from a fresh one.
  if (level > 0) {
    lastVolume = level;
    try { localStorage.setItem(VOLUME_KEY, String(level)); } catch {}
  }
}

muteBtn.addEventListener("click", () => {
  if (video.muted) applyVolume(lastVolume, false);
  else applyVolume(video.volume, true);
});

volumeSlider.addEventListener("input", () => {
  // Dragging off zero is an unmute: nobody moves the slider to stay silent.
  applyVolume(volumeSlider.value / 100, false);
});

applyVolume(lastVolume, true);

// --- UI helpers ---

const statusDot = document.createElement("span");
const statusText = document.createTextNode("");
statusEl.textContent = "";
statusEl.append(statusDot, " ", statusText);

function setStatus(color, text) {
  // Assign rather than rebuild with innerHTML: recreating the dot restarted its
  // animation and re-parsed the bar on every status change.
  statusDot.className = "dot dot-" + color;
  statusText.nodeValue = text;
}

function updateViewerCount() {
  if (role === "broadcaster") {
    const n = peerConnections.size;
    viewersEl.textContent = n + " viewer" + (n !== 1 ? "s" : "");
  }
}

connect();
</script>
</body>
</html>`;

server.listen(PORT, () => {
  const roomId = newRoomId();
  console.log("");
  console.log("  Live stream server running!");
  console.log("");
  console.log("  Local:   http://localhost:" + PORT + "/" + roomId);
  console.log("  Public:  " + PUBLIC_ORIGIN + "/" + roomId);
  console.log("");
  console.log("  Open the local link to start broadcasting,");
  console.log("  then share the public link with viewers.");
  console.log("");
});
