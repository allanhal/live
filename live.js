const http = require("http");
const crypto = require("crypto");
const express = require("express");
const { WebSocketServer } = require("ws");

const PORT = parseInt(process.env.PORT || "3000", 10);

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

// rooms: Map<roomId, { broadcaster: ws | null, viewers: Set<ws> }>
const rooms = new Map();

function getRoom(id) {
  if (!rooms.has(id)) rooms.set(id, { broadcaster: null, viewers: new Set() });
  return rooms.get(id);
}

function cleanRoom(id) {
  const room = rooms.get(id);
  if (room && !room.broadcaster && room.viewers.size === 0) rooms.delete(id);
}

// --- WebSocket signaling ---

wss.on("connection", (ws, req) => {
  const url = new URL(req.url, "http://localhost");
  const roomId = url.searchParams.get("room");
  if (!roomId) return ws.close(4000, "missing room");

  const room = getRoom(roomId);
  let role = null;

  ws.on("message", (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }

    switch (msg.type) {
      case "join": {
        if (!room.broadcaster) {
          role = "broadcaster";
          room.broadcaster = ws;
          ws.send(JSON.stringify({ type: "role", role: "broadcaster" }));
          console.log(`[${roomId}] broadcaster connected`);
        } else {
          role = "viewer";
          room.viewers.add(ws);
          ws.send(JSON.stringify({ type: "role", role: "viewer" }));
          room.broadcaster.send(JSON.stringify({ type: "viewer-joined", viewerId: idFor(ws) }));
          console.log(`[${roomId}] viewer connected (${room.viewers.size} total)`);
        }
        break;
      }

      case "offer":
      case "answer":
      case "ice-candidate": {
        if (role === "broadcaster" && msg.viewerId) {
          const target = [...room.viewers].find((v) => idFor(v) === msg.viewerId);
          if (target) target.send(JSON.stringify({ type: msg.type, data: msg.data }));
        } else if (role === "viewer" && room.broadcaster) {
          room.broadcaster.send(JSON.stringify({ type: msg.type, data: msg.data, viewerId: idFor(ws) }));
        }
        break;
      }
    }
  });

  ws.on("close", () => {
    if (role === "broadcaster") {
      room.broadcaster = null;
      for (const v of room.viewers) v.send(JSON.stringify({ type: "broadcaster-left" }));
      room.viewers.clear();
      console.log(`[${roomId}] broadcaster disconnected`);
    } else if (role === "viewer") {
      room.viewers.delete(ws);
      if (room.broadcaster) {
        room.broadcaster.send(JSON.stringify({ type: "viewer-left", viewerId: idFor(ws) }));
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

// --- HTTP routes ---

app.get("/", (_req, res) => {
  const roomId = crypto.randomBytes(4).toString("hex");
  res.redirect(`/${roomId}`);
});

app.get("/:roomId", (req, res) => {
  const { roomId } = req.params;
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
    background: rgba(0,0,0,0.8); backdrop-filter: blur(8px);
    font-size: 14px; z-index: 10;
  }
  .dot { display: inline-block; width: 8px; height: 8px; border-radius: 50%; margin-right: 6px; vertical-align: middle; }
  .dot-red { background: #e53e3e; }
  .dot-green { background: #38a169; animation: pulse 2s infinite; }
  .dot-yellow { background: #d69e2e; }
  @keyframes pulse { 0%,100% { opacity: 1; } 50% { opacity: 0.5; } }
  #video-container {
    position: relative; width: 100%; max-width: 1280px;
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
  #unmute-btn {
    position: absolute; bottom: 16px; right: 16px;
    padding: 8px 16px; font-size: 14px; font-weight: 600;
    background: #e53e3e; color: white; border: none; border-radius: 6px;
    cursor: pointer; z-index: 3; display: none;
  }
  #unmute-btn:hover { background: #c53030; }
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
  <button id="unmute-btn">🔇 Click to unmute</button>
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
const unmuteBtn = document.getElementById("unmute-btn");

let role = null;
let ws = null;
let localStream = null;
const peerConnections = new Map(); // viewerId -> RTCPeerConnection

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
          startBtn.style.display = "block";
          waitingEl.style.display = "none";
          setStatus("yellow", "Ready to broadcast — click Share Screen");
          infoEl.textContent = "Share this URL with viewers: " + location.href;
        } else {
          startBtn.style.display = "none";
          waitingEl.style.display = "block";
          setStatus("yellow", "Waiting for broadcaster to start...");
        }
        break;

      // --- Broadcaster events ---
      case "viewer-joined":
        if (role === "broadcaster" && localStream) {
          await createPeerForViewer(msg.viewerId);
        }
        updateViewerCount();
        break;

      case "viewer-left":
        if (peerConnections.has(msg.viewerId)) {
          peerConnections.get(msg.viewerId).close();
          peerConnections.delete(msg.viewerId);
        }
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

    // Tell the encoder this is fast-motion content (gaming)
    localStream.getVideoTracks()[0].contentHint = "motion";

    video.srcObject = localStream;
    video.muted = true;
    startBtn.style.display = "none";
    setStatus("green", "Broadcasting");

    localStream.getVideoTracks()[0].onended = () => {
      setStatus("red", "Screen share stopped");
      for (const [id, pc] of peerConnections) pc.close();
      peerConnections.clear();
      localStream = null;
      video.srcObject = null;
      startBtn.style.display = "block";
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

  // Prioritize frame rate over resolution for gaming
  const videoSender = pc.getSenders().find(s => s.track && s.track.kind === "video");
  if (videoSender) {
    const params = videoSender.getParameters();
    if (!params.encodings || params.encodings.length === 0) params.encodings = [{}];
    params.encodings[0].maxBitrate = QUALITY_TIERS[currentTierIndex].maxBitrate;
    params.degradationPreference = "maintain-framerate";
    await videoSender.setParameters(params);
  }

  pc.onicecandidate = (e) => {
    if (e.candidate) {
      ws.send(JSON.stringify({ type: "ice-candidate", viewerId, data: e.candidate }));
    }
  };

  const offer = await pc.createOffer();
  await pc.setLocalDescription(offer);
  ws.send(JSON.stringify({ type: "offer", viewerId, data: pc.localDescription }));

  monitorConnection(viewerId, pc);
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

function monitorConnection(viewerId, pc) {
  let prevBytesSent = 0;
  let prevTimestamp = 0;
  let lowCount = 0;
  let highCount = 0;

  const interval = setInterval(async () => {
    if (pc.connectionState === "closed" || pc.connectionState === "failed") {
      clearInterval(interval);
      return;
    }

    const stats = await pc.getStats();
    for (const report of stats.values()) {
      if (report.type === "outbound-rtp" && report.kind === "video") {
        if (prevTimestamp > 0) {
          const elapsed = (report.timestamp - prevTimestamp) / 1000;
          const bitrate = ((report.bytesSent - prevBytesSent) * 8) / elapsed;
          const tier = QUALITY_TIERS[currentTierIndex];

          // If actual bitrate is far below target, connection is constrained
          if (bitrate < tier.maxBitrate * 0.4 && currentTierIndex < QUALITY_TIERS.length - 1) {
            lowCount++;
            highCount = 0;
            if (lowCount >= 3) {
              currentTierIndex++;
              console.log("[bandwidth] downgrading to " + QUALITY_TIERS[currentTierIndex].label);
              for (const [, p] of peerConnections) applyQualityTier(p, currentTierIndex);
              setStatus("green", "Broadcasting (" + QUALITY_TIERS[currentTierIndex].label + " quality)");
              lowCount = 0;
            }
          }
          // If connection recovered, try upgrading
          else if (bitrate > tier.maxBitrate * 0.85 && currentTierIndex > 0) {
            highCount++;
            lowCount = 0;
            if (highCount >= 5) {
              currentTierIndex--;
              console.log("[bandwidth] upgrading to " + QUALITY_TIERS[currentTierIndex].label);
              for (const [, p] of peerConnections) applyQualityTier(p, currentTierIndex);
              setStatus("green", "Broadcasting (" + QUALITY_TIERS[currentTierIndex].label + " quality)");
              highCount = 0;
            }
          } else {
            lowCount = Math.max(0, lowCount - 1);
            highCount = Math.max(0, highCount - 1);
          }
        }
        prevBytesSent = report.bytesSent;
        prevTimestamp = report.timestamp;
        break;
      }
    }
  }, 2000);
}

// --- Viewer logic ---

let viewerPC = null;

async function handleOffer(offer) {
  viewerPC = new RTCPeerConnection({ iceServers: ICE_SERVERS });

  viewerPC.ontrack = (e) => {
    video.srcObject = e.streams[0] || new MediaStream([e.track]);
    video.muted = true;
    waitingEl.style.display = "none";
    setStatus("green", "Watching stream");
    video.play().then(() => {
      unmuteBtn.style.display = "block";
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

unmuteBtn.addEventListener("click", () => {
  video.muted = false;
  unmuteBtn.textContent = "🔊 Sound on";
  setTimeout(() => { unmuteBtn.style.display = "none"; }, 1500);
});

// --- UI helpers ---

function setStatus(color, text) {
  statusEl.innerHTML = '<span class="dot dot-' + color + '"></span> ' + text;
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
  const roomId = crypto.randomBytes(4).toString("hex");
  console.log("");
  console.log("  Live stream server running!");
  console.log("");
  console.log("  Local:   http://localhost:" + PORT + "/" + roomId);
  console.log("");
  console.log("  Open the link above to start broadcasting.");
  console.log("  Share the same path on your public domain for viewers.");
  console.log("");
});
