const http = require("http");
const crypto = require("crypto");
const express = require("express");
const { WebSocketServer } = require("ws");

const PORT = parseInt(process.env.PORT || "3000", 10);
const PUBLIC_ORIGIN = process.env.PUBLIC_ORIGIN || "https://live.allanpinheiro.com";

// The default is deliberately trivial because that is what was asked for. Any
// deployment reachable from outside should set ADMIN_PASSWORD: /admin can end
// every stream on the server.
const ADMIN_PASSWORD = process.env.ADMIN_PASSWORD || "1111";

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

// rooms: Map<roomId, { broadcaster: ws | null, viewers: Map<viewerId, ws> }>
// Viewers are keyed by id because every broadcaster-originated message has to
// find one: as a Set that meant rebuilding an array of all viewers per message,
// which is O(N^2) allocations across an ICE flood.
const rooms = new Map();

function getRoom(id) {
  if (!rooms.has(id)) {
    rooms.set(id, {
      broadcaster: null,
      viewers: new Map(),
      createdAt: Date.now(),
      uplink: null,              // newest sample from the broadcaster
      downlinks: new Map(),      // clientId -> newest sample from that viewer
      quality: null,             // admin override, null means the client decides
    });
  }
  return rooms.get(id);
}

// Clients are not trusted to send well-formed numbers, and these values go
// into the admin page: take the fields we display, coerce them, drop the rest.
function num(v, max) {
  const n = Number(v);
  if (!Number.isFinite(n) || n < 0) return null;
  return Math.round(Math.min(n, max) * 10) / 10;
}

function pickUplink(d) {
  return {
    kbps: num(d.kbps, 1e6),
    fps: num(d.fps, 240),
    width: num(d.width, 16384),
    height: num(d.height, 16384),
    tier: typeof d.tier === "string" ? d.tier.slice(0, 16) : null,
    pinned: Boolean(d.pinned),
    limitation: typeof d.limitation === "string" ? d.limitation.slice(0, 16) : null,
    encodeMs: num(d.encodeMs, 10000),
    peers: num(d.peers, 1000),
  };
}

function pickDownlink(d) {
  return {
    kbps: num(d.kbps, 1e6),
    fps: num(d.fps, 240),
    width: num(d.width, 16384),
    height: num(d.height, 16384),
    rttMs: num(d.rttMs, 60000),
    jitterMs: num(d.jitterMs, 60000),
    delayMs: num(d.delayMs, 60000),
    lossPct: num(d.lossPct, 100),
    freezeCount: num(d.freezeCount, 1e6),
  };
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
  ws.joinedAt = Date.now();
  ws.on("pong", () => { ws.isAlive = true; });
  ws.on("error", () => ws.terminate());

  const url = new URL(req.url, "http://localhost");
  const roomId = url.searchParams.get("room");
  if (!validRoomId(roomId)) return ws.close(4000, "invalid room");

  // Resolved on join rather than on connect. A socket that never joins should
  // not conjure a room, and a room created here could be swept before the join
  // arrived, leaving this socket holding an object no longer in `rooms` while
  // the next person to join the same URL got a different one.
  let room = null;
  let role = null;
  let clientId = null;

  ws.on("message", (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }

    switch (msg.type) {
      case "join": {
        if (room) break;
        room = getRoom(roomId);

        // The client's own id, not one minted per socket. A reconnecting
        // viewer has to come back under the id the broadcaster already has a
        // peer connection keyed on, or the broadcaster treats it as a new
        // arrival and opens a second connection to someone it is already
        // streaming to. Falls back to a server id for anything that does not
        // send one.
        clientId = CLIENT_ID_RE.test(String(msg.clientId)) ? msg.clientId : idFor(ws);
        ws.clientId = clientId;

        // Whatever is still registered under this id is a socket that has not
        // noticed it is dead yet -- that is what a reconnect means. Drop it
        // rather than keeping two entries for one client.
        if (room.broadcaster && room.broadcaster !== ws && room.broadcaster.clientId === clientId) {
          const stale = room.broadcaster;
          room.broadcaster = null;
          stale.terminate();
        }
        const staleViewer = room.viewers.get(clientId);
        if (staleViewer && staleViewer !== ws) {
          room.viewers.delete(clientId);
          staleViewer.terminate();
        }

        // Role is still first-come for a fresh visitor, but a client that says
        // it was a viewer never takes the broadcaster slot. Without that, a
        // restart is a race: every client reconnects at once and whichever
        // viewer wins becomes the broadcaster of an empty room while the real
        // broadcaster, still holding the screen, is demoted to viewer.
        if (!room.broadcaster && msg.role !== "viewer") {
          role = "broadcaster";
          room.broadcaster = ws;
          send(ws, { type: "role", role: "broadcaster" });
          // Viewers already waiting in this room would otherwise never be
          // offered a stream: "viewer-joined" only fired at their join, which
          // happened before there was a broadcaster to hear it.
          for (const viewerId of room.viewers.keys()) {
            send(ws, { type: "viewer-joined", viewerId });
          }
          console.log(`[${roomId}] broadcaster connected as ${clientId} (${room.viewers.size} viewers waiting)`);
        } else {
          role = "viewer";
          room.viewers.set(clientId, ws);
          send(ws, { type: "role", role: "viewer" });
          send(room.broadcaster, { type: "viewer-joined", viewerId: clientId });
          console.log(`[${roomId}] viewer ${clientId} connected (${room.viewers.size} total)`);
        }
        break;
      }

      // Measurements, not signaling: the server keeps only the newest sample
      // per room so /admin can show what the stream is actually doing. Nothing
      // is relayed to other clients and nothing is kept in history -- this is a
      // gauge, not a time series.
      case "stats": {
        if (!room || !msg.data || typeof msg.data !== "object") break;
        if (role === "broadcaster") {
          room.uplink = { at: Date.now(), ...pickUplink(msg.data) };
        } else if (role === "viewer") {
          room.downlinks.set(clientId, { at: Date.now(), ...pickDownlink(msg.data) });
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
          send(room.broadcaster, { type: msg.type, data: msg.data, viewerId: clientId });
        }
        break;
      }
    }
  });

  ws.on("close", () => {
    if (!room) return;
    if (role === "broadcaster") {
      if (room.broadcaster !== ws) return; // replaced by a reconnect; not ours to clear
      room.broadcaster = null;
      room.uplink = null;
      for (const v of room.viewers.values()) send(v, { type: "broadcaster-left" });
      // Keep the viewers: they are still connected and waiting, and clearing
      // them here deleted the room out from under them, so a returning
      // broadcaster got a fresh room object the old viewers were not in.
      console.log(`[${roomId}] broadcaster disconnected (${room.viewers.size} viewers waiting)`);
    } else if (role === "viewer") {
      // Only if this socket is still the one registered: a reconnect under the
      // same id replaces the entry, and the old socket's close must not then
      // delete the new one or tell the broadcaster that a live viewer left.
      if (room.viewers.get(clientId) === ws) {
        room.viewers.delete(clientId);
        room.downlinks.delete(clientId);
        if (room.broadcaster) {
          send(room.broadcaster, { type: "viewer-left", viewerId: clientId });
        }
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

// Short, speakable slugs like "onca-veloz-azul" so a room can be read out loud
// or typed from memory instead of copied character by character. Portuguese,
// since that is the audience, and written without accents or cedillas: the id
// has to survive ROOM_ID_RE, a URL bar and being spelled over a phone.
//
// Substantivo + adjetivo + cor, which is the order the words come in when
// spoken, so the slug reads as a phrase rather than three tags.
const SUBSTANTIVOS = [
  "onca", "tatu", "arara", "tucano", "jacare", "lobo", "raposa", "coruja",
  "falcao", "gaviao", "capivara", "lontra", "golfinho", "baleia", "tartaruga",
  "camaleao", "iguana", "tubarao", "polvo", "caranguejo", "formiga", "abelha",
  "besouro", "borboleta", "cigarra", "grilo", "sapo", "perereca", "cobra",
  "cavalo", "cabra", "ovelha", "galo", "pato", "ganso", "pombo", "andorinha",
  "colibri", "sabia", "canario", "papagaio", "periquito", "pinguim", "foca",
  "urso", "veado", "gato", "coelho", "esquilo", "morcego", "macaco", "mico",
  "preguica", "jaguatirica", "quati", "cutia", "paca", "anta", "bugio", "saci",
];

const ADJETIVOS = [
  "veloz", "rapido", "bravo", "calmo", "forte", "leve", "agil", "esperto",
  "sabio", "alegre", "feliz", "gentil", "nobre", "audaz", "destemido", "fiel",
  "firme", "fino", "garboso", "honesto", "jovem", "justo", "limpo", "lindo",
  "macio", "magro", "manso", "novo", "quieto", "radiante", "raro", "risonho",
  "robusto", "sereno", "simples", "sincero", "solto", "sonoro", "suave",
  "teimoso", "tranquilo", "vivo", "zeloso", "claro", "doce", "grato",
  "humilde", "valente", "ligeiro", "astuto",
];

const CORES = [
  "azul", "verde", "amarelo", "vermelho", "laranja", "roxo", "rosa", "branco",
  "preto", "cinza", "dourado", "prateado", "bronze", "marrom", "bege",
  "violeta", "turquesa", "indigo", "carmim", "escarlate", "safira",
  "esmeralda", "rubi", "ambar", "coral", "jade", "lilas", "magenta", "ocre",
  "vinho",
];

function newRoomId() {
  const pick = (arr) => arr[crypto.randomInt(arr.length)];
  return pick(SUBSTANTIVOS) + "-" + pick(ADJETIVOS) + "-" + pick(CORES);
}

// Any room id reaching :roomId is interpolated into the page, so keep it to a
// charset that cannot break out of the HTML or the JS string literal. Staying
// permissive lets people pick their own room, e.g. /standup.
const ROOM_ID_RE = /^[a-z0-9][a-z0-9-]{0,39}$/;

// Paths that are pages in their own right. They match ROOM_ID_RE, so without
// this a socket could still open a room named after one — reachable over the
// socket but not over HTTP, since the route serves the page instead.
const RESERVED_IDS = new Set(["admin", "healthz"]);

// Client-supplied, so it is a map key and goes into the admin page and into a
// URL path. Keep it to something that cannot be either of those things.
const CLIENT_ID_RE = /^[a-z0-9-]{1,40}$/;

function validRoomId(id) {
  return typeof id === "string" && ROOM_ID_RE.test(id) && !RESERVED_IDS.has(id);
}

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

// --- Admin ---

// Everything the panel does to a room is done by closing sockets, never by
// editing a room in place: the close handlers already know how to notify the
// other side and tidy up, and clients reconnect on their own. The only thing
// an admin action adds is a reason code the page can show instead of silently
// retrying.
const ADMIN_CODES = {
  closed: 4010,   // room shut down
  kicked: 4011,   // this socket removed
  moved: 4012,    // room renamed, go to the new URL
  reset: 4013,    // whole server flushed
};

function socketsOf(room) {
  const list = [];
  if (room.broadcaster) list.push(room.broadcaster);
  for (const v of room.viewers.values()) list.push(v);
  return list;
}

function evict(ws, kind, payload) {
  // A reset sweeps the rooms and then every remaining socket, so the same
  // socket can be reached twice; it should still only hear about it once.
  if (ws.evicting) return;
  ws.evicting = true;
  send(ws, Object.assign({ type: "admin-" + kind }, payload));
  // Let the frame leave before the FIN; a close() in the same tick can beat
  // the message out and the client only sees an unexplained drop.
  setTimeout(() => {
    if (ws.readyState === ws.OPEN || ws.readyState === ws.CONNECTING) {
      ws.close(ADMIN_CODES[kind], kind);
    }
  }, 50);
}

function adminCloseRoom(id) {
  const room = rooms.get(id);
  if (!room) return false;
  const sockets = socketsOf(room);
  rooms.delete(id);
  for (const ws of sockets) evict(ws, "closed");
  console.log(`[${id}] room closed by admin (${sockets.length} sockets)`);
  return true;
}

function adminStopBroadcast(id) {
  const room = rooms.get(id);
  if (!room || !room.broadcaster) return false;
  // Only the broadcaster goes. Its close handler tells the viewers the stream
  // ended, and they keep waiting in the room for the next one.
  evict(room.broadcaster, "kicked");
  console.log(`[${id}] broadcaster stopped by admin`);
  return true;
}

function adminKickViewer(id, viewerId) {
  const room = rooms.get(id);
  const ws = room && room.viewers.get(viewerId);
  if (!ws) return false;
  evict(ws, "kicked");
  console.log(`[${id}] viewer ${viewerId} kicked by admin`);
  return true;
}

// Renaming is a move, not a relabel: the room id is baked into every client's
// URL and socket closure, so instead of rewriting that state we send everyone
// to the new URL and let them rebuild the room there. The broadcaster has to
// pick its screen again — a page load cannot carry a MediaStream across.
function adminRenameRoom(id, to) {
  const room = rooms.get(id);
  if (!room) return { ok: false, error: "no such room" };
  if (!validRoomId(to)) return { ok: false, error: "invalid room id" };
  if (to === id) return { ok: false, error: "same room id" };
  if (rooms.has(to)) return { ok: false, error: "room id already in use" };

  const sockets = socketsOf(room);
  rooms.delete(id);
  for (const ws of sockets) evict(ws, "moved", { roomId: to });
  console.log(`[${id}] renamed to ${to} by admin (${sockets.length} sockets)`);
  return { ok: true, moved: sockets.length };
}

function adminReset() {
  const counts = { rooms: rooms.size, sockets: wss.clients.size };
  for (const [, room] of rooms) {
    for (const ws of socketsOf(room)) evict(ws, "reset");
  }
  rooms.clear();
  // Sockets that connected but never joined hold no room, so the loop above
  // misses them; a reset means nobody is left holding a stale socket.
  for (const ws of wss.clients) {
    if (ws.readyState === ws.OPEN && !ws.evicting) evict(ws, "reset");
  }
  console.log(`[admin] server state reset (${counts.rooms} rooms, ${counts.sockets} sockets)`);
  return counts;
}

// A sample older than a few rounds means the reporter stopped rather than that
// the stream is idle, and a stale number in a dashboard is worse than none.
const STATS_TTL_MS = 8000;
function fresh(sample) {
  if (!sample) return null;
  return Date.now() - sample.at <= STATS_TTL_MS ? sample : null;
}

function roomState() {
  const out = [];
  for (const [id, room] of rooms) {
    out.push({
      id,
      live: Boolean(room.broadcaster),
      viewers: [...room.viewers.keys()].map((viewerId) => ({
        viewerId,
        joinedAt: room.viewers.get(viewerId).joinedAt || null,
        stats: fresh(room.downlinks.get(viewerId)),
      })),
      createdAt: room.createdAt || null,
      uplink: fresh(room.uplink),
      quality: room.quality,
    });
  }
  out.sort((a, b) => (b.createdAt || 0) - (a.createdAt || 0));
  return out;
}

// --- Admin auth ---

// A four-digit password is guessable in a few thousand requests, so the
// throttle is load-bearing here rather than decorative.
const ADMIN_MAX_FAILURES = 8;
const ADMIN_LOCKOUT_MS = 60_000;
const adminFailures = new Map(); // ip -> { count, until }

function sha256(value) {
  return crypto.createHash("sha256").update(String(value)).digest();
}

function passwordOk(given) {
  // timingSafeEqual needs equal lengths, which the raw strings do not have;
  // comparing digests keeps the comparison constant-time either way.
  return crypto.timingSafeEqual(sha256(given), sha256(ADMIN_PASSWORD));
}

function requireAdmin(req, res, next) {
  const ip = req.ip || "unknown";
  const record = adminFailures.get(ip);
  if (record && record.until > Date.now()) {
    const wait = Math.ceil((record.until - Date.now()) / 1000);
    return res.status(429).json({ error: `too many attempts, retry in ${wait}s` });
  }

  const given = req.get("x-admin-password");
  if (typeof given === "string" && passwordOk(given)) {
    adminFailures.delete(ip);
    return next();
  }

  const count = (record ? record.count : 0) + 1;
  adminFailures.set(ip, {
    count,
    until: count >= ADMIN_MAX_FAILURES ? Date.now() + ADMIN_LOCKOUT_MS : 0,
  });
  res.status(401).json({ error: "unauthorized" });
}

// Registered before :roomId, which would otherwise serve "admin" as a room.
app.get("/admin", (_req, res) => {
  res.type("html").send(ADMIN_HTML);
});

const adminApi = express.Router();
adminApi.use(express.json({ limit: "4kb" }));
adminApi.use(requireAdmin);

adminApi.get("/state", (_req, res) => {
  res.json({
    rooms: roomState(),
    server: {
      clients: wss.clients.size,
      uptime: Math.round(process.uptime()),
      memoryMb: Math.round(process.memoryUsage().rss / 1048576),
      now: Date.now(),
    },
  });
});

// The tiers are the broadcaster's to apply -- it owns the senders -- so this
// is a command, not a setting the server enforces. "auto" hands control back.
const TIER_LABELS = ["high", "medium", "low", "potato"];

adminApi.post("/rooms/:roomId/quality", (req, res) => {
  const room = rooms.get(req.params.roomId);
  if (!room) return res.status(404).json({ error: "no such room" });
  if (!room.broadcaster) return res.status(409).json({ error: "no broadcaster to apply it" });

  const body = req.body || {};
  const tier = body.tier === "auto" ? "auto" : TIER_LABELS.indexOf(String(body.tier));
  if (tier !== "auto" && tier < 0) return res.status(400).json({ error: "tier must be auto, " + TIER_LABELS.join(", ") });

  const maxBitrate = body.maxBitrate === undefined || body.maxBitrate === null || body.maxBitrate === ""
    ? null : Math.max(100_000, Math.min(50_000_000, parseInt(body.maxBitrate, 10) || 0));
  const maxFramerate = body.maxFramerate === undefined || body.maxFramerate === null || body.maxFramerate === ""
    ? null : Math.max(1, Math.min(120, parseInt(body.maxFramerate, 10) || 0));

  room.quality = { tier, maxBitrate, maxFramerate, at: Date.now() };
  send(room.broadcaster, { type: "quality", tier, maxBitrate, maxFramerate });
  console.log(`[${req.params.roomId}] quality set by admin: ${JSON.stringify(room.quality)}`);
  res.json({ ok: true, quality: room.quality });
});

adminApi.post("/rooms/:roomId/stop", (req, res) => {
  const ok = adminStopBroadcast(req.params.roomId);
  res.status(ok ? 200 : 404).json(ok ? { ok: true } : { error: "no live broadcaster" });
});

adminApi.post("/rooms/:roomId/rename", (req, res) => {
  const to = String((req.body && req.body.to) || "").toLowerCase().trim();
  const result = adminRenameRoom(req.params.roomId, to);
  res.status(result.ok ? 200 : 400).json(result);
});

adminApi.post("/rooms/:roomId/viewers/:viewerId/kick", (req, res) => {
  const ok = adminKickViewer(req.params.roomId, req.params.viewerId);
  res.status(ok ? 200 : 404).json(ok ? { ok: true } : { error: "no such viewer" });
});

adminApi.delete("/rooms/:roomId", (req, res) => {
  const ok = adminCloseRoom(req.params.roomId);
  res.status(ok ? 200 : 404).json(ok ? { ok: true } : { error: "no such room" });
});

adminApi.post("/reset", (_req, res) => {
  res.json({ ok: true, ...adminReset() });
});

// A restart is only a restart if something brings the process back, so it is
// reported as an exit and left to the supervisor (systemd, pm2, Docker).
adminApi.post("/restart", (_req, res) => {
  adminReset();
  res.json({ ok: true, exiting: true });
  console.log("[admin] exiting for restart");
  setTimeout(() => process.exit(0), 250);
});

app.use("/admin/api", adminApi);

app.get("/:roomId", (req, res) => {
  const roomId = req.params.roomId.toLowerCase();
  if (!validRoomId(roomId)) return res.status(404).type("text").send("Not found");
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
// Set once an admin action explains the disconnect. The reconnect timer would
// otherwise rebuild the room we were just removed from, a second later.
let evicted = false;

// Stable for the life of the tab, so a reconnect comes back as the same
// participant: the broadcaster keeps its peer connections keyed correctly and
// does not open a second one to a viewer it is already streaming to. Per tab,
// not per browser, because two tabs are two participants.
const CLIENT_KEY = "live:client-id";
let clientId = null;
try { clientId = sessionStorage.getItem(CLIENT_KEY); } catch {}
if (!clientId || !/^[a-z0-9-]{1,40}$/.test(clientId)) {
  clientId = "c-" + Math.random().toString(36).slice(2, 10);
  try { sessionStorage.setItem(CLIENT_KEY, clientId); } catch {}
}
let localStream = null;
const peerConnections = new Map(); // viewerId -> RTCPeerConnection
const waitingViewers = new Set();  // viewerIds announced before we had a stream

const RECONNECT_MIN_MS = 400;
const RECONNECT_MAX_MS = 8000;
let reconnectDelay = RECONNECT_MIN_MS;

const ICE_SERVERS = [
  { urls: "stun:stun.l.google.com:19302" },
  { urls: "stun:stun1.l.google.com:19302" },
];

function connect() {
  const proto = location.protocol === "https:" ? "wss:" : "ws:";
  ws = new WebSocket(proto + "//" + location.host + "/?room=" + ROOM_ID);

  ws.onopen = () => {
    reconnectDelay = RECONNECT_MIN_MS;
    // Claiming the role we already hold is what makes a server restart
    // survivable: everyone reconnects at once, and without the claim the
    // broadcaster slot goes to whichever socket lands first.
    ws.send(JSON.stringify({ type: "join", clientId, role }));
    if (!role) setStatus("yellow", "Joined room, determining role...");
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
          // A reconnect while the stream is still playing must not put the
          // "waiting" card back over a live picture.
          if (video.srcObject) {
            waitingEl.style.display = "none";
            setStatus("green", "Watching stream");
          } else {
            waitingEl.style.display = "block";
            setStatus("yellow", "Waiting for broadcaster to start...");
          }
        }
        break;

      // --- Broadcaster events ---
      case "viewer-joined":
        if (role === "broadcaster") {
          // After a reconnect the server re-announces everyone in the room,
          // including viewers whose peer connection never dropped -- video is
          // peer-to-peer and outlived the signaling. Re-offering would build a
          // second connection to someone already being streamed to.
          const existing = peerConnections.get(msg.viewerId);
          if (existing && existing.connectionState !== "failed" && existing.connectionState !== "closed") break;
          if (existing) existing.close();
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

      case "quality":
        if (role === "broadcaster") {
          pinnedTier = msg.tier === "auto" ? null : msg.tier;
          bitrateOverride = msg.maxBitrate || null;
          framerateOverride = msg.maxFramerate || null;
          if (pinnedTier !== null) currentTierIndex = pinnedTier;
          await retuneAllPeers();
        }
        break;

      // --- Admin events ---
      case "admin-moved":
        evicted = true;
        location.replace("/" + msg.roomId);
        break;

      case "admin-closed":
        endSession("This room was closed by an admin");
        break;

      case "admin-kicked":
        endSession(role === "broadcaster"
          ? "Your broadcast was stopped by an admin"
          : "You were removed from this room by an admin");
        break;

      case "admin-reset":
        endSession("The server was reset by an admin");
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
    if (evicted) return;
    // Video is peer-to-peer: a signaling drop does not interrupt a stream that
    // is already flowing, so say "reconnecting", not "disconnected", and do not
    // touch the video. Reconnect fast -- a deploy is back within a second or
    // two -- and back off only if it is really gone.
    const seconds = Math.round(reconnectDelay / 100) / 10;
    if (role === "viewer" && video.srcObject) {
      setStatus("yellow", "Reconnecting to the server (stream still playing)...");
    } else {
      setStatus("yellow", "Reconnecting in " + seconds + "s...");
    }
    setTimeout(connect, reconnectDelay);
    reconnectDelay = Math.min(reconnectDelay * 2, RECONNECT_MAX_MS);
  };
}

// Tear the session down for good: no reconnect, no half-live video, and a
// reload is the only way back in.
function endSession(message) {
  evicted = true;
  setStatus("red", message);
  if (localStream) {
    for (const track of localStream.getTracks()) track.stop();
    localStream = null;
  }
  for (const [, pc] of peerConnections) pc.close();
  peerConnections.clear();
  waitingViewers.clear();
  stopQualityMonitor();
  if (viewerPC) {
    viewerPC.close();
    viewerPC = null;
  }
  video.srcObject = null;
  startBtn.style.display = "none";
  volumeEl.style.display = "none";
  waitingEl.textContent = message;
  waitingEl.style.display = "block";
  viewersEl.textContent = "";
  infoEl.textContent = "Reload the page to join again.";
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
// Set by /admin. While a tier is pinned the monitor still measures and reports
// but stops moving the tier, so an operator's choice is not undone 6s later.
let pinnedTier = null;
let bitrateOverride = null;
let framerateOverride = null;

async function applyQualityTier(pc, tierIndex) {
  const tier = QUALITY_TIERS[tierIndex];
  const sender = pc.getSenders().find(s => s.track && s.track.kind === "video");
  if (!sender) return;
  const params = sender.getParameters();
  if (!params.encodings || params.encodings.length === 0) params.encodings = [{}];
  params.encodings[0].maxBitrate = bitrateOverride || tier.maxBitrate;
  params.encodings[0].scaleResolutionDownBy = tier.scaleDown;
  // An explicit cap is the one knob that reliably cuts latency on a screen
  // share: dropping 60fps to 30 halves what the encoder and the uplink have to
  // keep up with, where a resolution change only shrinks each frame.
  if (framerateOverride) params.encodings[0].maxFramerate = framerateOverride;
  else delete params.encodings[0].maxFramerate;
  await sender.setParameters(params);
}

// Byte counters are cumulative, so a rate needs the previous sample.
const lastSent = new Map(); // viewerId -> { bytes, at }

async function collectUplinkStats() {
  let kbps = 0, fps = null, width = null, height = null, limitation = null, encodeMs = null, peers = 0;
  for (const [viewerId, pc] of peerConnections) {
    if (pc.connectionState !== "connected") continue;
    const stats = await pc.getStats();
    for (const r of stats.values()) {
      if (r.type !== "outbound-rtp" || r.kind !== "video") continue;
      peers++;
      const prev = lastSent.get(viewerId);
      const now = r.timestamp;
      if (prev && now > prev.at) {
        kbps += ((r.bytesSent - prev.bytes) * 8) / (now - prev.at);
      }
      lastSent.set(viewerId, { bytes: r.bytesSent, at: now });
      if (r.framesPerSecond != null) fps = Math.max(fps || 0, r.framesPerSecond);
      if (r.frameWidth) { width = r.frameWidth; height = r.frameHeight; }
      if (r.qualityLimitationReason && r.qualityLimitationReason !== "none") limitation = r.qualityLimitationReason;
      // Encode time per frame is the tell for "the machine cannot keep up",
      // which no amount of bitrate tuning fixes.
      if (r.totalEncodeTime != null && r.framesEncoded) encodeMs = (r.totalEncodeTime / r.framesEncoded) * 1000;
    }
  }
  for (const id of [...lastSent.keys()]) if (!peerConnections.has(id)) lastSent.delete(id);
  return {
    kbps: Math.round(kbps), fps, width, height, peers,
    tier: QUALITY_TIERS[currentTierIndex].label,
    pinned: pinnedTier !== null,
    limitation, encodeMs,
  };
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

    if (ws && ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify({ type: "stats", data: await collectUplinkStats() }));
    }

    if (!sawVideo) return;
    // Pinned from /admin: keep measuring and reporting, stop steering.
    if (pinnedTier !== null) return;

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
  const pinnedNote = pinnedTier !== null ? " — set by admin" : "";
  setStatus("green", currentTierIndex === 0 && !pinnedNote
    ? "Broadcasting"
    : "Broadcasting (" + tier.label + " quality" + pinnedNote + ")");
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
    startViewerStats();
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

// --- Viewer telemetry ---

// What the viewer sees is the only measurement that answers "is it laggy":
// jitter buffer delay is the wait before a frame is painted, and freezes are
// the symptom people actually report.
let viewerStatsTimer = null;
let lastRecv = null;
let lastJitter = null;

function startViewerStats() {
  if (viewerStatsTimer) return;
  viewerStatsTimer = setInterval(async () => {
    if (!viewerPC || viewerPC.connectionState !== "connected") return;
    if (!ws || ws.readyState !== WebSocket.OPEN) return;
    const stats = await viewerPC.getStats();
    const out = {};
    for (const r of stats.values()) {
      if (r.type === "inbound-rtp" && r.kind === "video") {
        if (lastRecv && r.timestamp > lastRecv.at) {
          out.kbps = Math.round(((r.bytesReceived - lastRecv.bytes) * 8) / (r.timestamp - lastRecv.at));
        }
        lastRecv = { bytes: r.bytesReceived, at: r.timestamp };
        if (r.framesPerSecond != null) out.fps = r.framesPerSecond;
        if (r.frameWidth) { out.width = r.frameWidth; out.height = r.frameHeight; }
        if (r.jitter != null) out.jitterMs = r.jitter * 1000;
        if (r.freezeCount != null) out.freezeCount = r.freezeCount;
        if (r.packetsLost != null && r.packetsReceived) {
          out.lossPct = (r.packetsLost / (r.packetsLost + r.packetsReceived)) * 100;
        }
        // Cumulative totals: the delta between samples is the current delay,
        // not the average since the stream began.
        if (r.jitterBufferDelay != null && r.jitterBufferEmittedCount) {
          if (lastJitter && r.jitterBufferEmittedCount > lastJitter.count) {
            out.delayMs = ((r.jitterBufferDelay - lastJitter.delay) /
                           (r.jitterBufferEmittedCount - lastJitter.count)) * 1000;
          }
          lastJitter = { delay: r.jitterBufferDelay, count: r.jitterBufferEmittedCount };
        }
      }
      if (r.type === "candidate-pair" && r.state === "succeeded" && r.currentRoundTripTime != null) {
        out.rttMs = r.currentRoundTripTime * 1000;
      }
    }
    ws.send(JSON.stringify({ type: "stats", data: out }));
  }, MONITOR_MS);
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

// --- Embedded admin page ---

const ADMIN_HTML = /* html */ `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<meta name="robots" content="noindex">
<title>Admin — Live Stream</title>
<style>
  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
  body {
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
    background: #0a0a0a; color: #e0e0e0;
    min-height: 100vh; padding: 24px 16px 48px;
  }
  main { max-width: 820px; margin: 0 auto; }
  h1 { font-size: 20px; font-weight: 600; margin-bottom: 4px; }
  .sub { font-size: 13px; color: #666; margin-bottom: 24px; }
  .card {
    background: #121212; border: 1px solid #1f1f1f; border-radius: 8px;
    padding: 16px; margin-bottom: 12px;
  }
  .row { display: flex; align-items: center; gap: 12px; flex-wrap: wrap; }
  .grow { flex: 1 1 180px; min-width: 0; }
  .dot { display: inline-block; width: 8px; height: 8px; border-radius: 50%; margin-right: 6px; vertical-align: middle; }
  .dot-green { background: #38a169; animation: pulse 2s infinite; }
  .dot-yellow { background: #d69e2e; }
  .dot-red { background: #e53e3e; }
  @keyframes pulse { 0%,100% { opacity: 1; } 50% { opacity: 0.5; } }
  .room-id { font-size: 16px; font-weight: 600; }
  .room-id a { color: inherit; text-decoration: none; border-bottom: 1px dotted #555; }
  .meta { font-size: 12px; color: #777; margin-top: 4px; }
  button, input {
    font-family: inherit; font-size: 13px;
  }
  button {
    padding: 7px 12px; border-radius: 6px; border: 1px solid #333;
    background: #1c1c1c; color: #e0e0e0; cursor: pointer;
  }
  button:hover { background: #262626; }
  button.danger { border-color: #5a2222; color: #ff8f8f; }
  button.danger:hover { background: #2a1414; }
  button:disabled { opacity: 0.5; cursor: default; }
  input[type=text], input[type=password] {
    padding: 8px 10px; border-radius: 6px; border: 1px solid #333;
    background: #0e0e0e; color: #e0e0e0; min-width: 0;
  }
  input:focus-visible, button:focus-visible { outline: 2px solid #4a90d9; outline-offset: 2px; }
  .metrics {
    margin-top: 10px; display: flex; flex-wrap: wrap; gap: 6px 14px;
    font-size: 12px; color: #9ab;
    font-variant-numeric: tabular-nums;
  }
  .metrics .k { color: #667; }
  .metrics .bad { color: #ff8f8f; }
  .metrics .warn { color: #d6a93e; }
  .controls {
    margin-top: 10px; display: flex; flex-wrap: wrap; gap: 8px; align-items: center;
    font-size: 12px; color: #777;
  }
  .controls select, .controls input {
    padding: 6px 8px; border-radius: 6px; border: 1px solid #333;
    background: #0e0e0e; color: #e0e0e0; font-size: 12px;
  }
  .controls input { width: 78px; }
  .pinned { color: #d6a93e; }
  .viewers { margin-top: 12px; border-top: 1px solid #1f1f1f; padding-top: 10px; display: grid; gap: 6px; }
  .viewer { display: flex; align-items: center; gap: 10px; font-size: 13px; color: #999; }
  .viewer .grow { flex: 1; }
  .empty { color: #666; font-size: 14px; text-align: center; padding: 24px 0; }
  #msg { font-size: 13px; min-height: 18px; margin-bottom: 12px; color: #999; }
  #msg.err { color: #ff8f8f; }
  #msg.ok { color: #7ddba1; }
  .login { max-width: 320px; margin: 15vh auto 0; }
  .login .row { margin-top: 12px; }
  .footer-stats { font-size: 12px; color: #777; }
  .warn { font-size: 12px; color: #a08040; margin-top: 10px; }
</style>
</head>
<body>

<main>
  <div id="login" class="login card" style="display:none">
    <h1>Admin</h1>
    <div class="sub">Enter the admin password.</div>
    <form id="login-form">
      <input id="pw" type="password" placeholder="Password" autocomplete="current-password" style="width:100%">
      <div class="row"><button type="submit">Sign in</button><span id="login-msg" style="font-size:13px;color:#ff8f8f"></span></div>
    </form>
  </div>

  <div id="panel" style="display:none">
    <h1>Live Stream admin</h1>
    <div class="sub">Rooms exist only while someone is connected; an empty room disappears on its own.</div>
    <div id="msg"></div>
    <div id="rooms"></div>

    <div class="card">
      <div class="row">
        <div class="grow footer-stats" id="stats"></div>
        <button id="refresh">Refresh</button>
        <button class="danger" id="reset">Reset state</button>
        <button class="danger" id="restart">Restart process</button>
        <button id="logout">Sign out</button>
      </div>
      <div class="warn">Reset state drops every room and connection. Restart process exits the server — it only comes back if a supervisor restarts it.</div>
    </div>
  </div>
</main>

<script>
const loginEl = document.getElementById("login");
const loginForm = document.getElementById("login-form");
const loginMsg = document.getElementById("login-msg");
const pwInput = document.getElementById("pw");
const panelEl = document.getElementById("panel");
const roomsEl = document.getElementById("rooms");
const statsEl = document.getElementById("stats");
const msgEl = document.getElementById("msg");

const PW_KEY = "live:admin-pw";
let password = null;
let timer = null;

function say(text, kind) {
  msgEl.textContent = text || "";
  msgEl.className = kind || "";
}

async function api(path, options) {
  const opts = Object.assign({ headers: {} }, options || {});
  opts.headers = Object.assign({ "x-admin-password": password }, opts.headers);
  if (opts.body !== undefined) {
    opts.headers["content-type"] = "application/json";
    opts.body = JSON.stringify(opts.body);
  }
  const res = await fetch("/admin/api" + path, opts);
  let body = {};
  try { body = await res.json(); } catch {}
  if (!res.ok) throw new Error(body.error || res.status + " " + res.statusText);
  return body;
}

function ago(ts, now) {
  if (!ts) return "unknown";
  const secs = Math.max(0, Math.round((now - ts) / 1000));
  if (secs < 60) return secs + "s";
  if (secs < 3600) return Math.floor(secs / 60) + "m " + (secs % 60) + "s";
  return Math.floor(secs / 3600) + "h " + Math.floor((secs % 3600) / 60) + "m";
}

// Built node by node rather than as an HTML string: room ids are user-chosen
// and go straight back into this page.
function roomCard(room, now) {
  const card = document.createElement("div");
  card.className = "card";

  const head = document.createElement("div");
  head.className = "row";

  const title = document.createElement("div");
  title.className = "grow";

  const idLine = document.createElement("div");
  idLine.className = "room-id";
  const dot = document.createElement("span");
  dot.className = "dot dot-" + (room.live ? "green" : "yellow");
  const link = document.createElement("a");
  link.href = "/" + room.id;
  link.target = "_blank";
  link.rel = "noopener";
  link.textContent = room.id;
  idLine.append(dot, link);

  const meta = document.createElement("div");
  meta.className = "meta";
  meta.textContent = (room.live ? "live" : "waiting for broadcaster")
    + " · " + room.viewers.length + " viewer" + (room.viewers.length === 1 ? "" : "s")
    + " · open " + ago(room.createdAt, now);

  title.append(idLine, meta);
  head.append(title);

  const stop = document.createElement("button");
  stop.textContent = "Stop stream";
  stop.disabled = !room.live;
  stop.onclick = () => act("Stopped broadcast in " + room.id, "/rooms/" + encodeURIComponent(room.id) + "/stop", { method: "POST" });

  const rename = document.createElement("button");
  rename.textContent = "Rename";
  rename.onclick = () => {
    // Double-escaped on purpose: this page is a template literal, so a single
    // backslash-n is consumed here and emitted as a real line break inside the
    // string literal below, which is a parse error for the whole script.
    const to = prompt("Move " + room.id + " to which room id?\\n\\nEveryone is redirected to the new URL. The broadcaster has to pick a screen again.", room.id);
    if (!to) return;
    act("Moved " + room.id + " to " + to.toLowerCase().trim(),
        "/rooms/" + encodeURIComponent(room.id) + "/rename",
        { method: "POST", body: { to: to } });
  };

  const close = document.createElement("button");
  close.className = "danger";
  close.textContent = "Close room";
  close.onclick = () => {
    if (!confirm("Close " + room.id + "? Everyone in it is disconnected.")) return;
    act("Closed " + room.id, "/rooms/" + encodeURIComponent(room.id), { method: "DELETE" });
  };

  head.append(stop, rename, close);
  card.append(head);

  // --- live measurements ---
  const rate = kbps => kbps == null ? null : kbps >= 1000 ? (kbps / 1000).toFixed(1) + " Mbps" : Math.round(kbps) + " kbps";
  const ms = v => v == null ? null : Math.round(v) + " ms";

  function metricsRow(pairs) {
    const el = document.createElement("div");
    el.className = "metrics";
    for (const [label, value, level] of pairs) {
      if (value == null) continue;
      const span = document.createElement("span");
      const k = document.createElement("span");
      k.className = "k";
      k.textContent = label + " ";
      const v = document.createElement("span");
      if (level) v.className = level;
      v.textContent = value;
      span.append(k, v);
      el.append(span);
    }
    return el;
  }

  const up = room.uplink;
  if (up) {
    card.append(metricsRow([
      ["up", rate(up.kbps)],
      ["res", up.width ? up.width + "x" + up.height : null],
      ["fps", up.fps != null ? Math.round(up.fps) : null, up.fps != null && up.fps < 15 ? "bad" : null],
      ["encode", up.encodeMs != null ? Math.round(up.encodeMs) + " ms" : null,
        up.encodeMs > 20 ? "bad" : up.encodeMs > 10 ? "warn" : null],
      ["tier", up.tier + (up.pinned ? " (pinned)" : " (auto)"), up.pinned ? "pinned" : null],
      ["limited by", up.limitation, "warn"],
      ["encodes", up.peers != null ? up.peers + "x" : null, up.peers > 2 ? "warn" : null],
    ]));
  } else if (room.live) {
    card.append(metricsRow([["up", "no samples yet — broadcaster has not started sharing", "k"]]));
  }

  // --- quality controls ---
  const controls = document.createElement("div");
  controls.className = "controls";
  const tierSel = document.createElement("select");
  for (const [value, label] of [["auto", "auto"], ["high", "high — 8 Mbps"], ["medium", "medium — 3 Mbps"],
                                ["low", "low — 1 Mbps"], ["potato", "potato — 500 kbps"]]) {
    const opt = document.createElement("option");
    opt.value = value; opt.textContent = label;
    tierSel.append(opt);
  }
  const q = room.quality;
  tierSel.value = q ? (q.tier === "auto" ? "auto" : ["high","medium","low","potato"][q.tier] || "auto") : "auto";
  const bitrateIn = document.createElement("input");
  bitrateIn.type = "text"; bitrateIn.placeholder = "kbps";
  if (q && q.maxBitrate) bitrateIn.value = String(Math.round(q.maxBitrate / 1000));
  const fpsIn = document.createElement("input");
  fpsIn.type = "text"; fpsIn.placeholder = "fps";
  if (q && q.maxFramerate) fpsIn.value = String(q.maxFramerate);
  const apply = document.createElement("button");
  apply.textContent = "Apply";
  apply.disabled = !room.live;
  apply.onclick = () => act("Quality applied to " + room.id,
    "/rooms/" + encodeURIComponent(room.id) + "/quality",
    { method: "POST", body: {
        tier: tierSel.value,
        maxBitrate: bitrateIn.value ? Math.round(parseFloat(bitrateIn.value) * 1000) : null,
        maxFramerate: fpsIn.value ? parseInt(fpsIn.value, 10) : null,
      } });
  const lbl = document.createElement("span");
  lbl.textContent = "quality";
  const capLbl = document.createElement("span");
  capLbl.textContent = "caps";
  controls.append(lbl, tierSel, capLbl, bitrateIn, fpsIn, apply);
  card.append(controls);

  if (room.viewers.length) {
    const list = document.createElement("div");
    list.className = "viewers";
    for (const viewer of room.viewers) {
      const line = document.createElement("div");
      line.className = "viewer";
      const label = document.createElement("span");
      label.className = "grow";
      label.textContent = "viewer " + viewer.viewerId + " · " + ago(viewer.joinedAt, now);
      if (viewer.stats) {
        const st = viewer.stats;
        const row = metricsRow([
          ["down", rate(st.kbps)],
          ["fps", st.fps != null ? Math.round(st.fps) : null, st.fps != null && st.fps < 15 ? "bad" : null],
          ["res", st.width ? st.width + "x" + st.height : null],
          ["delay", ms(st.delayMs), st.delayMs > 400 ? "bad" : st.delayMs > 150 ? "warn" : null],
          ["rtt", ms(st.rttMs), st.rttMs > 200 ? "warn" : null],
          ["loss", st.lossPct != null ? st.lossPct.toFixed(1) + "%" : null,
            st.lossPct > 3 ? "bad" : st.lossPct > 0.5 ? "warn" : null],
          ["freezes", st.freezeCount || null, st.freezeCount > 0 ? "warn" : null],
        ]);
        row.style.flexBasis = "100%";
        row.style.marginTop = "2px";
        line.style.flexWrap = "wrap";
        label.append(row);
      }
      const kick = document.createElement("button");
      kick.textContent = "Kick";
      kick.onclick = () => act("Kicked viewer " + viewer.viewerId,
        "/rooms/" + encodeURIComponent(room.id) + "/viewers/" + encodeURIComponent(viewer.viewerId) + "/kick",
        { method: "POST" });
      line.append(label, kick);
      list.append(line);
    }
    card.append(list);
  }

  return card;
}

async function act(successMessage, path, options) {
  try {
    await api(path, options);
    say(successMessage, "ok");
  } catch (err) {
    say(err.message, "err");
  }
  await refresh();
}

async function refresh() {
  let state;
  try {
    state = await api("/state");
  } catch (err) {
    if (/unauthorized/i.test(err.message)) return signOut("Session expired — sign in again.");
    say(err.message, "err");
    return;
  }

  const now = state.server.now;
  roomsEl.textContent = "";
  if (!state.rooms.length) {
    const empty = document.createElement("div");
    empty.className = "card empty";
    empty.textContent = "No active rooms.";
    roomsEl.append(empty);
  } else {
    for (const room of state.rooms) roomsEl.append(roomCard(room, now));
  }

  statsEl.textContent = state.rooms.length + " room" + (state.rooms.length === 1 ? "" : "s")
    + " · " + state.server.clients + " socket" + (state.server.clients === 1 ? "" : "s")
    + " · up " + ago(now - state.server.uptime * 1000, now)
    + " · " + state.server.memoryMb + " MB";
}

function startPolling() {
  loginEl.style.display = "none";
  panelEl.style.display = "block";
  refresh();
  // Rooms come and go with their sockets, and the measurements are resampled
  // every 2s at each client, so the panel polls at the same cadence.
  clearInterval(timer);
  timer = setInterval(refresh, 2000);
}

function signOut(message) {
  clearInterval(timer);
  password = null;
  try { sessionStorage.removeItem(PW_KEY); } catch {}
  panelEl.style.display = "none";
  loginEl.style.display = "block";
  loginMsg.textContent = message || "";
  pwInput.value = "";
  pwInput.focus();
}

loginForm.addEventListener("submit", async (e) => {
  e.preventDefault();
  loginMsg.textContent = "";
  password = pwInput.value;
  try {
    await api("/state");
    // sessionStorage, not localStorage: the password is gone when the tab is.
    try { sessionStorage.setItem(PW_KEY, password); } catch {}
    startPolling();
  } catch (err) {
    password = null;
    loginMsg.textContent = err.message;
  }
});

document.getElementById("refresh").onclick = () => refresh();
document.getElementById("logout").onclick = () => signOut();

document.getElementById("reset").onclick = async () => {
  if (!confirm("Reset server state? Every room and connection is dropped.")) return;
  await act("Server state reset", "/reset", { method: "POST" });
};

document.getElementById("restart").onclick = async () => {
  if (!confirm("Restart the process? If nothing supervises this server, it stays down.")) return;
  try {
    await api("/restart", { method: "POST" });
    say("Exiting — waiting for the server to come back...", "ok");
  } catch (err) {
    say(err.message, "err");
  }
};

(async () => {
  let stored = null;
  try { stored = sessionStorage.getItem(PW_KEY); } catch {}
  if (stored) {
    password = stored;
    try {
      await api("/state");
      return startPolling();
    } catch {
      password = null;
    }
  }
  signOut();
})();
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
  console.log("  Admin:   http://localhost:" + PORT + "/admin");
  console.log("");
  console.log("  Open the local link to start broadcasting,");
  console.log("  then share the public link with viewers.");
  console.log("");
});
