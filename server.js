import "dotenv/config";
import express from "express";
import http from "http";
import cors from "cors";
import { Server } from "socket.io";
import { domainToASCII, domainToUnicode } from "node:url";

const PORT = Number(process.env.PORT || 8787);

const DEFAULT_ALLOWED_ORIGINS = [
  "https://chat.aigen.domains",
  "https://sandbox.aigen.domains",
  "https://agents.aigen.domains",
  "https://aigen.domains",
  "https://api.namespace.domains",
  "http://localhost:3000",
  "http://localhost:5173",
  "http://127.0.0.1:3000",
  "http://127.0.0.1:5173",
];

const envAllowedOrigins = (process.env.ALLOWED_ORIGINS || "")
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean);

const allowedOrigins = Array.from(
  new Set(envAllowedOrigins.length ? envAllowedOrigins : DEFAULT_ALLOWED_ORIGINS)
);

const allowAllOrigins = allowedOrigins.includes("*");

const app = express();
app.use(express.json({ limit: "1mb" }));

app.use(
  cors({
    origin: (origin, cb) => {
      if (!origin || allowAllOrigins || allowedOrigins.includes(origin)) {
        return cb(null, true);
      }

      console.warn("[CORS BLOCKED]", origin);
      return cb(null, false);
    },
    credentials: true,
  })
);

const server = http.createServer(app);

const io = new Server(server, {
  cors: {
    origin: allowAllOrigins ? "*" : allowedOrigins,
    credentials: true,
    methods: ["GET", "POST"],
  },
  transports: ["websocket", "polling"],
});

const socketProfiles = new Map(); // socket.id -> { wallet, activeDomain, domains:Set(canonical) }
const domainSockets = new Map(); // canonical domain -> Set(socket.id)
const calls = new Map(); // callId -> call object

function cleanDomainInput(domain) {
  let value = String(domain || "").trim();

  value = value.replace(/^https?:\/\//i, "");
  value = value.replace(/^fns:\/\//i, "");
  value = value.replace(/^@/, "");
  value = value.split(/[/?#]/)[0] || value;
  value = value.replace(/\s+/g, "");
  value = value.replace(/^\.+/, "");
  value = value.replace(/\.+$/, "");
  value = value.toLowerCase();

  return value;
}

function canonicalDomain(domain) {
  const cleaned = cleanDomainInput(domain);
  if (!cleaned) return "";

  try {
    const ascii = domainToASCII(cleaned);
    return ascii || cleaned;
  } catch {
    return cleaned;
  }
}

function displayDomain(domain) {
  const cleaned = cleanDomainInput(domain);
  if (!cleaned) return "";

  try {
    return domainToUnicode(cleaned) || cleaned;
  } catch {
    return cleaned;
  }
}

function makeRoom(a, b) {
  const left = canonicalDomain(a);
  const right = canonicalDomain(b);
  return [left, right].sort().join("__");
}

function addDomainSocket(domain, socketId) {
  const key = canonicalDomain(domain);
  if (!key) return;

  if (!domainSockets.has(key)) {
    domainSockets.set(key, new Set());
  }

  domainSockets.get(key).add(socketId);
}

function removeSocket(socketId) {
  const profile = socketProfiles.get(socketId);

  if (profile) {
    for (const domain of profile.domains || []) {
      const key = canonicalDomain(domain);
      const set = domainSockets.get(key);

      if (set) {
        set.delete(socketId);

        if (!set.size) {
          domainSockets.delete(key);
        }
      }
    }
  }

  socketProfiles.delete(socketId);
}

function onlineSocketsFor(domain) {
  const key = canonicalDomain(domain);
  const set = domainSockets.get(key);

  if (!set) return [];

  return Array.from(set).filter((id) => io.sockets.sockets.has(id));
}

function socketOwnsDomain(socket, domain) {
  const profile = socketProfiles.get(socket.id);
  const key = canonicalDomain(domain);

  return !!profile && !!profile.domains && profile.domains.has(key);
}

function registerPresence(socket, payload = {}) {
  removeSocket(socket.id);

  const rawDomains = Array.from(
    new Set(
      (payload.domains || [])
        .map(String)
        .map(cleanDomainInput)
        .filter(Boolean)
    )
  );

  const activeRaw = cleanDomainInput(payload.activeDomain || rawDomains[0] || "");
  const activeCanonical = canonicalDomain(activeRaw);
  const wallet = String(payload.wallet || "");

  const canonicalDomains = Array.from(
    new Set(
      rawDomains
        .map(canonicalDomain)
        .filter(Boolean)
    )
  );

  if (activeCanonical && !canonicalDomains.includes(activeCanonical)) {
    canonicalDomains.unshift(activeCanonical);
  }

  socketProfiles.set(socket.id, {
    wallet,
    activeDomain: activeCanonical,
    domains: new Set(canonicalDomains),
    displayDomains: rawDomains,
  });

  for (const domain of canonicalDomains) {
    addDomainSocket(domain, socket.id);
    socket.join(`identity:${domain}`);
  }

  if (activeCanonical) {
    socket.join(`identity:${activeCanonical}`);
  }

  socket.emit("presence-registered", {
    ok: true,
    wallet,
    activeDomain: displayDomain(activeRaw || activeCanonical),
    activeCanonical,
    domains: rawDomains.map(displayDomain),
    canonicalDomains,
  });

  console.log("[presence]", {
    socket: socket.id,
    wallet: wallet ? `${wallet.slice(0, 6)}...${wallet.slice(-4)}` : "",
    activeCanonical,
    canonicalDomains,
  });
}

function startCall(socket, payload = {}) {
  const fromDomainRaw = String(payload.fromDomain || payload.from || "");
  const toDomainRaw = String(payload.toDomain || payload.to || "");

  const fromDomain = canonicalDomain(fromDomainRaw);
  const toDomain = canonicalDomain(toDomainRaw);

  if (!fromDomain || !toDomain) {
    socket.emit("call-error", {
      message: "Missing fromDomain or toDomain.",
    });
    return;
  }

  if (fromDomain === toDomain) {
    socket.emit("call-error", {
      message: "You cannot call the same domain identity.",
    });
    return;
  }

  if (!socketOwnsDomain(socket, fromDomain)) {
    socket.emit("call-error", {
      message: "Caller domain is not registered to this online wallet session.",
    });
    return;
  }

  const targets = onlineSocketsFor(toDomain);

  if (!targets.length) {
    socket.emit("call-error", {
      message: `${displayDomain(toDomainRaw || toDomain)} is not online right now.`,
      toDomain: displayDomain(toDomainRaw || toDomain),
      toCanonical: toDomain,
    });
    return;
  }

  const callId = `${Date.now()}-${Math.random().toString(16).slice(2)}`;
  const room = makeRoom(fromDomain, toDomain);

  const call = {
    callId,
    room,
    fromSocketId: socket.id,
    fromDomain: displayDomain(fromDomainRaw || fromDomain),
    toDomain: displayDomain(toDomainRaw || toDomain),
    fromCanonical: fromDomain,
    toCanonical: toDomain,
    targets,
    createdAt: Date.now(),
  };

  calls.set(callId, call);

  socket.emit("call-ringing", {
    callId,
    room,
    fromDomain: call.fromDomain,
    toDomain: call.toDomain,
    fromCanonical: call.fromCanonical,
    toCanonical: call.toCanonical,
  });

  for (const targetSocketId of targets) {
    io.to(targetSocketId).emit("incoming-call", {
      callId,
      fromDomain: call.fromDomain,
      toDomain: call.toDomain,
      fromCanonical: call.fromCanonical,
      toCanonical: call.toCanonical,
      room,
      mode: payload.mode || "video",
      at: Date.now(),
    });

    // Compatibility alias for older/sandbox clients.
    io.to(targetSocketId).emit("call-invite", {
      callId,
      fromDomain: call.fromDomain,
      toDomain: call.toDomain,
      fromCanonical: call.fromCanonical,
      toCanonical: call.toCanonical,
      room,
      mode: payload.mode || "video",
      at: Date.now(),
    });
  }

  console.log("[start-call]", {
    callId,
    room,
    from: call.fromCanonical,
    to: call.toCanonical,
    targets: targets.length,
  });
}

function acceptCall(socket, payload = {}) {
  const call = calls.get(payload.callId);

  if (!call) {
    socket.emit("call-error", {
      message: "Call no longer exists.",
    });
    return;
  }

  const calleeOwnsTarget =
    socketOwnsDomain(socket, call.toCanonical) ||
    socketOwnsDomain(socket, call.toDomain);

  if (!calleeOwnsTarget) {
    socket.emit("call-error", {
      message: "This wallet session is not registered as the called identity.",
    });
    return;
  }

  socket.join(call.room);

  const caller = io.sockets.sockets.get(call.fromSocketId);
  if (caller) {
    caller.join(call.room);
  }

  io.to(call.fromSocketId).emit("call-started", {
    callId: call.callId,
    room: call.room,
    fromDomain: call.fromDomain,
    toDomain: call.toDomain,
    fromCanonical: call.fromCanonical,
    toCanonical: call.toCanonical,
    peerDomain: call.toDomain,
    peerCanonical: call.toCanonical,
    role: "caller",
    at: Date.now(),
  });

  socket.emit("call-started", {
    callId: call.callId,
    room: call.room,
    fromDomain: call.toDomain,
    toDomain: call.fromDomain,
    fromCanonical: call.toCanonical,
    toCanonical: call.fromCanonical,
    peerDomain: call.fromDomain,
    peerCanonical: call.fromCanonical,
    role: "callee",
    at: Date.now(),
  });

  calls.delete(call.callId);

  console.log("[accept-call]", {
    callId: call.callId,
    room: call.room,
    caller: call.fromCanonical,
    callee: call.toCanonical,
  });
}

function rejectCall(socket, payload = {}) {
  const call = calls.get(payload.callId);
  if (!call) return;

  io.to(call.fromSocketId).emit("call-error", {
    message: `${call.toDomain} declined the call.`,
    callId: call.callId,
    room: call.room,
  });

  calls.delete(call.callId);

  console.log("[reject-call]", {
    callId: call.callId,
    by: socket.id,
  });
}

function relayDomainMessage(socket, payload = {}) {
  const fromDomain = String(payload.fromDomain || payload.from || "");
  const toDomain = String(payload.toDomain || payload.to || "");
  const text = String(payload.text || payload.message || "").slice(0, 4000);
  const room = String(payload.room || makeRoom(fromDomain, toDomain));

  if (!room || !text) return;

  socket.to(room).emit("domain-message", {
    fromDomain: displayDomain(fromDomain),
    toDomain: displayDomain(toDomain),
    fromCanonical: canonicalDomain(fromDomain),
    toCanonical: canonicalDomain(toDomain),
    text,
    room,
    at: Date.now(),
  });

  // Compatibility alias for older/sandbox clients.
  socket.to(room).emit("chat-message", {
    fromDomain: displayDomain(fromDomain),
    toDomain: displayDomain(toDomain),
    fromCanonical: canonicalDomain(fromDomain),
    toCanonical: canonicalDomain(toDomain),
    text,
    message: text,
    room,
    at: Date.now(),
  });
}

function endChat(socket, payload = {}) {
  const room = String(payload.room || makeRoom(payload.fromDomain, payload.toDomain));
  if (!room) return;

  socket.to(room).emit("chat-ended", {
    fromDomain: payload.fromDomain,
    toDomain: payload.toDomain,
    room,
    at: Date.now(),
  });

  socket.to(room).emit("end-chat", {
    fromDomain: payload.fromDomain,
    toDomain: payload.toDomain,
    room,
    at: Date.now(),
  });

  socket.leave(room);
}

app.get("/", (_req, res) => {
  res.json({
    ok: true,
    service: "aigen-domain-chat-signal-server",
    version: "20260511-sandbox-compatible",
  });
});

app.get("/health", (_req, res) => {
  res.json({
    ok: true,
    service: "aigen-domain-chat-signal-server",
    version: "20260511-sandbox-compatible",
    onlineSockets: socketProfiles.size,
    onlineDomains: domainSockets.size,
    pendingCalls: calls.size,
    allowedOrigins: allowAllOrigins ? ["*"] : allowedOrigins,
  });
});

app.get("/presence/:domain", (req, res) => {
  const domain = req.params.domain;
  const canonical = canonicalDomain(domain);

  res.json({
    ok: true,
    domain: displayDomain(domain),
    canonical,
    online: onlineSocketsFor(canonical).length > 0,
    sockets: onlineSocketsFor(canonical).length,
  });
});

app.get("/debug/presence", (_req, res) => {
  const domains = [];

  for (const [domain, sockets] of domainSockets.entries()) {
    domains.push({
      domain: displayDomain(domain),
      canonical: domain,
      sockets: Array.from(sockets).filter((id) => io.sockets.sockets.has(id)).length,
    });
  }

  res.json({
    ok: true,
    onlineSockets: socketProfiles.size,
    onlineDomains: domains.length,
    domains,
  });
});

io.on("connection", (socket) => {
  console.log("[socket connected]", {
    id: socket.id,
    origin: socket.handshake.headers.origin,
  });

  socket.emit("signal-ready", {
    ok: true,
    socketId: socket.id,
    version: "20260511-sandbox-compatible",
  });

  socket.on("register-presence", (payload = {}) => {
    registerPresence(socket, payload);
  });

  // Sandbox / compatibility alias.
  socket.on("join-identity", (payload = {}) => {
    const identity = payload.identity || payload.activeDomain || payload.domain;
    registerPresence(socket, {
      wallet: payload.wallet || "",
      activeDomain: identity,
      domains: payload.domains && payload.domains.length ? payload.domains : [identity],
    });
  });

  socket.on("start-call", (payload = {}) => {
    startCall(socket, payload);
  });

  // Sandbox / compatibility alias.
  socket.on("call-invite", (payload = {}) => {
    startCall(socket, {
      fromDomain: payload.fromDomain || payload.from,
      toDomain: payload.toDomain || payload.to,
      mode: payload.mode || "video",
    });
  });

  socket.on("accept-call", (payload = {}) => {
    acceptCall(socket, payload);
  });

  socket.on("reject-call", (payload = {}) => {
    rejectCall(socket, payload);
  });

  socket.on("domain-message", (payload = {}) => {
    relayDomainMessage(socket, payload);
  });

  // Sandbox / compatibility alias.
  socket.on("chat-message", (payload = {}) => {
    relayDomainMessage(socket, payload);
  });

  socket.on("end-chat", (payload = {}) => {
    endChat(socket, payload);
  });

  socket.on("webrtc-signal", (payload = {}) => {
    const room = String(payload.room || "");
    const data = payload.data;

    if (!room || !data) return;

    socket.to(room).emit("webrtc-signal", {
      room,
      data,
      fromDomain: payload.fromDomain,
      at: Date.now(),
    });
  });

  socket.on("video-toggle", (payload = {}) => {
    const room = String(payload.room || "");

    if (!room) return;

    const fromDomain = String(payload.fromDomain || "");
    const enabled = !!payload.enabled;

    socket.to(room).emit("video-toggle", {
      room,
      fromDomain: displayDomain(fromDomain),
      fromCanonical: canonicalDomain(fromDomain),
      enabled,
      at: Date.now(),
    });
  });

  socket.on("disconnect", (reason) => {
    console.log("[socket disconnected]", {
      id: socket.id,
      reason,
    });

    removeSocket(socket.id);
  });
});

setInterval(() => {
  const now = Date.now();

  for (const [callId, call] of calls.entries()) {
    if (now - call.createdAt > 2 * 60 * 1000) {
      calls.delete(callId);

      io.to(call.fromSocketId).emit("call-error", {
        message: "Call expired with no answer.",
        callId,
        room: call.room,
      });
    }
  }
}, 30 * 1000);

server.listen(PORT, () => {
  console.log(`AIGEN signal server listening on :${PORT}`);
  console.log("Allowed origins:", allowAllOrigins ? "*" : allowedOrigins.join(", "));
});
