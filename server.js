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

const allowedOrigins = Array.from(
  new Set(
    (process.env.ALLOWED_ORIGINS || DEFAULT_ALLOWED_ORIGINS.join(","))
      .split(",")
      .map((s) => s.trim())
      .filter(Boolean)
  )
);

const allowAllOrigins = allowedOrigins.includes("*");

const LIMITS = {
  maxConnectionsPerIp: Number(process.env.MAX_CONNECTIONS_PER_IP || 12),
  maxDomainsPerSocket: Number(process.env.MAX_DOMAINS_PER_SOCKET || 75),
  registerPresencePerMinute: Number(process.env.REGISTER_PRESENCE_PER_MINUTE || 8),
  startCallPerMinute: Number(process.env.START_CALL_PER_MINUTE || 10),
  messagePerMinute: Number(process.env.MESSAGE_PER_MINUTE || 80),
  webrtcSignalPerMinute: Number(process.env.WEBRTC_SIGNAL_PER_MINUTE || 220),
  videoTogglePerMinute: Number(process.env.VIDEO_TOGGLE_PER_MINUTE || 30),
  pendingCallTtlMs: Number(process.env.PENDING_CALL_TTL_MS || 120000),
  maxTextLength: Number(process.env.MAX_TEXT_LENGTH || 2000),
};

const app = express();
app.set("trust proxy", 1);
app.use(express.json({ limit: "256kb" }));

function getOrigin(req) {
  return req.headers.origin || "";
}

function isAllowedOrigin(origin) {
  return !origin || allowAllOrigins || allowedOrigins.includes(origin);
}

app.use(
  cors({
    origin: (origin, cb) => {
      if (isAllowedOrigin(origin)) return cb(null, true);
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

const socketProfiles = new Map(); // socket.id -> profile
const domainSockets = new Map();  // canonical domain -> Set(socket.id)
const calls = new Map();          // callId -> call
const ipSockets = new Map();      // ip -> Set(socket.id)
const buckets = new Map();        // key -> { count, resetAt }

function ipForSocket(socket) {
  const xfwd = socket.handshake.headers["x-forwarded-for"];
  if (typeof xfwd === "string" && xfwd.trim()) return xfwd.split(",")[0].trim();
  return socket.handshake.address || "unknown";
}

function rateLimit(key, max, windowMs = 60000) {
  const now = Date.now();
  const existing = buckets.get(key);

  if (!existing || now >= existing.resetAt) {
    buckets.set(key, { count: 1, resetAt: now + windowMs });
    return { ok: true, remaining: Math.max(0, max - 1) };
  }

  existing.count += 1;

  if (existing.count > max) {
    return { ok: false, retryAfterMs: existing.resetAt - now };
  }

  return { ok: true, remaining: Math.max(0, max - existing.count) };
}

function cleanDomainInput(domain) {
  let value = String(domain || "").trim();
  value = value.replace(/^https?:\/\//i, "");
  value = value.replace(/^fns:\/\//i, "");
  value = value.replace(/^@/, "");
  value = value.split(/[/?#]/)[0] || value;
  value = value.replace(/\s+/g, "");
  value = value.replace(/^\.+/, "");
  value = value.replace(/\.+$/, "");
  return value.toLowerCase();
}

function canonicalDomain(domain) {
  const cleaned = cleanDomainInput(domain);
  if (!cleaned) return "";
  try {
    return domainToASCII(cleaned) || cleaned;
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
  return [canonicalDomain(a), canonicalDomain(b)].sort().join("__");
}

function addDomainSocket(domain, socketId) {
  const key = canonicalDomain(domain);
  if (!key) return;
  if (!domainSockets.has(key)) domainSockets.set(key, new Set());
  domainSockets.get(key).add(socketId);
}

function removeSocket(socketId) {
  const profile = socketProfiles.get(socketId);

  if (profile) {
    for (const domain of profile.domains || []) {
      const set = domainSockets.get(domain);
      if (set) {
        set.delete(socketId);
        if (!set.size) domainSockets.delete(domain);
      }
    }
  }

  socketProfiles.delete(socketId);

  for (const [ip, set] of ipSockets.entries()) {
    set.delete(socketId);
    if (!set.size) ipSockets.delete(ip);
  }
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

function emitRateLimit(socket, area, result) {
  socket.emit("rate-limited", {
    area,
    message: `Too many ${area} requests. Try again shortly.`,
    retryAfterMs: result.retryAfterMs || 0,
  });
}

function registerPresence(socket, payload = {}) {
  const ip = ipForSocket(socket);
  const rl = rateLimit(`presence:${socket.id}`, LIMITS.registerPresencePerMinute);
  if (!rl.ok) return emitRateLimit(socket, "presence", rl);

  const rawDomains = Array.from(
    new Set(
      (payload.domains || [])
        .map(String)
        .map(cleanDomainInput)
        .filter(Boolean)
    )
  );

  if (!rawDomains.length && payload.activeDomain) {
    rawDomains.push(cleanDomainInput(payload.activeDomain));
  }

  if (!rawDomains.length) {
    socket.emit("presence-error", { message: "No domains supplied for presence registration." });
    return;
  }

  if (rawDomains.length > LIMITS.maxDomainsPerSocket) {
    socket.emit("presence-error", {
      message: `Too many identities for one session. Limit is ${LIMITS.maxDomainsPerSocket}.`,
    });
    return;
  }

  removeSocket(socket.id);

  const wallet = String(payload.wallet || "").trim();
  const activeCanonical = canonicalDomain(payload.activeDomain || rawDomains[0]);
  const canonicalDomains = Array.from(new Set(rawDomains.map(canonicalDomain).filter(Boolean)));

  if (activeCanonical && !canonicalDomains.includes(activeCanonical)) {
    canonicalDomains.unshift(activeCanonical);
  }

  socketProfiles.set(socket.id, {
    ip,
    wallet,
    activeDomain: activeCanonical,
    domains: new Set(canonicalDomains),
    createdAt: Date.now(),
  });

  if (!ipSockets.has(ip)) ipSockets.set(ip, new Set());
  ipSockets.get(ip).add(socket.id);

  for (const domain of canonicalDomains) {
    addDomainSocket(domain, socket.id);
    socket.join(`identity:${domain}`);
  }

  socket.emit("presence-registered", {
    ok: true,
    wallet,
    activeDomain: displayDomain(activeCanonical),
    activeCanonical,
    domains: canonicalDomains.map(displayDomain),
    canonicalDomains,
  });
}

function startCall(socket, payload = {}) {
  const rl = rateLimit(`start-call:${socket.id}`, LIMITS.startCallPerMinute);
  if (!rl.ok) return emitRateLimit(socket, "calls", rl);

  const fromDomainRaw = String(payload.fromDomain || payload.from || "");
  const toDomainRaw = String(payload.toDomain || payload.to || "");

  const fromDomain = canonicalDomain(fromDomainRaw);
  const toDomain = canonicalDomain(toDomainRaw);

  if (!fromDomain || !toDomain) {
    socket.emit("call-error", { message: "Missing fromDomain or toDomain." });
    return;
  }

  if (fromDomain === toDomain) {
    socket.emit("call-error", { message: "You cannot call the same identity." });
    return;
  }

  if (!socketOwnsDomain(socket, fromDomain)) {
    socket.emit("call-error", {
      message: "Caller identity is not registered to this online wallet session.",
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
}

function acceptCall(socket, payload = {}) {
  const call = calls.get(payload.callId);

  if (!call) {
    socket.emit("call-error", { message: "Call no longer exists." });
    return;
  }

  if (!socketOwnsDomain(socket, call.toCanonical)) {
    socket.emit("call-error", {
      message: "This wallet session is not registered as the called identity.",
    });
    return;
  }

  socket.join(call.room);

  const caller = io.sockets.sockets.get(call.fromSocketId);
  if (caller) caller.join(call.room);

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
}

function relayDomainMessage(socket, payload = {}) {
  const rl = rateLimit(`message:${socket.id}`, LIMITS.messagePerMinute);
  if (!rl.ok) return emitRateLimit(socket, "messages", rl);

  const fromDomain = String(payload.fromDomain || payload.from || "");
  const toDomain = String(payload.toDomain || payload.to || "");
  const text = String(payload.text || payload.message || "").slice(0, LIMITS.maxTextLength);
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
    version: "20260512-abuse-protected",
  });
});

app.get("/health", (_req, res) => {
  res.json({
    ok: true,
    service: "aigen-domain-chat-signal-server",
    version: "20260512-abuse-protected",
    onlineSockets: socketProfiles.size,
    onlineDomains: domainSockets.size,
    pendingCalls: calls.size,
    allowedOrigins: allowAllOrigins ? ["*"] : allowedOrigins,
    limits: LIMITS,
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

io.use((socket, next) => {
  const origin = socket.handshake.headers.origin || "";

  if (!isAllowedOrigin(origin)) {
    return next(new Error("origin_not_allowed"));
  }

  const ip = ipForSocket(socket);
  const current = ipSockets.get(ip);

  if (current && current.size >= LIMITS.maxConnectionsPerIp) {
    return next(new Error("too_many_connections"));
  }

  return next();
});

io.on("connection", (socket) => {
  const ip = ipForSocket(socket);

  if (!ipSockets.has(ip)) ipSockets.set(ip, new Set());
  ipSockets.get(ip).add(socket.id);

  socket.emit("signal-ready", {
    ok: true,
    socketId: socket.id,
    version: "20260512-abuse-protected",
  });

  socket.on("register-presence", (payload = {}) => registerPresence(socket, payload));

  socket.on("join-identity", (payload = {}) => {
    const identity = payload.identity || payload.activeDomain || payload.domain;
    registerPresence(socket, {
      wallet: payload.wallet || "",
      activeDomain: identity,
      domains: payload.domains && payload.domains.length ? payload.domains : [identity],
    });
  });

  socket.on("start-call", (payload = {}) => startCall(socket, payload));

  socket.on("call-invite", (payload = {}) => {
    startCall(socket, {
      fromDomain: payload.fromDomain || payload.from,
      toDomain: payload.toDomain || payload.to,
      mode: payload.mode || "video",
    });
  });

  socket.on("accept-call", (payload = {}) => acceptCall(socket, payload));
  socket.on("reject-call", (payload = {}) => rejectCall(socket, payload));
  socket.on("domain-message", (payload = {}) => relayDomainMessage(socket, payload));
  socket.on("chat-message", (payload = {}) => relayDomainMessage(socket, payload));
  socket.on("end-chat", (payload = {}) => endChat(socket, payload));

  socket.on("webrtc-signal", (payload = {}) => {
    const rl = rateLimit(`webrtc:${socket.id}`, LIMITS.webrtcSignalPerMinute);
    if (!rl.ok) return emitRateLimit(socket, "webrtc", rl);

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
    const rl = rateLimit(`video-toggle:${socket.id}`, LIMITS.videoTogglePerMinute);
    if (!rl.ok) return emitRateLimit(socket, "video-toggle", rl);

    const room = String(payload.room || "");
    if (!room) return;

    socket.to(room).emit("video-toggle", {
      room,
      fromDomain: displayDomain(payload.fromDomain || ""),
      fromCanonical: canonicalDomain(payload.fromDomain || ""),
      enabled: !!payload.enabled,
      at: Date.now(),
    });
  });

  socket.on("disconnect", () => removeSocket(socket.id));
});

setInterval(() => {
  const now = Date.now();

  for (const [callId, call] of calls.entries()) {
    if (now - call.createdAt > LIMITS.pendingCallTtlMs) {
      calls.delete(callId);
      io.to(call.fromSocketId).emit("call-error", {
        message: "Call expired with no answer.",
        callId,
        room: call.room,
      });
    }
  }

  for (const [key, bucket] of buckets.entries()) {
    if (now >= bucket.resetAt + 60000) buckets.delete(key);
  }
}, 30000);

server.listen(PORT, () => {
  console.log(`AIGEN signal server listening on :${PORT}`);
  console.log("Allowed origins:", allowAllOrigins ? "*" : allowedOrigins.join(", "));
});
