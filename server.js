import "dotenv/config";
import express from "express";
import http from "http";
import cors from "cors";
import { Server } from "socket.io";

const PORT = Number(process.env.PORT || 8787);
const allowedOrigins = (process.env.ALLOWED_ORIGINS || "*")
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean);

const app = express();
app.use(express.json());
app.use(cors({
  origin: (origin, cb) => {
    if (!origin || allowedOrigins.includes("*") || allowedOrigins.includes(origin)) return cb(null, true);
    return cb(null, false);
  },
  credentials: true
}));

const server = http.createServer(app);
const io = new Server(server, {
  cors: {
    origin: allowedOrigins.includes("*") ? "*" : allowedOrigins,
    credentials: true
  }
});

const socketProfiles = new Map(); // socket.id -> { wallet, activeDomain, domains:Set }
const domainSockets = new Map();  // normalized domain -> Set(socket.id)
const calls = new Map();          // callId -> call

function norm(domain) {
  return String(domain || "").trim().toLowerCase();
}

function addDomainSocket(domain, socketId) {
  const key = norm(domain);
  if (!key) return;
  if (!domainSockets.has(key)) domainSockets.set(key, new Set());
  domainSockets.get(key).add(socketId);
}

function removeSocket(socketId) {
  const profile = socketProfiles.get(socketId);
  if (profile) {
    for (const d of profile.domains || []) {
      const key = norm(d);
      const set = domainSockets.get(key);
      if (set) {
        set.delete(socketId);
        if (!set.size) domainSockets.delete(key);
      }
    }
  }
  socketProfiles.delete(socketId);
}

function onlineSocketsFor(domain) {
  const set = domainSockets.get(norm(domain));
  if (!set) return [];
  return Array.from(set).filter((id) => io.sockets.sockets.has(id));
}

function makeRoom(a, b) {
  return [norm(a), norm(b)].sort().join("__");
}

app.get("/health", (_req, res) => {
  res.json({
    ok: true,
    service: "aigen-domain-chat-signal-server",
    onlineSockets: socketProfiles.size,
    onlineDomains: domainSockets.size
  });
});

app.get("/presence/:domain", (req, res) => {
  const domain = req.params.domain;
  res.json({
    ok: true,
    domain,
    online: onlineSocketsFor(domain).length > 0
  });
});

io.on("connection", (socket) => {
  socket.on("register-presence", (payload = {}) => {
    removeSocket(socket.id);

    const domains = Array.from(new Set((payload.domains || []).map(String).filter(Boolean)));
    const activeDomain = String(payload.activeDomain || domains[0] || "");
    const wallet = String(payload.wallet || "");

    socketProfiles.set(socket.id, {
      wallet,
      activeDomain,
      domains: new Set(domains)
    });

    for (const domain of domains) addDomainSocket(domain, socket.id);

    socket.emit("presence-registered", {
      ok: true,
      wallet,
      activeDomain,
      domains
    });
  });

  socket.on("start-call", (payload = {}) => {
    const fromDomain = String(payload.fromDomain || "");
    const toDomain = String(payload.toDomain || "");

    if (!fromDomain || !toDomain) {
      socket.emit("call-error", { message: "Missing fromDomain or toDomain." });
      return;
    }

    if (norm(fromDomain) === norm(toDomain)) {
      socket.emit("call-error", { message: "You cannot call the same domain identity." });
      return;
    }

    const profile = socketProfiles.get(socket.id);
    if (!profile || !profile.domains.has(fromDomain)) {
      socket.emit("call-error", { message: "Caller domain is not registered to this online wallet session." });
      return;
    }

    const targets = onlineSocketsFor(toDomain);
    if (!targets.length) {
      socket.emit("call-error", { message: `${toDomain} is not online right now.` });
      return;
    }

    const callId = `${Date.now()}-${Math.random().toString(16).slice(2)}`;
    const room = makeRoom(fromDomain, toDomain);
    calls.set(callId, { callId, room, fromSocketId: socket.id, fromDomain, toDomain, targets });

    for (const targetSocketId of targets) {
      io.to(targetSocketId).emit("incoming-call", { callId, fromDomain, toDomain, room });
    }
  });

  socket.on("accept-call", (payload = {}) => {
    const call = calls.get(payload.callId);
    if (!call) {
      socket.emit("call-error", { message: "Call no longer exists." });
      return;
    }

    socket.join(call.room);
    const caller = io.sockets.sockets.get(call.fromSocketId);
    if (caller) caller.join(call.room);

    io.to(call.fromSocketId).emit("call-started", {
      room: call.room,
      fromDomain: call.fromDomain,
      toDomain: call.toDomain,
      peerDomain: call.toDomain
    });

    socket.emit("call-started", {
      room: call.room,
      fromDomain: call.toDomain,
      toDomain: call.fromDomain,
      peerDomain: call.fromDomain
    });

    calls.delete(call.callId);
  });

  socket.on("reject-call", (payload = {}) => {
    const call = calls.get(payload.callId);
    if (!call) return;
    io.to(call.fromSocketId).emit("call-error", { message: `${call.toDomain} declined the chat.` });
    calls.delete(call.callId);
  });

  socket.on("domain-message", (payload = {}) => {
    const room = String(payload.room || makeRoom(payload.fromDomain, payload.toDomain));
    const fromDomain = String(payload.fromDomain || "");
    const toDomain = String(payload.toDomain || "");
    const text = String(payload.text || "").slice(0, 4000);

    if (!fromDomain || !toDomain || !text) return;

    socket.to(room).emit("domain-message", { fromDomain, toDomain, text, room, at: Date.now() });
  });

  socket.on("end-chat", (payload = {}) => {
    const room = String(payload.room || makeRoom(payload.fromDomain, payload.toDomain));
    socket.to(room).emit("chat-ended", {
      fromDomain: payload.fromDomain,
      toDomain: payload.toDomain,
      room
    });
    socket.leave(room);
  });

  socket.on("disconnect", () => {
    removeSocket(socket.id);
  });
});

server.listen(PORT, () => {
  console.log(`AIGEN signal server listening on :${PORT}`);
});
