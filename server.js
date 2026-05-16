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

const socketProfiles = new Map(); // socket.id -> { wallet, activeDomain, identityKeys:Set, walletKeys:Set }
const presenceByIdentity = new Map(); // normalized identity -> Set(socket.id)
const presenceByWallet = new Map();   // normalized wallet -> Set(socket.id)
const calls = new Map();          // callId -> call

function normalizeIdentity(value) {
  return String(value || "")
    .trim()
    .replace(/^@/, "")
    .toLowerCase();
}

function normalizeWallet(value) {
  const s = String(value || "").trim().toLowerCase();
  return /^0x[a-f0-9]{40}$/.test(s) ? s : "";
}

function norm(domain) {
  return normalizeIdentity(domain);
}

function addPresence(map, key, socketId) {
  if (!key) return;
  if (!map.has(key)) map.set(key, new Set());
  map.get(key).add(socketId);
}

function removePresence(map, key, socketId) {
  const set = map.get(key);
  if (!set) return;
  set.delete(socketId);
  if (!set.size) map.delete(key);
}

function identityKeysFromPayload(payload = {}) {
  const keys = new Set([
    normalizeIdentity(payload.normalizedIdentity),
    normalizeIdentity(payload.selectedIdentity),
    normalizeIdentity(payload.identity),
    normalizeIdentity(payload.domain),
    normalizeIdentity(payload.selectedDomain),
    normalizeIdentity(payload.selectedFNSDomain),
    normalizeIdentity(payload.activeDomain),
    normalizeIdentity(payload.name),
    normalizeIdentity(payload.fromDomain),
    normalizeIdentity(payload.toDomain),
    normalizeIdentity(payload.targetIdentity),
    normalizeIdentity(payload.calleeIdentity),
    normalizeIdentity(payload.to),
    normalizeIdentity(payload.target),
    normalizeIdentity(payload.callee),
    normalizeIdentity(payload.normalizedToIdentity),
    normalizeIdentity(payload.normalizedFromIdentity),
    normalizeIdentity(payload.toIdentity?.normalizedIdentity),
    normalizeIdentity(payload.toIdentity?.identity),
    normalizeIdentity(payload.toIdentity?.name),
    normalizeIdentity(payload.fromIdentity?.normalizedIdentity),
    normalizeIdentity(payload.fromIdentity?.identity),
    normalizeIdentity(payload.fromIdentity?.name)
  ].filter(Boolean));

  for (const domain of payload.domains || []) keys.add(normalizeIdentity(domain));
  for (const domain of payload.receivingIdentities || []) keys.add(normalizeIdentity(domain));
  for (const domain of payload.reachableIdentities || []) {
    if (!normalizeWallet(domain)) keys.add(normalizeIdentity(domain));
  }
  for (const identity of payload.identities || []) {
    keys.add(normalizeIdentity(identity?.normalizedIdentity));
    keys.add(normalizeIdentity(identity?.identity));
    keys.add(normalizeIdentity(identity?.name));
  }

  return keys;
}

function walletKeysFromPayload(payload = {}) {
  const keys = new Set([
    normalizeWallet(payload.connectedWallet),
    normalizeWallet(payload.ownerWallet),
    normalizeWallet(payload.resolvedWallet),
    normalizeWallet(payload.wallet),
    normalizeWallet(payload.targetWallet),
    normalizeWallet(payload.callerWallet),
    normalizeWallet(payload.toIdentity?.ownerWallet),
    normalizeWallet(payload.toIdentity?.resolvedWallet),
    normalizeWallet(payload.toIdentity?.connectedWallet),
    normalizeWallet(payload.fromIdentity?.ownerWallet),
    normalizeWallet(payload.fromIdentity?.resolvedWallet),
    normalizeWallet(payload.fromIdentity?.connectedWallet)
  ].filter(Boolean));

  for (const wallet of payload.resolvedWallets || []) keys.add(normalizeWallet(wallet));
  for (const wallet of payload.reachableIdentities || []) keys.add(normalizeWallet(wallet));
  for (const wallet of payload.toIdentity?.resolvedWallets || []) keys.add(normalizeWallet(wallet));
  for (const wallet of payload.fromIdentity?.resolvedWallets || []) keys.add(normalizeWallet(wallet));
  for (const identity of payload.identities || []) {
    keys.add(normalizeWallet(identity?.ownerWallet));
    keys.add(normalizeWallet(identity?.resolvedWallet));
    keys.add(normalizeWallet(identity?.connectedWallet));
    for (const wallet of identity?.resolvedWallets || []) keys.add(normalizeWallet(wallet));
  }

  keys.delete("");
  return keys;
}

function removeSocket(socketId) {
  const profile = socketProfiles.get(socketId);
  if (profile) {
    for (const key of profile.identityKeys || []) removePresence(presenceByIdentity, key, socketId);
    for (const key of profile.walletKeys || []) removePresence(presenceByWallet, key, socketId);
  }
  socketProfiles.delete(socketId);
}

function onlineSocketsFromMap(map, key) {
  const set = map.get(key);
  if (!set) return [];
  return Array.from(set).filter((id) => io.sockets.sockets.has(id));
}

function onlineSocketsFor(domain) {
  return onlineSocketsFromMap(presenceByIdentity, norm(domain));
}

function resolvePresenceTarget(payload = {}) {
  const identityKeys = identityKeysFromPayload(payload);
  const walletKeys = walletKeysFromPayload(payload);

  for (const key of identityKeys) {
    const sockets = onlineSocketsFromMap(presenceByIdentity, key);
    if (sockets.length) return { sockets, matchedBy: "identity", targetIdentity: key, targetWallet: "" };
  }

  for (const key of walletKeys) {
    const sockets = onlineSocketsFromMap(presenceByWallet, key);
    if (sockets.length) return { sockets, matchedBy: "wallet", targetIdentity: Array.from(identityKeys)[0] || "", targetWallet: key };
  }

  return {
    sockets: [],
    matchedBy: "",
    targetIdentity: Array.from(identityKeys)[0] || "",
    targetWallet: Array.from(walletKeys)[0] || ""
  };
}

function makeRoom(a, b) {
  return [norm(a), norm(b)].sort().join("__");
}

app.get("/health", (_req, res) => {
  res.json({
    ok: true,
    service: "aigen-domain-chat-signal-server",
    onlineSockets: socketProfiles.size,
    onlineDomains: presenceByIdentity.size,
    onlineIdentities: presenceByIdentity.size,
    onlineWallets: presenceByWallet.size
  });
});

app.get("/presence/:domain", (req, res) => {
  const domain = req.params.domain;
  const result = resolvePresenceTarget({
    domain,
    identity: domain,
    normalizedIdentity: domain,
    wallet: domain,
    ownerWallet: domain,
    connectedWallet: domain
  });
  console.log("[AIGEN signal presence check]", {
    targetIdentity: result.targetIdentity,
    targetWallets: result.targetWallet ? [result.targetWallet] : [],
    matchedBy: result.matchedBy,
    online: result.sockets.length > 0
  });
  res.json({
    ok: true,
    domain,
    canonical: result.targetIdentity || normalizeIdentity(domain),
    online: result.sockets.length > 0,
    presenceStatus: result.sockets.length > 0 ? "online" : "offline",
    matchedBy: result.matchedBy,
    matchedKey: result.matchedBy === "wallet" ? result.targetWallet : (result.matchedBy === "identity" ? result.targetIdentity : "")
  });
});

function emitPresenceResult(socket, payload = {}, eventName = "presence-result") {
  const result = resolvePresenceTarget(payload);
  const response = {
    ok: true,
    online: result.sockets.length > 0,
    presenceStatus: result.sockets.length > 0 ? "online" : "offline",
    matchedBy: result.matchedBy,
    matchedKey: result.matchedBy === "wallet" ? result.targetWallet : (result.matchedBy === "identity" ? result.targetIdentity : ""),
    targetIdentity: result.targetIdentity,
    targetWallet: result.targetWallet,
    sockets: result.sockets.length
  };
  console.log("[AIGEN signal presence check]", {
    targetIdentity: result.targetIdentity,
    targetWallets: result.targetWallet ? [result.targetWallet] : [],
    matchedBy: result.matchedBy,
    online: response.online
  });
  socket.emit(eventName, response);
}

io.on("connection", (socket) => {
  function registerPresence(payload = {}) {
    removeSocket(socket.id);

    const domains = Array.from(new Set((payload.domains || []).map(String).filter(Boolean)));
    const activeDomain = normalizeIdentity(payload.activeDomain || payload.selectedIdentity || payload.normalizedIdentity || domains[0] || "");
    const wallet = normalizeWallet(payload.connectedWallet || payload.wallet || "");
    const identityKeys = identityKeysFromPayload({ ...payload, activeDomain });
    const walletKeys = walletKeysFromPayload(payload);

    socketProfiles.set(socket.id, {
      wallet,
      activeDomain,
      domains: identityKeys,
      identityKeys,
      walletKeys,
      namespaceType: payload.namespaceType || ""
    });

    for (const key of identityKeys) addPresence(presenceByIdentity, key, socket.id);
    for (const key of walletKeys) addPresence(presenceByWallet, key, socket.id);

    console.log("[AIGEN signal presence register]", {
      socketId: socket.id,
      identityKeys: Array.from(identityKeys),
      walletKeys: Array.from(walletKeys),
      namespaceType: payload.namespaceType || ""
    });

    socket.emit("presence-registered", {
      ok: true,
      wallet,
      activeDomain,
      domains: Array.from(identityKeys),
      identityKeys: Array.from(identityKeys),
      walletKeys: Array.from(walletKeys)
    });
  }

  socket.on("register-presence", registerPresence);
  socket.on("join", registerPresence);
  socket.on("identify", registerPresence);

  socket.on("presence-check", (payload = {}) => emitPresenceResult(socket, payload, "presence-result"));
  socket.on("check-presence", (payload = {}) => emitPresenceResult(socket, payload, "presence-result"));
  socket.on("is-online", (payload = {}) => emitPresenceResult(socket, payload, "presence-result"));
  socket.on("target-presence", (payload = {}) => emitPresenceResult(socket, payload, "target-presence-result"));
  socket.on("lookup-presence", (payload = {}) => emitPresenceResult(socket, payload, "lookup-presence-result"));
  socket.on("callee-online", (payload = {}) => emitPresenceResult(socket, payload, "callee-online-result"));

  socket.on("start-call", (payload = {}) => {
    const fromDomain = String(payload.fromDomain || "");
    const toDomain = String(payload.toDomain || "");
    const toIdentity = (payload.toIdentity && typeof payload.toIdentity === "object")
      ? payload.toIdentity
      : ((payload.targetIdentityPacket && typeof payload.targetIdentityPacket === "object") ? payload.targetIdentityPacket : {});
    const fromIdentity = (payload.fromIdentity && typeof payload.fromIdentity === "object")
      ? payload.fromIdentity
      : ((payload.callerIdentityPacket && typeof payload.callerIdentityPacket === "object") ? payload.callerIdentityPacket : {});

    if (!fromDomain || !toDomain) {
      socket.emit("call-error", { message: "Missing fromDomain or toDomain." });
      return;
    }

    if (norm(fromDomain) === norm(toDomain)) {
      socket.emit("call-error", { message: "You cannot call the same domain identity." });
      return;
    }

    const profile = socketProfiles.get(socket.id);
    const fromKeys = identityKeysFromPayload({
      ...fromIdentity,
      domain: fromDomain,
      identity: fromDomain,
      normalizedIdentity: payload.normalizedFromIdentity || fromIdentity.normalizedIdentity
    });
    const callerRegistered = !!profile && Array.from(fromKeys).some((key) => profile.identityKeys?.has(key));
    if (!callerRegistered) {
      socket.emit("call-error", { message: "Caller domain is not registered to this online wallet session." });
      return;
    }

    const target = resolvePresenceTarget({
      ...toIdentity,
      domain: toDomain,
      identity: toDomain,
      normalizedIdentity: payload.normalizedToIdentity || toIdentity.normalizedIdentity,
      targetIdentity: payload.targetIdentity,
      calleeIdentity: payload.calleeIdentity,
      to: payload.to,
      target: payload.target,
      callee: payload.callee,
      ownerWallet: toIdentity.ownerWallet || payload.ownerWallet,
      resolvedWallet: toIdentity.resolvedWallet || payload.targetIdentityPacket?.resolvedWallet || payload.resolvedWallet || payload.targetWallet,
      connectedWallet: payload.targetConnectedWallet || "",
      wallet: payload.wallet
    });
    const targets = target.sockets.filter((id) => id !== socket.id);
    console.log("[AIGEN signal route target]", {
      targetIdentity: target.targetIdentity || normalizeIdentity(toDomain),
      targetWallet: target.targetWallet,
      resolvedSocketId: targets[0] || "",
      matchedBy: target.matchedBy
    });
    if (!targets.length) {
      socket.emit("call-error", { message: `${toDomain} has no active signal session right now.` });
      return;
    }

    const callId = `${Date.now()}-${Math.random().toString(16).slice(2)}`;
    const room = makeRoom(fromDomain, toDomain);
    calls.set(callId, { callId, room, fromSocketId: socket.id, fromDomain, toDomain, targets, payload });

    for (const targetSocketId of targets) {
      io.to(targetSocketId).emit("incoming-call", { ...payload, callId, fromDomain, toDomain, room });
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
      ...(call.payload || {}),
      room: call.room,
      fromDomain: call.fromDomain,
      toDomain: call.toDomain,
      peerDomain: call.toDomain
    });

    socket.emit("call-started", {
      ...(call.payload || {}),
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

    const target = resolvePresenceTarget({
      ...((payload.toIdentity && typeof payload.toIdentity === "object")
        ? payload.toIdentity
        : ((payload.targetIdentityPacket && typeof payload.targetIdentityPacket === "object") ? payload.targetIdentityPacket : {})),
      domain: toDomain,
      identity: toDomain,
      normalizedIdentity: payload.normalizedToIdentity || payload.toIdentity?.normalizedIdentity,
      targetIdentity: payload.targetIdentity,
      calleeIdentity: payload.calleeIdentity,
      to: payload.to,
      target: payload.target,
      callee: payload.callee,
      ownerWallet: payload.toIdentity?.ownerWallet || payload.ownerWallet,
      resolvedWallet: payload.toIdentity?.resolvedWallet || payload.targetIdentityPacket?.resolvedWallet || payload.resolvedWallet || payload.targetWallet,
      connectedWallet: payload.targetConnectedWallet || "",
      wallet: payload.wallet
    });
    console.log("[AIGEN signal route target]", {
      targetIdentity: target.targetIdentity || normalizeIdentity(toDomain),
      targetWallet: target.targetWallet,
      resolvedSocketId: target.sockets[0] || "",
      matchedBy: target.matchedBy
    });

    const message = { ...payload, fromDomain, toDomain, text, room, at: Date.now() };
    socket.to(room).emit("domain-message", message);
    for (const targetSocketId of target.sockets) {
      if (targetSocketId !== socket.id) io.to(targetSocketId).emit("domain-message", message);
    }
  });

  socket.on("webrtc-signal", (payload = {}) => {
    const room = String(payload.room || makeRoom(payload.fromDomain, payload.toDomain));
    if (!room) return;
    socket.to(room).emit("webrtc-signal", { ...payload, room });
  });

  socket.on("video-toggle", (payload = {}) => {
    const room = String(payload.room || makeRoom(payload.fromDomain, payload.toDomain));
    if (!room) return;
    socket.to(room).emit("video-toggle", { ...payload, room });
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
