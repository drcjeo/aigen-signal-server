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
const PRESENCE_DEBUG_KEYS_ENABLED = String(process.env.AIGEN_PRESENCE_DEBUG || "").toLowerCase() === "1";

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
  for (const identity of payload.identityPackets || []) {
    keys.add(normalizeIdentity(identity?.normalizedIdentity));
    keys.add(normalizeIdentity(identity?.identity));
    keys.add(normalizeIdentity(identity?.name));
    keys.add(normalizeIdentity(identity?.domain));
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
  for (const identity of payload.identityPackets || []) {
    keys.add(normalizeWallet(identity?.ownerWallet));
    keys.add(normalizeWallet(identity?.resolvedWallet));
    keys.add(normalizeWallet(identity?.connectedWallet));
    keys.add(normalizeWallet(identity?.wallet));
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
    if (sockets.length) return {
      sockets,
      matchedBy: "identity",
      matchedKey: key,
      targetIdentity: key,
      targetWallet: "",
      identityCandidates: Array.from(identityKeys),
      walletCandidates: Array.from(walletKeys)
    };
  }

  for (const key of walletKeys) {
    const sockets = onlineSocketsFromMap(presenceByWallet, key);
    if (sockets.length) return {
      sockets,
      matchedBy: "wallet",
      matchedKey: key,
      targetIdentity: Array.from(identityKeys)[0] || "",
      targetWallet: key,
      identityCandidates: Array.from(identityKeys),
      walletCandidates: Array.from(walletKeys)
    };
  }

  return {
    sockets: [],
    matchedBy: "",
    matchedKey: "",
    targetIdentity: Array.from(identityKeys)[0] || "",
    targetWallet: Array.from(walletKeys)[0] || "",
    identityCandidates: Array.from(identityKeys),
    walletCandidates: Array.from(walletKeys)
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
    identityCandidates: result.identityCandidates,
    walletCandidates: result.walletCandidates,
    matchedBy: result.matchedBy,
    matchedKey: result.matchedKey,
    online: result.sockets.length > 0
  });
  res.json({
    ok: true,
    domain,
    canonical: result.targetIdentity || normalizeIdentity(domain),
    online: result.sockets.length > 0,
    presenceStatus: result.sockets.length > 0 ? "online" : "offline",
    matchedBy: result.matchedBy,
    matchedKey: result.matchedKey,
    identityCandidates: result.identityCandidates,
    walletCandidates: result.walletCandidates
  });
});

function emitPresenceResult(socket, payload = {}, eventName = "presence-result") {
  const result = resolvePresenceTarget(payload);
  const response = {
    ok: true,
    online: result.sockets.length > 0,
    presenceStatus: result.sockets.length > 0 ? "online" : "offline",
    matchedBy: result.matchedBy,
    matchedKey: result.matchedKey,
    targetIdentity: result.targetIdentity,
    targetWallet: result.targetWallet,
    identityCandidates: result.identityCandidates,
    walletCandidates: result.walletCandidates,
    sockets: result.sockets.length
  };
  console.log("[AIGEN signal presence check]", {
    targetIdentity: result.targetIdentity,
    identityCandidates: result.identityCandidates,
    walletCandidates: result.walletCandidates,
    matchedBy: result.matchedBy,
    matchedKey: result.matchedKey,
    online: response.online
  });
  socket.emit(eventName, response);
}

io.on("connection", (socket) => {
  console.log("[AIGEN signal socket connected]", {
    socketId: socket.id
  });

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
      connectedWallet: wallet,
      identityKeys: Array.from(identityKeys),
      walletKeys: Array.from(walletKeys),
      namespaceType: payload.namespaceType || "",
      rawPayload: payload
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
  socket.on("presence-debug", (payload = {}) => {
    const queryIdentity = normalizeIdentity(payload.identity || payload.name || payload.domain || payload.normalizedIdentity || "");
    const queryWallet = normalizeWallet(payload.wallet || payload.ownerWallet || payload.resolvedWallet || "");
    const result = resolvePresenceTarget({
      ...payload,
      normalizedIdentity: queryIdentity || payload.normalizedIdentity,
      identity: queryIdentity || payload.identity,
      domain: queryIdentity || payload.domain,
      ownerWallet: queryWallet || payload.ownerWallet,
      resolvedWallet: queryWallet || payload.resolvedWallet,
      wallet: queryWallet || payload.wallet
    });
    const response = {
      ok: true,
      socketCount: socketProfiles.size,
      queryIdentity,
      queryWallet,
      online: result.sockets.length > 0,
      matchedBy: result.matchedBy,
      matchedKey: result.matchedKey,
      identityCandidates: result.identityCandidates,
      walletCandidates: result.walletCandidates
    };
    if (PRESENCE_DEBUG_KEYS_ENABLED) {
      response.identityKeys = Array.from(presenceByIdentity.keys());
      response.walletKeys = Array.from(presenceByWallet.keys());
    }
    socket.emit("presence-debug-result", response);
  });

  socket.on("start-call", (payload = {}) => {
    const callerIdentity = normalizeIdentity(
      payload.callerIdentity
      || payload.fromIdentity
      || payload.primaryIdentity
      || payload.selectedIdentity
      || payload.normalizedIdentity
      || payload.domain
      || payload.fromDomain
    );
    const targetIdentity = normalizeIdentity(
      payload.targetIdentity
      || payload.toIdentity
      || payload.calleeIdentity
      || payload.to
      || payload.callee
      || payload.target
      || payload.toDomain
    );
    const targetWallet = normalizeWallet(
      payload.targetWallet
      || payload.resolvedWallet
      || payload.ownerWallet
      || payload.toWallet
    );
    const callerWallet = normalizeWallet(payload.callerWallet || payload.connectedWallet || payload.wallet || "");
    const fromDomain = String(payload.fromDomain || callerIdentity || "");
    const toDomain = String(payload.toDomain || targetIdentity || "");
    const toIdentity = (payload.toIdentity && typeof payload.toIdentity === "object")
      ? payload.toIdentity
      : ((payload.targetIdentityPacket && typeof payload.targetIdentityPacket === "object") ? payload.targetIdentityPacket : {});
    const fromIdentity = (payload.fromIdentity && typeof payload.fromIdentity === "object")
      ? payload.fromIdentity
      : ((payload.callerIdentityPacket && typeof payload.callerIdentityPacket === "object") ? payload.callerIdentityPacket : {});

    console.log("[AIGEN signal start-call received]", {
      socketId: socket.id,
      callerIdentity,
      targetIdentity,
      targetWallet,
      media: payload.media || payload.mode || "video"
    });

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
      targetIdentity: targetIdentity || payload.targetIdentity,
      calleeIdentity: targetIdentity || payload.calleeIdentity,
      to: payload.to,
      target: payload.target,
      callee: payload.callee,
      ownerWallet: toIdentity.ownerWallet || payload.ownerWallet || targetWallet,
      resolvedWallet: toIdentity.resolvedWallet || payload.targetIdentityPacket?.resolvedWallet || payload.resolvedWallet || payload.targetWallet || targetWallet,
      connectedWallet: payload.targetConnectedWallet || "",
      wallet: targetWallet || payload.wallet
    });
    const targets = target.sockets.filter((id) => id !== socket.id);
    const matchedKey = target.matchedKey;
    console.log("[AIGEN signal target resolved]", {
      targetIdentity: target.targetIdentity || normalizeIdentity(toDomain),
      targetWallet: target.targetWallet,
      resolvedSocketId: targets[0] || "",
      matchedBy: target.matchedBy,
      matchedKey
    });
    if (!targets.length) {
      socket.emit("call-unavailable", {
        ok: false,
        reason: "offline",
        targetIdentity: targetIdentity || toDomain
      });
      return;
    }

    const callId = `${Date.now()}-${Math.random().toString(16).slice(2)}`;
    const room = makeRoom(fromDomain, toDomain);
    calls.set(callId, {
      callId,
      room,
      fromSocketId: socket.id,
      calleeSocketIds: targets,
      acceptedSocketId: "",
      fromDomain,
      toDomain,
      callerIdentity,
      targetIdentity: targetIdentity || target.targetIdentity || normalizeIdentity(toDomain),
      callerWallet,
      targetWallet: targetWallet || target.targetWallet,
      matchedBy: target.matchedBy,
      matchedKey,
      payload
    });

    socket.emit("call-ringing", {
      callId,
      room,
      targetIdentity: targetIdentity || target.targetIdentity || normalizeIdentity(toDomain),
      targetWallet: targetWallet || target.targetWallet,
      matchedBy: target.matchedBy,
      matchedKey
    });

    for (const targetSocketId of targets) {
      const invite = {
        ...payload,
        callId,
        callerSocketId: socket.id,
        callerIdentity,
        fromIdentity: callerIdentity,
        primaryIdentity: callerIdentity,
        callerWallet,
        targetIdentity: targetIdentity || target.targetIdentity || normalizeIdentity(toDomain),
        toIdentity: targetIdentity || target.targetIdentity || normalizeIdentity(toDomain),
        targetWallet: targetWallet || target.targetWallet,
        fromDomain,
        toDomain,
        room,
        media: payload.media || payload.mode || "video",
        wantsVideo: payload.wantsVideo !== false,
        wantsAudio: payload.wantsAudio !== false
      };
      io.to(targetSocketId).emit("incoming-call", invite);
      console.log("[AIGEN signal incoming-call emitted]", {
        callId,
        callerIdentity,
        targetIdentity: invite.targetIdentity,
        targetSocketId,
        matchedBy: target.matchedBy,
        matchedKey
      });
    }
  });

  function acceptCall(payload = {}) {
    const call = calls.get(payload.callId);
    if (!call) {
      socket.emit("call-error", { message: "Call no longer exists." });
      return;
    }
    if (!call.calleeSocketIds.includes(socket.id)) {
      socket.emit("call-error", { message: "Only the invited callee can accept this call." });
      return;
    }

    call.acceptedSocketId = socket.id;
    socket.join(call.room);
    const caller = io.sockets.sockets.get(call.fromSocketId);
    if (caller) caller.join(call.room);

    console.log("[AIGEN signal call accepted]", {
      callId: call.callId,
      callerSocketId: call.fromSocketId,
      calleeSocketId: socket.id
    });

    io.to(call.fromSocketId).emit("call-accepted", {
      ...(call.payload || {}),
      callId: call.callId,
      room: call.room,
      fromDomain: call.fromDomain,
      toDomain: call.toDomain,
      peerDomain: call.toDomain,
      callerIdentity: call.callerIdentity,
      targetIdentity: call.targetIdentity,
      role: "caller"
    });

    socket.emit("call-accepted", {
      ...(call.payload || {}),
      callId: call.callId,
      room: call.room,
      fromDomain: call.toDomain,
      toDomain: call.fromDomain,
      peerDomain: call.fromDomain,
      callerIdentity: call.callerIdentity,
      targetIdentity: call.targetIdentity,
      role: "callee"
    });

    calls.delete(call.callId);
  }

  socket.on("accept-call", acceptCall);
  socket.on("call-accept", acceptCall);

  function declineCall(payload = {}) {
    const call = calls.get(payload.callId);
    if (!call) return;
    console.log("[AIGEN signal call declined]", {
      callId: call.callId,
      callerSocketId: call.fromSocketId,
      calleeSocketId: socket.id
    });
    io.to(call.fromSocketId).emit("call-declined", {
      callId: call.callId,
      targetIdentity: call.targetIdentity,
      toDomain: call.toDomain
    });
    calls.delete(call.callId);
  }

  socket.on("decline-call", declineCall);
  socket.on("call-decline", declineCall);
  socket.on("reject-call", declineCall);

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
    console.log("[AIGEN signal webrtc relay]", {
      socketId: socket.id,
      room,
      type: payload.data?.type || ""
    });
    socket.to(room).emit("webrtc-signal", { ...payload, room });
  });

  socket.on("video-toggle", (payload = {}) => {
    const room = String(payload.room || makeRoom(payload.fromDomain, payload.toDomain));
    if (!room) return;
    socket.to(room).emit("video-toggle", { ...payload, room });
  });

  function endChat(payload = {}) {
    const room = String(payload.room || makeRoom(payload.fromDomain, payload.toDomain));
    socket.to(room).emit("chat-ended", {
      fromDomain: payload.fromDomain,
      toDomain: payload.toDomain,
      room
    });
    socket.leave(room);
  }

  socket.on("end-chat", endChat);
  socket.on("end-call", endChat);

  socket.on("disconnect", () => {
    removeSocket(socket.id);
  });
});

server.listen(PORT, () => {
  console.log(`AIGEN signal server listening on :${PORT}`);
});
