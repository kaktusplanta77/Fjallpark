// Fjällpark – spelserver på Cloudflare Workers.
// Visar spelet (public/index.html) och skickar spelarnas lägen mellan varandra
// via en Durable Object ("Hub") över WebSocket på /ws.
import { DurableObject } from 'cloudflare:workers';

const ROOM_RE = /^(lobby|pub-[a-z0-9]{8}|prv-[a-z0-9]{6})$/;
const MAX_IN_ROOM_LOBBY = 500, MAX_IN_ROOM_GAME = 16, MAX_ROOMS_PER_CLIENT = 4, MAX_MSG = 8192;

export class Hub extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    this.rooms = null;           // roomName -> Map(clientId -> ws)
    this.pres = new Map();       // clientId -> Map(roomName -> presence)
    this.rate = new Map();       // clientId -> { b, t }
  }

  // Bygg upp rummen igen efter att objektet har vilat (hibernation).
  load() {
    if (this.rooms) return;
    this.rooms = new Map();
    for (const ws of this.ctx.getWebSockets()) {
      const a = ws.deserializeAttachment(); if (!a) continue;
      for (const r of a.rooms) this.room(r).set(a.id, ws);
      if (a.lp) this.setP(a.id, 'lobby', a.lp);
    }
  }
  room(name) { let r = this.rooms.get(name); if (!r) { r = new Map(); this.rooms.set(name, r); } return r; }
  getP(id, name) { const m = this.pres.get(id); return (m && m.get(name)) || {}; }
  setP(id, name, p) { let m = this.pres.get(id); if (!m) { m = new Map(); this.pres.set(id, m); } m.set(name, p); }
  send(ws, obj) { try { ws.send(JSON.stringify(obj)); } catch (e) { } }
  bcast(room, from, obj) { const s = JSON.stringify(obj); for (const [pid, w] of room) if (pid !== from) { try { w.send(s); } catch (e) { } } }

  async fetch(request) {
    this.load();
    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);
    const id = 'p' + crypto.randomUUID().replace(/-/g, '').slice(0, 12);
    this.ctx.acceptWebSocket(server);
    server.serializeAttachment({ id, rooms: [], lp: null });
    this.send(server, { t: 'hello', id });
    return new Response(null, { status: 101, webSocket: client });
  }

  async webSocketMessage(ws, raw) {
    this.load();
    if (typeof raw !== 'string' || raw.length > MAX_MSG) return;
    const a = ws.deserializeAttachment(); if (!a) return;
    const now = Date.now(); let rl = this.rate.get(a.id);
    if (!rl) { rl = { b: 80, t: now }; this.rate.set(a.id, rl); }
    rl.b = Math.min(120, rl.b + (now - rl.t) * 0.06); rl.t = now;   // ~60 meddelanden/s
    if (rl.b < 1) return; rl.b--;
    let m; try { m = JSON.parse(raw); } catch (e) { return; }
    if (!m || typeof m.r !== 'string' || !ROOM_RE.test(m.r)) return;
    const name = m.r, id = a.id;
    if (m.t === 'join') {
      if (a.rooms.includes(name) || a.rooms.length >= MAX_ROOMS_PER_CLIENT) return;
      const room = this.room(name);
      if (room.size >= (name === 'lobby' ? MAX_IN_ROOM_LOBBY : MAX_IN_ROOM_GAME)) { this.send(ws, { t: 'full', r: name }); return; }
      this.send(ws, { t: 'peers', r: name, list: [...room.keys()].map(pid => ({ id: pid, p: this.getP(pid, name) })) });
      room.set(id, ws); a.rooms.push(name); ws.serializeAttachment(a);
      this.bcast(room, id, { t: 'join', r: name, id });
    } else if (m.t === 'pres') {
      const room = this.rooms.get(name); if (!room || !room.has(id)) return;
      const p = m.p && typeof m.p === 'object' && !Array.isArray(m.p) ? m.p : {};
      this.setP(id, name, p);
      if (name === 'lobby') { const s = JSON.stringify(p); const lp = s.length < 1500 ? p : null; if (JSON.stringify(a.lp) !== JSON.stringify(lp)) { a.lp = lp; ws.serializeAttachment(a); } }
      this.bcast(room, id, { t: 'pres', r: name, id, p });
    } else if (m.t === 'leave') {
      this.leave(ws, a, name);
    }
  }

  leave(ws, a, name) {
    a.rooms = a.rooms.filter(x => x !== name);
    if (name === 'lobby') a.lp = null;
    try { ws.serializeAttachment(a); } catch (e) { }
    const pm = this.pres.get(a.id); if (pm) pm.delete(name);
    const room = this.rooms.get(name);
    if (!room || !room.delete(a.id)) return;
    this.bcast(room, a.id, { t: 'left', r: name, id: a.id });
    if (!room.size) this.rooms.delete(name);
  }

  async webSocketClose(ws, code) {
    this.load();
    const a = ws.deserializeAttachment();
    if (a) { for (const r of [...a.rooms]) this.leave(ws, a, r); this.pres.delete(a.id); this.rate.delete(a.id); }
    try { ws.close(1000, 'hej då'); } catch (e) { }
  }
  async webSocketError(ws) { return this.webSocketClose(ws); }
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === '/ws') {
      if (request.headers.get('Upgrade') !== 'websocket') return new Response('Förväntade WebSocket', { status: 426 });
      return env.HUB.get(env.HUB.idFromName('hub')).fetch(request);
    }
    if (url.pathname === '/health') return Response.json({ ok: true });
    return env.ASSETS.fetch(request);
  },
};
