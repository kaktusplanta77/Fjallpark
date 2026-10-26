// Fjällpark – spelserver
// Serverar spelet (index.html) och skickar spelarnas lägen mellan varandra över WebSocket.
// Start:  npm install  &&  npm start      (port 8080, ändra med PORT=xxxx)
'use strict';
const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

const PORT = Number(process.env.PORT) || 8080;
const GAME_FILE = path.join(__dirname, 'index.html');
const ROOM_RE = /^(lobby|pub-[a-z0-9]{8}|prv-[a-z0-9]{6})$/;
const MAX_IN_ROOM = { lobby: 500, game: 16 };
const MAX_ROOMS_PER_CLIENT = 4;
const MAX_MSG_BYTES = 8 * 1024;

const rooms = new Map();          // roomName -> Map(clientId -> { ws, p })
let clientCount = 0, nextId = 1;

const server = http.createServer((req, res) => {
  const url = (req.url || '/').split('?')[0];
  if (url === '/health') {
    res.writeHead(200, { 'content-type': 'application/json', 'cache-control': 'no-store' });
    return res.end(JSON.stringify({ ok: true, players: clientCount, rooms: rooms.size }));
  }
  if (url === '/' || url === '/index.html') {
    fs.readFile(GAME_FILE, (err, data) => {
      if (err) { res.writeHead(500); return res.end('index.html saknas bredvid server.js'); }
      res.writeHead(200, { 'content-type': 'text/html; charset=utf-8', 'cache-control': 'no-cache' });
      res.end(data);
    });
    return;
  }
  res.writeHead(404, { 'content-type': 'text/plain; charset=utf-8' });
  res.end('Hittades inte');
});

const wss = new WebSocketServer({ server, path: '/ws', maxPayload: 16 * 1024 });

function send(ws, obj) { if (ws.readyState === 1) ws.send(JSON.stringify(obj)); }
function broadcast(room, fromId, obj) {
  const s = JSON.stringify(obj);
  for (const [id, c] of room) if (id !== fromId && c.ws.readyState === 1) c.ws.send(s);
}

wss.on('connection', ws => {
  const id = 'p' + (nextId++).toString(36) + Math.random().toString(36).slice(2, 7);
  const mine = new Set();
  let budget = 80, last = Date.now();
  clientCount++;
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });
  send(ws, { t: 'hello', id });

  function leave(name) {
    const room = rooms.get(name); mine.delete(name);
    if (!room || !room.has(id)) return;
    room.delete(id);
    broadcast(room, id, { t: 'left', r: name, id });
    if (!room.size) rooms.delete(name);
  }

  ws.on('message', raw => {
    const now = Date.now();                       // ~60 meddelanden/s per spelare
    budget = Math.min(120, budget + (now - last) * 0.06); last = now;
    if (budget < 1) return; budget--;
    if (raw.length > MAX_MSG_BYTES) return;
    let m; try { m = JSON.parse(raw); } catch (e) { return; }
    if (!m || typeof m.r !== 'string' || !ROOM_RE.test(m.r)) return;
    const name = m.r;
    if (m.t === 'join') {
      if (mine.has(name) || mine.size >= MAX_ROOMS_PER_CLIENT) return;
      let room = rooms.get(name);
      if (!room) { room = new Map(); rooms.set(name, room); }
      if (room.size >= (name === 'lobby' ? MAX_IN_ROOM.lobby : MAX_IN_ROOM.game)) { send(ws, { t: 'full', r: name }); return; }
      send(ws, { t: 'peers', r: name, list: [...room].map(([pid, c]) => ({ id: pid, p: c.p })) });
      room.set(id, { ws, p: {} }); mine.add(name);
      broadcast(room, id, { t: 'join', r: name, id });
    } else if (m.t === 'pres') {
      const room = rooms.get(name); if (!room || !room.has(id)) return;
      const p = m.p && typeof m.p === 'object' && !Array.isArray(m.p) ? m.p : {};
      room.get(id).p = p;
      broadcast(room, id, { t: 'pres', r: name, id, p });
    } else if (m.t === 'leave') leave(name);
  });

  ws.on('close', () => { for (const name of [...mine]) leave(name); clientCount--; });
  ws.on('error', () => { });
});

// Koppla bort döda anslutningar
setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.isAlive) { ws.terminate(); continue; }
    ws.isAlive = false; try { ws.ping(); } catch (e) { }
  }
}, 30000);

server.listen(PORT, () => console.log(`Fjällpark-servern kör på http://localhost:${PORT}  (WebSocket: /ws, status: /health)`));
