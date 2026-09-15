// SUBLEVEL 7 — 対戦用の簡易リレーサーバー
// ゲームの中身は一切知らず、同じ部屋(room)にいる2人の間でメッセージを
// そのまま転送するだけのプログラムです。

const http = require('http');
const { WebSocketServer } = require('ws');
const PORT = process.env.PORT || 8080;

// RenderやRailwayはヘルスチェックのため普通のHTTPアクセスをしてくるので、
// それに200で答えるだけの最小サーバーにWebSocketをぶら下げる。
const httpServer = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' });
  res.end('SUBLEVEL 7 relay server is running.');
});
const wss = new WebSocketServer({ server: httpServer });
const rooms = new Map(); // room名 -> [ws, ws]

wss.on('connection', (ws) => {
  let room = null;

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }

    if (msg.type === 'join') {
      room = String(msg.room || '').slice(0, 40) || 'default';
      if (!rooms.has(room)) rooms.set(room, []);
      const arr = rooms.get(room);

      if (arr.length >= 2) {
        ws.send(JSON.stringify({ type: 'full' }));
        ws.close();
        return;
      }

      arr.push(ws);
      ws._room = room;
      ws.send(JSON.stringify({ type: 'joined', slot: arr.length }));

      if (arr.length === 2) {
        for (const peer of arr) peer.send(JSON.stringify({ type: 'ready' }));
      }
      return;
    }

    // それ以外は同じ部屋のもう一人にそのまま転送する
    if (!room) return;
    const arr = rooms.get(room) || [];
    for (const peer of arr) {
      if (peer !== ws && peer.readyState === 1) peer.send(raw.toString());
    }
  });

  ws.on('close', () => {
    if (!room) return;
    const arr = rooms.get(room);
    if (!arr) return;
    const i = arr.indexOf(ws);
    if (i >= 0) arr.splice(i, 1);
    for (const peer of arr) {
      if (peer.readyState === 1) peer.send(JSON.stringify({ type: 'peer_left' }));
    }
    if (arr.length === 0) rooms.delete(room);
  });
});

httpServer.listen(PORT, () => {
  console.log(`relay server listening on ws://0.0.0.0:${PORT}`);
});
