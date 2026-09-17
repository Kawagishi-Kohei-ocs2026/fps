// SUBLEVEL 7 — 対戦用の簡易リレーサーバー（複数人対応）
// ゲームの中身は一切知らず、同じ部屋(room)にいる複数人の間でメッセージを
// 転送するだけのプログラムです。
//
// メッセージの宛先ルール：
//   - msg.to が指定されていれば、その id のプレイヤーにだけ転送する。
//   - 指定が無ければ、部屋にいる自分以外の全員に転送する（ブロードキャスト）。
//   - どちらの場合も、転送時にサーバー側で msg.from = 送信者id を付与する。

const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');
const PORT = process.env.PORT || 8080;

const DEFAULT_MAX = 8;
const HARD_MAX = 8; // これ以上は同時対戦に対応しない

// ブラウザでこのURLを開いたら、ゲーム本体(fps.html)をそのまま返す。
// WebSocketの相手を探すときはリレー処理へ回る。
const GAME_HTML = path.join(__dirname, 'fps.html');
const httpServer = http.createServer((req, res) => {
  fs.readFile(GAME_HTML, (err, data) => {
    if (err) {
      res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' });
      res.end('SUBLEVEL 7 relay server is running. (fps.html が見つかりません)');
      return;
    }
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    res.end(data);
  });
});
const wss = new WebSocketServer({ server: httpServer });

// room名 -> { players: Map(id -> ws), hostId, started, max }
const rooms = new Map();

function broadcast(r, obj, exceptId) {
  const data = JSON.stringify(obj);
  for (const [id, peer] of r.players) {
    if (id === exceptId) continue;
    if (peer.readyState === 1) peer.send(data);
  }
}

wss.on('connection', (ws) => {
  let roomName = null, myId = null;

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }

    if (msg.type === 'join') {
      if (roomName) return; // 二重join防止
      roomName = String(msg.room || '').slice(0, 40) || 'default';
      if (!rooms.has(roomName)) {
        rooms.set(roomName, {
          players: new Map(),
          hostId: null,
          started: false,
          max: Math.max(2, Math.min(HARD_MAX, (msg.max | 0) || DEFAULT_MAX)),
        });
      }
      const r = rooms.get(roomName);

      if (r.started) {
        ws.send(JSON.stringify({ type: 'started' }));
        roomName = null;
        return;
      }
      if (r.players.size >= r.max) {
        ws.send(JSON.stringify({ type: 'full' }));
        roomName = null;
        return;
      }

      myId = crypto.randomUUID();
      ws._room = roomName; ws._id = myId;
      const isHost = r.players.size === 0;
      if (isHost) r.hostId = myId;
      const existing = [...r.players.keys()];
      r.players.set(myId, ws);

      ws.send(JSON.stringify({
        type: 'joined', id: myId, hostId: r.hostId, isHost, max: r.max, peers: existing,
      }));
      broadcast(r, { type: 'peer_joined', id: myId }, myId);
      return;
    }

    if (!roomName) return;
    const r = rooms.get(roomName);
    if (!r) return;

    if (msg.type === 'start') {
      // ホストのみ有効。以後このルームは新規参加を締め切る。
      if (myId !== r.hostId) return;
      r.started = true;
      const out = { ...msg, from: myId };
      broadcast(r, out, myId);
      return;
    }

    // それ以外は宛先(to)があればその1人へ、無ければ全員へリレーする
    const out = { ...msg, from: myId };
    if (msg.to) {
      const peer = r.players.get(msg.to);
      if (peer && peer.readyState === 1) peer.send(JSON.stringify(out));
    } else {
      broadcast(r, out, myId);
    }
  });

  ws.on('close', () => {
    if (!roomName) return;
    const r = rooms.get(roomName);
    if (!r) return;
    r.players.delete(myId);
    if (r.players.size === 0) {
      rooms.delete(roomName);
      return;
    }
    if (r.hostId === myId) {
      // ホストが抜けたら、残った中で一番古い参加者を新ホストにする
      r.hostId = r.players.keys().next().value;
      broadcast(r, { type: 'host', id: r.hostId }, null);
    }
    broadcast(r, { type: 'peer_left', id: myId }, null);
  });
});

httpServer.listen(PORT, () => {
  console.log(`relay server listening on ws://0.0.0.0:${PORT}`);
});