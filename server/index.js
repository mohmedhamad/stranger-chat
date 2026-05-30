const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const path = require('path');

const app = express();
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: '*' } });

app.use(express.static(path.join(__dirname, '../public')));

const waitingQueue = [];
const pairs = new Map();
const userMeta = new Map();
let realUserCount = 0;

function broadcastUserCount() {
  io.emit('user_count', realUserCount);
}

function tryMatch(socket) {
  const idx = waitingQueue.findIndex(s => s.id !== socket.id);
  if (idx === -1) {
    waitingQueue.push(socket);
    socket.emit('waiting');
    return;
  }
  const partner = waitingQueue.splice(idx, 1)[0];
  pairs.set(socket.id, partner.id);
  pairs.set(partner.id, socket.id);
  const myMeta = userMeta.get(socket.id) || {};
  const partnerMeta = userMeta.get(partner.id) || {};
  socket.emit('matched', { partnerMeta });
  partner.emit('matched', { partnerMeta: myMeta });
}

function unpair(socket) {
  const qi = waitingQueue.indexOf(socket);
  if (qi !== -1) waitingQueue.splice(qi, 1);
  const partnerId = pairs.get(socket.id);
  if (partnerId) {
    const partner = io.sockets.sockets.get(partnerId);
    if (partner) partner.emit('partner_left');
    pairs.delete(partnerId);
  }
  pairs.delete(socket.id);
}

io.on('connection', (socket) => {
  realUserCount++;
  broadcastUserCount();

  // Heartbeat every 15s to detect silent disconnects
  const hbInterval = setInterval(() => socket.emit('ping_check'), 15000);
  socket.on('pong_check', () => {});

  socket.on('set_meta', ({ country, flag }) => {
    userMeta.set(socket.id, { country: country || '', flag: flag || '' });
  });

  socket.on('find_match', () => { unpair(socket); tryMatch(socket); });

  socket.on('message', ({ text, msgId }) => {
    if (typeof text !== 'string') return;
    text = text.slice(0, 500);
    const partnerId = pairs.get(socket.id);
    if (!partnerId) return;
    const partner = io.sockets.sockets.get(partnerId);
    if (partner) partner.emit('message', { text, msgId });
  });

  socket.on('reaction', ({ msgId, emoji }) => {
    if (typeof emoji !== 'string') return;
    const partnerId = pairs.get(socket.id);
    if (!partnerId) return;
    const partner = io.sockets.sockets.get(partnerId);
    if (partner) partner.emit('reaction', { msgId, emoji });
  });

  socket.on('typing', (isTyping) => {
    const partnerId = pairs.get(socket.id);
    if (!partnerId) return;
    const partner = io.sockets.sockets.get(partnerId);
    if (partner) partner.emit('typing', isTyping);
  });

  socket.on('next', () => { unpair(socket); tryMatch(socket); });
  socket.on('leave', () => { unpair(socket); });

  socket.on('disconnect', () => {
    clearInterval(hbInterval);
    realUserCount = Math.max(0, realUserCount - 1);
    unpair(socket);
    userMeta.delete(socket.id);
    broadcastUserCount();
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`Chatcha → http://localhost:${PORT}`));

