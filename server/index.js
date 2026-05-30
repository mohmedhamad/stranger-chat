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
const userCount = { value: 0 };

function broadcastUserCount() { io.emit('user_count', userCount.value); }

function tryMatch(socket) {
  const idx = waitingQueue.findIndex(s => s.id !== socket.id);
  if (idx === -1) { waitingQueue.push(socket); socket.emit('waiting'); return; }
  const partner = waitingQueue.splice(idx, 1)[0];
  pairs.set(socket.id, partner.id);
  pairs.set(partner.id, socket.id);
  socket.emit('matched');
  partner.emit('matched');
}

function disconnect(socket) {
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
  userCount.value++;
  broadcastUserCount();

  socket.on('find_match', () => { disconnect(socket); tryMatch(socket); });

  socket.on('message', (text) => {
    if (typeof text !== 'string') return;
    text = text.slice(0, 500);
    const partnerId = pairs.get(socket.id);
    if (!partnerId) return;
    const partner = io.sockets.sockets.get(partnerId);
    if (partner) partner.emit('message', { text, msgId: socket.id + '-' + Date.now() });
  });

  socket.on('reaction', ({ msgId, emoji }) => {
    if (typeof emoji !== 'string') return;
    const partnerId = pairs.get(socket.id);
    if (!partnerId) return;
    const partner = io.sockets.sockets.get(partnerId);
    if (partner) partner.emit('reaction', { msgId, emoji });
  });

  socket.on('map_msg_id', ({ remoteId, localId }) => {
    const partnerId = pairs.get(socket.id);
    if (!partnerId) return;
    const partner = io.sockets.sockets.get(partnerId);
    if (partner) partner.emit('map_msg_id', { remoteId: localId, localId: remoteId });
  });

  socket.on('typing', (isTyping) => {
    const partnerId = pairs.get(socket.id);
    if (!partnerId) return;
    const partner = io.sockets.sockets.get(partnerId);
    if (partner) partner.emit('typing', isTyping);
  });

  socket.on('next', () => { disconnect(socket); tryMatch(socket); });

  socket.on('disconnect', () => {
    userCount.value = Math.max(0, userCount.value - 1);
    disconnect(socket);
    broadcastUserCount();
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`Stranger chat → http://localhost:${PORT}`));
