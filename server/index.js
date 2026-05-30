const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const path = require('path');

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: '*' },
  pingTimeout: 20000,
  pingInterval: 10000
});

app.use(express.static(path.join(__dirname, '../public')));

const waitingQueue = [];   // socket ids waiting
const pairs = new Map();   // socketId -> partnerId
const userMeta = new Map();
let realUserCount = 0;

function broadcast() { io.emit('user_count', realUserCount); }

function removeFromQueue(sid) {
  const i = waitingQueue.indexOf(sid);
  if (i !== -1) waitingQueue.splice(i, 1);
}

function unpair(sid, notifyPartner = true) {
  removeFromQueue(sid);
  const partnerId = pairs.get(sid);
  if (partnerId) {
    pairs.delete(partnerId);
    pairs.delete(sid);
    if (notifyPartner) {
      const partner = io.sockets.sockets.get(partnerId);
      if (partner) partner.emit('partner_left');
    }
  } else {
    pairs.delete(sid);
  }
}

function tryMatch(sid) {
  const socket = io.sockets.sockets.get(sid);
  if (!socket) return;
  const idx = waitingQueue.findIndex(id => id !== sid);
  if (idx === -1) {
    if (!waitingQueue.includes(sid)) waitingQueue.push(sid);
    socket.emit('waiting_no_match');
    return;
  }
  const partnerId = waitingQueue.splice(idx, 1)[0];
  removeFromQueue(sid);
  const partner = io.sockets.sockets.get(partnerId);
  if (!partner) { tryMatch(sid); return; }
  pairs.set(sid, partnerId);
  pairs.set(partnerId, sid);
  const myMeta = userMeta.get(sid) || {};
  const pMeta  = userMeta.get(partnerId) || {};
  socket.emit('matched', { partnerMeta: pMeta });
  partner.emit('matched', { partnerMeta: myMeta });
}

io.on('connection', (socket) => {
  realUserCount++;
  broadcast();

  socket.on('set_meta', ({ country, flag }) => {
    userMeta.set(socket.id, { country: country||'', flag: flag||'' });
  });

  socket.on('find_match', () => { unpair(socket.id, true); tryMatch(socket.id); });
  socket.on('leave', () => { unpair(socket.id, true); });
  socket.on('cancel', () => { removeFromQueue(socket.id); });

  socket.on('message', ({ text, msgId }) => {
    if (typeof text !== 'string') return;
    text = text.slice(0, 500);
    const partnerId = pairs.get(socket.id);
    if (!partnerId) return;
    const partner = io.sockets.sockets.get(partnerId);
    // Send message AND the sender's socket id so receiver can build the id map
    if (partner) partner.emit('message', { text, msgId, senderId: socket.id });
  });

  socket.on('reaction', ({ msgId, emoji, senderIsMe }) => {
    if (typeof emoji !== 'string') return;
    const partnerId = pairs.get(socket.id);
    if (!partnerId) return;
    const partner = io.sockets.sockets.get(partnerId);
    // relay reaction with original msgId — receiver maps it to their local id
    if (partner) partner.emit('reaction', { msgId, emoji });
  });

  socket.on('typing', (v) => {
    const partnerId = pairs.get(socket.id);
    if (!partnerId) return;
    const partner = io.sockets.sockets.get(partnerId);
    if (partner) partner.emit('typing', v);
  });

  socket.on('disconnect', () => {
    realUserCount = Math.max(0, realUserCount - 1);
    unpair(socket.id, true);
    userMeta.delete(socket.id);
    broadcast();
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`Chatcha on :${PORT}`));
