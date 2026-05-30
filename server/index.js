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

// queue: array of socket ids waiting
const waitingQueue = [];
// pairs: Map<socketId, partnerId>
const pairs = new Map();
// meta: Map<socketId, {country,flag}>
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
  // find someone else in queue
  const idx = waitingQueue.findIndex(id => id !== sid);
  if (idx === -1) {
    // no one available — add to queue, tell client to just wait (no countdown)
    if (!waitingQueue.includes(sid)) waitingQueue.push(sid);
    socket.emit('waiting_no_match');
    return;
  }
  const partnerId = waitingQueue.splice(idx, 1)[0];
  removeFromQueue(sid);
  const partner = io.sockets.sockets.get(partnerId);
  if (!partner) {
    // partner disappeared, try again
    tryMatch(sid);
    return;
  }
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

  socket.on('find_match', () => {
    unpair(socket.id, true);
    tryMatch(socket.id);
  });

  socket.on('leave', () => {
    unpair(socket.id, true);
  });

  socket.on('cancel', () => {
    // just remove from queue, don't notify anyone
    removeFromQueue(socket.id);
  });

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

