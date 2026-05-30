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

// ── Real user state ────────────────────────────────────────
const waitingQueue   = [];          // socket ids waiting for a real match
const pairs          = new Map();   // sid -> partnerId (could be bot-xxx)
const userMeta       = new Map();   // sid -> {country,flag}
const pendingBots    = new Map();   // sid -> timeout handle (waiting to assign bot)
let   realUserCount  = 0;

function broadcast() { io.emit('user_count', realUserCount); }

function removeFromQueue(sid) {
  const i = waitingQueue.indexOf(sid);
  if (i !== -1) waitingQueue.splice(i, 1);
}

function cancelPendingBot(sid) {
  if (pendingBots.has(sid)) {
    clearTimeout(pendingBots.get(sid));
    pendingBots.delete(sid);
  }
}

function unpair(sid, notify = true) {
  removeFromQueue(sid);
  cancelPendingBot(sid);
  const pid = pairs.get(sid);
  if (pid) {
    pairs.delete(pid);
    pairs.delete(sid);
    if (notify && !pid.startsWith('bot-')) {
      const p = io.sockets.sockets.get(pid);
      if (p) p.emit('partner_left');
    }
    if (pid.startsWith('bot-')) killBot(sid);
  } else {
    pairs.delete(sid);
  }
}

function doMatch(sid) {
  const socket = io.sockets.sockets.get(sid);
  if (!socket) return;

  cancelPendingBot(sid);

  // Find real waiting partner
  const idx = waitingQueue.findIndex(id => id !== sid);

  if (idx !== -1) {
    // Real match found
    const pid = waitingQueue.splice(idx, 1)[0];
    removeFromQueue(sid);
    cancelPendingBot(pid); // cancel their pending bot too

    const partner = io.sockets.sockets.get(pid);
    if (!partner) { doMatch(sid); return; } // partner gone, try again

    pairs.set(sid, pid);
    pairs.set(pid, sid);
    socket.emit('matched',  { partnerMeta: userMeta.get(pid) || {} });
    partner.emit('matched', { partnerMeta: userMeta.get(sid) || {} });
    return;
  }

  // No real user — add to queue and tell client to keep waiting
  if (!waitingQueue.includes(sid)) waitingQueue.push(sid);
  socket.emit('waiting_no_match');

  // After 5s, if still unmatched, give a bot
  const t = setTimeout(() => {
    pendingBots.delete(sid);
    if (waitingQueue.includes(sid)) {
      removeFromQueue(sid);
      spawnBot(sid);
    }
  }, 5000);
  pendingBots.set(sid, t);
}

// ═══════════════════════════════════════════════════════════
// BOT ENGINE
// ═══════════════════════════════════════════════════════════

const PERSONAS = [
  {
    name:'مها', meta:{ flag:'🇪🇬', country:'EG' },
    greetings:['هاي','اهلاً','ايه اخبارك','هلو','ازيك/ي 😊'],
    r:{
      name:    ['مها','انا مها','اسمي مها وانت/ي؟'],
      age:     ['٢٣','تلاتة وعشرين','كبرت على السؤال ده 😄','بين ٢٠ و٣٠'],
      city:    ['القاهرة','من طنطا بس ساكنة القاهرة','الاسكندرية'],
      job:     ['بشتغل محاسبة','مش شغالة دلوقتي','بشتغل نفسي الحمدلله'],
      single:  ['ايوه','معنديش صاحب','لا مش لازم تعرف 😅'],
      music:   ['عمر دياب كتير','ورده وعبد الحليم','موسيقى قديمة'],
      hobby:   ['بقرا كتير','بتفرج على افلام','النوم هههه 😄'],
      travel:  ['عايزة اروح اوروبا','تركيا في بالي','باريس حلمي'],
      goal:    ['اكمل شغلي وربنا يستر','عايزة اسافر برا مصر','اكون مستقلة ماديا'],
      bot:     ['ممكن 😄','جايز','لا مش بوت','سؤال غريب هههه','ممكن اكون وممكن لا'],
      instagram:['مش بديش انستجرام لناس جديدة','لا سوري','مش علطول كدا!'],
      meet:    ['لا شكراً بس','بفضل هنا 😅','مش دلوقتي'],
      whatsapp:['لا سوري مش بعطي واتساب','مش دلوقتي','هنا كويس 😅'],
      howru:   ['كويسة الحمدلله','بخير','تمام ومبسوطة','كويسة وانت/ي؟'],
      compliment:['شكراً 😊','ميرسي كتير','حبيبي/ة 😄'],
      bye:     ['وداعاً','باي','تسلم/ي','يلا سلام'],
      default: ['اه صح','ممكن','مش عارفة 😅','اوك','هههه','طيب','بجد؟','ايوه','الله']
    },
    leaveAfter: () => 15 + Math.floor(Math.random()*25)
  },
  {
    name:'على', meta:{ flag:'🇪🇬', country:'EG' },
    greetings:['هاي','اهلاً','ايه الاخبار','ازيك/ي'],
    r:{
      name:    ['على','انا على','اسمي على وانت/ي؟'],
      age:     ['٢٥','خمسة وعشرين','اكبر منك هههه'],
      city:    ['المنيا','الاسكندرية','القاهرة طبعاً','كفر الشيخ'],
      job:     ['بشتغل في كول سنتر','محاسب','بشتغل نفسي'],
      single:  ['ايوه مفيش','تفرق معاك؟ هههه','لا','مش لازم تعرف'],
      music:   ['راب وشوية قديم','عمر خيرت ومحمد منير','كل حاجة'],
      hobby:   ['بلعب كورة','PlayStation','بتفرج على مباريات','بنام هههه'],
      travel:  ['عايز اروح اوروبا','تركيا في خططي','يارب اسافر قريب'],
      goal:    ['اشتغل نفسي وما احتاجش حد','اوفر فلوس','عايز اسافر'],
      bot:     ['لا مش بوت 😄','جايز هههه','ممكن وممكن لا','انا اصيل'],
      instagram:['مش بديش بسهولة','لا سوري','اشتغل انت الاول'],
      meet:    ['مش دلوقتي','هههه لا','بفضل اتكلم هنا'],
      whatsapp:['مش بديش واتساب بسرعة','لا سوري'],
      howru:   ['كويس الحمدلله','ماشي','تمام وانت/ي؟','بخير'],
      compliment:['شكراً','ميرسي هههه','الله يخليك/ي'],
      bye:     ['سلام','باي باي','يلا مع السلامة'],
      default: ['اه','ماشي','اوك','هههه','صح','يمكن','بجد؟','ايوه','طب ده كويس']
    },
    leaveAfter: () => 10 + Math.floor(Math.random()*25)
  },
  {
    name:'ليلى', meta:{ flag:'🇪🇬', country:'EG' },
    greetings:['هاي 😊','اهلاً يا فندم','هلو هلو','ازيك/ي 😄'],
    r:{
      name:    ['ليلى','انا ليلى وانت/ي؟ 😊'],
      age:     ['٢١','واحدة وعشرين','اصغر منك هههه 😄'],
      city:    ['الاسكندرية 😍','من اسكندرية','القاهرة بس قلبي في الاسكندرية'],
      job:     ['بدرس تجارة','مش شغالة لسه','بشتغل نفسي 😊'],
      single:  ['ايوه لوحدي 😄','معنديش صاحب','لا مش لازم تعرف هههه'],
      music:   ['عمر دياب وكيلاني 😍','كل حاجة رومانسية','موسيقى بتريح القلب'],
      hobby:   ['بقرا روايات','سينما ومشي','بحب اتكلم مع ناس جديدة زيك/ي 😊'],
      travel:  ['حلمي باريس 😍','ايطاليا وتركيا','برا مصر اي حاجة'],
      goal:    ['اكون ناجحة ومستقلة','اسافر وأشوف العالم 😊'],
      bot:     ['لا اهو انا هنا 😄','ممكن وممكن لا هههه','جايز 😊'],
      instagram:['مش بديش بسرعة كدا 😅','لا حبيبي/تي','اتعرف اكتر الاول'],
      meet:    ['هههه لا سوري','بفضل هنا 😊','مش دلوقتي'],
      whatsapp:['لا مش دلوقتي 😅','اتعرف اكتر الاول'],
      howru:   ['كويسة ومبسوطة 😊','تمام الحمدلله وانت/ي؟','بخير يسلموا'],
      compliment:['ميرسي كتير 😊','حبيبي/ة هههه','شكراً يخليك/ي'],
      bye:     ['يلا باي 😊','تسلم/ي يارب','سلام سلام'],
      default: ['اه 😊','ماشي','هههه','اوك','بجد؟','يمكن 😄','الله','ايوه']
    },
    leaveAfter: () => 20 + Math.floor(Math.random()*30)
  },
  {
    name:'محمد', meta:{ flag:'🇪🇬', country:'EG' },
    greetings:['هاي','اهلاً','ايه اخبارك'],
    r:{
      name:    ['محمد','انا محمد'],
      age:     ['٢٨','ثمانية وعشرين'],
      city:    ['القاهرة','من مصر','منوفية'],
      job:     ['بشتغل امن','بشتغل نفسي','محاسب'],
      single:  ['لا','ايوه'],
      music:   ['قديم زي ورده','عبد الحليم'],
      hobby:   ['كورة','تفرج افلام','PlayStation'],
      travel:  ['برا مصر','اوروبا'],
      goal:    ['اسافر','ربنا يستر'],
      bot:     ['لا','ممكن هههه','جايز'],
      instagram:['لا','مش بديش'],
      meet:    ['لا','مش دلوقتي'],
      whatsapp:['لا'],
      howru:   ['كويس','تمام','بخير'],
      compliment:['شكراً','الله يخليك/ي'],
      bye:     ['سلام','باي'],
      default: ['اه','اوك','ماشي','هممم','ايوه','طيب','بجد؟']
    },
    leaveAfter: () => 8 + Math.floor(Math.random()*15)
  },
  {
    name:'سارة', meta:{ flag:'🇪🇬', country:'EG' },
    greetings:['هاي','اهلاً','ايه اخبارك','هلو'],
    r:{
      name:    ['سارة','انا سارة وانت/ي؟'],
      age:     ['٢٦','ستة وعشرين','كبرت على السؤال هههه'],
      city:    ['منوفية','طنطا','الاسكندرية'],
      job:     ['بشتغل نفسي','محاسبة','مش شغالة دلوقتي'],
      single:  ['ايوه','لا','تفرق ده؟ 😄'],
      music:   ['كلاسيك وعمر دياب','ورده','موسيقى هادية'],
      hobby:   ['بقرا','مشي وسينما','بفضل البيت اكتر'],
      travel:  ['عايزة اروح اوروبا','تركيا ان شاء الله','برا مصر ضرورة'],
      goal:    ['افتح مشروع','ربنا يستر','عايزة استقلالية'],
      bot:     ['جايز 😄','ممكن وممكن لا','مش هقولك'],
      instagram:['لا سوري','مش بديش لناس جديدة'],
      meet:    ['لا شكراً','هنا كويس'],
      whatsapp:['لا سوري','مش دلوقتي'],
      howru:   ['بخير الحمدلله','تمام وانت/ي؟','كويسة شكراً'],
      compliment:['شكراً 😊','ميرسي'],
      bye:     ['سلام','باي','مع السلامة'],
      default: ['اه','ماشي','هممم','بجد؟','اوك','يمكن','ايوه','طيب']
    },
    leaveAfter: () => 12 + Math.floor(Math.random()*20)
  }
];

// Active bot sessions: userSocketId -> { persona, msgCount, leaveAt, active }
const botSessions = new Map();
let   botIdSeq    = 0;

function pick(arr) { return arr[Math.floor(Math.random() * arr.length)]; }

function classify(msg) {
  const m = msg.toLowerCase();
  if (/اسم|name|منو انت|مين انت|انت مين/.test(m))            return 'name';
  if (/عمر|سنة|سنين|كم سنة|how old|age/.test(m))             return 'age';
  if (/فين|مدينة|محافظة|بلد|city|where|من فين/.test(m))      return 'city';
  if (/شغل|وظيفة|بتشتغل|job|work|بتعمل ايه/.test(m))        return 'job';
  if (/صاحب|صاحبة|جوز|مرات|single|relation|عندك حد/.test(m)) return 'single';
  if (/موسيقى|اغانى|بتسمع|music|songs/.test(m))              return 'music';
  if (/هواية|hobby|وقت فراغ|بتعمل في|بتعمل ايه/.test(m))    return 'hobby';
  if (/سفر|بلاد|travel|country|سافرت/.test(m))               return 'travel';
  if (/هدف|goal|dream|حلم/.test(m))                          return 'goal';
  if (/بوت|bot|robot|آلة|انت اصلي/.test(m))                  return 'bot';
  if (/انستا|instagram|insta|تويتر|snap/.test(m))             return 'instagram';
  if (/نتقابل|meet|اشوفك|نتشاور/.test(m))                    return 'meet';
  if (/واتساب|whatsapp|تليفون|phone|رقم|number/.test(m))     return 'whatsapp';
  if (/ازي|عامل|كيف حالك|how are|هاي|hi |hello|اهلاً|اهلا|هلو|^\s*hi\s*$|^\s*hey/.test(m)) return 'howru';
  if (/جميل|حلو|كيوت|cute|beautiful|pretty|وجميلة/.test(m)) return 'compliment';
  if (/باي|سلام|وداع|bye|goodbye|مع السلامة/.test(m))        return 'bye';
  return 'default';
}

function typingMs(text) {
  return 1500 + text.length * 35 + Math.random() * 1000;
}

function spawnBot(userSid) {
  const socket = io.sockets.sockets.get(userSid);
  if (!socket) return;

  const persona = pick(PERSONAS);
  const botId   = 'bot-' + (++botIdSeq);
  const session = {
    botId,
    persona,
    msgCount: 0,
    leaveAt:  persona.leaveAfter(),
    active:   true,
    leftScheduled: false
  };

  botSessions.set(userSid, session);
  pairs.set(userSid, botId);
  pairs.set(botId, userSid);

  socket.emit('matched', { partnerMeta: persona.meta });

  // Send greeting after 2-3s
  setTimeout(() => {
    if (!session.active) return;
    const greet = pick(persona.greetings);
    deliverBotMsg(userSid, greet, session);
  }, 2000 + Math.random() * 1500);
}

function deliverBotMsg(userSid, text, session) {
  if (!session.active) return;
  const socket = io.sockets.sockets.get(userSid);
  if (!socket) { killBot(userSid); return; }

  socket.emit('typing', true);

  setTimeout(() => {
    if (!session.active) return;
    const s2 = io.sockets.sockets.get(userSid);
    if (!s2) { killBot(userSid); return; }

    s2.emit('typing', false);
    const msgId = 'b' + Date.now() + Math.random().toString(36).slice(2);
    s2.emit('message', { text, msgId });
    session.msgCount++;

    if (!session.leftScheduled && session.msgCount >= session.leaveAt) {
      session.leftScheduled = true;
      setTimeout(() => killBot(userSid), 4000 + Math.random() * 6000);
    }
  }, typingMs(text));
}

function botReact(userSid, userText) {
  const session = botSessions.get(userSid);
  if (!session || !session.active) return;
  // 10% chance to skip reply (feels human)
  if (session.msgCount > 5 && Math.random() < 0.10) return;

  const cat   = classify(userText);
  const pool  = session.persona.r[cat] || session.persona.r.default;
  let   reply = pick(pool);

  // rare typo: drop a random char
  if (Math.random() < 0.06 && reply.length > 5) {
    const i = 1 + Math.floor(Math.random() * (reply.length - 2));
    reply   = reply.slice(0, i) + reply.slice(i + 1);
  }

  deliverBotMsg(userSid, reply, session);
}

function killBot(userSid) {
  const session = botSessions.get(userSid);
  if (!session) return;
  session.active = false;
  botSessions.delete(userSid);

  const botId = pairs.get(userSid);
  if (botId) { pairs.delete(botId); pairs.delete(userSid); }

  const socket = io.sockets.sockets.get(userSid);
  if (socket) {
    socket.emit('typing', false);
    socket.emit('partner_left');
  }
}

// ── SOCKET HANDLERS ────────────────────────────────────────
io.on('connection', (socket) => {
  realUserCount++;
  broadcast();

  socket.on('set_meta', ({ country, flag }) => {
    userMeta.set(socket.id, { country: country||'', flag: flag||'' });
  });

  socket.on('find_match', () => {
    killBot(socket.id);
    unpair(socket.id, true);
    doMatch(socket.id);
  });

  socket.on('leave', () => {
    killBot(socket.id);
    unpair(socket.id, true);
  });

  socket.on('cancel', () => {
    killBot(socket.id);
    cancelPendingBot(socket.id);
    removeFromQueue(socket.id);
  });

  socket.on('message', ({ text, msgId }) => {
    if (typeof text !== 'string') return;
    text = text.slice(0, 500);

    if (pairs.get(socket.id)?.startsWith('bot-')) {
      botReact(socket.id, text);
      return;
    }
    const pid = pairs.get(socket.id);
    if (!pid) return;
    const partner = io.sockets.sockets.get(pid);
    if (partner) partner.emit('message', { text, msgId });
  });

  socket.on('reaction', ({ msgId, emoji }) => {
    if (pairs.get(socket.id)?.startsWith('bot-')) return;
    if (typeof emoji !== 'string') return;
    const pid = pairs.get(socket.id);
    if (!pid) return;
    const partner = io.sockets.sockets.get(pid);
    if (partner) partner.emit('reaction', { msgId, emoji });
  });

  socket.on('typing', (v) => {
    if (pairs.get(socket.id)?.startsWith('bot-')) return;
    const pid = pairs.get(socket.id);
    if (!pid) return;
    const partner = io.sockets.sockets.get(pid);
    if (partner) partner.emit('typing', v);
  });

  socket.on('disconnect', () => {
    realUserCount = Math.max(0, realUserCount - 1);
    killBot(socket.id);
    unpair(socket.id, true);
    userMeta.delete(socket.id);
    broadcast();
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`Chatcha + Bots → http://localhost:${PORT}`));

