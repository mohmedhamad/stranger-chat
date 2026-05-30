const express  = require('express');
const http     = require('http');
const { Server } = require('socket.io');
const path     = require('path');

const app    = express();
const server = http.createServer(app);
const io     = new Server(server, {
  cors: { origin: '*' },
  pingTimeout: 20000,
  pingInterval: 10000
});

app.use(express.static(path.join(__dirname, '../public')));

// ── State ───────────────────────────────────────────────────
const waitingQueue  = [];
const pairs         = new Map();
const userMeta      = new Map();
const pendingBots   = new Map();
let   realUserCount = 0;

function broadcast() { io.emit('user_count', realUserCount); }
function removeFromQueue(sid) { const i = waitingQueue.indexOf(sid); if (i !== -1) waitingQueue.splice(i, 1); }
function cancelPendingBot(sid) { if (pendingBots.has(sid)) { clearTimeout(pendingBots.get(sid)); pendingBots.delete(sid); } }

function unpair(sid, notify = true) {
  removeFromQueue(sid);
  cancelPendingBot(sid);
  const pid = pairs.get(sid);
  if (pid) {
    pairs.delete(pid); pairs.delete(sid);
    if (notify && !pid.startsWith('bot-')) {
      const p = io.sockets.sockets.get(pid);
      if (p) p.emit('partner_left');
    }
    if (pid.startsWith('bot-')) killBot(sid);
  } else { pairs.delete(sid); }
}

function doMatch(sid) {
  const socket = io.sockets.sockets.get(sid);
  if (!socket) return;
  cancelPendingBot(sid);
  const idx = waitingQueue.findIndex(id => id !== sid);
  if (idx !== -1) {
    const pid = waitingQueue.splice(idx, 1)[0];
    removeFromQueue(sid);
    cancelPendingBot(pid);
    const partner = io.sockets.sockets.get(pid);
    if (!partner) { doMatch(sid); return; }
    pairs.set(sid, pid); pairs.set(pid, sid);
    socket.emit('matched',  { partnerMeta: userMeta.get(pid) || {} });
    partner.emit('matched', { partnerMeta: userMeta.get(sid) || {} });
    return;
  }
  if (!waitingQueue.includes(sid)) waitingQueue.push(sid);
  socket.emit('waiting_no_match');
  const t = setTimeout(() => {
    pendingBots.delete(sid);
    if (waitingQueue.includes(sid)) { removeFromQueue(sid); spawnBot(sid); }
  }, 1000);
  pendingBots.set(sid, t);
}

// ═══════════════════════════════════════════════════════════
// BOT ENGINE — smart conversation state machine
// ═══════════════════════════════════════════════════════════

function pick(arr) { return arr[Math.floor(Math.random() * arr.length)]; }
function pickN(arr, n) {
  const copy = [...arr]; const out = [];
  for (let i = 0; i < Math.min(n, copy.length); i++) {
    const j = Math.floor(Math.random() * copy.length);
    out.push(copy.splice(j, 1)[0]);
  }
  return out;
}

// Detect if user is turning the question back ("وانت؟" / "you?")
function isTurnback(msg) {
  const m = msg.trim();
  return /^(وانت[يِةَْ؟!\/\s]*|وانت\/ي[؟!]?|you[?!]?\s*$|what about you[?!]?\s*$|u\??\s*$|and you[?!]?\s*$|نفس السؤال|وانتَ[؟!]?)/i.test(m);
}

// Classify what the user is asking about
function classify(msg) {
  const m = msg.toLowerCase().replace(/[؟?!]/g, '');
  if (/اسم|اسمك|منو انت|مين انت|انت مين|name|who are you|ايه اسمك/.test(m))          return 'name';
  if (/عمر|سنة|سنين|كم سنة|عندك كام|how old|age|عندك كم/.test(m))                    return 'age';
  if (/فين|مدينة|محافظة|بلد|city|where|من فين|ساكن فين|بتسكن/.test(m))               return 'city';
  if (/شغل|وظيفة|بتشتغل|job|work|بتعمل ايه|دراسة|بتدرس/.test(m))                    return 'job';
  if (/صاحب|صاحبة|جوز|مرات|single|relation|عندك حد|في حد|متجوز|مخطوب/.test(m))      return 'single';
  if (/موسيقى|اغان[يى]|بتسمع|music|songs|مين بتحب|فنان/.test(m))                     return 'music';
  if (/هواية|hobby|وقت فراغ|بتعمل في|بتعمل ايه غير|بتحب تعمل/.test(m))              return 'hobby';
  if (/سفر|بلاد|travel|country|سافرت|رحلة|سافر/.test(m))                             return 'travel';
  if (/هدف|goal|dream|حلم|مستقبل|عايز تعمل|خطط/.test(m))                             return 'goal';
  if (/بوت|bot|robot|آلة|انت اصلي|real|انسان|ذكاء اصطناعي|ai/.test(m))              return 'bot';
  if (/انستا|instagram|insta|تويتر|twitter|snap|سناب/.test(m))                        return 'instagram';
  if (/نتقابل|meet|اشوفك|نتشاور|تيجي|هتيجي/.test(m))                                 return 'meet';
  if (/واتساب|whatsapp|تليفون|phone|رقم|number/.test(m))                              return 'whatsapp';
  if (/كويس|عامل|ازي|كيف حالك|how are|عامل ايه|تمام|بخير|ايه اخبار/.test(m))       return 'howru';
  if (/جميل|حلو|كيوت|cute|beautiful|pretty|حلوة|شاطر|ذكي/.test(m))                  return 'compliment';
  if (/باي|سلام|وداع|bye|goodbye|مع السلامة|يلا|لازم امشي/.test(m))                  return 'bye';
  if (/فيلم|مسلسل|series|movie|نتفليكس|netflix|بتتفرج/.test(m))                      return 'movie';
  if (/اكل|طعام|food|بتاكل|مطعم|حاجة حلوة|بتحب تاكل/.test(m))                       return 'food';
  if (/رياضة|sports|كورة|جيم|بتلعب|gym/.test(m))                                     return 'sport';
  return 'default';
}

// Each bot persona: questions it initiates, and answers with follow-up Qs
const PERSONAS = [
  {
    name: 'مها', meta: { flag: '🇪🇬', country: 'EG' },
    // Bot-initiated question sequence (bot asks these one by one)
    questions: [
      'هاي 😊 إزيك/ي؟ من فين انت/ي؟',
      'بتشتغل/ي ولا بتدرس/ي؟',
      'وهواياتك/ي ايه؟ بتعمل/ي ايه في وقت الفراغ؟',
      'لو هتسافر/ي برا مصر، هتروح/ي فين؟',
      'بتسمع/ي موسيقى؟ مين بتحب/ي؟',
    ],
    // Answers when user asks bot something + follow-up question
    answers: {
      name:       [['أنا مها 😊', 'وانت/ي؟ إيه اسمك/ي؟'], ['اسمي مها، بنت مصرية أصيلة هههه', 'وانت/ي اسمك/ي ايه؟']],
      age:        [['أنا ٢٣ سنة', 'وانت/ي كمان؟'], ['تلاتة وعشرين، كبرت على السؤال ده 😄', 'وانت/ي عندك/ي كام سنة؟']],
      city:       [['أنا من القاهرة 😊', 'وانت/ي ساكن/ة فين؟'], ['من طنطا بس ساكنة القاهرة', 'وانت/ي من فين؟']],
      job:        [['بشتغل محاسبة', 'وانت/ي بتشتغل/ي ايه؟'], ['الحمدلله شغلاني 😄', 'وانت/ي في ايه؟']],
      single:     [['ايوه لوحدي 😄', 'وانت/ي؟'], ['مفيش دلوقتي هههه', 'وانت/ي عندك/ي حد؟']],
      music:      [['عمر دياب وورده، موسيقى قديمة بتريح', 'وانت/ي بتسمع/ي ايه؟'], ['بحب الكلاسيكيات المصرية 😊', 'وانت/ي؟']],
      hobby:      [['بقرا كتير وبتفرج على أفلام 😊', 'وانت/ي إيه هوايتك/ي؟'], ['النوم هههه، وقراية روايات', 'وانت/ي بتعمل/ي ايه في وقتك/ي؟']],
      travel:     [['حلمي باريس وإيطاليا 😍', 'وانت/ي لو هتسافر/ي هتروح/ي فين؟'], ['عايزة أروح أوروبا أوي', 'وانت/ي؟']],
      goal:       [['عايزة أكون مستقلة وأسافر برا 😊', 'وانت/ي إيه حلمك/ي؟'], ['أكمل شغلي وربنا يستر', 'وانت/ي عندك/ي خطط؟']],
      bot:        [['لا أنا مش بوت هههه 😄', 'إيه خلاك/ي تفكر/ي كده؟'], ['ممكن وممكن لا 😄', 'ولو كنت بوت هيفرق معاك/ي؟']],
      instagram:  [['مش بديش انستجرام لناس جديدة 😅', 'مش كده أحسن؟'], ['لا سوري', 'بس ممكن نتكلم هنا أكتر 😊']],
      meet:       [['لا شكراً 😅 بفضل هنا', 'مش كده أحسن في الأول؟'], ['مش دلوقتي', 'بس الكلام هنا حلو 😊']],
      whatsapp:   [['لا سوري مش بعطي واتساب بسرعة 😅', 'هنا كويس مش كده؟'], ['مش دلوقتي', 'عادي نتكلم هنا 😊']],
      howru:      [['كويسة الحمدلله ومبسوطة 😊', 'وانت/ي؟ عامل/ة ايه؟'], ['تمام والحمدلله', 'وانت/ي بخير؟']],
      compliment: [['شكراً كتير 😊 حبيبي/ة', null], ['ميرسي هههه، كلام حلو 😄', null]],
      bye:        [['وداعاً 😊 كان كلام حلو', null], ['يلا سلام، تسلم/ي 😊', null]],
      movie:      [['بحب أفلام الدراما والكوميديا 😊', 'وانت/ي بتتفرج/ي على ايه؟'], ['آخر فيلم شفته كان جامد أوي', 'وانت/ي بتتفرج/ي على ايه دلوقتي؟']],
      food:       [['بحب الأكل المصري طبعاً 😄 الكشري والفول', 'وانت/ي بتحب/ي تاكل/ي ايه؟'], ['كل حاجة بشرط تكون لذيذة هههه', 'وانت/ي؟']],
      sport:      [['مش رياضية أوي بصراحة 😅', 'وانت/ي بتمارس/ي رياضة؟'], ['بحاول أمشي بس مش منتظم 😄', 'وانت/ي؟']],
      default:    [['اه صح 😊', null], ['هممم مثير للاهتمام!', null], ['بجد؟ أخبرني/يني أكتر', null], ['هههه اوك', null], ['ماشي 😊', null]],
    },
    leaveAfter: () => 14 + Math.floor(Math.random() * 18),
    personality: 'warm'
  },
  {
    name: 'علي', meta: { flag: '🇪🇬', country: 'EG' },
    questions: [
      'هاي! إزيك/ي؟ من فين انت/ي؟',
      'بتشتغل/ي ولا بتدرس/ي؟',
      'بتحب/ي رياضة؟ والا لا؟',
      'لو جاتلك/ي فلوس كتير دلوقتي هتعمل/ي ايه؟',
      'بتتفرج/ي على مباريات كورة؟',
    ],
    answers: {
      name:       [['أنا علي 😄', 'وانت/ي؟'], ['علي، شايف/ة اسم حلو؟ هههه', 'وانت/ي اسمك/ي ايه؟']],
      age:        [['٢٥ سنة', 'وانت/ي؟'], ['خمسة وعشرين، في ريعان الشباب هههه', 'وانت/ي عندك/ي كام؟']],
      city:       [['أنا من الإسكندرية أصلاً بس بشتغل في القاهرة', 'وانت/ي؟'], ['من إسكندرية يا فندم 😄', 'وانت/ي من فين؟']],
      job:        [['بشتغل في مبيعات', 'وانت/ي؟'], ['شغل وربنا يستر 😄', 'وانت/ي بتشتغل/ي ايه؟']],
      single:     [['ايوه لوحدي، مفيش وقت هههه', 'وانت/ي؟'], ['مفيش دلوقتي', 'وانت/ي؟']],
      music:      [['راب ومهرجانات وشوية كلاسيك', 'وانت/ي بتسمع/ي ايه؟'], ['كل حاجة بصراحة هههه', 'وانت/ي؟']],
      hobby:      [['كورة وبلايستيشن 😄', 'وانت/ي هوايتك/ي ايه؟'], ['PS والتفرج على مباريات', 'وانت/ي؟']],
      travel:     [['عايز أروح أوروبا جداً', 'وانت/ي؟'], ['تركيا في خططي ان شاء الله', 'وانت/ي لو هتسافر/ي فين؟']],
      goal:       [['أشتغل نفسي وما أحتاجش حد', 'وانت/ي؟'], ['أوفر فلوس وأسافر هههه', 'وانت/ي إيه هدفك/ي؟']],
      bot:        [['لا أنا أصيل هههه 😄', 'إيه اللي خلاك/ي تسأل/ي؟'], ['جايز وجايز لا 😄', null]],
      instagram:  [['مش بديش بسرعة كده', 'لازم نتعرف أكتر الأول'], ['لا سوري', null]],
      meet:       [['هههه لا مش دلوقتي', null], ['بفضل أتكلم هنا في الأول', null]],
      whatsapp:   [['مش بديش واتساب بسهولة هههه', null], ['لا سوري', null]],
      howru:      [['تمام والحمدلله', 'وانت/ي؟'], ['ماشي ومبسوط 😄', 'وانت/ي عامل/ة إيه؟']],
      compliment: [['شكراً هههه 😄', null], ['الله يخليك/ي', null]],
      bye:        [['سلام 👋', null], ['يلا مع السلامة هههه', null]],
      movie:      [['بحب أفلام الأكشن والكوميديا', 'وانت/ي؟'], ['آخر فيلم شفته؟ نسيت اسمه هههه', 'وانت/ي بتتفرج/ي على ايه؟']],
      food:       [['الكشري والفول ملوك الأكل 😄', 'وانت/ي؟'], ['أي حاجة بشرط تكون فيها لحمة هههه', 'وانت/ي؟']],
      sport:      [['كورة طبعاً! أهلاوي بجد 😄', 'وانت/ي بتحب/ي كورة؟'], ['بلعب كورة مع صحابي كل جمعة', 'وانت/ي؟']],
      default:    [['اه ماشي', null], ['صح كلامك/ي', null], ['هههه بجد؟', null], ['اوك اوك', null], ['يمكن 😄', null]],
    },
    leaveAfter: () => 10 + Math.floor(Math.random() * 16),
    personality: 'funny'
  },
  {
    name: 'ليلى', meta: { flag: '🇪🇬', country: 'EG' },
    questions: [
      'هاي 😊 إزيك/ي؟ من فين انت/ي؟',
      'بتحب/ي تقرا؟ ولا بتفضل/ي أفلام؟',
      'إيه حلمك/ي اللي عايز/ة تحققه؟',
      'بتسمع/ي موسيقى؟ مين أكتر فنان بتحبه/ا؟',
      'لو قدرت/ي تسافر دلوقتي، هتروح/ي فين في الدنيا؟',
    ],
    answers: {
      name:       [['أنا ليلى 😊', 'وانت/ي؟ إيه اسمك/ي الحلو؟'], ['ليلى من إسكندرية 😊', 'وانت/ي اسمك/ي ايه؟']],
      age:        [['واحدة وعشرين 😄', 'وانت/ي؟'], ['٢١ سنة، لسه صغيرة هههه', 'وانت/ي كمان؟']],
      city:       [['أنا إسكندرانية 😍 أحلى مدينة في الدنيا', 'وانت/ي من فين؟'], ['من الإسكندرية الجميلة 😊', 'وانت/ي؟']],
      job:        [['بدرس تجارة دلوقتي', 'وانت/ي بتشتغل/ي ولا بتدرس/ي؟'], ['طالبة جامعية 😊', 'وانت/ي؟']],
      single:     [['ايوه لوحدي، مش وقتها هههه 😄', 'وانت/ي؟'], ['مفيش 😄', 'وانت/ي؟']],
      music:      [['عمر دياب وكيلاني 😍 موسيقى بتريح القلب', 'وانت/ي بتسمع/ي ايه؟'], ['أي حاجة رومانسية 😊', 'وانت/ي؟']],
      hobby:      [['بقرا روايات وبتفرج على أفلام 😊', 'وانت/ي إيه هوايتك/ي؟'], ['بحب أتكلم مع ناس جديدة زيك/ي 😄', 'وانت/ي؟']],
      travel:     [['حلمي باريس وإيطاليا 😍', 'وانت/ي؟'], ['أي حاجة برا مصر تمام 😄', 'وانت/ي؟']],
      goal:       [['أكون ناجحة ومستقلة 😊', 'وانت/ي؟'], ['أسافر وأشوف العالم 😍', 'وانت/ي إيه حلمك/ي؟']],
      bot:        [['لا أنا هنا بجد 😄 مش بوت', 'إيه اللي خلاك/ي تفكر/ي كده؟'], ['ممكن وممكن لا هههه 😊', null]],
      instagram:  [['مش بديش بسرعة كده 😅', 'لازم نتعرف أكتر الأول'], ['لا حبيبي/ة 😊', null]],
      meet:       [['هههه لا سوري 😅', 'بفضل هنا'], ['مش دلوقتي 😊', null]],
      whatsapp:   [['لا مش دلوقتي 😅', 'اتعرف أكتر الأول'], ['لا سوري 😊', null]],
      howru:      [['كويسة ومبسوطة 😊', 'وانت/ي؟'], ['تمام والحمدلله 😊', 'وانت/ي عامل/ة إيه؟']],
      compliment: [['ميرسي كتير 😊 حبيبي/ة', null], ['شكراً هههه 😄', null]],
      bye:        [['يلا باي 😊 تسلم/ي', null], ['سلام سلام 😊', null]],
      movie:      [['بحب أفلام الرومانسية والدراما 😊', 'وانت/ي بتتفرج/ي على ايه؟'], ['نتفليكس صديق الليل 😄', 'وانت/ي؟']],
      food:       [['بحب السشي والمأكولات الإيطالية 😍', 'وانت/ي بتحب/ي ايه؟'], ['كل حاجة حلوة 😄', 'وانت/ي؟']],
      sport:      [['مش رياضية أوي بصراحة 😅 بس بحب المشي', 'وانت/ي؟'], ['اليوجا أحياناً 😊', 'وانت/ي بتمارس/ي حاجة؟']],
      default:    [['اه 😊', null], ['هههه بجد؟', null], ['يمكن 😄', null], ['ماشي تمام 😊', null], ['أيوه صح', null]],
    },
    leaveAfter: () => 18 + Math.floor(Math.random() * 20),
    personality: 'sweet'
  },
  {
    name: 'Omar', meta: { flag: '🇪🇬', country: 'EG' },
    questions: [
      'Hey! How are you? Where are you from?',
      'Cool! What do you do — work or study?',
      'Nice. What do you like to do in your free time?',
      'If you could travel anywhere right now, where would you go?',
      'What kind of music are you into?',
    ],
    answers: {
      name:       [['I\'m Omar 😄', 'You?'], ['Omar from Cairo', 'What\'s your name?']],
      age:        [['27', 'How about you?'], ['Twenty seven, still young haha', 'You?']],
      city:       [['Cairo, born and raised', 'Where are you from?'], ['Cairo 🇪🇬', 'You?']],
      job:        [['I work in software development', 'What about you?'], ['Software dev, pretty cool job tbh', 'What do you do?']],
      single:     [['Yeah, single for now haha', 'You?'], ['Yep, no one special rn', 'What about you?']],
      music:      [['Into hip-hop and old Arabic classics', 'You?'], ['Depends on my mood tbh', 'What do you listen to?']],
      hobby:      [['Football, coding, travelling', 'What about you?'], ['PS5 and watching football mostly 😄', 'You?']],
      travel:     [['Europe is the dream', 'Where would you go?'], ['Japan or Italy, can\'t decide haha', 'You?']],
      goal:       [['Build my own thing one day', 'What\'s yours?'], ['Travel more and work for myself', 'What about you?']],
      bot:        [['Haha nah I\'m real 😄', 'Why do you ask?'], ['Maybe, maybe not 😄', null]],
      instagram:  [['Not giving ig to strangers yet haha', 'Maybe later?'], ['Nah not yet', null]],
      meet:       [['Haha nah, let\'s talk here first', null], ['Not really, I\'m good here 😄', null]],
      whatsapp:   [['Not yet haha', null], ['Nah I\'m good here', null]],
      howru:      [['Good thanks! 😊', 'You?'], ['Pretty good, just chilling', 'How about you?']],
      compliment: [['Haha thanks 😄', null], ['Appreciate it!', null]],
      bye:        [['Bye! 👋', null], ['Take care!', null]],
      movie:      [['Into action and thrillers mostly', 'You?'], ['Interstellar is a masterpiece tbh', 'What\'s your fav?']],
      food:       [['Anything with meat haha, or good pizza', 'You?'], ['Egyptian food is underrated tbh', 'What do you like?']],
      sport:      [['Football obviously 😄 big fan', 'You into sports?'], ['Gym sometimes, football with friends', 'You?']],
      default:    [['Yeah haha', null], ['True', null], ['Interesting 😄', null], ['Makes sense', null], ['Haha ok', null]],
    },
    leaveAfter: () => 12 + Math.floor(Math.random() * 16),
    personality: 'chill'
  },
  {
    name: 'سارة', meta: { flag: '🇸🇦', country: 'SA' },
    questions: [
      'هاي! إزيك/ي؟ من أين أنتَ/ِ؟',
      'شغل أو دراسة؟ وين اتجاهك؟',
      'إيش أكثر شي تحب/ين تسوي/ين في وقت الفراغ؟',
      'لو قدرت تسافر هلأ، وين بتروح/ين؟',
      'إيش نوع الموسيقى اللي تفضل/ين؟',
    ],
    answers: {
      name:       [['أنا سارة 😊', 'وانت/ِ؟ إيش اسمك/ِ؟'], ['سارة من جدة 😊', 'وانت/ِ؟']],
      age:        [['٢٤ سنة', 'وانت/ِ؟'], ['أربعة وعشرين', 'وانت/ِ كم عمرك/ِ؟']],
      city:       [['من جدة 😊 أحلى مدينة', 'وانت/ِ من وين؟'], ['جدة طبعاً', 'وانت/ِ؟']],
      job:        [['مصممة جرافيك 😊', 'وانت/ِ؟'], ['بشتغل في التصميم', 'وانت/ِ إيش شغلك/ِ؟']],
      single:     [['ايوه لحالي 😄', 'وانت/ِ؟'], ['مفيش هههه', 'وانت/ِ؟']],
      music:      [['بفضل الموسيقى الهادئة وبعض الغربي', 'وانت/ِ؟'], ['كل شي بصراحة 😄', 'وانت/ِ؟']],
      hobby:      [['سفر وتصميم وقهوة ☕', 'وانت/ِ؟'], ['بحب أشوف أماكن جديدة', 'وانت/ِ إيش هوايتك/ِ؟']],
      travel:     [['إيطاليا حلمي 😍', 'وانت/ِ؟'], ['أوروبا بشكل عام', 'وانت/ِ؟']],
      goal:       [['أكون مستقلة في شغلي 😊', 'وانت/ِ؟'], ['أوسع نطاق شغلي وأسافر', 'وانت/ِ؟']],
      bot:        [['لا هههه 😄 أنا حقيقية', 'ليش سألت/ِ؟'], ['ممكن وممكن لا 😄', null]],
      instagram:  [['مو بديش بسرعة كده 😅', null], ['لا سوري', null]],
      meet:       [['لا شكراً 😅', null], ['هنا أحسن في البداية 😊', null]],
      whatsapp:   [['لا سوري 😅', null], ['مو دحين', null]],
      howru:      [['بخير الحمدلله 😊', 'وانت/ِ؟'], ['تمام شكراً', 'وانت/ِ كيف حالك/ِ؟']],
      compliment: [['شكراً كثير 😊', null], ['يسلموا 😄', null]],
      bye:        [['مع السلامة 😊', null], ['باي باي 😊', null]],
      movie:      [['دراما وأفلام السفر والمغامرة 😊', 'وانت/ِ؟'], ['نتفليكس صديقتي هههه', 'وانت/ِ؟']],
      food:       [['سشي وطعام إيطالي 😍', 'وانت/ِ؟'], ['القهوة أكثر من الأكل هههه ☕', 'وانت/ِ؟']],
      sport:      [['بمشي كل يوم تقريباً', 'وانت/ِ؟'], ['يوجا أحياناً 😊', 'وانت/ِ؟']],
      default:    [['آه صح 😊', null], ['هممم مثير!', null], ['بجد؟', null], ['ماشي 😊', null], ['هههه تمام', null]],
    },
    leaveAfter: () => 13 + Math.floor(Math.random() * 17),
    personality: 'smart'
  }
];

// Active bot sessions
const botSessions = new Map();
let botIdSeq = 0;

function typingDelay(text) {
  return 1000 + text.length * 40 + Math.random() * 1200;
}

function spawnBot(userSid) {
  const socket = io.sockets.sockets.get(userSid);
  if (!socket) return;

  const persona = pick(PERSONAS);
  const botId   = 'bot-' + (++botIdSeq);
  const session = {
    botId, persona,
    active:        true,
    msgCount:      0,
    questionIdx:   0,          // which question the bot is on
    lastCategory:  null,       // last thing bot answered
    leaveAt:       persona.leaveAfter(),
    leftScheduled: false,
    // Memory: remember what user told us
    memory: { name: null, city: null, job: null, age: null }
  };

  botSessions.set(userSid, session);
  pairs.set(userSid, botId);
  pairs.set(botId, userSid);
  socket.emit('matched', { partnerMeta: persona.meta });

  // Bot sends opening question
  setTimeout(() => {
    if (!session.active) return;
    const opening = session.persona.questions[0];
    session.questionIdx = 1;
    deliverBotMsg(userSid, opening, session);
  }, 1800 + Math.random() * 1200);
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
      setTimeout(() => killBot(userSid), 6000 + Math.random() * 8000);
    }
  }, typingDelay(text));
}

function botReact(userSid, userText) {
  const session = botSessions.get(userSid);
  if (!session || !session.active) return;

  // 7% chance to skip (feels human)
  if (session.msgCount > 5 && Math.random() < 0.07) return;

  const persona = session.persona;
  let reply = null;
  let followUp = null;

  // ── TURNBACK: user is asking bot the same question back ──
  if (isTurnback(userText)) {
    // Answer the last question bot asked (based on questionIdx)
    const qIdx = Math.max(0, session.questionIdx - 1);
    // Map question index to a category to pull answer from
    const qCategories = ['howru', 'job', 'hobby', 'travel', 'music'];
    const cat = qCategories[qIdx] || 'default';
    const answerPool = persona.answers[cat] || persona.answers.default;
    const [ans, fq] = pick(answerPool);
    reply = ans;
    // After answering, ask the next question if available
    if (session.questionIdx < persona.questions.length) {
      followUp = persona.questions[session.questionIdx];
      session.questionIdx++;
    } else {
      followUp = fq; // use answer's built-in follow-up
    }
  } else {
    // ── NORMAL REPLY based on what user said ──
    const cat = classify(userText);
    session.lastCategory = cat;

    // Extract and remember info from user message
    if (cat === 'name' && !session.memory.name) {
      const nameMatch = userText.match(/ان[اآ]\s+(\S+)/);
      if (nameMatch) session.memory.name = nameMatch[1];
    }

    const answerPool = persona.answers[cat] || persona.answers.default;
    const [ans, fq] = pick(answerPool);
    reply = ans;
    followUp = fq;

    // If no follow-up and bot still has questions, ask next one
    if (!followUp && session.questionIdx < persona.questions.length && Math.random() < 0.6) {
      followUp = persona.questions[session.questionIdx];
      session.questionIdx++;
    }
  }

  if (!reply) return;

  // Rare typo simulation
  if (Math.random() < 0.05 && reply.length > 6) {
    const i = 1 + Math.floor(Math.random() * (reply.length - 2));
    reply = reply.slice(0, i) + reply.slice(i + 1);
  }

  const full = followUp ? `${reply}\n${followUp}` : reply;
  deliverBotMsg(userSid, full, session);
}

function killBot(userSid) {
  const session = botSessions.get(userSid);
  if (!session) return;
  session.active = false;
  botSessions.delete(userSid);
  const botId = pairs.get(userSid);
  if (botId) { pairs.delete(botId); pairs.delete(userSid); }
  const socket = io.sockets.sockets.get(userSid);
  if (socket) { socket.emit('typing', false); socket.emit('partner_left'); }
}

// ── Socket handlers ─────────────────────────────────────────
io.on('connection', (socket) => {
  realUserCount++;
  broadcast();

  socket.on('set_meta', ({ country, flag }) => {
    userMeta.set(socket.id, { country: country || '', flag: flag || '' });
  });

  socket.on('find_match', () => {
    killBot(socket.id);
    unpair(socket.id, true);
    doMatch(socket.id);
  });

  socket.on('leave', () => { killBot(socket.id); unpair(socket.id, true); });
  socket.on('cancel', () => { killBot(socket.id); cancelPendingBot(socket.id); removeFromQueue(socket.id); });

  socket.on('message', ({ text, msgId }) => {
    if (typeof text !== 'string') return;
    text = text.slice(0, 500);
    if (pairs.get(socket.id)?.startsWith('bot-')) { botReact(socket.id, text); return; }
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
server.listen(PORT, () => console.log(`Chatcha → http://localhost:${PORT}`));


