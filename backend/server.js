require('dotenv').config();
const path = require('path');
const express = require('express');
const http = require('http');
const cors = require('cors');
const { Server } = require('socket.io');
const { createClient } = require('redis');
const { createAdapter } = require('@socket.io/redis-adapter');
const admin = require('firebase-admin');

// ---------- Firebase Admin ----------
// Используется для: проверки входа (Auth), профилей пользователей (Firestore 
// "users"), и метаданных групп/личных чатов (Firestore "groups"/"dms").
// Сами сообщения (текст + медиа) в Firestore НЕ пишутся — только в Redis.
const serviceAccount = process.env.FIREBASE_SERVICE_ACCOUNT
  ? JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT)
  : require('./serviceAccountKey.json');

admin.initializeApp({
  credential: admin.credential.cert(serviceAccount),
});

const db = admin.firestore();
const usersCol = db.collection('users');
const groupsCol = db.collection('groups');
const dmsCol = db.collection('dms');

// ---------- Константы ----------
const GENERAL_ROOM = 'general';
const MAX_MESSAGES = 200; // сколько последних сообщений хранить на комнату
const MAX_MEDIA_BYTES = 8 * 1024 * 1024; // ~8 МБ на файл (фото/гифка/видео), оценка по base64
const MAX_AVATAR_CHARS = 1_500_000; // ограничение на длину base64-аватарки в Firestore-документе

// Чтобы неожиданная ошибка где-то в асинхронном коде не роняла весь процесс
// молча — Render в таком случае просто перезапускает сервис без объяснений,
// а в логах теперь будет видно, что именно произошло.
process.on('unhandledRejection', (reason) => {
  console.error('Unhandled Rejection:', reason);
});
process.on('uncaughtException', (err) => {
  console.error('Uncaught Exception:', err);
});

// ---------- Express ----------
const app = express();

// Всё теперь на одном сервисе (фронтенд + бэкенд + сокет на одном порту),
// поэтому CORS фронтенду больше не нужен — он ходит на тот же origin.
// CORS_ORIGIN оставлен на случай локальной разработки/сторонних клиентов.
const allowedOrigins = (process.env.CORS_ORIGIN || '*')
  .split(',')
  .map((o) => o.trim())
  .filter(Boolean);

const corsOptions = {
  origin: allowedOrigins.includes('*') ? '*' : allowedOrigins,
};
app.use(cors(corsOptions));
app.use(express.json({ limit: '3mb' })); // с запасом под base64-аватарку в /profile

app.get('/health', (_req, res) => res.json({ ok: true }));

// ---------- Статика фронтенда ----------
app.use(express.static(path.join(__dirname, 'public')));

const server = http.createServer(app);
const io = new Server(server, {
  cors: corsOptions,
  maxHttpBufferSize: 10 * 1024 * 1024, // фото/видео идут через сокет — поднимаем лимит буфера
});

// ---------- Redis: сообщения (хранение, по комнатам) + pub/sub между инстансами ----------
const messagesKey = (roomId) => `chat:${roomId}`;

const redisUrl = process.env.REDIS_URL;
if (!redisUrl) {
  console.warn('REDIS_URL не задан — без него сервер работать не сможет (сообщения хранятся в Redis).');
}

let redisClient;

async function setupRedis() {
  redisClient = createClient({ url: redisUrl });
  redisClient.on('error', (e) => console.error('Redis error', e));
  await redisClient.connect();

  const pubClient = redisClient.duplicate();
  const subClient = redisClient.duplicate();
  pubClient.on('error', (e) => console.error('Redis pub error', e));
  subClient.on('error', (e) => console.error('Redis sub error', e));
  await Promise.all([pubClient.connect(), subClient.connect()]);
  io.adapter(createAdapter(pubClient, subClient));

  console.log('Redis подключён (сообщения + адаптер Socket.io)');
}

// ---------- Комнаты: general (фиксированная) / group:<id> / dm:<uidA_uidB> ----------
function dmRoomKey(uidA, uidB) {
  return [uidA, uidB].sort().join('_');
}

async function getUserGroups(uid) {
  const snap = await groupsCol.where('members', 'array-contains', uid).get();
  return snap.docs.map((d) => ({ id: d.id, ...d.data() }));
}

async function getUserDms(uid) {
  const snap = await dmsCol.where('members', 'array-contains', uid).get();
  return snap.docs.map((d) => ({ id: d.id, ...d.data() }));
}

async function isMember(uid, roomId) {
  if (roomId === GENERAL_ROOM) return true;
  if (roomId.startsWith('group:')) {
    const doc = await groupsCol.doc(roomId.slice('group:'.length)).get();
    return doc.exists && (doc.data().members || []).includes(uid);
  }
  if (roomId.startsWith('dm:')) {
    const doc = await dmsCol.doc(roomId.slice('dm:'.length)).get();
    return doc.exists && (doc.data().members || []).includes(uid);
  }
  return false;
}

// Профиль пользователя (имя + аватар). Создаётся один раз при первом входе,
// дальше редактируется только через PUT /profile — не перезаписывается токеном.
async function ensureUserProfile(user) {
  const ref = usersCol.doc(user.uid);
  const snap = await ref.get();
  if (!snap.exists) {
    const initial = { uid: user.uid, name: user.name || 'Без имени', avatar: '', lastSeenAt: Date.now() };
    await ref.set(initial);
    return initial;
  }
  await ref.update({ lastSeenAt: Date.now() });
  return snap.data();
}

// ---------- Кто сейчас онлайн: uid -> Set(socket), чтобы мгновенно подключать
// пользователя к новой группе/ЛС без переподключения сокета ----------
const uidToSockets = new Map();

function trackSocket(uid, socket) {
  if (!uidToSockets.has(uid)) uidToSockets.set(uid, new Set());
  uidToSockets.get(uid).add(socket);
}
function untrackSocket(uid, socket) {
  const set = uidToSockets.get(uid);
  if (!set) return;
  set.delete(socket);
  if (!set.size) uidToSockets.delete(uid);
}
function forEachUidSocket(uid, fn) {
  const set = uidToSockets.get(uid);
  if (set) set.forEach(fn);
}

// ---------- REST: требует Firebase ID token в заголовке Authorization ----------
async function requireAuth(req, res, next) {
  try {
    const authHeader = req.headers.authorization || '';
    const token = authHeader.startsWith('Bearer ') ? authHeader.slice(7) : null;
    if (!token) return res.status(401).json({ error: 'No token' });
    const decoded = await admin.auth().verifyIdToken(token);
    req.uid = decoded.uid;
    next();
  } catch (err) {
    res.status(401).json({ error: 'Auth failed' });
  }
}

// Мой собственный профиль (имя + аватар) — для экрана редактирования профиля
app.get('/me', requireAuth, async (req, res) => {
  try {
    const snap = await usersCol.doc(req.uid).get();
    if (!snap.exists) return res.json({ uid: req.uid, name: '', avatar: '' });
    res.json(snap.data());
  } catch (err) {
    console.error('Ошибка загрузки своего профиля:', err);
    res.status(500).json({ error: 'failed to load profile' });
  }
});

// Список чатов пользователя: общий чат + группы + ЛС (с данными собеседника)
app.get('/rooms', requireAuth, async (req, res) => {
  try {
    const [groups, dms] = await Promise.all([getUserGroups(req.uid), getUserDms(req.uid)]);

    const dmRooms = await Promise.all(
      dms.map(async (d) => {
        const partnerUid = (d.members || []).find((m) => m !== req.uid) || req.uid;
        const partnerSnap = await usersCol.doc(partnerUid).get();
        const partner = partnerSnap.exists ? partnerSnap.data() : { name: 'Без имени', avatar: '' };
        return {
          id: d.id,
          roomId: `dm:${d.id}`,
          type: 'dm',
          name: partner.name || 'Без имени',
          avatar: partner.avatar || '',
          partnerUid,
        };
      })
    );

    const groupRooms = groups.map((g) => ({
      id: g.id,
      roomId: `group:${g.id}`,
      type: 'group',
      name: g.name,
      memberCount: (g.members || []).length,
    }));

    res.json({
      general: { id: GENERAL_ROOM, roomId: GENERAL_ROOM, type: 'general', name: 'Общий чат' },
      groups: groupRooms,
      dms: dmRooms,
    });
  } catch (err) {
    console.error('Ошибка загрузки списка чатов:', err);
    res.status(500).json({ error: 'failed to load rooms' });
  }
});

// История сообщений конкретной комнаты — из Redis
app.get('/messages/:roomId', requireAuth, async (req, res) => {
  try {
    const { roomId } = req.params;
    if (!(await isMember(req.uid, roomId))) return res.status(403).json({ error: 'forbidden' });
    if (!redisClient) return res.json([]);
    const raw = await redisClient.lRange(messagesKey(roomId), -50, -1);
    res.json(raw.map((item) => JSON.parse(item)));
  } catch (err) {
    console.error('Ошибка чтения истории из Redis:', err);
    res.status(500).json({ error: 'failed to load messages' });
  }
});

// Поиск пользователей по имени — для ЛС и приглашения в группы
app.get('/users/search', requireAuth, async (req, res) => {
  try {
    const q = String(req.query.q || '').trim().toLowerCase();
    if (!q) return res.json([]);
    const snap = await usersCol.get();
    const results = snap.docs
      .map((d) => d.data())
      .filter((u) => u.uid !== req.uid && (u.name || '').toLowerCase().includes(q))
      .slice(0, 20)
      .map((u) => ({ uid: u.uid, name: u.name || 'Без имени', avatar: u.avatar || '' }));
    res.json(results);
  } catch (err) {
    console.error('Ошибка поиска пользователей:', err);
    res.status(500).json({ error: 'search failed' });
  }
});

// Создать свою группу
app.post('/groups', requireAuth, async (req, res) => {
  try {
    const name = String(req.body?.name || '').trim().slice(0, 100);
    const memberUids = Array.isArray(req.body?.memberUids)
      ? req.body.memberUids.filter((u) => typeof u === 'string')
      : [];
    if (!name) return res.status(400).json({ error: 'name required' });

    const members = Array.from(new Set([req.uid, ...memberUids]));
    const ref = await groupsCol.add({ name, members, createdBy: req.uid, createdAt: Date.now() });
    const roomId = `group:${ref.id}`;

    // сразу подключаем всех участников (кто сейчас онлайн) к комнате
    members.forEach((uid) => {
      forEachUidSocket(uid, (s) => {
        s.join(roomId);
        s.emit('rooms-changed');
      });
    });

    res.json({ id: ref.id, roomId, name, members });
  } catch (err) {
    console.error('Ошибка создания группы:', err);
    res.status(500).json({ error: 'failed to create group' });
  }
});

// Открыть / создать личный чат с пользователем
app.post('/dms', requireAuth, async (req, res) => {
  try {
    const targetUid = String(req.body?.targetUid || '');
    if (!targetUid || targetUid === req.uid) return res.status(400).json({ error: 'invalid target' });

    const id = dmRoomKey(req.uid, targetUid);
    const ref = dmsCol.doc(id);
    const snap = await ref.get();
    if (!snap.exists) {
      await ref.set({ members: [req.uid, targetUid], createdAt: Date.now() });
    }
    const roomId = `dm:${id}`;

    [req.uid, targetUid].forEach((uid) => {
      forEachUidSocket(uid, (s) => {
        s.join(roomId);
        s.emit('rooms-changed');
      });
    });

    const partnerSnap = await usersCol.doc(targetUid).get();
    const partner = partnerSnap.exists ? partnerSnap.data() : { name: 'Без имени', avatar: '' };
    res.json({ id, roomId, name: partner.name || 'Без имени', avatar: partner.avatar || '', partnerUid: targetUid });
  } catch (err) {
    console.error('Ошибка создания ЛС:', err);
    res.status(500).json({ error: 'failed to start dm' });
  }
});

// Редактирование профиля (имя + аватар)
app.put('/profile', requireAuth, async (req, res) => {
  try {
    const update = {};
    if (req.body?.name !== undefined) {
      const name = String(req.body.name).trim().slice(0, 60);
      if (!name) return res.status(400).json({ error: 'name cannot be empty' });
      update.name = name;
    }
    if (req.body?.avatar !== undefined) {
      const avatar = String(req.body.avatar);
      if (avatar.length > MAX_AVATAR_CHARS) return res.status(413).json({ error: 'avatar too large' });
      update.avatar = avatar;
    }
    if (Object.keys(update).length === 0) return res.status(400).json({ error: 'nothing to update' });

    await usersCol.doc(req.uid).set(update, { merge: true });

    // обновляем имя/аватар у уже подключённых сокетов этого пользователя,
    // чтобы новые сообщения сразу шли с новым именем/аватаркой
    forEachUidSocket(req.uid, (s) => {
      if (update.name !== undefined) s.user.name = update.name;
      if (update.avatar !== undefined) s.user.avatar = update.avatar;
    });

    res.json({ ok: true });
  } catch (err) {
    console.error('Ошибка обновления профиля:', err);
    res.status(500).json({ error: 'failed to update profile' });
  }
});

// ---------- Аутентификация сокетов через Firebase ID token ----------
io.use(async (socket, next) => {
  try {
    const token = socket.handshake.auth?.token;
    if (!token) return next(new Error('No auth token'));
    const decoded = await admin.auth().verifyIdToken(token);
    socket.user = { uid: decoded.uid, name: decoded.name || 'Без имени', avatar: '' };
    next();
  } catch (err) {
    next(new Error('Auth failed'));
  }
});

io.on('connection', (socket) => {
  console.log(`Подключился: ${socket.user.uid}`);

  // Общий чат и обработчики регистрируем СРАЗУ, синхронно, до любых await —
  // если ниже что-то упадёт при обращении к Firestore, сокет всё равно
  // остаётся рабочим и слышит события (иначе сообщения улетали бы в никуда).
  trackSocket(socket.user.uid, socket);
  socket.join(GENERAL_ROOM);

  socket.on('message', async (payload) => {
    const roomId = String(payload?.roomId || GENERAL_ROOM);
    if (!(await isMember(socket.user.uid, roomId))) return;

    const text = String(payload?.text || '').slice(0, 2000);

    let media = null;
    if (payload?.media && payload.media.data) {
      const data = String(payload.media.data);
      const approxBytes = (data.length * 3) / 4; // грубая оценка размера исходного файла по base64
      if (approxBytes > MAX_MEDIA_BYTES) {
        socket.emit('chat-error', { message: 'Файл слишком большой (максимум ~8 МБ).' });
        return;
      }
      media = {
        type: payload.media.type === 'video' ? 'video' : 'image', // gif идёт как image (это и есть image/gif)
        data,
        name: String(payload.media.name || '').slice(0, 200),
      };
    }

    if (!text && !media) return;

    const message = {
      id: `${Date.now()}_${Math.random().toString(36).slice(2, 8)}`,
      roomId,
      text,
      media,
      uid: socket.user.uid,
      name: socket.user.name,
      avatar: socket.user.avatar,
      createdAt: Date.now(),
    };

    try {
      if (redisClient) {
        await redisClient.rPush(messagesKey(roomId), JSON.stringify(message));
        await redisClient.lTrim(messagesKey(roomId), -MAX_MESSAGES, -1);
      }
    } catch (err) {
      console.error('Ошибка записи сообщения в Redis:', err);
    }

    io.to(roomId).emit('message', message);
  });

  socket.on('typing', (payload) => {
    const roomId = String(payload?.roomId || GENERAL_ROOM);
    socket.to(roomId).emit('typing', { roomId, uid: socket.user.uid, name: socket.user.name });
  });

  // клиент вызывает это сразу после успешного PUT /profile, чтобы подхватить
  // новое имя/аватар без переподключения сокета (на случай другого таба/устройства)
  socket.on('refresh-profile', async () => {
    try {
      const snap = await usersCol.doc(socket.user.uid).get();
      if (snap.exists) {
        const d = snap.data();
        socket.user.name = d.name || socket.user.name;
        socket.user.avatar = d.avatar || '';
      }
    } catch (err) {
      console.error('Ошибка обновления профиля в сокете:', err);
    }
  });

  socket.on('disconnect', () => {
    console.log(`Отключился: ${socket.user.uid}`);
    untrackSocket(socket.user.uid, socket);
    socket.broadcast.emit('presence', { uid: socket.user.uid, status: 'offline' });
  });

  // Асинхронная часть — подгрузка профиля (имя/аватар) и подключение к
  // комнатам групп/ЛС. Обёрнута в try/catch: если Firestore недоступен или
  // упадёт с ошибкой, общий чат (уже подключён выше) продолжает работать,
  // просто имя/аватар останутся дефолтными, а группы/ЛС не подключатся.
  (async () => {
    try {
      const profile = await ensureUserProfile(socket.user);
      socket.user.name = profile.name;
      socket.user.avatar = profile.avatar || '';

      const [groups, dms] = await Promise.all([getUserGroups(socket.user.uid), getUserDms(socket.user.uid)]);
      groups.forEach((g) => socket.join(`group:${g.id}`));
      dms.forEach((d) => socket.join(`dm:${d.id}`));

      socket.broadcast.emit('presence', { uid: socket.user.uid, status: 'online' });
    } catch (err) {
      console.error('Ошибка инициализации сокета (профиль/комнаты):', err);
      socket.emit('chat-error', { message: 'Не удалось загрузить профиль или список чатов. Попробуйте перезайти.' });
    }
  })();
});

// Любой прочий GET-запрос (например, обновление страницы) — отдаём index.html
app.get('*', (req, res, next) => {
  if (
    req.path.startsWith('/socket.io') ||
    req.path.startsWith('/me') ||
    req.path.startsWith('/messages') ||
    req.path.startsWith('/rooms') ||
    req.path.startsWith('/users') ||
    req.path.startsWith('/groups') ||
    req.path.startsWith('/dms') ||
    req.path.startsWith('/profile') ||
    req.path === '/health'
  ) {
    return next();
  }
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

const PORT = process.env.PORT || 3000;
setupRedis()
  .then(() => {
    server.listen(PORT, () => console.log(`Сервер запущен на порту ${PORT}`));
  })
  .catch((err) => {
    console.error('Не удалось подключиться к Redis, сервер не запущен:', err);
    process.exit(1);
  });
