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
// Используется ТОЛЬКО для пользователей: проверка входа (Auth) и хранение
// профилей пользователей в Firestore (коллекция "users"). Сообщения сюда не пишутся.
const serviceAccount = process.env.FIREBASE_SERVICE_ACCOUNT
  ? JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT)
  : require('./serviceAccountKey.json');

admin.initializeApp({
  credential: admin.credential.cert(serviceAccount),
});

const db = admin.firestore();
const usersCol = db.collection('users');

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
app.use(express.json());

app.get('/health', (_req, res) => res.json({ ok: true }));

// ---------- Статика фронтенда ----------
// index.html лежит в backend/public — Express отдаёт его с того же порта,
// на котором работает Socket.io, так что и сайт, и веб-сокет — один сервис.
app.use(express.static(path.join(__dirname, 'public')));

const server = http.createServer(app);
const io = new Server(server, { cors: corsOptions });

// ---------- Redis: сообщения (хранение) + pub/sub между инстансами ----------
const MESSAGES_KEY = 'chat:general';
const MAX_MESSAGES = 200;

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

// История сообщений — из Redis
app.get('/messages', async (_req, res) => {
  try {
    if (!redisClient) return res.json([]);
    const raw = await redisClient.lRange(MESSAGES_KEY, -50, -1);
    res.json(raw.map((item) => JSON.parse(item)));
  } catch (err) {
    console.error('Ошибка чтения истории из Redis:', err);
    res.status(500).json({ error: 'failed to load messages' });
  }
});

// ---------- Аутентификация сокетов через Firebase ID token ----------
io.use(async (socket, next) => {
  try {
    const token = socket.handshake.auth?.token;
    if (!token) return next(new Error('No auth token'));
    const decoded = await admin.auth().verifyIdToken(token);
    socket.user = { uid: decoded.uid, name: decoded.name || 'Аноним' };
    next();
  } catch (err) {
    next(new Error('Auth failed'));
  }
});

// Создаёт/обновляет профиль пользователя в Firestore (коллекция users).
// Это и есть "хранение данных юзеров" в Firebase — сами сообщения тут не хранятся.
async function upsertUserProfile(user) {
  try {
    await usersCol.doc(user.uid).set(
      {
        uid: user.uid,
        name: user.name,
        lastSeenAt: Date.now(),
      },
      { merge: true }
    );
  } catch (err) {
    console.error('Ошибка записи профиля пользователя в Firestore:', err);
  }
}

io.on('connection', (socket) => {
  console.log(`Подключился: ${socket.user.uid}`);
  upsertUserProfile(socket.user);
  socket.broadcast.emit('presence', { uid: socket.user.uid, status: 'online' });

  socket.on('message', async (payload) => {
    const message = {
      text: String(payload?.text || '').slice(0, 2000),
      uid: socket.user.uid,
      name: socket.user.name,
      createdAt: Date.now(),
    };
    if (!message.text) return;

    try {
      if (redisClient) {
        await redisClient.rPush(MESSAGES_KEY, JSON.stringify(message));
        await redisClient.lTrim(MESSAGES_KEY, -MAX_MESSAGES, -1);
      }
    } catch (err) {
      console.error('Ошибка записи сообщения в Redis:', err);
    }

    io.emit('message', message);
  });

  socket.on('typing', () => {
    socket.broadcast.emit('typing', { uid: socket.user.uid, name: socket.user.name });
  });

  socket.on('disconnect', () => {
    console.log(`Отключился: ${socket.user.uid}`);
    socket.broadcast.emit('presence', { uid: socket.user.uid, status: 'offline' });
  });
});

// Любой прочий GET-запрос (например, обновление страницы) — отдаём index.html
app.get('*', (req, res, next) => {
  if (req.path.startsWith('/socket.io') || req.path === '/messages' || req.path === '/health') {
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
