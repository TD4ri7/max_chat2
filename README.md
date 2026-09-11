# Облачный чат: один сервис на Render + Firebase (пользователи) + Redis (сообщения)

Архитектура (всё на одном веб-сервисе Render — один порт, один WebSocket):

```
Браузер
   │  открывает https://cloud-chat.onrender.com
   ▼
Render — один Web Service (Node.js + Express + Socket.io)
   │  Express отдаёт index.html из backend/public (статика)
   │  тот же HTTP-сервер держит Socket.io (WebSocket) — тот же origin, тот же порт
   │  Firebase Admin SDK — проверка ID-токена + профиль пользователя в Firestore
   ▼
Firebase (Firestore)              Redis (Upstash / Redis Cloud)
— коллекция users:                — список chat:messages: сама переписка
  uid, имя, lastSeenAt             — pub/sub между инстансами Render (если их несколько)
```

Роли:
- **Render** — единственный сервис: раздаёт сайт и держит Socket.io на одном порту/домене.
- **Firebase** — только пользователи: вход (Auth) + профиль в Firestore.
- **Redis** — вся переписка: хранение сообщений и рассылка в реальном времени.

Поскольку фронтенд и бэкенд теперь на одном домене, CORS между ними не нужен,
а `BACKEND_URL` во фронтенде — это просто `window.location.origin`.

---

## 1. Firebase (пользователи)

1. Создайте проект на https://console.firebase.google.com
2. **Authentication → Sign-in method** → включите **Email/Password**.
3. **Firestore Database → Create database** (production mode, любой регион).
   Здесь будут храниться только профили пользователей (коллекция `users`), не сообщения.
4. Загрузите правила `firestore.rules` из этого репозитория:
   ```bash
   npm install -g firebase-tools
   firebase login
   firebase init firestore   # выберите свой проект, укажите firestore.rules
   firebase deploy --only firestore:rules
   ```
5. **Project settings → Service accounts → Generate new private key** — скачается JSON,
   он пойдёт в переменную `FIREBASE_SERVICE_ACCOUNT` на Render.
6. **Project settings → General** → добавьте веб-приложение (иконка `</>`, если ещё нет),
   скопируйте `apiKey`, `authDomain`, `projectId` — вставите их в `backend/public/index.html`.
7. **Важно:** когда получите адрес сервиса на Render (шаг 3), добавьте его в
   **Authentication → Settings → Authorized domains** → Add domain. Без этого
   вход будет блокироваться.

## 2. Redis (сообщения)

**Upstash** (бесплатный тариф, серверless):
1. https://upstash.com → Create database.
2. Скопируйте `REDIS_URL` (вид `redis://default:...@...upstash.io:6379`).

## 3. Один сервис на Render

Если используете `render.yaml` из корня репозитория — Render сам создаст сервис через
"New → Blueprint". Иначе вручную:

1. New → Web Service → подключите репозиторий.
2. **Root Directory: `backend`**
3. Build command: `npm install`, Start command: `npm start`.
4. Environment variables:
   - `REDIS_URL` — из шага 2
   - `FIREBASE_SERVICE_ACCOUNT` — весь JSON сервисного аккаунта одной строкой
   - `CORS_ORIGIN` — не обязателен (фронтенд теперь на том же домене); задайте,
     только если планируете открывать сокет ещё откуда-то (например, для разработки).
5. Деплой → получите единый адрес `https://cloud-chat.onrender.com`.
   Откройте его в браузере — там же и сайт, и WebSocket.
6. Добавьте этот адрес в Firebase → Authorized domains (шаг 1.7).
7. Впишите `firebaseConfig` в `backend/public/index.html` (см. шаг 1.6) и задеплойте снова.

---

## Как это работает

- Пользователь заходит на единственный адрес сервиса → Express отдаёт `index.html` из
  `backend/public`. Firebase Auth логинит его по email/паролю и выдаёт ID-токен.
- Тот же браузер открывает Socket.io-соединение на тот же адрес (`window.location.origin`),
  передавая токен; бэкенд проверяет его через Firebase Admin SDK и сохраняет/обновляет
  профиль в Firestore (`users/{uid}`: имя, время последнего захода).
- При отправке сообщения бэкенд кладёт его в Redis-список `chat:messages` (`RPUSH`),
  обрезает список до последних 200 (`LTRIM`) и рассылает всем клиентам (`io.emit`).
- Redis-адаптер Socket.io гарантирует доставку всем, даже если Render поднимет
  несколько инстансов сервера.
- При открытии чата история подгружается через `GET /messages` — `LRANGE` по тому же
  Redis-списку.

## Локальный запуск

```bash
cd backend
cp .env.example .env   # заполните REDIS_URL и FIREBASE_SERVICE_ACCOUNT
npm install
npm start
```

Откройте `http://localhost:3000` — сайт и сокет будут на одном и том же адресе,
ничего дополнительно указывать не нужно. Добавьте `localhost` в Firebase Authorized domains.

## Возможные доработки

- Список онлайн-пользователей (`presence`-события уже приходят на клиент).
- Комнаты/каналы — `socket.join(roomId)` + отдельный Redis-ключ на комнату.
- Хранить в Firestore больше данных о пользователе (аватар, статус) — просто расширьте
  объект в `upsertUserProfile()`.
