# Stranger/ — Random Chat App

Real-time anonymous chat. Two strangers are matched randomly. No login, no history.

## Stack
- **Backend**: Node.js + Express + Socket.io
- **Frontend**: Vanilla HTML/CSS/JS (served statically)

## Run locally

```bash
npm install
npm start
# → http://localhost:3000
```

Open two browser tabs to test matching between "two people".

For live reload during development:
```bash
npm run dev   # uses nodemon
```

## How it works

1. User clicks "Start Chatting" → emits `find_match`
2. Server checks a waiting queue → pairs two sockets together
3. Both get a `matched` event → chat screen opens
4. Messages relay through the server: `socket → server → partner socket`
5. `next` → old pair is broken, user re-enters the queue
6. Disconnect → partner gets `partner_left` event

## Deploy to Render (free tier)

1. Push this folder to a GitHub repo
2. Go to https://render.com → New → Web Service
3. Connect your repo, set:
   - **Build command**: `npm install`
   - **Start command**: `node server/index.js`
   - **Environment**: Node
4. Deploy — you get a public URL instantly

## Deploy to Railway

```bash
npm install -g @railway/cli
railway login
railway init
railway up
```

## Environment variables

| Variable | Default | Description |
|----------|---------|-------------|
| `PORT`   | `3000`  | Port to listen on |

## File structure

```
stranger-chat/
├── server/
│   └── index.js       ← Socket.io server + matchmaking logic
├── public/
│   └── index.html     ← Full frontend (HTML + CSS + JS)
├── package.json
└── README.md
```

## Extending it

- **Interest tags**: Let users pick topics → match by shared interests
- **Country filter**: Pass user locale on connect, filter queue
- **Report/block**: Emit a `report` event, server can disconnect offenders
- **Message cap**: Server already caps messages at 500 chars
- **Rate limiting**: Add `socket.io-rate-limiter` to prevent spam
