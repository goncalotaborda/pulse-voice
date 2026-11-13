const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: '*' },
  maxHttpBufferSize: 1e7 // Suporta prints até 10 MB
});

const PORT = process.env.PORT || 3000;
const SERVER_PASSWORD = process.env.SERVER_PASSWORD || 'teste123';
const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, 'data');
const DB_FILE = path.join(DATA_DIR, 'concord-db.json');

if (!fs.existsSync(DATA_DIR)) {
  fs.mkdirSync(DATA_DIR, { recursive: true });
}

const CHANNELS = [
  { id: 'global', name: 'Global', icon: 'globe' },
  { id: 'cinema', name: 'Cinema & Streaming', icon: 'tv' },
  { id: 'music', name: 'Música', icon: 'music' }
];

// Base de dados persistente (Contas + Histórico Global do Chat)
let db = {
  users: {},    // usernameKey -> { username, passHash, salt, color, avatar, createdAt }
  messages: []  // últimas 50 mensagens e imagens
};

function loadDatabase() {
  try {
    if (fs.existsSync(DB_FILE)) {
      const raw = fs.readFileSync(DB_FILE, 'utf8');
      const parsed = JSON.parse(raw);
      db.users = parsed.users || {};
      db.messages = Array.isArray(parsed.messages) ? parsed.messages : [];
    }
  } catch (e) {
    console.error('Erro ao ler base de dados:', e);
  }
}

let saveTimeout = null;
function saveDatabase() {
  if (saveTimeout) clearTimeout(saveTimeout);
  saveTimeout = setTimeout(() => {
    try {
      fs.writeFileSync(DB_FILE, JSON.stringify(db, null, 2), 'utf8');
    } catch (e) {
      console.error('Erro ao gravar base de dados:', e);
    }
  }, 250);
}

loadDatabase();

function hashPassword(password, salt) {
  return crypto.pbkdf2Sync(String(password), salt, 10000, 32, 'sha256').toString('hex');
}

app.use(express.static(path.join(__dirname, 'public')));

// Utilizadores ligados no momento: socket.id -> dados de sessão
const activeSessions = new Map();

function buildDirectoryState() {
  const occupants = {};
  CHANNELS.forEach(c => { occupants[c.id] = []; });

  const onlineUserKeys = new Set();
  const onlineList = [];

  for (const session of activeSessions.values()) {
    onlineUserKeys.add(session.userKey);
    onlineList.push({
      id: session.id,
      username: session.name,
      name: session.name,
      color: session.color,
      avatar: session.avatar,
      activity: session.activity,
      channelId: session.channelId
    });

    if (session.channelId && occupants[session.channelId]) {
      occupants[session.channelId].push({
        id: session.id,
        name: session.name,
        color: session.color,
        avatar: session.avatar,
        muted: session.muted,
        deafened: session.deafened,
        sharing: session.sharing,
        streamQuality: session.streamQuality,
        speaking: session.speaking,
        activity: session.activity
      });
    }
  }

  const offlineList = [];
  for (const [key, account] of Object.entries(db.users)) {
    if (!onlineUserKeys.has(key)) {
      offlineList.push({
        username: account.username,
        name: account.username,
        color: account.color || '#6366f1',
        avatar: account.avatar || null
      });
    }
  }

  onlineList.sort((a, b) => a.name.localeCompare(b.name));
  offlineList.sort((a, b) => a.name.localeCompare(b.name));

  return {
    channels: CHANNELS,
    occupants,
    members: {
      online: onlineList,
      offline: offlineList
    }
  };
}

function broadcastDirectory() {
  io.emit('directory-update', buildDirectoryState());
}

io.on('connection', (socket) => {
  socket.on('authenticate', (payload, cb) => {
    const mode = payload?.mode || 'login';
    const rawUsername = String(payload?.username || payload?.name || '').trim().slice(0, 22);
    const password = String(payload?.password || '');
    const serverCode = String(payload?.serverCode || '');
    const color = payload?.color || '#6366f1';
    const avatar = payload?.avatar || null;

    if (!rawUsername || rawUsername.length < 2) {
      return cb?.({ ok: false, error: 'O nome de utilizador deve ter pelo menos 2 caracteres.' });
    }
    if (!password || password.length < 3) {
      return cb?.({ ok: false, error: 'A palavra-passe da conta deve ter pelo menos 3 caracteres.' });
    }

    const userKey = rawUsername.toLowerCase();
    let account = db.users[userKey];

    if (mode === 'register') {
      if (serverCode !== SERVER_PASSWORD) {
        return cb?.({ ok: false, error: 'Código de convite do servidor incorreto.' });
      }
      if (account) {
        return cb?.({ ok: false, error: 'Esse nome de utilizador já existe. Escolhe "Entrar na Conta".' });
      }

      const salt = crypto.randomBytes(16).toString('hex');
      account = {
        username: rawUsername,
        passHash: hashPassword(password, salt),
        salt,
        color,
        avatar,
        createdAt: new Date().toISOString()
      };
      db.users[userKey] = account;
      saveDatabase();
    } else {
      if (!account) {
        return cb?.({ ok: false, needsRegister: true, error: 'Conta não encontrada. Muda para "Criar Conta" para te registares.' });
      }
      const checkHash = hashPassword(password, account.salt);
      if (checkHash !== account.passHash) {
        return cb?.({ ok: false, error: 'Palavra-passe incorreta para esta conta.' });
      }
      if (avatar && avatar !== account.avatar) {
        account.avatar = avatar;
        saveDatabase();
      }
    }

    // Se a mesma conta já estava ligada noutro separador/janela, desliga a sessão antiga
    for (const [oldSocketId, oldSess] of activeSessions.entries()) {
      if (oldSess.userKey === userKey && oldSocketId !== socket.id) {
        const oldSock = io.sockets.sockets.get(oldSocketId);
        oldSock?.disconnect(true);
        activeSessions.delete(oldSocketId);
      }
    }

    const session = {
      id: socket.id,
      userKey,
      name: account.username,
      color: account.color || color,
      avatar: account.avatar || null,
      channelId: null,
      muted: false,
      deafened: false,
      sharing: false,
      streamQuality: null,
      speaking: false,
      activity: null
    };

    activeSessions.set(socket.id, session);

    cb?.({
      ok: true,
      id: socket.id,
      user: {
        username: account.username,
        color: account.color,
        avatar: account.avatar
      },
      history: db.messages,
      state: buildDirectoryState()
    });

    broadcastDirectory();
  });

  socket.on('join-channel', ({ channelId }) => {
    const user = activeSessions.get(socket.id);
    if (!user || !CHANNELS.some(c => c.id === channelId)) return;

    if (user.channelId && user.channelId !== channelId) {
      const oldRoom = user.channelId;
      socket.leave(oldRoom);
      socket.to(oldRoom).emit('peer-left', { peerId: socket.id });
    }

    user.channelId = channelId;
    socket.join(channelId);

    const peersInRoom = [];
    for (const [id, u] of activeSessions.entries()) {
      if (id !== socket.id && u.channelId === channelId) {
        peersInRoom.push(u);
      }
    }

    socket.emit('channel-joined', { channelId, peers: peersInRoom });
    socket.to(channelId).emit('peer-joined', { peer: user });
    broadcastDirectory();
  });

  socket.on('leave-channel', () => {
    const user = activeSessions.get(socket.id);
    if (!user || !user.channelId) return;

    const oldRoom = user.channelId;
    socket.leave(oldRoom);
    user.channelId = null;
    user.sharing = false;
    user.streamQuality = null;
    user.speaking = false;

    socket.to(oldRoom).emit('peer-left', { peerId: socket.id });
    broadcastDirectory();
  });

  socket.on('webrtc-signal', ({ to, data }) => {
    const sender = activeSessions.get(socket.id);
    if (!sender || !to) return;
    io.to(to).emit('webrtc-signal', {
      from: socket.id,
      peer: sender,
      data
    });
  });

  socket.on('update-media-state', (partial) => {
    const user = activeSessions.get(socket.id);
    if (!user) return;

    Object.assign(user, partial);

    // Se atualizou a foto de perfil, grava permanentemente na conta da VPS
    if (partial && Object.prototype.hasOwnProperty.call(partial, 'avatar')) {
      const account = db.users[user.userKey];
      if (account) {
        account.avatar = partial.avatar;
        saveDatabase();
      }
    }

    if (user.channelId) {
      socket.to(user.channelId).emit('peer-media-updated', user);
    }
    broadcastDirectory();
  });

  socket.on('speaking-state', ({ speaking }) => {
    const user = activeSessions.get(socket.id);
    if (!user) return;
    user.speaking = Boolean(speaking);
    if (user.channelId) {
      socket.to(user.channelId).emit('peer-speaking', { peerId: socket.id, speaking: user.speaking });
    }
  });

  // Chat Global Único com histórico guardado na VPS
  socket.on('chat-message', ({ text, image }) => {
    const user = activeSessions.get(socket.id);
    if (!user) return;

    const cleanText = String(text || '').trim().slice(0, 2000);
    const cleanImage = (typeof image === 'string' && image.startsWith('data:image/')) ? image : null;
    if (!cleanText && !cleanImage) return;

    const now = new Date();
    const timestamp = now.toLocaleTimeString('pt-PT', { hour: '2-digit', minute: '2-digit' });

    const msg = {
      id: `${Date.now()}-${Math.random().toString(36).slice(2, 7)}`,
      senderName: user.name,
      senderColor: user.color,
      senderAvatar: user.avatar || null,
      text: cleanText,
      image: cleanImage,
      timestamp
    };

    db.messages.push(msg);
    if (db.messages.length > 50) {
      db.messages = db.messages.slice(-50);
    }
    saveDatabase();

    io.emit('chat-message', msg);
  });

  socket.on('ping-check', (cb) => cb?.());

  socket.on('disconnect', () => {
    const user = activeSessions.get(socket.id);
    if (user) {
      if (user.channelId) {
        socket.to(user.channelId).emit('peer-left', { peerId: socket.id });
      }
      activeSessions.delete(socket.id);
      broadcastDirectory();
    }
  });
});

server.listen(PORT, () => {
  console.log(`Concord Studio Server ativo na porta ${PORT}`);
});