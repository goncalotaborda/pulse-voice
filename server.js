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
  maxHttpBufferSize: 1e7
});

const PORT = process.env.PORT || 3000;
const SERVER_PASSWORD = process.env.SERVER_PASSWORD || 'teste123';
const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, 'data');
const UPLOADS_DIR = path.join(DATA_DIR, 'uploads');
const SOUNDBOARD_DIR = path.join(DATA_DIR, 'soundboard');
const DB_FILE = path.join(DATA_DIR, 'concord-db.json');

[DATA_DIR, UPLOADS_DIR, SOUNDBOARD_DIR].forEach(dir => {
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
});

const CHANNELS = [
  { id: 'global', name: 'Global', icon: 'globe' },
  { id: 'cinema', name: 'Cinema & Streaming', icon: 'tv' },
  { id: 'music', name: 'Música', icon: 'music' }
];

// CORS global para uploads HTTP e ficheiros
app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Methods', 'GET, POST, DELETE, OPTIONS');
  res.header('Access-Control-Allow-Headers', 'Content-Type, X-Username, X-Password, X-Filename, X-Filesize');
  if (req.method === 'OPTIONS') return res.sendStatus(200);
  next();
});

app.use(express.json({ limit: '6mb' }));
app.use(express.static(path.join(__dirname, 'public')));
app.use('/soundboard-files', express.static(SOUNDBOARD_DIR));

// Base de dados persistente
let db = {
  users: {},      // userKey -> { username, passHash, salt, color, avatar, createdAt }
  messages: [],   // últimas 60 mensagens/ficheiros do Chat Global
  soundboard: [], // lista de sons { id, name, emoji, filename, duration, createdBy }
  files: []       // registo de ficheiros temporários (24h)
};

function loadDatabase() {
  try {
    if (fs.existsSync(DB_FILE)) {
      const raw = fs.readFileSync(DB_FILE, 'utf8');
      const parsed = JSON.parse(raw);
      db.users = parsed.users || {};
      db.messages = Array.isArray(parsed.messages) ? parsed.messages : [];
      db.soundboard = Array.isArray(parsed.soundboard) ? parsed.soundboard : [];
      db.files = Array.isArray(parsed.files) ? parsed.files : [];
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

function verifyUserCredentials(username, password) {
  const key = String(username || '').trim().toLowerCase();
  const account = db.users[key];
  if (!account) return null;
  const checkHash = hashPassword(password, account.salt);
  return checkHash === account.passHash ? account : null;
}

// Limpeza automática de ficheiros com mais de 24 horas
function cleanupExpiredFiles() {
  const now = Date.now();
  let changed = false;

  db.files = db.files.filter(f => {
    if (now >= f.expiresAt) {
      const fullPath = path.join(UPLOADS_DIR, f.storedName);
      try {
        if (fs.existsSync(fullPath)) fs.unlinkSync(fullPath);
      } catch (e) {}
      db.messages.forEach(m => {
        if (m.file && m.file.id === f.id) {
          m.file.expired = true;
        }
      });
      changed = true;
      return false;
    }
    return true;
  });

  if (changed) {
    saveDatabase();
    io.emit('chat-history-sync', db.messages);
  }
}

setInterval(cleanupExpiredFiles, 10 * 60 * 1000);
cleanupExpiredFiles();

// ============================================================================
// ENDPOINTS HTTP: UPLOAD DE FICHEIROS (ATÉ 100 MB / 24H) E SOUNDBOARD
// ============================================================================
const MAX_UPLOAD_BYTES = 100 * 1024 * 1024; // 100 MB
const FILE_TTL_MS = 24 * 60 * 60 * 1000;    // 24 Horas

app.post('/upload', (req, res) => {
  const username = decodeURIComponent(req.headers['x-username'] || '');
  const password = decodeURIComponent(req.headers['x-password'] || '');
  const account = verifyUserCredentials(username, password);
  if (!account) {
    return res.status(401).json({ ok: false, error: 'Sessão inválida.' });
  }

  const rawName = decodeURIComponent(req.headers['x-filename'] || 'ficheiro');
  const safeOriginalName = path.basename(rawName).replace(/[^a-zA-Z0-9._\- ()\[\]]/g, '_').slice(0, 120) || 'ficheiro';
  const fileId = `${Date.now()}-${crypto.randomBytes(4).toString('hex')}`;
  const storedName = `${fileId}-${safeOriginalName}`;
  const destPath = path.join(UPLOADS_DIR, storedName);

  let receivedBytes = 0;
  let aborted = false;
  const writeStream = fs.createWriteStream(destPath);

  req.on('data', (chunk) => {
    if (aborted) return;
    receivedBytes += chunk.length;
    if (receivedBytes > MAX_UPLOAD_BYTES) {
      aborted = true;
      writeStream.destroy();
      try { if (fs.existsSync(destPath)) fs.unlinkSync(destPath); } catch (e) {}
      return res.status(413).json({ ok: false, error: 'O ficheiro excede o limite de 100 MB.' });
    }
  });

  req.pipe(writeStream);

  writeStream.on('finish', () => {
    if (aborted) return;
    const expiresAt = Date.now() + FILE_TTL_MS;
    const fileRecord = {
      id: fileId,
      originalName: safeOriginalName,
      storedName,
      size: receivedBytes,
      expiresAt,
      uploadedBy: account.username
    };
    db.files.push(fileRecord);

    const timestamp = new Date().toLocaleTimeString('pt-PT', { hour: '2-digit', minute: '2-digit' });
    const msg = {
      id: `${Date.now()}-${Math.random().toString(36).slice(2, 7)}`,
      senderName: account.username,
      senderColor: account.color,
      senderAvatar: account.avatar || null,
      text: '',
      image: null,
      file: {
        id: fileId,
        name: safeOriginalName,
        size: receivedBytes,
        urlPath: `/files/${fileId}/${encodeURIComponent(safeOriginalName)}`,
        expiresAt,
        expired: false
      },
      timestamp
    };

    db.messages.push(msg);
    if (db.messages.length > 60) db.messages = db.messages.slice(-60);
    saveDatabase();

    io.emit('chat-message', msg);
    res.json({ ok: true, file: msg.file });
  });

  writeStream.on('error', () => {
    if (!aborted) res.status(500).json({ ok: false, error: 'Erro ao guardar ficheiro na VPS.' });
  });
});

app.get('/files/:fileId/:filename', (req, res) => {
  const record = db.files.find(f => f.id === req.params.fileId);
  if (!record || Date.now() >= record.expiresAt) {
    return res.status(410).send('Este ficheiro expirou após 24 horas.');
  }
  const fullPath = path.join(UPLOADS_DIR, record.storedName);
  if (!fs.existsSync(fullPath)) {
    return res.status(404).send('Ficheiro não encontrado.');
  }
  res.download(fullPath, record.originalName);
});

// Adicionar som ao Soundboard (máx. 5s)
app.post('/soundboard', (req, res) => {
  const { username, password, name, emoji, audioBase64, duration } = req.body || {};
  const account = verifyUserCredentials(username, password);
  if (!account) {
    return res.status(401).json({ ok: false, error: 'Sessão inválida.' });
  }

  const cleanName = String(name || '').trim().slice(0, 24);
  const cleanEmoji = String(emoji || '🔊').trim().slice(0, 8) || '🔊';
  if (!cleanName || typeof audioBase64 !== 'string' || !audioBase64.startsWith('data:audio/')) {
    return res.status(400).json({ ok: false, error: 'Dados do áudio inválidos.' });
  }

  const base64Data = audioBase64.split(',')[1];
  const buffer = Buffer.from(base64Data, 'base64');
  if (buffer.length > 2.5 * 1024 * 1024) {
    return res.status(413).json({ ok: false, error: 'O áudio recortado é demasiado grande.' });
  }

  const soundId = `snd-${Date.now()}-${crypto.randomBytes(3).toString('hex')}`;
  const filename = `${soundId}.wav`;
  fs.writeFileSync(path.join(SOUNDBOARD_DIR, filename), buffer);

  const entry = {
    id: soundId,
    name: cleanName,
    emoji: cleanEmoji,
    filename,
    duration: Math.min(5, Number(duration) || 5),
    createdBy: account.username
  };

  db.soundboard.push(entry);
  saveDatabase();

  io.emit('soundboard-updated', db.soundboard);
  res.json({ ok: true, sound: entry });
});

app.delete('/soundboard/:id', (req, res) => {
  const idx = db.soundboard.findIndex(s => s.id === req.params.id);
  if (idx === -1) return res.status(404).json({ ok: false });

  const [removed] = db.soundboard.splice(idx, 1);
  try {
    const p = path.join(SOUNDBOARD_DIR, removed.filename);
    if (fs.existsSync(p)) fs.unlinkSync(p);
  } catch (e) {}

  saveDatabase();
  io.emit('soundboard-updated', db.soundboard);
  res.json({ ok: true });
});

// ============================================================================
// SOCKET.IO: CHAMADAS, SOUNDBOARD NA CALL, MÚSICA E CHAT GLOBAL
// ============================================================================
const activeSessions = new Map();
let musicRoomState = {
  currentUrl: '',
  currentTitle: '',
  queue: [] // [{ id, url, title, platform, addedBy }]
};

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
        return cb?.({ ok: false, needsRegister: true, error: 'Conta não encontrada. Muda para "Criar Conta Nova".' });
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
      soundboard: db.soundboard,
      musicState: musicRoomState,
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

    socket.emit('channel-joined', {
      channelId,
      peers: peersInRoom,
      musicState: channelId === 'music' ? musicRoomState : null
    });
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

  // Tocar som do Soundboard APENAS na call onde o utilizador está no momento
  socket.on('play-soundboard', ({ soundId }) => {
    const user = activeSessions.get(socket.id);
    if (!user || !user.channelId) return;
    const sound = db.soundboard.find(s => s.id === soundId);
    if (!sound) return;

    io.to(user.channelId).emit('soundboard-played', {
      soundId: sound.id,
      name: sound.name,
      emoji: sound.emoji,
      filename: sound.filename,
      senderId: socket.id,
      senderName: user.name
    });
  });

  // Sincronização do Canal Música (Jukebox Colaborativa)
  socket.on('music-action', (payload) => {
    const user = activeSessions.get(socket.id);
    if (!user || user.channelId !== 'music') return;

    if (payload.action === 'play-now') {
      musicRoomState.currentUrl = payload.url;
      musicRoomState.currentTitle = payload.title || payload.url;
      io.to('music').emit('music-state-update', musicRoomState);
    } else if (payload.action === 'queue-add') {
      const item = {
        id: `mq-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`,
        url: payload.url,
        title: payload.title || payload.url,
        platform: payload.platform || 'Música',
        addedBy: user.name
      };
      musicRoomState.queue.push(item);
      if (!musicRoomState.currentUrl) {
        musicRoomState.currentUrl = item.url;
        musicRoomState.currentTitle = item.title;
      }
      io.to('music').emit('music-state-update', musicRoomState);
    } else if (payload.action === 'queue-remove') {
      musicRoomState.queue = musicRoomState.queue.filter(q => q.id !== payload.id);
      io.to('music').emit('music-state-update', musicRoomState);
    } else if (payload.action === 'next') {
      if (musicRoomState.queue.length > 0) {
        const idx = payload.shuffle
          ? Math.floor(Math.random() * musicRoomState.queue.length)
          : 0;
        const [chosen] = musicRoomState.queue.splice(idx, 1);
        musicRoomState.currentUrl = chosen.url;
        musicRoomState.currentTitle = chosen.title;
      }
      io.to('music').emit('music-state-update', musicRoomState);
    } else if (payload.action === 'stop') {
      musicRoomState.currentUrl = '';
      musicRoomState.currentTitle = '';
      io.to('music').emit('music-state-update', musicRoomState);
    }
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

  socket.on('chat-message', ({ text, image }) => {
    const user = activeSessions.get(socket.id);
    if (!user) return;

    const cleanText = String(text || '').trim().slice(0, 2000);
    const cleanImage = (typeof image === 'string' && image.startsWith('data:image/')) ? image : null;
    if (!cleanText && !cleanImage) return;

    const timestamp = new Date().toLocaleTimeString('pt-PT', { hour: '2-digit', minute: '2-digit' });

    const msg = {
      id: `${Date.now()}-${Math.random().toString(36).slice(2, 7)}`,
      senderName: user.name,
      senderColor: user.color,
      senderAvatar: user.avatar || null,
      text: cleanText,
      image: cleanImage,
      file: null,
      timestamp
    };

    db.messages.push(msg);
    if (db.messages.length > 60) {
      db.messages = db.messages.slice(-60);
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