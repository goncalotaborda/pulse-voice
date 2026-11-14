const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { AccessToken } = require('livekit-server-sdk');

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: '*', methods: ['GET', 'POST', 'DELETE'] },
  maxHttpBufferSize: 15 * 1024 * 1024
});

const PORT = process.env.PORT || 3000;
const SERVER_PASSWORD = process.env.SERVER_PASSWORD || 'teste123';
const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, 'data');
const MUSIC_DIR = process.env.MUSIC_DIR || '/app/music';

const LIVEKIT_API_KEY = process.env.LIVEKIT_API_KEY || 'concord_api_key';
const LIVEKIT_API_SECRET = process.env.LIVEKIT_API_SECRET || 'concord_secret_key_oracle_2026_x9k2m8';
const LIVEKIT_URL = process.env.LIVEKIT_URL || 'ws://100.123.196.21:7880';

const UPLOADS_DIR = path.join(DATA_DIR, 'uploads');
const SOUNDBOARD_DIR = path.join(DATA_DIR, 'soundboard');
const USERS_FILE = path.join(DATA_DIR, 'users.json');
const CHAT_FILE = path.join(DATA_DIR, 'chat.json');
const SOUNDBOARD_FILE = path.join(DATA_DIR, 'soundboard.json');

[DATA_DIR, UPLOADS_DIR, SOUNDBOARD_DIR].forEach((dir) => {
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
});

app.use(express.json({ limit: '20mb' }));
app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Methods', 'GET, POST, DELETE, OPTIONS');
  res.header('Access-Control-Allow-Headers', 'Content-Type, X-Username, X-Password, X-Filename');
  if (req.method === 'OPTIONS') return res.sendStatus(200);
  next();
});

if (fs.existsSync(path.join(__dirname, 'public'))) {
  app.use(express.static(path.join(__dirname, 'public')));
}

// ============================================================================
// PERSISTÊNCIA DE UTILIZADORES, CHAT E SOUNDBOARD
// ============================================================================
function loadJson(filePath, fallback) {
  try {
    if (!fs.existsSync(filePath)) return fallback;
    return JSON.parse(fs.readFileSync(filePath, 'utf8'));
  } catch (e) {
    return fallback;
  }
}

function saveJson(filePath, data) {
  try {
    fs.writeFileSync(filePath, JSON.stringify(data, null, 2), 'utf8');
  } catch (e) {
    console.error('Erro ao gravar JSON:', e.message);
  }
}

function hashPassword(password) {
  return crypto.createHash('sha256').update(String(password)).digest('hex');
}

let usersDb = loadJson(USERS_FILE, {});
let chatHistory = loadJson(CHAT_FILE, []);
let soundboardList = loadJson(SOUNDBOARD_FILE, []);

function verifyUserCredentials(username, password) {
  if (!username || !password) return null;
  const key = username.trim().toLowerCase();
  const user = usersDb[key];
  if (!user) return null;
  if (user.passwordHash !== hashPassword(password)) return null;
  return user;
}

// Limpeza automática de ficheiros do chat com mais de 24 horas
const FILE_TTL_MS = 24 * 60 * 60 * 1000;
function cleanupExpiredChatFiles() {
  const now = Date.now();
  let changed = false;

  chatHistory.forEach((msg) => {
    if (msg.file && !msg.file.expired && msg.file.expiresAt && now > msg.file.expiresAt) {
      msg.file.expired = true;
      changed = true;
      const target = path.join(UPLOADS_DIR, path.basename(msg.file.storedName || ''));
      if (fs.existsSync(target)) {
        try { fs.unlinkSync(target); } catch (e) {}
      }
    }
  });

  if (changed) {
    saveJson(CHAT_FILE, chatHistory);
    io.emit('chat-history-sync', chatHistory);
  }
}
setInterval(cleanupExpiredChatFiles, 15 * 60 * 1000);

// ============================================================================
// ENDPOINTS HTTP (UPLOADS, SOUNDBOARD E LIVEKIT SFU)
// ============================================================================
app.use('/uploads', express.static(UPLOADS_DIR));
app.use('/soundboard-files', express.static(SOUNDBOARD_DIR));

app.post('/upload', (req, res) => {
  const username = decodeURIComponent(req.headers['x-username'] || '');
  const password = decodeURIComponent(req.headers['x-password'] || '');
  const originalName = decodeURIComponent(req.headers['x-filename'] || 'ficheiro.bin');

  const account = verifyUserCredentials(username, password);
  if (!account) {
    return res.status(401).json({ ok: false, error: 'Não autorizado.' });
  }

  const maxBytes = 100 * 1024 * 1024;
  let receivedBytes = 0;
  const safeBase = originalName.replace(/[^a-zA-Z0-9._-]/g, '_');
  const storedName = `${Date.now()}_${safeBase}`;
  const destPath = path.join(UPLOADS_DIR, storedName);
  const writeStream = fs.createWriteStream(destPath);
  let aborted = false;

  req.on('data', (chunk) => {
    if (aborted) return;
    receivedBytes += chunk.length;
    if (receivedBytes > maxBytes) {
      aborted = true;
      writeStream.destroy();
      try { fs.unlinkSync(destPath); } catch (e) {}
      return res.status(413).json({ ok: false, error: 'Limite de 100 MB excedido.' });
    }
    writeStream.write(chunk);
  });

  req.on('end', () => {
    if (aborted) return;
    writeStream.end(() => {
      const now = new Date();
      const timestamp = `${String(now.getHours()).padStart(2, '0')}:${String(now.getMinutes()).padStart(2, '0')}`;
      const msg = {
        id: crypto.randomUUID(),
        senderName: account.username,
        senderColor: account.color || '#6366f1',
        senderAvatar: account.avatar || null,
        text: '',
        timestamp,
        file: {
          name: originalName,
          storedName,
          size: receivedBytes,
          urlPath: `/uploads/${encodeURIComponent(storedName)}`,
          expiresAt: Date.now() + FILE_TTL_MS,
          expired: false
        }
      };
      chatHistory.push(msg);
      if (chatHistory.length > 150) chatHistory = chatHistory.slice(-150);
      saveJson(CHAT_FILE, chatHistory);
      io.emit('chat-message', msg);
      res.json({ ok: true, message: msg });
    });
  });

  req.on('error', () => {
    aborted = true;
    writeStream.destroy();
    try { fs.unlinkSync(destPath); } catch (e) {}
  });
});

app.post('/soundboard', (req, res) => {
  const { username, password, name, emoji, duration, audioBase64 } = req.body || {};
  const account = verifyUserCredentials(username, password);
  if (!account) {
    return res.status(401).json({ ok: false, error: 'Não autorizado.' });
  }
  if (!name || !audioBase64) {
    return res.status(400).json({ ok: false, error: 'Dados incompletos.' });
  }

  try {
    const base64Data = audioBase64.replace(/^data:audio\/\w+;base64,/, '');
    const buffer = Buffer.from(base64Data, 'base64');
    const id = crypto.randomUUID();
    const filename = `${id}.wav`;
    fs.writeFileSync(path.join(SOUNDBOARD_DIR, filename), buffer);

    const entry = {
      id,
      name: String(name).trim().slice(0, 24),
      emoji: String(emoji || '🔊').trim().slice(0, 4),
      duration: Math.min(5.0, Math.max(0.2, Number(duration || 5))),
      filename,
      createdBy: account.username
    };

    soundboardList.push(entry);
    saveJson(SOUNDBOARD_FILE, soundboardList);
    io.emit('soundboard-updated', soundboardList);
    res.json({ ok: true, sound: entry });
  } catch (err) {
    res.status(500).json({ ok: false, error: 'Erro ao guardar áudio.' });
  }
});

app.delete('/soundboard/:id', (req, res) => {
  const id = req.params.id;
  const idx = soundboardList.findIndex((s) => s.id === id);
  if (idx === -1) return res.status(404).json({ ok: false });

  const [removed] = soundboardList.splice(idx, 1);
  saveJson(SOUNDBOARD_FILE, soundboardList);
  const fPath = path.join(SOUNDBOARD_DIR, path.basename(removed.filename || ''));
  if (fs.existsSync(fPath)) {
    try { fs.unlinkSync(fPath); } catch (e) {}
  }
  io.emit('soundboard-updated', soundboardList);
  res.json({ ok: true });
});

app.post('/livekit-token', async (req, res) => {
  const { username, password, channelId } = req.body || {};
  const account = verifyUserCredentials(username, password);
  if (!account) {
    return res.status(401).json({ ok: false, error: 'Sessão inválida.' });
  }
  if (!CHANNELS.some((c) => c.id === channelId)) {
    return res.status(400).json({ ok: false, error: 'Canal inválido.' });
  }

  try {
    const at = new AccessToken(LIVEKIT_API_KEY, LIVEKIT_API_SECRET, {
      identity: account.username,
      name: account.username,
      ttl: '6h'
    });
    at.addGrant({
      roomJoin: true,
      room: `concord-${channelId}`,
      canPublish: true,
      canSubscribe: true,
      canPublishData: true
    });

    const token = await at.toJwt();
    res.json({ ok: true, token, url: LIVEKIT_URL });
  } catch (err) {
    res.status(500).json({ ok: false, error: 'Erro ao gerar token SFU.' });
  }
});

// ============================================================================
// BIBLIOTECA DE MÚSICA PRIVADA NA VPS (/app/music)
// ============================================================================
function scanVpsMusicLibrary() {
  try {
    if (!fs.existsSync(MUSIC_DIR)) return [];
    const files = fs.readdirSync(MUSIC_DIR)
      .filter((f) => /\.(mp3|m4a|wav|ogg|flac)$/i.test(f))
      .sort((a, b) => a.localeCompare(b, 'pt'));

    return files.map((filename) => {
      const baseName = filename.replace(/\.[^/.]+$/, '');
      const parts = baseName.split(' - ');
      const artist = parts.length > 1 ? parts[0].trim() : 'Biblioteca VPS';
      const title = parts.length > 1 ? parts.slice(1).join(' - ').trim() : baseName.trim();
      return {
        id: Buffer.from(filename).toString('base64url'),
        filename,
        title,
        artist,
        platform: 'VPS Oracle'
      };
    });
  } catch (err) {
    return [];
  }
}

const vpsMusicRoomState = {
  playlistTitle: 'Biblioteca Privada VPS',
  queue: [],
  currentTrack: null,
  isPlaying: false,
  shuffle: false,
  currentTime: 0,
  updatedAt: Date.now()
};

function getComputedMusicState() {
  let elapsed = vpsMusicRoomState.currentTime || 0;
  if (vpsMusicRoomState.isPlaying && vpsMusicRoomState.currentTrack) {
    elapsed += (Date.now() - vpsMusicRoomState.updatedAt) / 1000;
  }
  return {
    playlistTitle: vpsMusicRoomState.playlistTitle,
    queue: vpsMusicRoomState.queue,
    currentTrack: vpsMusicRoomState.currentTrack,
    isPlaying: vpsMusicRoomState.isPlaying,
    shuffle: vpsMusicRoomState.shuffle,
    currentTime: Math.max(0, elapsed)
  };
}

app.get('/vps-music/list', (req, res) => {
  const username = decodeURIComponent(req.headers['x-username'] || '');
  const password = decodeURIComponent(req.headers['x-password'] || '');
  if (!verifyUserCredentials(username, password)) {
    return res.status(401).json({ ok: false, error: 'Não autorizado.' });
  }
  const tracks = scanVpsMusicLibrary();
  vpsMusicRoomState.queue = tracks;
  if (!vpsMusicRoomState.currentTrack && tracks.length > 0) {
    vpsMusicRoomState.currentTrack = tracks[0];
  }
  res.json({ ok: true, tracks, state: getComputedMusicState() });
});

app.get('/vps-music/stream/:filename', (req, res) => {
  const username = req.query.u || '';
  const password = req.query.p || '';
  if (!verifyUserCredentials(username, password)) {
    return res.status(401).send('Não autorizado');
  }

  const safeName = path.basename(req.params.filename || '');
  const filePath = path.join(MUSIC_DIR, safeName);
  if (!fs.existsSync(filePath)) {
    return res.status(404).send('Ficheiro não encontrado');
  }

  const stat = fs.statSync(filePath);
  const fileSize = stat.size;
  const ext = path.extname(safeName).toLowerCase();
  const mimeMap = {
    '.mp3': 'audio/mpeg',
    '.m4a': 'audio/mp4',
    '.wav': 'audio/wav',
    '.ogg': 'audio/ogg',
    '.flac': 'audio/flac'
  };
  const contentType = mimeMap[ext] || 'audio/mpeg';
  const range = req.headers.range;

  if (range) {
    const parts = range.replace(/bytes=/, '').split('-');
    const start = parseInt(parts[0], 10);
    const end = parts[1] ? parseInt(parts[1], 10) : fileSize - 1;
    const chunkSize = (end - start) + 1;
    const fileStream = fs.createReadStream(filePath, { start, end });

    res.writeHead(206, {
      'Content-Range': `bytes ${start}-${end}/${fileSize}`,
      'Accept-Ranges': 'bytes',
      'Content-Length': chunkSize,
      'Content-Type': contentType
    });
    fileStream.pipe(res);
  } else {
    res.writeHead(200, {
      'Content-Length': fileSize,
      'Content-Type': contentType,
      'Accept-Ranges': 'bytes'
    });
    fs.createReadStream(filePath).pipe(res);
  }
});

// ============================================================================
// SOCKET.IO: SALAS DE VOZ, PRESENÇA, CHAT E MÚSICA SINCRONIZADA
// ============================================================================
const CHANNELS = [
  { id: 'lobby', name: 'Global', icon: 'globe' },
  { id: 'cinema', name: 'Cinema & Streaming', icon: 'tv' },
  { id: 'music', name: 'Música', icon: 'music' }
];

const activeSessions = new Map();

function buildDirectoryState() {
  const occupants = {};
  CHANNELS.forEach((c) => { occupants[c.id] = []; });

  const onlineMap = new Map();
  activeSessions.forEach((sess) => {
    const userObj = {
      id: sess.id,
      name: sess.name,
      color: sess.color,
      avatar: sess.avatar,
      muted: sess.muted,
      deafened: sess.deafened,
      sharing: sess.sharing,
      streamQuality: sess.streamQuality,
      speaking: sess.speaking,
      activity: sess.activity
    };
    onlineMap.set(sess.name.toLowerCase(), userObj);
    if (sess.channelId && occupants[sess.channelId]) {
      occupants[sess.channelId].push(userObj);
    }
  });

  const online = Array.from(onlineMap.values());
  const offline = [];
  Object.values(usersDb).forEach((u) => {
    if (!onlineMap.has(u.username.toLowerCase())) {
      offline.push({
        name: u.username,
        color: u.color || '#52525b',
        avatar: u.avatar || null
      });
    }
  });

  return {
    channels: CHANNELS,
    occupants,
    members: { online, offline }
  };
}

function broadcastDirectory() {
  io.emit('directory-update', buildDirectoryState());
}

io.on('connection', (socket) => {
  socket.on('authenticate', ({ mode, username, password, serverCode, color, avatar }, ack) => {
    const cleanName = String(username || '').trim().slice(0, 22);
    const key = cleanName.toLowerCase();

    if (!cleanName || !password) {
      return ack?.({ ok: false, error: 'Preenche o nome de utilizador e a palavra-passe.' });
    }

    if (mode === 'register') {
      if (serverCode !== SERVER_PASSWORD) {
        return ack?.({ ok: false, error: 'Código de convite do servidor incorreto.' });
      }
      if (usersDb[key]) {
        return ack?.({ ok: false, error: 'Esse nome de utilizador já existe. Escolhe "Entrar na Conta".' });
      }
      usersDb[key] = {
        username: cleanName,
        passwordHash: hashPassword(password),
        color: color || '#6366f1',
        avatar: avatar || null,
        createdAt: Date.now()
      };
      saveJson(USERS_FILE, usersDb);
    } else {
      const existingUser = usersDb[key];
      if (!existingUser) {
        return ack?.({ ok: false, needsRegister: true, error: 'Conta não encontrada. Muda para "Criar Conta Nova".' });
      }
      if (existingUser.passwordHash !== hashPassword(password)) {
        return ack?.({ ok: false, error: 'Palavra-passe incorreta.' });
      }
      if (avatar) {
        existingUser.avatar = avatar;
        saveJson(USERS_FILE, usersDb);
      }
    }

    const account = usersDb[key];
    activeSessions.set(socket.id, {
      id: socket.id,
      name: account.username,
      color: account.color || color || '#6366f1',
      avatar: avatar || account.avatar || null,
      channelId: null,
      muted: false,
      deafened: false,
      sharing: false,
      streamQuality: null,
      speaking: false,
      activity: null
    });

    ack?.({
      ok: true,
      id: socket.id,
      user: { username: account.username, avatar: account.avatar, color: account.color },
      state: buildDirectoryState(),
      history: chatHistory,
      soundboard: soundboardList
    });
    broadcastDirectory();
  });

  socket.on('join-channel', ({ channelId }) => {
    const sess = activeSessions.get(socket.id);
    if (!sess || !CHANNELS.some((c) => c.id === channelId)) return;

    if (sess.channelId && sess.channelId !== channelId) {
      socket.to(sess.channelId).emit('peer-left', { peerId: socket.id });
      socket.leave(sess.channelId);
    }

    sess.channelId = channelId;
    sess.sharing = false;
    sess.streamQuality = null;
    socket.join(channelId);

    const existingPeers = [];
    activeSessions.forEach((other) => {
      if (other.id !== socket.id && other.channelId === channelId) {
        existingPeers.push(other);
      }
    });

    if (channelId === 'music' && !vpsMusicRoomState.queue.length) {
      vpsMusicRoomState.queue = scanVpsMusicLibrary();
      if (!vpsMusicRoomState.currentTrack && vpsMusicRoomState.queue.length) {
        vpsMusicRoomState.currentTrack = vpsMusicRoomState.queue[0];
      }
    }

    socket.emit('channel-joined', {
      channelId,
      peers: existingPeers,
      musicState: channelId === 'music' ? getComputedMusicState() : null
    });

    socket.to(channelId).emit('peer-joined', { peer: sess });
    broadcastDirectory();
  });

  socket.on('leave-channel', () => {
    const sess = activeSessions.get(socket.id);
    if (!sess || !sess.channelId) return;
    const oldCh = sess.channelId;
    sess.channelId = null;
    sess.sharing = false;
    sess.streamQuality = null;
    sess.speaking = false;
    socket.leave(oldCh);
    socket.to(oldCh).emit('peer-left', { peerId: socket.id });
    broadcastDirectory();
  });

  socket.on('update-media-state', (patch = {}) => {
    const sess = activeSessions.get(socket.id);
    if (!sess) return;
    Object.assign(sess, patch);

    if (patch.avatar) {
      const key = sess.name.toLowerCase();
      if (usersDb[key]) {
        usersDb[key].avatar = patch.avatar;
        saveJson(USERS_FILE, usersDb);
      }
    }

    if (sess.channelId) {
      socket.to(sess.channelId).emit('peer-media-updated', sess);
    }
    broadcastDirectory();
  });

  socket.on('speaking-state', ({ speaking }) => {
    const sess = activeSessions.get(socket.id);
    if (!sess || !sess.channelId) return;
    sess.speaking = Boolean(speaking);
    socket.to(sess.channelId).emit('peer-speaking', { peerId: socket.id, speaking: sess.speaking });
  });

  socket.on('webrtc-signal', ({ to, data }) => {
    if (!to || !data) return;
    io.to(to).emit('webrtc-signal', { from: socket.id, data });
  });

  socket.on('play-soundboard', ({ soundId }) => {
    const sess = activeSessions.get(socket.id);
    if (!sess || !sess.channelId) return;
    const snd = soundboardList.find((s) => s.id === soundId);
    if (!snd) return;
    io.to(sess.channelId).emit('soundboard-played', {
      senderId: socket.id,
      senderName: sess.name,
      emoji: snd.emoji,
      name: snd.name,
      filename: snd.filename
    });
  });

  socket.on('music-action', (payload = {}, ack) => {
    if (!vpsMusicRoomState.queue.length) {
      vpsMusicRoomState.queue = scanVpsMusicLibrary();
    }
    const queue = vpsMusicRoomState.queue;
    const { action } = payload;

    if (action === 'sync-request') {
      if (typeof ack === 'function') ack({ ok: true, state: getComputedMusicState() });
      return;
    }

    if (action === 'refresh-library') {
      vpsMusicRoomState.queue = scanVpsMusicLibrary();
      io.to('music').emit('music-state-update', getComputedMusicState());
      if (typeof ack === 'function') ack({ ok: true });
      return;
    }

    if (action === 'play-track') {
      const found = queue.find((t) => t.id === payload.id);
      if (found) {
        vpsMusicRoomState.currentTrack = found;
        vpsMusicRoomState.isPlaying = true;
        vpsMusicRoomState.currentTime = 0;
        vpsMusicRoomState.updatedAt = Date.now();
      }
    } else if (action === 'toggle-play') {
      if (!vpsMusicRoomState.currentTrack && queue.length) {
        vpsMusicRoomState.currentTrack = queue[0];
      }
      const computed = getComputedMusicState();
      vpsMusicRoomState.currentTime = computed.currentTime;
      vpsMusicRoomState.isPlaying = !vpsMusicRoomState.isPlaying;
      vpsMusicRoomState.updatedAt = Date.now();
    } else if (action === 'toggle-shuffle') {
      vpsMusicRoomState.shuffle = !vpsMusicRoomState.shuffle;
    } else if (action === 'seek') {
      vpsMusicRoomState.currentTime = Math.max(0, Number(payload.time || 0));
      vpsMusicRoomState.updatedAt = Date.now();
    } else if (action === 'next') {
      if (queue.length > 0) {
        if (vpsMusicRoomState.shuffle && queue.length > 1) {
          const candidates = queue.filter((t) => t.id !== vpsMusicRoomState.currentTrack?.id);
          vpsMusicRoomState.currentTrack = candidates[Math.floor(Math.random() * candidates.length)];
        } else {
          const idx = queue.findIndex((t) => t.id === vpsMusicRoomState.currentTrack?.id);
          const nextIdx = (idx + 1) % queue.length;
          vpsMusicRoomState.currentTrack = queue[nextIdx];
        }
        vpsMusicRoomState.isPlaying = true;
        vpsMusicRoomState.currentTime = 0;
        vpsMusicRoomState.updatedAt = Date.now();
      }
    }

    io.to('music').emit('music-state-update', getComputedMusicState());
    if (typeof ack === 'function') ack({ ok: true });
  });

  socket.on('chat-message', ({ text, image }) => {
    const sess = activeSessions.get(socket.id);
    if (!sess) return;
    const cleanText = String(text || '').trim().slice(0, 2000);
    if (!cleanText && !image) return;

    const now = new Date();
    const timestamp = `${String(now.getHours()).padStart(2, '0')}:${String(now.getMinutes()).padStart(2, '0')}`;
    const msg = {
      id: crypto.randomUUID(),
      senderName: sess.name,
      senderColor: sess.color,
      senderAvatar: sess.avatar || null,
      text: cleanText,
      image: image || null,
      timestamp
    };

    chatHistory.push(msg);
    if (chatHistory.length > 150) chatHistory = chatHistory.slice(-150);
    saveJson(CHAT_FILE, chatHistory);
    io.emit('chat-message', msg);
  });

  socket.on('ping-check', (cb) => {
    if (typeof cb === 'function') cb();
  });

  socket.on('disconnect', () => {
    const sess = activeSessions.get(socket.id);
    if (sess) {
      if (sess.channelId) {
        socket.to(sess.channelId).emit('peer-left', { peerId: socket.id });
      }
      activeSessions.delete(socket.id);
      broadcastDirectory();
    }
  });
});

server.listen(PORT, '0.0.0.0', () => {
  console.log(`Concord Studio Server ativo na porta ${PORT}`);
});