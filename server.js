const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const path = require('path');

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: '*' },
  pingInterval: 5000,
  pingTimeout: 10000
});

const PORT = process.env.PORT || 3000;
const SERVER_PASSWORD = process.env.SERVER_PASSWORD || '';

app.use(express.static(path.join(__dirname, 'public')));

// Canais predefinidos do servidor
const CHANNELS = [
  { id: 'lobby', name: 'Lobby Principal', icon: 'volume-2' },
  { id: 'gaming', name: 'Gaming & Tryhard', icon: 'gamepad-2' },
  { id: 'cinema', name: 'Cinema & Watch Party', icon: 'tv' },
  { id: 'chill', name: 'Chill & Conversa', icon: 'coffee' }
];

// Estado em memória dos utilizadores ligados
const users = new Map(); // socket.id -> { id, name, channel, muted, deafened, sharing, screenStreamId, color }

function getDirectoryState() {
  const channelState = {};
  CHANNELS.forEach(c => { channelState[c.id] = []; });
  for (const [, user] of users.entries()) {
    if (user.channel && channelState[user.channel]) {
      channelState[user.channel].push(user);
    }
  }
  return { channels: CHANNELS, occupants: channelState };
}

io.on('connection', (socket) => {
  // Autenticação inicial e registo do perfil
  socket.on('authenticate', ({ name, password, color }, callback) => {
    if (SERVER_PASSWORD && password !== SERVER_PASSWORD) {
      return callback({ ok: false, error: 'Palavra-passe do servidor incorreta.' });
    }

    const cleanName = (name || 'Utilizador').trim().slice(0, 24);
    users.set(socket.id, {
      id: socket.id,
      name: cleanName,
      color: color || '#6366f1',
      channel: null,
      muted: false,
      deafened: false,
      sharing: false,
      screenStreamId: null
    });

    callback({ ok: true, id: socket.id, state: getDirectoryState() });
    io.emit('directory-update', getDirectoryState());
  });

  // Entrar num canal de voz
  socket.on('join-channel', ({ channelId }) => {
    const user = users.get(socket.id);
    if (!user) return;

    // Sair do canal anterior se estiver num
    if (user.channel) {
      const prevChannel = user.channel;
      socket.leave(prevChannel);
      socket.to(prevChannel).emit('peer-left', { peerId: socket.id, name: user.name });
    }

    user.channel = channelId;
    user.sharing = false;
    user.screenStreamId = null;
    socket.join(channelId);

    // Obter lista de quem já está na sala
    const existingPeers = [];
    for (const [id, u] of users.entries()) {
      if (u.channel === channelId && id !== socket.id) {
        existingPeers.push(u);
      }
    }

    // Enviar os membros atuais a quem acabou de entrar
    socket.emit('channel-joined', { channelId, peers: existingPeers });

    // Avisar quem já estava na sala para iniciar ligação WebRTC
    socket.to(channelId).emit('peer-joined', { peer: user });
    io.emit('directory-update', getDirectoryState());
  });

  // Sair da chamada de voz
  socket.on('leave-channel', () => {
    const user = users.get(socket.id);
    if (!user || !user.channel) return;

    const prevChannel = user.channel;
    socket.leave(prevChannel);
    user.channel = null;
    user.sharing = false;
    user.screenStreamId = null;

    socket.to(prevChannel).emit('peer-left', { peerId: socket.id, name: user.name });
    io.emit('directory-update', getDirectoryState());
  });

  // Sinalização WebRTC (Offer, Answer, ICE Candidates)
  socket.on('webrtc-signal', ({ to, data }) => {
    const sender = users.get(socket.id);
    if (!sender) return;
    io.to(to).emit('webrtc-signal', {
      from: socket.id,
      peer: sender,
      data
    });
  });

  // Atualizar estado (Mute, Deafen, Partilha de Ecrã)
  socket.on('update-media-state', ({ muted, deafened, sharing, screenStreamId }) => {
    const user = users.get(socket.id);
    if (!user) return;

    if (typeof muted === 'boolean') user.muted = muted;
    if (typeof deafened === 'boolean') user.deafened = deafened;
    if (typeof sharing === 'boolean') user.sharing = sharing;
    if (screenStreamId !== undefined) user.screenStreamId = screenStreamId;

    if (user.channel) {
      io.to(user.channel).emit('peer-media-updated', user);
    }
    io.emit('directory-update', getDirectoryState());
  });

  // Chat de texto da sala
  socket.on('chat-message', ({ text }) => {
    const user = users.get(socket.id);
    if (!user || !text || !text.trim()) return;

    const message = {
      id: Date.now().toString(36) + Math.random().toString(36).slice(2, 6),
      senderId: socket.id,
      senderName: user.name,
      senderColor: user.color,
      text: text.trim().slice(0, 1000),
      timestamp: new Date().toLocaleTimeString('pt-PT', { hour: '2-digit', minute: '2-digit' })
    };

    io.emit('chat-message', message);
  });

  // Medidor de Ping
  socket.on('ping-check', (cb) => {
    if (typeof cb === 'function') cb();
  });

  socket.on('disconnect', () => {
    const user = users.get(socket.id);
    if (user) {
      if (user.channel) {
        socket.to(user.channel).emit('peer-left', { peerId: socket.id, name: user.name });
      }
      users.delete(socket.id);
      io.emit('directory-update', getDirectoryState());
    }
  });
});

server.listen(PORT, () => {
  console.log(`Pulse Voice Server ativo na porta ${PORT}`);
});