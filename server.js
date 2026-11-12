const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const path = require('path');

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: '*' },
  maxHttpBufferSize: 8e6, // Permite partilhar prints/imagens até 8MB no chat
  pingInterval: 4000,
  pingTimeout: 8000
});

const PORT = process.env.PORT || 3000;
const SERVER_PASSWORD = process.env.SERVER_PASSWORD || '';

app.use(express.static(path.join(__dirname, 'public')));

const CHANNELS = [
  { id: 'lobby', name: 'Lounge Principal', icon: 'sparkles', desc: 'Conversa geral e ponto de encontro' },
  { id: 'gaming', name: 'Gaming & Squad', icon: 'gamepad-2', desc: 'Baixa latência • 60/120 FPS' },
  { id: 'cinema', name: 'Cinema & Watch Party', icon: 'film', desc: 'Partilha de ecrã com áudio Hi-Fi' },
  { id: 'focus', name: 'Co-Working & Chill', icon: 'coffee', desc: 'Ambiente relaxante e estudo' }
];

const users = new Map();

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
  socket.on('authenticate', ({ name, password, color }, callback) => {
    if (SERVER_PASSWORD && password !== SERVER_PASSWORD) {
      return callback({ ok: false, error: 'Palavra-passe incorreta.' });
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
      camera: false,
      speaking: false,
      streamQuality: null
    });

    callback({ ok: true, id: socket.id, state: getDirectoryState() });
    io.emit('directory-update', getDirectoryState());
  });

  socket.on('join-channel', ({ channelId }) => {
    const user = users.get(socket.id);
    if (!user) return;

    if (user.channel) {
      const prevChannel = user.channel;
      socket.leave(prevChannel);
      socket.to(prevChannel).emit('peer-left', { peerId: socket.id, name: user.name });
    }

    user.channel = channelId;
    user.sharing = false;
    user.camera = false;
    user.streamQuality = null;
    socket.join(channelId);

    const existingPeers = [];
    for (const [id, u] of users.entries()) {
      if (u.channel === channelId && id !== socket.id) {
        existingPeers.push(u);
      }
    }

    socket.emit('channel-joined', { channelId, peers: existingPeers });
    socket.to(channelId).emit('peer-joined', { peer: user });
    io.emit('directory-update', getDirectoryState());
  });

  socket.on('leave-channel', () => {
    const user = users.get(socket.id);
    if (!user || !user.channel) return;

    const prevChannel = user.channel;
    socket.leave(prevChannel);
    user.channel = null;
    user.sharing = false;
    user.camera = false;
    user.speaking = false;
    user.streamQuality = null;

    socket.to(prevChannel).emit('peer-left', { peerId: socket.id, name: user.name });
    io.emit('directory-update', getDirectoryState());
  });

  socket.on('webrtc-signal', ({ to, data }) => {
    const sender = users.get(socket.id);
    if (!sender) return;
    io.to(to).emit('webrtc-signal', {
      from: socket.id,
      peer: sender,
      data
    });
  });

  socket.on('update-media-state', (patch) => {
    const user = users.get(socket.id);
    if (!user) return;

    if (typeof patch.muted === 'boolean') user.muted = patch.muted;
    if (typeof patch.deafened === 'boolean') user.deafened = patch.deafened;
    if (typeof patch.sharing === 'boolean') user.sharing = patch.sharing;
    if (typeof patch.camera === 'boolean') user.camera = patch.camera;
    if (patch.streamQuality !== undefined) user.streamQuality = patch.streamQuality;

    if (user.channel) {
      io.to(user.channel).emit('peer-media-updated', user);
    }
    io.emit('directory-update', getDirectoryState());
  });

  // Sincronização instantânea de quem está a falar
  socket.on('speaking-state', ({ speaking }) => {
    const user = users.get(socket.id);
    if (!user || user.speaking === speaking) return;
    user.speaking = Boolean(speaking);
    if (user.channel) {
      socket.to(user.channel).emit('peer-speaking', { peerId: socket.id, speaking: user.speaking });
    }
    io.emit('directory-update', getDirectoryState());
  });

  // Reações flutuantes na sala
  socket.on('send-reaction', ({ emoji }) => {
    const user = users.get(socket.id);
    if (!user || !user.channel) return;
    io.to(user.channel).emit('call-reaction', {
      peerId: socket.id,
      name: user.name,
      emoji
    });
  });

  // Chat com suporte a texto, links e imagens coladas (Ctrl+V)
  socket.on('chat-message', ({ text, image }) => {
    const user = users.get(socket.id);
    if (!user) return;
    if ((!text || !text.trim()) && !image) return;

    const message = {
      id: Date.now().toString(36) + Math.random().toString(36).slice(2, 6),
      senderId: socket.id,
      senderName: user.name,
      senderColor: user.color,
      text: text ? text.trim().slice(0, 1200) : '',
      image: image || null,
      timestamp: new Date().toLocaleTimeString('pt-PT', { hour: '2-digit', minute: '2-digit' })
    };

    io.emit('chat-message', message);
  });

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
  console.log(`Pulse Voice v2.0 ativo na porta ${PORT}`);
});