const express = require('express');
const http = require('http');
const { Server } = require('socket.io');

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: '*' },
  maxHttpBufferSize: 10e6,
  pingInterval: 4000,
  pingTimeout: 8000
});

const PORT = process.env.PORT || 3000;
const SERVER_PASSWORD = process.env.SERVER_PASSWORD || '';

app.get('/', (_, res) => {
  res.send('Pulse Studio Desktop Server Online.');
});

const CHANNELS = [
  { id: 'lobby', name: 'Lounge Principal', icon: 'sparkles' },
  { id: 'gaming', name: 'Gaming & Squad', icon: 'gamepad-2' },
  { id: 'cinema', name: 'Cinema & Streams', icon: 'tv' },
  { id: 'chill', name: 'Chill & Estudo', icon: 'coffee' }
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
      return callback({ ok: false, error: 'Palavra-passe do servidor incorreta.' });
    }

    users.set(socket.id, {
      id: socket.id,
      name: (name || 'Utilizador').trim().slice(0, 22),
      color: color || '#6366f1',
      channel: null,
      muted: false,
      deafened: false,
      sharing: false,
      speaking: false,
      streamQuality: null,
      activity: null
    });

    callback({ ok: true, id: socket.id, state: getDirectoryState() });
    io.emit('directory-update', getDirectoryState());
  });

  socket.on('join-channel', ({ channelId }) => {
    const user = users.get(socket.id);
    if (!user) return;

    if (user.channel) {
      const prev = user.channel;
      socket.leave(prev);
      socket.to(prev).emit('peer-left', { peerId: socket.id });
    }

    user.channel = channelId;
    user.sharing = false;
    user.streamQuality = null;
    socket.join(channelId);

    const existingPeers = [];
    for (const [id, u] of users.entries()) {
      if (u.channel === channelId && id !== socket.id) existingPeers.push(u);
    }

    socket.emit('channel-joined', { channelId, peers: existingPeers });
    socket.to(channelId).emit('peer-joined', { peer: user });
    io.emit('directory-update', getDirectoryState());
  });

  socket.on('leave-channel', () => {
    const user = users.get(socket.id);
    if (!user || !user.channel) return;
    const prev = user.channel;
    socket.leave(prev);
    user.channel = null;
    user.sharing = false;
    user.speaking = false;
    user.streamQuality = null;
    socket.to(prev).emit('peer-left', { peerId: socket.id });
    io.emit('directory-update', getDirectoryState());
  });

  socket.on('webrtc-signal', ({ to, data }) => {
    const sender = users.get(socket.id);
    if (!sender) return;
    io.to(to).emit('webrtc-signal', { from: socket.id, peer: sender, data });
  });

  socket.on('update-media-state', (patch) => {
    const user = users.get(socket.id);
    if (!user) return;
    if (typeof patch.muted === 'boolean') user.muted = patch.muted;
    if (typeof patch.deafened === 'boolean') user.deafened = patch.deafened;
    if (typeof patch.sharing === 'boolean') user.sharing = patch.sharing;
    if (patch.streamQuality !== undefined) user.streamQuality = patch.streamQuality;
    if (patch.activity !== undefined) user.activity = patch.activity;

    if (user.channel) io.to(user.channel).emit('peer-media-updated', user);
    io.emit('directory-update', getDirectoryState());
  });

  socket.on('speaking-state', ({ speaking }) => {
    const user = users.get(socket.id);
    if (!user || user.speaking === speaking) return;
    user.speaking = Boolean(speaking);
    if (user.channel) {
      socket.to(user.channel).emit('peer-speaking', { peerId: socket.id, speaking: user.speaking });
    }
    io.emit('directory-update', getDirectoryState());
  });

  socket.on('chat-message', ({ text, image }) => {
    const user = users.get(socket.id);
    if (!user || ((!text || !text.trim()) && !image)) return;
    io.emit('chat-message', {
      id: Date.now().toString(36),
      senderName: user.name,
      senderColor: user.color,
      text: text ? text.trim().slice(0, 1200) : '',
      image: image || null,
      timestamp: new Date().toLocaleTimeString('pt-PT', { hour: '2-digit', minute: '2-digit' })
    });
  });

  socket.on('ping-check', (cb) => { if (typeof cb === 'function') cb(); });

  socket.on('disconnect', () => {
    const user = users.get(socket.id);
    if (user) {
      if (user.channel) socket.to(user.channel).emit('peer-left', { peerId: socket.id });
      users.delete(socket.id);
      io.emit('directory-update', getDirectoryState());
    }
  });
});

server.listen(PORT, () => console.log(`Pulse Desktop Backend na porta ${PORT}`));