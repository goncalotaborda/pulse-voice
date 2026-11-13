const express = require('express');
const http = require('http');
const https = require('https');
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
const SPOTIFY_CLIENT_ID = (process.env.SPOTIFY_CLIENT_ID || '').trim();
const SPOTIFY_CLIENT_SECRET = (process.env.SPOTIFY_CLIENT_SECRET || '').trim();

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

let db = {
  users: {},
  messages: [],
  soundboard: [],
  files: []
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
// PEDIDOS HTTPS E EXTRATORES COMPLETOS (YOUTUBE / YT MUSIC / APPLE / SPOTIFY)
// ============================================================================
function httpsRequestText(targetUrl, options = {}, redirects = 0) {
  return new Promise((resolve, reject) => {
    if (redirects > 5) return reject(new Error('Demasiados redirecionamentos'));
    const parsedUrl = new URL(targetUrl);
    const reqOpts = {
      hostname: parsedUrl.hostname,
      port: parsedUrl.port || 443,
      path: parsedUrl.pathname + parsedUrl.search,
      method: options.method || 'GET',
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        'Accept-Language': 'en-US,en;q=0.9,pt;q=0.8',
        'Cookie': 'CONSENT=YES+cb.20210328-17-p0.en+FX+417; SOCS=CAESEwgDEgk0ODE3Nzk3MjQaAmVuIAEaBgiA_LyaBg',
        ...(options.headers || {})
      },
      timeout: 12000
    };

    const req = https.request(reqOpts, (res) => {
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        const nextUrl = new URL(res.headers.location, targetUrl).toString();
        return resolve(httpsRequestText(nextUrl, options, redirects + 1));
      }
      let data = '';
      res.setEncoding('utf8');
      res.on('data', chunk => { data += chunk; });
      res.on('end', () => resolve({ status: res.statusCode, body: data }));
    });

    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error('Timeout')); });
    if (options.body) req.write(options.body);
    req.end();
  });
}

async function fetchHttpsText(targetUrl, headers = {}) {
  const res = await httpsRequestText(targetUrl, { method: 'GET', headers });
  return res.body;
}

// Extrator completo de Playlists YouTube & YouTube Music (suporta 900+ músicas)
async function fetchFullYouTubePlaylist(listId, addedBy) {
  const playlistUrl = `https://www.youtube.com/playlist?list=${encodeURIComponent(listId)}&hl=en`;
  const html = await fetchHttpsText(playlistUrl);

  const dataMatch = html.match(/var ytInitialData\s*=\s*(\{[\s\S]*?\});\s*<\/script>/);
  if (!dataMatch) {
    throw new Error('Não foi possível ler a playlist do YouTube. Verifica se está definida como Pública ou Não Listada.');
  }

  const initialData = JSON.parse(dataMatch[1]);
  const playlistTitle =
    initialData?.metadata?.playlistMetadataRenderer?.title ||
    initialData?.header?.playlistHeaderRenderer?.title?.simpleText ||
    'YouTube Music Playlist';

  const apiKeyMatch = html.match(/"INNERTUBE_API_KEY":"([^"]+)"/);
  const clientVersionMatch = html.match(/"INNERTUBE_CLIENT_VERSION":"([^"]+)"/);
  const apiKey = apiKeyMatch ? apiKeyMatch[1] : null;
  const clientVersion = clientVersionMatch ? clientVersionMatch[1] : '2.20261101.00.00';

  const tracks = [];
  const seenIds = new Set();

  function parseRendererList(items) {
    let nextContinuation = null;
    if (!Array.isArray(items)) return null;

    for (const item of items) {
      const v = item?.playlistVideoRenderer;
      if (v && v.videoId && !seenIds.has(v.videoId)) {
        const title = v.title?.runs?.[0]?.text || v.title?.simpleText || '';
        const artist = v.shortBylineText?.runs?.[0]?.text || 'YouTube Music';
        if (title && title !== '[Deleted video]' && title !== '[Private video]') {
          seenIds.add(v.videoId);
          const thumbs = v.thumbnail?.thumbnails || [];
          const cover = thumbs.length ? thumbs[thumbs.length - 1].url.split('?')[0] : `https://i.ytimg.com/vi/${v.videoId}/hqdefault.jpg`;

          tracks.push({
            id: `trk-${Date.now()}-${tracks.length}-${crypto.randomBytes(2).toString('hex')}`,
            title,
            artist,
            cover,
            platform: 'YouTube Music',
            youtubeId: v.videoId,
            addedBy
          });
        }
      }

      const cont = item?.continuationItemRenderer?.continuationEndpoint?.continuationCommand?.token;
      if (cont) nextContinuation = cont;
    }
    return nextContinuation;
  }

  const tabs = initialData?.contents?.twoColumnBrowseResultsRenderer?.tabs || [];
  let initialItems = [];
  for (const tab of tabs) {
    const sections = tab?.tabRenderer?.content?.sectionListRenderer?.contents || [];
    for (const sec of sections) {
      const playlistContents = sec?.itemSectionRenderer?.contents?.[0]?.playlistVideoListRenderer?.contents;
      if (Array.isArray(playlistContents)) {
        initialItems = playlistContents;
        break;
      }
    }
  }

  let continuationToken = parseRendererList(initialItems);

  // Paginar automaticamente até carregar todas as 900+ músicas
  let pagesFetched = 0;
  while (continuationToken && apiKey && tracks.length < 1500 && pagesFetched < 15) {
    pagesFetched++;
    try {
      const contRes = await httpsRequestText(`https://www.youtube.com/youtubei/v1/browse?key=${apiKey}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          context: {
            client: { clientName: 'WEB', clientVersion }
          },
          continuation: continuationToken
        })
      });

      if (contRes.status !== 200) break;
      const contJson = JSON.parse(contRes.body);
      const actions = contJson?.onResponseReceivedActions || [];
      let nextItems = [];
      for (const act of actions) {
        const appended = act?.appendContinuationItemsAction?.continuationItems;
        if (Array.isArray(appended)) {
          nextItems = appended;
          break;
        }
      }
      continuationToken = parseRendererList(nextItems);
    } catch (e) {
      break;
    }
  }

  if (!tracks.length) {
    throw new Error('A playlist do YouTube está vazia ou privada.');
  }

  return { sourceName: playlistTitle, tracks };
}

// Token de acesso Spotify (Client Credentials) em cache
let cachedSpotifyToken = { token: '', expiresAt: 0 };

async function getSpotifyApiToken() {
  if (!SPOTIFY_CLIENT_ID || !SPOTIFY_CLIENT_SECRET || SPOTIFY_CLIENT_ID.startsWith('cola_aqui')) {
    return null;
  }
  if (cachedSpotifyToken.token && Date.now() < cachedSpotifyToken.expiresAt) {
    return cachedSpotifyToken.token;
  }

  const basicAuth = Buffer.from(`${SPOTIFY_CLIENT_ID}:${SPOTIFY_CLIENT_SECRET}`).toString('base64');
  const res = await httpsRequestText('https://accounts.spotify.com/api/token', {
    method: 'POST',
    headers: {
      'Authorization': `Basic ${basicAuth}`,
      'Content-Type': 'application/x-www-form-urlencoded'
    },
    body: 'grant_type=client_credentials'
  });

  if (res.status !== 200) return null;
  const json = JSON.parse(res.body);
  if (!json.access_token) return null;

  cachedSpotifyToken = {
    token: json.access_token,
    expiresAt: Date.now() + ((json.expires_in || 3600) - 60) * 1000
  };
  return cachedSpotifyToken.token;
}

async function fetchFullSpotifyPlaylistViaApi(kind, id, addedBy) {
  const token = await getSpotifyApiToken();
  if (!token) return null;

  const authHeaders = { 'Authorization': `Bearer ${token}` };
  const metaRes = await httpsRequestText(`https://api.spotify.com/v1/${kind}s/${id}`, { headers: authHeaders });
  if (metaRes.status !== 200) return null;

  const meta = JSON.parse(metaRes.body);
  const sourceName = meta.name || 'Spotify Playlist';
  const defaultCover = meta.images?.[0]?.url || '';
  const tracks = [];

  if (kind === 'track') {
    const artists = Array.isArray(meta.artists) ? meta.artists.map(a => a.name).join(', ') : 'Spotify';
    return {
      sourceName,
      tracks: [{
        id: `trk-${Date.now()}-${crypto.randomBytes(2).toString('hex')}`,
        title: meta.name || 'Faixa Spotify',
        artist: artists,
        cover: meta.album?.images?.[0]?.url || defaultCover,
        platform: 'Spotify',
        youtubeId: null,
        addedBy
      }]
    };
  }

  let nextUrl = kind === 'playlist'
    ? `https://api.spotify.com/v1/playlists/${id}/items?limit=100&offset=0`
    : `https://api.spotify.com/v1/albums/${id}/tracks?limit=50&offset=0`;

  let usedLegacyEndpoint = false;

  while (nextUrl && tracks.length < 1500) {
    const pageRes = await httpsRequestText(nextUrl, { headers: authHeaders });
    if (pageRes.status !== 200) {
      if (kind === 'playlist' && !usedLegacyEndpoint) {
        usedLegacyEndpoint = true;
        nextUrl = `https://api.spotify.com/v1/playlists/${id}/tracks?limit=100&offset=0`;
        continue;
      }
      break;
    }

    const pageJson = JSON.parse(pageRes.body);
    const list = pageJson.items || [];

    for (const entry of list) {
      const t = kind === 'playlist' ? (entry.item || entry.track) : entry;
      if (!t || !t.name) continue;
      const artists = Array.isArray(t.artists) ? t.artists.map(a => a.name).join(', ') : 'Spotify';
      const cover = t.album?.images?.[0]?.url || defaultCover;

      tracks.push({
        id: `trk-${Date.now()}-${tracks.length}-${crypto.randomBytes(2).toString('hex')}`,
        title: t.name,
        artist: artists,
        cover,
        platform: 'Spotify',
        youtubeId: null,
        addedBy
      });
    }

    nextUrl = pageJson.next || null;
  }

  return tracks.length > 0 ? { sourceName, tracks } : null;
}

// Extrator Completo Apple Music (lê dados embutidos e pagina via amp-api)
async function fetchFullAppleMusic(url, html, addedBy) {
  const titleMatch = html.match(/<title>(.*?)<\/title>/i);
  const pageTitle = titleMatch ? titleMatch[1].replace(/\s*-\s*Apple Music.*$/i, '').trim() : 'Apple Music';

  const parsedUrl = new URL(url);
  const pathParts = parsedUrl.pathname.split('/').filter(Boolean);
  const storefront = pathParts[0] || 'pt';
  const kind = pathParts[1] === 'album' ? 'albums' : 'playlists';
  const appleId = pathParts[pathParts.length - 1];

  try {
    let jwtToken = null;
    const inlineJwt = html.match(/eyJhbGciOiJFUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6[a-zA-Z0-9._-]+/);
    if (inlineJwt) {
      jwtToken = inlineJwt[0];
    } else {
      const scriptMatches = [...html.matchAll(/src="(\/assets\/[^"]+\.js)"/gi)];
      for (const sm of scriptMatches.slice(0, 4)) {
        const jsCode = await fetchHttpsText(`https://music.apple.com${sm[1]}`);
        const found = jsCode.match(/eyJhbGciOiJFUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6[a-zA-Z0-9._-]+/);
        if (found) {
          jwtToken = found[0];
          break;
        }
      }
    }

    if (jwtToken && appleId) {
      const ampHeaders = {
        'Authorization': `Bearer ${jwtToken}`,
        'Origin': 'https://music.apple.com',
        'Referer': 'https://music.apple.com/'
      };

      let nextPath = `/v1/catalog/${storefront}/${kind}/${appleId}/tracks?l=pt-PT&limit=100&offset=0`;
      const tracks = [];

      while (nextPath && tracks.length < 1500) {
        const apiRes = await httpsRequestText(`https://amp-api.music.apple.com${nextPath}`, { headers: ampHeaders });
        if (apiRes.status !== 200) break;

        const json = JSON.parse(apiRes.body);
        const items = json.data || [];

        for (const item of items) {
          const attr = item.attributes;
          if (!attr || !attr.name) continue;
          const rawArt = attr.artwork?.url || '';
          const cover = rawArt ? rawArt.replace('{w}', '300').replace('{h}', '300') : '';

          tracks.push({
            id: `trk-${Date.now()}-${tracks.length}-${crypto.randomBytes(2).toString('hex')}`,
            title: attr.name,
            artist: attr.artistName || 'Apple Music',
            cover,
            platform: 'Apple Music',
            youtubeId: null,
            addedBy
          });
        }

        nextPath = json.next || null;
      }

      if (tracks.length > 0) {
        return { sourceName: pageTitle, tracks };
      }
    }
  } catch (e) {}

  throw new Error('Não foi possível ler as faixas desta playlist do Apple Music. Tenta usar uma playlist pública do YouTube / YouTube Music.');
}

async function resolveYouTubeVideo(query) {
  try {
    const searchUrl = `https://www.youtube.com/results?search_query=${encodeURIComponent(query + ' official audio')}`;
    const html = await fetchHttpsText(searchUrl);
    const match = html.match(/"videoRenderer":\{"videoId":"([a-zA-Z0-9_-]{11})"/);
    if (match && match[1]) {
      return {
        videoId: match[1],
        thumbnail: `https://i.ytimg.com/vi/${match[1]}/hqdefault.jpg`
      };
    }
  } catch (e) {}
  return null;
}

async function extractTracksFromUrl(rawUrl, addedBy) {
  let url = String(rawUrl || '').trim();
  if (!/^https?:\/\//i.test(url)) url = 'https://' + url;

  // 1. Playlists do YouTube e YouTube Music (ex: music.youtube.com/playlist?list=... ou youtube.com/playlist?list=...)
  if (/(?:youtube\.com|youtu\.be)/i.test(url) && /[?&]list=([a-zA-Z0-9_-]+)/i.test(url)) {
    const listMatch = url.match(/[?&]list=([a-zA-Z0-9_-]+)/i);
    if (listMatch && listMatch[1]) {
      return await fetchFullYouTubePlaylist(listMatch[1], addedBy);
    }
  }

  // 2. Spotify (API completa 900+ faixas se houver chaves; ou embed até 100 faixas)
  const spMatch = url.match(/open\.spotify\.com\/(?:intl-[a-z]+\/)?(playlist|album|track)\/([a-zA-Z0-9]+)/i);
  if (spMatch) {
    const kind = spMatch[1].toLowerCase();
    const id = spMatch[2];

    const apiResult = await fetchFullSpotifyPlaylistViaApi(kind, id, addedBy);
    if (apiResult) return apiResult;

    const html = await fetchHttpsText(`https://open.spotify.com/embed/${kind}/${id}`);
    const nextDataMatch = html.match(/<script id="__NEXT_DATA__" type="application\/json">([\s\S]*?)<\/script>/);
    if (!nextDataMatch) throw new Error('Não foi possível ler a playlist do Spotify. Verifica se é pública.');

    const json = JSON.parse(nextDataMatch[1]);
    const entity = json?.props?.pageProps?.state?.data?.entity;
    if (!entity) throw new Error('Playlist do Spotify vazia ou privada.');

    const playlistCover = entity.coverArt?.sources?.[0]?.url || entity.visualIdentity?.image?.[0]?.url || '';
    const playlistName = entity.name || 'Spotify Playlist';

    if (Array.isArray(entity.trackList) && entity.trackList.length > 0) {
      return {
        sourceName: playlistName,
        tracks: entity.trackList.map((t, idx) => ({
          id: `trk-${Date.now()}-${idx}-${crypto.randomBytes(2).toString('hex')}`,
          title: t.title || 'Faixa sem título',
          artist: t.subtitle || 'Spotify',
          cover: playlistCover,
          platform: 'Spotify',
          youtubeId: null,
          addedBy
        }))
      };
    }

    const artistNames = Array.isArray(entity.artists) ? entity.artists.map(a => a.name).join(', ') : 'Spotify';
    return {
      sourceName: entity.name || 'Spotify',
      tracks: [{
        id: `trk-${Date.now()}-${crypto.randomBytes(2).toString('hex')}`,
        title: entity.name || 'Faixa Spotify',
        artist: artistNames,
        cover: playlistCover,
        platform: 'Spotify',
        youtubeId: null,
        addedBy
      }]
    };
  }

  // 3. Apple Music
  if (/music\.apple\.com/i.test(url)) {
    const html = await fetchHttpsText(url);
    return await fetchFullAppleMusic(url, html, addedBy);
  }

  // 4. Vídeo individual do YouTube / YouTube Music
  const ytMatch = url.match(/(?:youtube\.com\/(?:watch\?v=|shorts\/|live\/)|youtu\.be\/)([a-zA-Z0-9_-]{11})/i);
  if (ytMatch) {
    const videoId = ytMatch[1];
    let title = 'Música do YouTube';
    let author = 'YouTube';
    try {
      const oembedRaw = await fetchHttpsText(`https://www.youtube.com/oembed?url=https://www.youtube.com/watch?v=${videoId}&format=json`);
      const oembed = JSON.parse(oembedRaw);
      if (oembed.title) title = oembed.title;
      if (oembed.author_name) author = oembed.author_name;
    } catch (e) {}

    return {
      sourceName: title,
      tracks: [{
        id: `trk-${Date.now()}-${crypto.randomBytes(2).toString('hex')}`,
        title,
        artist: author,
        cover: `https://i.ytimg.com/vi/${videoId}/hqdefault.jpg`,
        platform: 'YouTube',
        youtubeId: videoId,
        addedBy
      }]
    };
  }

  // 5. Se for outro link não reconhecido, devolve erro em vez de pesquisar o URL no YouTube
  if (/^https?:\/\//i.test(rawUrl.trim())) {
    throw new Error('Link não suportado. Cola um link público de playlist do YouTube, YouTube Music, Spotify ou Apple Music.');
  }

  // 6. Pesquisa direta por nome da música/artista
  const queryText = rawUrl.trim();
  const resolved = await resolveYouTubeVideo(queryText);
  if (!resolved) throw new Error('Não foi encontrada nenhuma música correspondente.');

  return {
    sourceName: queryText,
    tracks: [{
      id: `trk-${Date.now()}-${crypto.randomBytes(2).toString('hex')}`,
      title: queryText,
      artist: 'Pesquisa Direta',
      cover: resolved.thumbnail,
      platform: 'YouTube',
      youtubeId: resolved.videoId,
      addedBy
    }]
  };
}

// ============================================================================
// ENDPOINTS HTTP: UPLOAD (ATÉ 100 MB / 24H) E SOUNDBOARD
// ============================================================================
const MAX_UPLOAD_BYTES = 100 * 1024 * 1024;
const FILE_TTL_MS = 24 * 60 * 60 * 1000;

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
    db.files.push({
      id: fileId,
      originalName: safeOriginalName,
      storedName,
      size: receivedBytes,
      expiresAt,
      uploadedBy: account.username
    });

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
// SOCKET.IO: CHAMADAS, SOUNDBOARD, CANAL MÚSICA E CHAT GLOBAL
// ============================================================================
const activeSessions = new Map();

let musicRoomState = {
  playlistTitle: '',
  currentTrack: null,
  isPlaying: true,
  shuffle: false,
  queue: []
};

async function startTrackInMusicRoom(track) {
  if (!track) {
    musicRoomState.currentTrack = null;
    io.to('music').emit('music-state-update', musicRoomState);
    return;
  }

  if (!track.youtubeId) {
    const resolved = await resolveYouTubeVideo(`${track.artist} - ${track.title}`);
    if (resolved) {
      track.youtubeId = resolved.videoId;
      if (!track.cover) track.cover = resolved.thumbnail;
    }
  }

  musicRoomState.currentTrack = track;
  musicRoomState.isPlaying = true;
  io.to('music').emit('music-state-update', musicRoomState);
}

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

  socket.on('music-action', async (payload, cb) => {
    const user = activeSessions.get(socket.id);
    if (!user || user.channelId !== 'music') return cb?.({ ok: false });

    try {
      if (payload.action === 'load-url') {
        const extracted = await extractTracksFromUrl(payload.url, user.name);
        if (!extracted.tracks.length) {
          return cb?.({ ok: false, error: 'Nenhuma faixa encontrada neste link.' });
        }

        if (payload.playNow) {
          musicRoomState.playlistTitle = extracted.sourceName;
          musicRoomState.queue = [...extracted.tracks];
          const firstIdx = musicRoomState.shuffle
            ? Math.floor(Math.random() * musicRoomState.queue.length)
            : 0;
          const firstTrack = musicRoomState.queue[firstIdx];
          await startTrackInMusicRoom(firstTrack);
        } else {
          if (!musicRoomState.playlistTitle) {
            musicRoomState.playlistTitle = extracted.sourceName;
          }
          musicRoomState.queue.push(...extracted.tracks);
          if (!musicRoomState.currentTrack) {
            await startTrackInMusicRoom(musicRoomState.queue[0]);
          } else {
            io.to('music').emit('music-state-update', musicRoomState);
          }
        }
        return cb?.({ ok: true });
      }

      if (payload.action === 'play-track') {
        const target = musicRoomState.queue.find(t => t.id === payload.id);
        if (target) {
          await startTrackInMusicRoom(target);
        }
        return cb?.({ ok: true });
      }

      if (payload.action === 'toggle-play') {
        if (musicRoomState.currentTrack) {
          musicRoomState.isPlaying = !musicRoomState.isPlaying;
          io.to('music').emit('music-state-update', musicRoomState);
        }
        return cb?.({ ok: true });
      }

      if (payload.action === 'toggle-shuffle') {
        musicRoomState.shuffle = !musicRoomState.shuffle;
        io.to('music').emit('music-state-update', musicRoomState);
        return cb?.({ ok: true });
      }

      if (payload.action === 'next') {
        if (musicRoomState.queue.length > 0) {
          const curIdx = musicRoomState.currentTrack
            ? musicRoomState.queue.findIndex(t => t.id === musicRoomState.currentTrack.id)
            : -1;

          let nextIdx = 0;
          if (musicRoomState.shuffle && musicRoomState.queue.length > 1) {
            do {
              nextIdx = Math.floor(Math.random() * musicRoomState.queue.length);
            } while (nextIdx === curIdx);
          } else {
            nextIdx = (curIdx + 1) % musicRoomState.queue.length;
          }

          await startTrackInMusicRoom(musicRoomState.queue[nextIdx]);
        }
        return cb?.({ ok: true });
      }

      if (payload.action === 'queue-remove') {
        const wasCurrent = musicRoomState.currentTrack?.id === payload.id;
        musicRoomState.queue = musicRoomState.queue.filter(t => t.id !== payload.id);
        if (wasCurrent) {
          await startTrackInMusicRoom(musicRoomState.queue[0] || null);
        } else {
          io.to('music').emit('music-state-update', musicRoomState);
        }
        return cb?.({ ok: true });
      }
    } catch (err) {
      return cb?.({ ok: false, error: err.message || 'Erro ao carregar playlist.' });
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