const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const { EventEmitter, once } = require('node:events');
const ts = require('typescript');
const { WebSocket, WebSocketServer } = require('ws');
const quiet = { log() {}, info() {}, warn() {}, error() {} };

function load(file, mocks = {}, globals = {}) {
  const source = fs.readFileSync(path.join(__dirname, '..', file), 'utf8');
  const code = ts.transpileModule(source, { compilerOptions: {
    module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2020, esModuleInterop: true,
  } }).outputText;
  const module = { exports: {} };
  vm.runInNewContext(code, { module, exports: module.exports, console: quiet, Buffer, ...globals,
    require(id) { if (id in mocks) return mocks[id]; throw new Error('Import inesperado: ' + id); },
  });
  return module.exports;
}
const protocol = load('src/uploadProtocol.ts');
const meta = (fileName = 'foto.jpg', size = 3) => ({ type: 'file-meta', fileName, size,
  mime: 'image/jpeg', totalBase64Length: 4 * Math.ceil(size / 3), toSession: 'TEST0001' });
const chunk = (fileName = 'foto.jpg', seq = 0, chunkBase64 = 'AQID') => ({
  type: 'file-chunk', fileName, seq, chunkBase64, toSession: 'TEST0001',
});
const complete = (fileName = 'foto.jpg') => ({ type: 'file-complete', fileName, toSession: 'TEST0001' });
function bridge() {
  return load('src/wsBridge.ts', { ws: { WebSocket }, './uploadProtocol': protocol,
    './auth': { verifyToken: () => null }, './database/sqlite': { DataSource: class {
      getSessionByCode(code, cb) { cb(null, { id: code }); } updateSession() {}
    } },
  });
}
class Socket extends EventEmitter {
  readyState = 1; OPEN = 1; messages = []; closes = []; terminated = false;
  send(text, callback) { this.messages.push(JSON.parse(text)); callback?.(); }
  close(...args) { this.closes.push(args); this.readyState = 3; }
  terminate() { this.terminated = true; this.readyState = 3; }
}
function register(api, socket, role, version = 1) {
  api.handleMessage(socket, { type: 'register', role, id: role + '-synthetic', sessionId: 'TEST0001',
    ...(version === undefined ? {} : { protocolVersion: version }),
  });
}

test('limites: aceita 100 fotos e recusa a 101ª, arquivo grande e tamanho adulterado', () => {
  const session = new protocol.UploadSession();
  for (let i = 0; i < 100; i++) {
    const name = `foto-${i}.jpg`;
    session.accept(meta(name)); session.accept(chunk(name)); session.accept(complete(name));
  }
  assert.throws(() => session.accept(meta('foto-101.jpg')), { code: 'UPLOAD_LIMIT_EXCEEDED' });
  session.finish();
  assert.throws(() => session.finish(), { code: 'INCOMPLETE_TRANSFER' });
  assert.throws(() => new protocol.UploadSession().accept(meta('grande.jpg', 20 * 1024 * 1024 + 1)), { code: 'UPLOAD_LIMIT_EXCEEDED' });
  assert.throws(() => new protocol.UploadSession().accept({ ...meta(), totalBase64Length: 8 }), { code: 'INVALID_FILE' });
});

test('contadores recusam chunk excedente, sequência inválida, duplicata e arquivo incompleto', () => {
  const session = new protocol.UploadSession(); session.accept(meta());
  for (const invalid of [chunk('foto.jpg', 1), chunk('foto.jpg', 0, 'AQIDAQID'), chunk('foto.jpg', 0, '!!!!')]) {
    assert.throws(() => session.accept(invalid), { code: 'INVALID_CHUNK' });
  }
  assert.throws(() => session.accept(meta()), { code: 'DUPLICATE_FILE' });
  assert.throws(() => session.accept(complete()), { code: 'INCOMPLETE_FILE' });
  session.accept(chunk()); session.accept(complete());
  assert.throws(() => session.accept(chunk()), { code: 'INVALID_FILE' });
});

test('arquivo com vários blocos respeita o teto de 64 KiB e aceita padding no final', () => {
  const session = new protocol.UploadSession();
  session.accept(meta('blocos.jpg', 48 * 1024 + 1));
  assert.throws(() => session.accept(chunk('blocos.jpg', 0, 'A'.repeat(65540))), { code: 'INVALID_CHUNK' });
  session.accept(chunk('blocos.jpg', 0, 'A'.repeat(65536)));
  session.accept(chunk('blocos.jpg', 1, 'AQ=='));
  session.accept(complete('blocos.jpg'));
  session.finish();
});

test('registro repetido mantém uma entrada e não reinicia os contadores do upload', () => {
  const api = bridge(), kiosk = new Socket(), web = new Socket();
  register(api, kiosk, 'kiosk'); register(api, kiosk, 'kiosk'); register(api, web, 'web');
  const response = web.messages.find(m => m.type === 'registered');
  assert.equal(response.protocolVersion, 1);
  assert.equal(response.uploadLimits.maxFiles, 100);
  api.handleMessage(web, meta());
  assert.equal(kiosk.messages.filter(m => m.type === 'file-meta').length, 1);
  register(api, web, 'web');
  api.handleMessage(web, meta());
  assert.equal(web.messages.at(-1).code, 'DUPLICATE_FILE');
  assert.equal(web.closes.at(-1)[0], 1008);
  assert.equal(kiosk.messages.at(-1).type, 'transfer-error');
});

test('legado continua encaminhado; v1 recusa destino ausente e sessão diferente', () => {
  const api = bridge(), kiosk = new Socket(), legacy = new Socket();
  register(api, kiosk, 'kiosk');
  api.handleMessage(legacy, { type: 'register', role: 'web', id: 'legacy', sessionId: 'TEST0001' });
  api.handleMessage(legacy, meta());
  assert.equal(kiosk.messages.at(-1).type, 'file-meta');
  api.cleanup(legacy); api.cleanup(kiosk);
  const web = new Socket(); register(api, web, 'web'); api.handleMessage(web, meta());
  assert.equal(web.messages.at(-1).code, 'KIOSK_UNAVAILABLE');
  api.cleanup(web);
  const other = new Socket(); register(api, other, 'web');
  api.handleMessage(other, { ...meta(), toSession: 'OTHER001' });
  assert.equal(other.messages.at(-1).code, 'INVALID_SESSION');
});

test('erro de envio por callback ou exceção limpa o socket sem derrubar o Bridge', () => {
  for (const asyncError of [true, false]) {
    const api = bridge(), kiosk = new Socket(), healthy = new Socket();
    register(api, kiosk, 'kiosk'); register(api, healthy, 'kiosk');
    kiosk.send = (text, callback) => { if (asyncError) callback(new Error('sintético')); else throw new Error('sintético'); };
    assert.doesNotThrow(() => api.sendToKiosk('TEST0001', { type: 'notify' }));
    assert.equal(kiosk.terminated, true);
    assert.equal(healthy.messages.at(-1).type, 'notify');
    api.cleanup(kiosk); api.cleanup(kiosk);
    assert.equal(api.sendToKiosk('TEST0001', { type: 'notify' }), true);
  }
});

test('erro do Kiosk é encaminhado somente ao web da sessão registrada', () => {
  const api = bridge(), kiosk = new Socket(), web = new Socket(), other = new Socket();
  register(api, kiosk, 'kiosk'); register(api, web, 'web');
  api.handleMessage(other, { type: 'register', role: 'web', id: 'other-web', sessionId: 'OTHER001', protocolVersion: 1 });
  api.handleMessage(web, { type: 'transfer-error' });
  assert.equal(web.closes.length, 0, 'web não pode abortar por este canal reservado ao Kiosk');
  api.handleMessage(kiosk, { type: 'transfer-error', toSession: 'OTHER001' });
  assert.equal(web.messages.at(-1).code, 'KIOSK_UPLOAD_ERROR');
  assert.equal(web.closes.at(-1)[0], 1008);
  assert.equal(other.closes.length, 0);
  assert.equal(other.messages.at(-1).type, 'registered');
});

test('listener de erro da conexão termina somente o socket afetado', () => {
  const api = bridge(); let wss;
  const app = { use() {}, options() {}, get() {}, post() {}, put() {} };
  const express = Object.assign(() => app, { json: () => () => {} });
  const server = Object.assign(new EventEmitter(), { listen(port, cb) { cb(); } });
  load('src/index.ts', { express, http: { createServer: () => server }, cors: () => () => {},
    ws: { WebSocketServer: class extends EventEmitter { constructor(options) { super(); assert.equal(options.maxPayload, 128 * 1024); wss = this; } } },
    './wsBridge': api, './database/sqlite': { DataSource: class {} },
    './util/MsUntilNextMidnight': { msUntilNextMidnight: () => 1 },
  }, { process: { env: {} }, setTimeout() {}, setInterval() {} });
  const socket = new Socket(); wss.emit('connection', socket);
  assert.doesNotThrow(() => socket.emit('error', new Error('conexão sintética')));
  assert.equal(socket.terminated, true);
});

function message(ws, predicate) {
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => { ws.off('message', onMessage); reject(new Error('Timeout aguardando mensagem')); }, 3000);
    function onMessage(raw) {
      const data = JSON.parse(raw.toString());
      if (!predicate(data)) return;
      clearTimeout(timeout); ws.off('message', onMessage); resolve(data);
    }
    ws.on('message', onMessage);
  });
}

test('integração local: encaminha 100 fotos por sockets reais e informa rejeição sem queda', async t => {
  const api = bridge();
  const server = new WebSocketServer({ port: 0, host: '127.0.0.1' });
  server.on('connection', ws => {
    ws.on('message', raw => api.handleMessage(ws, JSON.parse(raw.toString())));
    ws.on('close', () => api.cleanup(ws));
    ws.on('error', () => { api.cleanup(ws); ws.terminate(); });
  });
  await once(server, 'listening');
  const sockets = [];
  t.after(async () => {
    for (const socket of sockets) socket.terminate();
    for (const socket of server.clients) socket.terminate();
    await new Promise(resolve => server.close(resolve));
  });
  async function connect(role) {
    const ws = new WebSocket(`ws://127.0.0.1:${server.address().port}`); sockets.push(ws);
    await once(ws, 'open');
    const registered = message(ws, data => data.type === 'registered');
    ws.send(JSON.stringify({ type: 'register', role, id: role, sessionId: 'TEST0001', protocolVersion: 1 }));
    await registered; return ws;
  }
  const kiosk = await connect('kiosk'), web = await connect('web');
  const received = []; kiosk.on('message', raw => received.push(JSON.parse(raw.toString())));
  const finished = message(kiosk, data => data.message === 'transfer_complete');
  for (let i = 0; i < 100; i++) for (const data of [meta(`foto-${i}`), chunk(`foto-${i}`), complete(`foto-${i}`)]) web.send(JSON.stringify(data));
  web.send(JSON.stringify({ type: 'notify', toSession: 'TEST0001', message: 'transfer_complete' }));
  await finished;
  assert.equal(received.filter(data => data.type === 'file-complete').length, 100);
  const rejected = message(web, data => data.type === 'transfer-error');
  web.send(JSON.stringify(meta('extra.jpg')));
  assert.equal((await rejected).code, 'UPLOAD_FINISHED');
  assert.equal(server.address().port > 0, true);
});
