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
const confirmedProtocol = load('src/confirmedProtocol.ts', { './uploadProtocol': protocol });
const confirmedModule = load('src/confirmedTransfers.ts', { ws: { WebSocket }, 'node:crypto': require('node:crypto'),
  './confirmedProtocol': confirmedProtocol }, { setTimeout, clearTimeout });
const meta = (fileName = 'foto.jpg', size = 3) => ({ type: 'file-meta', fileName, size,
  mime: 'image/jpeg', totalBase64Length: 4 * Math.ceil(size / 3), toSession: 'TEST0001' });
const chunk = (fileName = 'foto.jpg', seq = 0, chunkBase64 = 'AQID') => ({
  type: 'file-chunk', fileName, seq, chunkBase64, toSession: 'TEST0001',
});
const complete = (fileName = 'foto.jpg') => ({ type: 'file-complete', fileName, toSession: 'TEST0001' });
function bridge() {
  return load('src/wsBridge.ts', { ws: { WebSocket }, './uploadProtocol': protocol,
    './confirmedProtocol': confirmedProtocol, './confirmedTransfers': confirmedModule,
    './auth': { verifyToken: () => null }, './database/sqlite': { DataSource: class {
      getSessionByCode(code, cb) { cb(null, { id: code }); } updateSession() {}
    } },
  });
}
class Socket extends EventEmitter {
  readyState = 1; OPEN = 1; bufferedAmount = 0; messages = []; closes = []; terminated = false;
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
    register(api, kiosk, 'kiosk'); api.handleMessage(healthy, { type: 'register', role: 'kiosk', id: 'healthy', sessionId: 'OTHER001', protocolVersion: 1 });
    kiosk.send = (text, callback) => { if (asyncError) callback(new Error('sintético')); else throw new Error('sintético'); };
    assert.doesNotThrow(() => api.sendToKiosk('TEST0001', { type: 'notify' }));
    assert.equal(kiosk.terminated, true);
    assert.equal(api.sendToKiosk('OTHER001', { type: 'notify' }), true);
    assert.equal(healthy.messages.at(-1).type, 'notify');
    api.cleanup(kiosk); api.cleanup(kiosk);
    assert.equal(api.sendToKiosk('TEST0001', { type: 'notify' }), false);
    assert.equal(api.sendToKiosk('OTHER001', { type: 'notify' }), true);
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

function confirmedSession(api = bridge(), sessionId = 'TEST0001') {
  const kiosk = new Socket(), web = new Socket();
  api.handleMessage(kiosk, { type: 'register', role: 'kiosk', id: 'kiosk-' + sessionId, sessionId, protocolVersion: 2 });
  api.handleMessage(kiosk, { type: 'unlock', sessionId });
  const ticket = kiosk.messages.findLast(message => message.type === 'upload-permission').uploadTicket;
  assert.equal(api.claimUploadPermission(sessionId, ticket, 'customer-' + sessionId), true);
  api.handleMessage(web, { type: 'register', role: 'web', id: 'customer-' + sessionId, sessionId, protocolVersion: 2, uploadTicket: ticket });
  assert.equal(web.messages.at(-1).protocolVersion, 2);
  return { api, kiosk, web, ticket, sessionId };
}
const v2request = (requestId, operation, extra = {}) => ({ type: 'upload-request', protocolVersion: 2,
  transferId: 'synthetic-transfer', requestId, operation, ...extra });
const v2files = [{ fileId: 'file-a', name: 'foto.jpg', mime: 'image/jpeg', size: 3 }];
const v2chunk = () => ({ fileId: 'file-a', seq: 0, chunkBase64: 'AQID', sha256: require('node:crypto').createHash('sha256').update(Buffer.from([1,2,3])).digest('hex') });
const acknowledge = (app, requestId) => app.api.handleMessage(app.kiosk, { type: 'upload-ack', protocolVersion: 2, transferId: 'synthetic-transfer', requestId });

test('v2: só o Kiosk confirma; duplicatas pendentes não aumentam a fila', () => {
  const app = confirmedSession(), { api, web, kiosk } = app;
  api.handleMessage(web, v2request(0, 'start', { files: v2files }));
  api.handleMessage(web, { type: 'upload-ack', protocolVersion: 2, transferId: 'synthetic-transfer', requestId: 0 });
  assert.equal(web.messages.some(message => message.type === 'upload-ack'), false); acknowledge(app, 0);
  const request = v2request(1, 'chunk', v2chunk()); api.handleMessage(web, request); api.handleMessage(web, request);
  assert.equal(kiosk.messages.filter(message => message.operation === 'chunk').length, 1);
  acknowledge(app, 1); api.handleMessage(web, request);
  assert.equal(web.messages.filter(message => message.type === 'upload-ack' && message.requestId === 1).length, 2);
  assert.equal(kiosk.messages.filter(message => message.operation === 'chunk').length, 1);
  api.handleMessage(web, v2request(2, 'file-complete', { fileId: 'file-a' })); acknowledge(app, 2);
  api.handleMessage(web, v2request(3, 'finish'));
  assert.equal(web.messages.some(message => message.type === 'upload-ack' && message.requestId === 3), false); acknowledge(app, 3);
  assert.equal(web.messages.at(-1).requestId, 3); api.handleMessage(web, v2request(3, 'finish')); assert.equal(web.messages.at(-1).requestId, 3);
  assert.equal(api.claimUploadPermission(app.sessionId, app.ticket, 'customer-' + app.sessionId), false);
  api.cleanup(web); api.cleanup(kiosk);
});
test('v2: replay de QR, troca de sessão, Kiosk duplicado e unlock pelo web são recusados', () => {
  const app = confirmedSession(), intruder = new Socket();
  app.api.handleMessage(intruder, { type: 'register', role: 'web', id: 'intruder', sessionId: app.sessionId, protocolVersion: 2, uploadTicket: app.ticket });
  assert.equal(intruder.messages.at(-1).type, 'transfer-error');
  assert.equal(app.api.claimUploadPermission('OTHER001', app.ticket, 'intruder'), false);
  app.api.handleMessage(app.web, { type: 'unlock', sessionId: app.sessionId }); assert.equal(app.web.messages.at(-1).type, 'transfer-error');
  const duplicate = new Socket(); app.api.handleMessage(duplicate, { type: 'register', role: 'kiosk', id: 'other', sessionId: app.sessionId, protocolVersion: 2 });
  assert.equal(duplicate.messages.at(-1).code, 'DUPLICATE_KIOSK'); app.api.cleanup(app.web); app.api.cleanup(app.kiosk);
});
test('v2: nomes iguais são independentes; manifestos e repetições adulteradas falham', () => {
  const validator = new confirmedProtocol.ConfirmedManifest([...v2files, { ...v2files[0], fileId: 'file-b' }]);
  validator.accept(v2request(1, 'chunk', v2chunk())); validator.accept(v2request(2, 'file-complete', { fileId: 'file-a' }));
  validator.accept(v2request(3, 'chunk', { ...v2chunk(), fileId: 'file-b' })); validator.accept(v2request(4, 'file-complete', { fileId: 'file-b' }));
  validator.accept(v2request(5, 'finish'));
  for (const invalid of [[], [...v2files, ...v2files], [{ ...v2files[0], size: 20 * 1024 * 1024 + 1 }]]) assert.throws(() => new confirmedProtocol.ConfirmedManifest(invalid));
  for (const invalid of [v2request(1, 'chunk', v2chunk()), v2request(0, 'start', { files: [{ ...v2files[0], size: 4 }] })]) {
    const app = confirmedSession(); app.api.handleMessage(app.web, v2request(0, 'start', { files: v2files })); app.api.handleMessage(app.web, invalid);
    assert.equal(app.web.messages.at(-1).type, 'transfer-error'); app.api.cleanup(app.web); app.api.cleanup(app.kiosk);
  }
});
test('v2: orçamento por sessão bloqueia produtor e desconexão revoga o QR', () => {
  const app = confirmedSession(); app.api.handleMessage(app.web, v2request(0, 'start', { files: v2files })); acknowledge(app, 0);
  app.kiosk.bufferedAmount = confirmedProtocol.FLOW_LIMITS.frameBytes; app.api.handleMessage(app.web, v2request(1, 'chunk', v2chunk()));
  assert.equal(app.web.messages.at(-1).type, 'transfer-error'); app.api.cleanup(app.web); app.api.cleanup(app.kiosk);
  assert.equal(app.api.claimUploadPermission(app.sessionId, app.ticket, 'customer-' + app.sessionId), false);
});
test('v2: limite global de 32 transferências preserva as sessões já admitidas', () => {
  const api = bridge(), sessions = [];
  for (let i = 0; i < 33; i++) {
    const app = confirmedSession(api, 'LOAD' + String(i).padStart(4, '0')); sessions.push(app);
    api.handleMessage(app.web, v2request(0, 'start', { files: v2files })); assert.equal(app.web.messages.at(-1).type === 'transfer-error', i === 32);
  }
  acknowledge(sessions[0], 0); assert.equal(sessions[0].web.messages.at(-1).type, 'upload-ack');
  for (const app of sessions) { api.cleanup(app.web); api.cleanup(app.kiosk); }
});
test('v2: deadline aborta as duas pontas e não reabre a permissão', () => {
  const timers = [];
  const timed = load('src/confirmedTransfers.ts', { ws: { WebSocket }, 'node:crypto': require('node:crypto'), './confirmedProtocol': confirmedProtocol },
    { setTimeout(callback, delay) { timers.push({ callback, delay }); return { unref() {} }; }, clearTimeout() {} });
  const manager = new timed.ConfirmedTransfers((socket, message) => { socket.send(JSON.stringify(message)); return true; });
  const kiosk = new Socket(), web = new Socket(); manager.register(kiosk, 'kiosk', 'TIME0001', 'kiosk'); manager.unlock(kiosk);
  const ticket = kiosk.messages.at(-1).uploadTicket; assert.equal(manager.claim('TIME0001', ticket, 'customer'), true);
  manager.register(web, 'web', 'TIME0001', 'customer', ticket); manager.handle(web, v2request(0, 'start', { files: v2files }));
  assert.equal(timers.at(-1).delay, 60000); timers.at(-1).callback();
  assert.equal(web.messages.at(-1).code, 'ACK_TIMEOUT'); assert.equal(kiosk.messages.at(-1).code, 'ACK_TIMEOUT');
  assert.equal(manager.claim('TIME0001', ticket, 'customer'), false); manager.cleanup(kiosk); manager.cleanup(kiosk);
});

test('v2 integração: 100 fotos em duas sessões, destino lento não bloqueia o rápido nem duplica blocos', async t => {
  const api = bridge(), server = new WebSocketServer({ port: 0, host: '127.0.0.1', maxPayload: 128 * 1024 }), sockets = [];
  server.on('connection', ws => {
    ws.on('message', raw => api.handleMessage(ws, JSON.parse(raw.toString())));
    ws.on('close', () => api.cleanup(ws)); ws.on('error', () => { api.cleanup(ws); ws.terminate(); });
  });
  await once(server, 'listening');
  t.after(async () => { for (const ws of sockets) ws.terminate(); for (const ws of server.clients) ws.terminate(); await new Promise(resolve => server.close(resolve)); });
  async function connect(role, sessionId, ticket) {
    const ws = new WebSocket('ws://127.0.0.1:' + server.address().port); sockets.push(ws); await once(ws, 'open');
    const ready = message(ws, data => data.type === 'registered');
    ws.send(JSON.stringify({ type: 'register', role, id: role + '-' + sessionId, sessionId, protocolVersion: 2, uploadTicket: ticket }));
    await ready; return ws;
  }
  const finished = [];
  async function transfer(sessionId, delay) {
    const kiosk = await connect('kiosk', sessionId);
    const grant = message(kiosk, data => data.type === 'upload-permission'); kiosk.send(JSON.stringify({ type: 'unlock', sessionId }));
    const ticket = (await grant).uploadTicket; assert.equal(api.claimUploadPermission(sessionId, ticket, 'web-' + sessionId), true);
    const web = await connect('web', sessionId, ticket); let chunks = 0;
    kiosk.on('message', raw => { const request = JSON.parse(raw.toString()); if (request.type !== 'upload-request') return;
      if (request.operation === 'chunk') chunks++;
      setTimeout(() => kiosk.send(JSON.stringify({ type: 'upload-ack', protocolVersion: 2, transferId: request.transferId, requestId: request.requestId })), delay);
    });
    let id = 0;
    async function send(operation, extra) {
      const request = v2request(id++, operation, extra), ack = message(web, data => data.type === 'upload-ack' && data.requestId === request.requestId);
      web.send(JSON.stringify(request)); if (operation === 'chunk') web.send(JSON.stringify(request)); await ack;
    }
    const files = Array.from({ length: 100 }, (_, index) => ({ ...v2files[0], fileId: 'file-' + index }));
    await send('start', { files });
    for (const file of files) { await send('chunk', { ...v2chunk(), fileId: file.fileId }); await send('file-complete', { fileId: file.fileId }); }
    await send('finish'); assert.equal(chunks, 100); finished.push(sessionId); web.close(); kiosk.close();
  }
  await Promise.all([transfer('SLOW0001', 12), transfer('FAST0001', 0)]);
  assert.deepEqual(finished, ['FAST0001', 'SLOW0001']);
  const stats = api.confirmedUploadStats(); assert.equal(stats.activeTransfers, 0); assert.equal(stats.inFlightBytes, 0);
  assert.equal(stats.peakInFlightBytes <= 2 * confirmedProtocol.FLOW_LIMITS.frameBytes, true);
});

test('v2: conexão antiga e seu cleanup não revogam o novo QR após reset do totem', () => {
  const app = confirmedSession(); app.api.handleMessage(app.kiosk, { type: 'unlock', sessionId: app.sessionId });
  const ticket = app.kiosk.messages.at(-1).uploadTicket; assert.notEqual(ticket, app.ticket);
  app.api.handleMessage(app.web, v2request(0, 'start', { files: v2files }));
  assert.equal(app.api.claimUploadPermission(app.sessionId, ticket, 'new-customer'), true);
  const web = new Socket(); app.api.handleMessage(web, { type: 'register', role: 'web', id: 'new-customer', sessionId: app.sessionId, protocolVersion: 2, uploadTicket: ticket });
  assert.equal(web.messages.at(-1).type, 'registered');
  app.api.cleanup(app.web); app.api.cleanup(app.web);
  app.api.handleMessage(web, v2request(0, 'start', { files: v2files }));
  assert.equal(app.kiosk.messages.at(-1).operation, 'start'); app.api.cleanup(web); app.api.cleanup(app.kiosk);
});

test('HTTP: ticket inválido é recusado antes de alterar SQLite; dono autorizado recebe sua identidade', () => {
  const api = bridge(), kiosk = new Socket(), routes = new Map(), writes = [];
  api.handleMessage(kiosk, { type: 'register', role: 'kiosk', id: 'kiosk', sessionId: 'HTTP0001', protocolVersion: 2 });
  api.handleMessage(kiosk, { type: 'unlock', sessionId: 'HTTP0001' }); const ticket = kiosk.messages.at(-1).uploadTicket;
  const app = { use() {}, options() {}, get(path, handler) { routes.set(path, handler); }, post() {}, put() {} };
  const express = Object.assign(() => app, { json: () => () => {} });
  const server = Object.assign(new EventEmitter(), { listen(port, callback) { callback(); } });
  load('src/index.ts', { express, http: { createServer: () => server }, cors: () => () => {},
    ws: { WebSocketServer: class extends EventEmitter {} }, './wsBridge': api,
    './database/sqlite': { DataSource: class { getSessionByCode(code, callback) { callback(null, { id: 'synthetic-row', code, in_use: 0 }); }
      updateSession(...args) { writes.push(args); } } },
    './util/MsUntilNextMidnight': { msUntilNextMidnight: () => 1 },
  }, { process: { env: {} }, setTimeout() {}, setInterval() {} });
  function query(value, customerId = 'customer') {
    const response = { statusCode: 200, status(code) { this.statusCode = code; return this; }, json(body) { this.body = body; return this; } };
    routes.get('/session/:code/:customerId')({ params: { code: 'HTTP0001', customerId }, query: { uploadTicket: value } }, response); return response;
  }
  assert.equal(query('invalid').statusCode, 403); assert.equal(writes.length, 0);
  const result = query(ticket); assert.equal(result.statusCode, 200); assert.equal(result.body.customerId, 'customer'); assert.equal(writes.length, 1);
  assert.equal(query(ticket, 'another-customer').statusCode, 403); assert.equal(writes.length, 1); api.cleanup(kiosk);
});
test('v2: permissão expirada e mudança de versão no mesmo socket são recusadas', () => {
  let now = 0;
  const timed = load('src/confirmedTransfers.ts', { ws: { WebSocket }, 'node:crypto': require('node:crypto'), './confirmedProtocol': confirmedProtocol },
    { setTimeout, clearTimeout, Date: class extends Date { static now() { return now; } } });
  const manager = new timed.ConfirmedTransfers((socket, message) => { socket.send(JSON.stringify(message)); return true; });
  const kiosk = new Socket(); manager.register(kiosk, 'kiosk', 'TIME0001', 'kiosk'); manager.unlock(kiosk);
  const ticket = kiosk.messages.at(-1).uploadTicket; now = 600001; assert.equal(manager.claim('TIME0001', ticket, 'customer'), false); manager.cleanup(kiosk);
  const app = confirmedSession(); app.api.handleMessage(app.web, { type: 'register', role: 'web', sessionId: app.sessionId, id: 'customer-' + app.sessionId, protocolVersion: 1 });
  assert.equal(app.web.messages.at(-1).code, 'INVALID_REGISTRATION'); app.api.cleanup(app.web); app.api.cleanup(app.kiosk);
});
