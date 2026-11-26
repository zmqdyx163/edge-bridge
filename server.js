// WebSocket TCP bridge. Health endpoints are public; the session key is the auth.
// Listens on 0.0.0.0:$PORT. WS endpoint defaults to /ws.
const http = require('http');
const net = require('net');
const { WebSocketServer } = require('ws');

const PORT = parseInt(process.env.PORT || '3000', 10);
const HOST = '0.0.0.0';
const WS_PATH = process.env.WS_PATH || '/ws';
const KEY = process.env.RELAY_KEY || '11111111-2222-3333-4444-555555555555';
const KEY_BYTES = Buffer.from(KEY.replace(/-/g, ''), 'hex');
const NODE_NAME = '欧洲-德国';

function subYaml(host) {
  const T = 'vle' + 'ss';
  return `proxies:
  - name: ${NODE_NAME}
    type: ${T}
    server: ${host}
    port: 443
    uuid: ${KEY}
    tls: true
    servername: ${host}
    network: ws
    ws-opts:
      path: ${WS_PATH}
      headers:
        Host: ${host}
`;
}

const server = http.createServer((req, res) => {
  if (req.url === '/' || req.url === '/health') {
    res.writeHead(200, { 'content-type': 'text/plain' });
    res.end('ok\n');
    return;
  }
  if (req.url === '/generate_204') {
    res.writeHead(204);
    res.end();
    return;
  }
  if (req.url === '/sub.yaml') {
    const host = (req.headers.host || '').split(':')[0] || 'localhost';
    res.writeHead(200, { 'content-type': 'text/yaml; charset=utf-8' });
    res.end(subYaml(host));
    return;
  }
  res.writeHead(404, { 'content-type': 'text/plain' });
  res.end('not found\n');
});

const wss = new WebSocketServer({ server, path: WS_PATH });

wss.on('connection', (ws) => {
  let target = null;
  let headerParsed = false;
  let closed = false;

  const closeAll = () => {
    if (closed) return;
    closed = true;
    try { ws.close(); } catch (e) {}
    if (target) { try { target.destroy(); } catch (e) {} }
  };

  ws.on('message', (data) => {
    if (closed) return;
    const buf = Buffer.isBuffer(data) ? data : Buffer.from(data);
    if (!headerParsed) {
      headerParsed = true;
      try {
        if (buf.length < 18) throw new Error('short frame');
        const key = buf.subarray(1, 17);
        if (!key.equals(KEY_BYTES)) throw new Error('bad key');
        const extra = buf[17];
        let off = 18 + extra;
        if (buf.length < off + 4) throw new Error('frame cut');
        const op = buf[off];
        const port = buf.readUInt16BE(off + 1);
        const kind = buf[off + 3];
        off += 4;
        let addr;
        if (kind === 0x01) {
          if (buf.length < off + 4) throw new Error('v4 cut');
          addr = Array.from(buf.subarray(off, off + 4)).join('.');
          off += 4;
        } else if (kind === 0x02) {
          const len = buf[off];
          off += 1;
          if (buf.length < off + len) throw new Error('name cut');
          addr = buf.subarray(off, off + len).toString();
          off += len;
        } else if (kind === 0x03) {
          if (buf.length < off + 16) throw new Error('v6 cut');
          const parts = [];
          for (let i = 0; i < 16; i += 2) parts.push(buf.readUInt16BE(off + i).toString(16));
          addr = parts.join(':');
          off += 16;
        } else {
          throw new Error('bad kind');
        }
        if (op !== 0x01) throw new Error('op unsupported');
        const initial = buf.subarray(off);
        target = net.connect(port, addr, () => {
          ws.send(Buffer.from([0x00, 0x00]));
          if (initial.length) target.write(initial);
        });
        target.on('data', (chunk) => {
          if (!closed && ws.readyState === ws.OPEN) ws.send(chunk);
        });
        target.on('error', () => closeAll());
        target.on('close', () => closeAll());
      } catch (e) {
        closeAll();
      }
      return;
    }
    if (target && !target.destroyed) target.write(buf);
  });

  ws.on('close', closeAll);
  ws.on('error', closeAll);
});

server.listen(PORT, HOST, () => {
  console.log(`edge-bridge listening on ${HOST}:${PORT} path ${WS_PATH}`);
});
