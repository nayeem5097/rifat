// Tarneeb online relay server. No dependencies, just Node 18+.
const http = require("http"), fs = require("fs"), path = require("path");
const PORT = process.env.PORT || 3000;
const GRACE = +process.env.GRACE_MS || 8000;          // how long a dropped player keeps their seat
const PUB = path.join(__dirname, "public");
const rooms = new Map();                                // room name -> { peers: Map(id -> {tok,res,timer}) }
const MIME = { ".html": "text/html; charset=utf-8", ".svg": "image/svg+xml", ".webmanifest": "application/manifest+json", ".png": "image/png", ".js": "text/javascript", ".json": "application/json" };
const okName = s => /^[a-z0-9][a-z0-9_.-]{0,47}$/.test(s || "");
const okId = s => /^[A-Za-z0-9_-]{8,64}$/.test(s || "");
const send = (res, ev, obj) => res.write(`event: ${ev}\ndata: ${JSON.stringify(obj)}\n\n`);
const alive = r => [...r.peers].filter(([, p]) => p.res).map(([id]) => id);
const bcast = (r, ev, obj, except) => { for (const [id, p] of r.peers) if (p.res && id !== except) send(p.res, ev, obj); };

http.createServer((req, res) => {
  const u = new URL(req.url, "http://x");
  const name = u.searchParams.get("room"), id = u.searchParams.get("id"), tok = u.searchParams.get("tok");

  if (u.pathname === "/events") {
    if (!okName(name) || !okId(id) || !okId(tok)) return res.writeHead(400).end();
    let r = rooms.get(name);
    if (!r) { if (rooms.size >= 500) return res.writeHead(503).end(); r = { peers: new Map() }; rooms.set(name, r); }
    let p = r.peers.get(id);
    if (p && p.tok !== tok) return res.writeHead(403).end();
    if (!p && r.peers.size >= 12) return res.writeHead(409).end();
    const rejoin = !!(p && p.timer);
    if (!p) { p = { tok }; r.peers.set(id, p); }
    if (p.res) try { p.res.end(); } catch (e) {}
    clearTimeout(p.timer); p.timer = null; p.res = res;
    res.writeHead(200, { "Content-Type": "text/event-stream", "Cache-Control": "no-cache, no-transform", "Connection": "keep-alive", "X-Accel-Buffering": "no" });
    send(res, "hello", { id, peers: alive(r) });
    if (!rejoin) bcast(r, "peers", { all: alive(r), joined: [id], left: [] }, id);
    const hb = setInterval(() => res.write(":hb\n\n"), 20000);
    req.on("close", () => {
      clearInterval(hb);
      if (p.res !== res) return;
      p.res = null;
      p.timer = setTimeout(() => {
        r.peers.delete(id);
        bcast(r, "peers", { all: alive(r), joined: [], left: [id] });
        if (!r.peers.size) rooms.delete(name);
      }, GRACE);
    });
    return;
  }

  if (u.pathname === "/emit" && req.method === "POST") {
    let body = "";
    req.on("data", c => { body += c; if (body.length > 8192) req.destroy(); });
    req.on("end", () => {
      const r = rooms.get(name), p = r && r.peers.get(id);
      if (!p || p.tok !== tok) return res.writeHead(403).end();
      let m; try { m = JSON.parse(body); } catch (e) { return res.writeHead(400).end(); }
      if (!m || typeof m.topic !== "string" || m.topic.length > 20) return res.writeHead(400).end();
      bcast(r, "msg", { topic: m.topic, data: m.data, peer: id }, id);
      res.writeHead(204).end();
    });
    return;
  }

  let f = u.pathname === "/" ? "/index.html" : u.pathname;
  const fp = path.join(PUB, path.normalize(f));
  if (!fp.startsWith(PUB)) return res.writeHead(404).end();
  fs.readFile(fp, (e, b) => {
    if (e) return res.writeHead(404).end("Not found");
    res.writeHead(200, { "Content-Type": MIME[path.extname(fp)] || "application/octet-stream" }).end(b);
  });
}).listen(PORT, () => console.log("Tarneeb server on http://localhost:" + PORT));
