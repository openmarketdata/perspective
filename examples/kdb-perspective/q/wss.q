// wss.q — Perspective WebSocket server (.z.ws / .z.wo / .z.wc)
//
// KDB+ 4.1 built-in WebSocket support:
//   .z.ws  — called on each incoming binary WebSocket frame (x = byte vector)
//   .z.wo  — called on client connect (.z.w = client handle)
//   .z.wc  — called on client disconnect (.z.w = client handle)
//   (neg h) data — sends a binary frame to handle h asynchronously

// Session registry: handle -> session state dict
.psp.wss.sessions: ()!();

// ---------------------------------------------------------------------------
// .z.ws — main WebSocket message handler
// ---------------------------------------------------------------------------
// x is the raw binary frame (byte vector 0x...) sent by the Perspective JS
// client.  Each frame is one complete protobuf-encoded Request message.
// ---------------------------------------------------------------------------
.z.ws:{
    h: .z.w;
    resp: .psp.vs.handle[h; x];
    // resp is () when no reply is needed (e.g. ignored request types)
    if[not 0 = count resp; (neg h) resp]
 };

// ---------------------------------------------------------------------------
// .z.wo — client connected
// ---------------------------------------------------------------------------
.z.wo:{
    h: .z.w;
    .psp.wss.sessions[h]: .psp.vs.newSession[];
    .psp.log.info"[wss] client connected: ",string h
 };

// ---------------------------------------------------------------------------
// .z.wc — client disconnected
// ---------------------------------------------------------------------------
.z.wc:{
    h: .z.w;
    .psp.vs.closeSession[h];
    .psp.wss.sessions: h _ .psp.wss.sessions;
    .psp.log.info"[wss] client disconnected: ",string h
 };

// ---------------------------------------------------------------------------
// Start the HTTP/WebSocket listener
// ---------------------------------------------------------------------------
.psp.wss.start:{[]
    p: .psp.cfg.port;
    system"p ",string p;
    .psp.log.info"[wss] Perspective KDB+ server listening on ws://0.0.0.0:",string p
 };

// ---------------------------------------------------------------------------
// Minimal structured logger (timestamped, stderr-safe)
// ---------------------------------------------------------------------------
.psp.log.info:  {-1 (string .z.p)," INFO  "  ,x;};
.psp.log.warn:  {-1 (string .z.p)," WARN  "  ,x;};
.psp.log.error: {-1 (string .z.p)," ERROR "  ,x;};
