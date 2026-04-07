// psp.q — Perspective KDB+ WebSocket Server entrypoint
//
// Usage:
//   q psp.q -p 8765
//   or from within a running q session:
//   \l psp.q
//
// The server starts automatically when this file is loaded.
// Tables present in the default namespace (.) are exposed to the browser.
//
// Configuration (override before loading or via environment):
//   .psp.cfg.port      : WebSocket port (default 8765)
//   .psp.cfg.libpath   : path to nanopb_bridge.so (default ./lib/nanopb_bridge.so)
//   .psp.cfg.arrowkdb  : path to arrowkdb.q (default arrowkdb.q on QPATH)
//   .psp.cfg.namespaces: list of namespace syms to expose (default enlist `.)

// ---------------------------------------------------------------------------
// Default configuration
// ---------------------------------------------------------------------------
.psp.cfg.port:       8765;
.psp.cfg.libpath:    `:lib/nanopb_bridge.so;
.psp.cfg.arrowkdb:   `arrowkdb.q;
.psp.cfg.namespaces: enlist `.;

// ---------------------------------------------------------------------------
// Override config from environment variables (if set)
// ---------------------------------------------------------------------------
if[not ""~getenv`PSP_PORT;      .psp.cfg.port:       "J"$getenv`PSP_PORT];
if[not ""~getenv`PSP_LIB;       .psp.cfg.libpath:    `$getenv`PSP_LIB];
if[not ""~getenv`PSP_ARROWKDB;  .psp.cfg.arrowkdb:   `$getenv`PSP_ARROWKDB];

// ---------------------------------------------------------------------------
// Load modules in dependency order
// Resolve the directory containing this script so that \l works regardless
// of the working directory from which q was invoked.
// ---------------------------------------------------------------------------
// .z.f is the script path supplied to q (e.g. "q/psp.q" or "/abs/path/psp.q").
// We convert to an absolute hsym, strip the filename, and cd into that directory.
.psp.SRCDIR: 1_string first ` vs hsym .z.f;
if[not "/"~first .psp.SRCDIR;
    .psp.SRCDIR: (system"pwd"),"/",raze{(c _ x),c:last where x="/"} .psp.SRCDIR];
system "cd ",.psp.SRCDIR;

\l type_map.q
\l arrow.q
\l pb.q
\l engine.q
\l vs.q
\l wss.q
\l realtime.q

// ---------------------------------------------------------------------------
// Start the WebSocket listener
// ---------------------------------------------------------------------------
.psp.wss.start[];
