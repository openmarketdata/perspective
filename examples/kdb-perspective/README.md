# Perspective KDB+/Q Direct WebSocket Server

A pure-Q library that connects a KDB+ 4.1 process **directly** to the Perspective JS viewer via WebSocket, with **no Python or intermediate bridge process**.

```
Browser                                     KDB+ Process (Q)
┌─────────────────────┐                     ┌──────────────────────────────┐
│  perspective-viewer │  WebSocket (binary  │  .psp.ws namespace           │
│  JS client          │  protobuf frames)   │  ├─ wss.q   WebSocket server │
│  perspective.       │◄──────────────────►│  ├─ pb.q    protobuf FFI     │
│   websocket(url)    │                     │  ├─ vs.q    VirtualServer    │
└─────────────────────┘                     │  ├─ engine.q  Q query engine │
                                            │  ├─ type_map.q type mapping  │
                                            │  ├─ arrow.q  Arrow IPC       │
                                            │  └─ realtime.q live push     │
                                            └──────────────────────────────┘
```

The JS client calls `perspective.websocket("ws://kdb-host:8765")` exactly as it does against a Rust/Python Perspective server — no client changes are required.

---

## Prerequisites

| Component | Version | Notes |
|-----------|---------|-------|
| KDB+ | **4.1** | Requires WebSocket support (`.z.ws`) |
| Python | ≥ 3.8 | Only needed for build; not at runtime |
| nanopb | ≥ 0.4 | `pip install nanopb` |
| arrowkdb | ≥ 3.0 | [KxSystems/arrowkdb](https://github.com/KxSystems/arrowkdb) installed in `$QHOME` |
| C compiler | gcc / clang | For building the nanopb bridge |

---

## Build

```bash
# 1. Install nanopb Python package (build-time only)
pip install nanopb

# 2. Build the nanopb protobuf bridge shared library
cd examples/kdb-perspective
make

# Output: lib/nanopb_bridge.so  (Linux)
#         lib/nanopb_bridge.dylib  (macOS)
```

`make` performs three steps automatically:
1. Generates `proto/perspective.pb.c` and `proto/perspective.pb.h` from `../../rust/perspective-client/perspective.proto` using the nanopb generator.
2. Compiles the nanopb C runtime + generated codec + `src/nanopb_bridge.c` into a shared library.
3. The library is loaded at runtime by KDB+ via the `2:` FFI.

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `QHOME` | `~/q` | KDB+ installation root (must contain `<arch>/k.h`) |

---

## Running

```bash
# Start the Perspective KDB+ server on port 8765 (default)
cd examples/kdb-perspective
q q/psp.q -p 8765
```

Configuration can be overridden before loading or via environment:

```q
// Change port
.psp.cfg.port: 9000;

// Path to the compiled shared library
.psp.cfg.libpath: `:/path/to/nanopb_bridge.so;

// Path to arrowkdb.q (default: searches QPATH)
.psp.cfg.arrowkdb: `:/opt/kdb/arrowkdb.q;

// Which namespaces to expose as tables (default: default namespace only)
.psp.cfg.namespaces: `..`myns;
```

---

## Browser Client

Open `index.html` in any browser. Adjust the server URL if needed:

```
http://localhost:8080/index.html?host=kdb-server&port=8765&table=trade
```

Or connect programmatically from any Perspective JS application:

```js
import perspective from "@finos/perspective";

const server = await perspective.websocket("ws://kdb-host:8765");
const table  = await server.open_table("trade");
await viewer.load(table);
```

No changes to the JS client are needed. The viewer works identically to connecting to a Rust/Python Perspective server.

---

## Architecture

### Wire Protocol

Each WebSocket binary frame carries exactly one protobuf-encoded `Request` or `Response` message (no length prefix). This is the standard Perspective WebSocket framing as implemented by `websocket.ts` in the JS client.

### Component 1 — WebSocket Server (`q/wss.q`)

Uses KDB+ 4.1 built-in WebSocket support:
- `\p 8765` starts the HTTP/WebSocket listener
- `.z.ws` callback dispatches each incoming binary frame
- `(neg h) resp` sends a binary response frame back to handle `h`

### Component 2 — Protobuf Codec (`src/nanopb_bridge.c` + `q/pb.q`)

**nanopb** (Option B): A C shared library compiled from `nanopb_bridge.c` is loaded by Q via the `2:` FFI. It provides:

- `psp_decode_request(bytes)` — decodes a full `Request` protobuf message into a Q mixed list `(msg_id; entity_id; req_type; payload)`
- Per-type encode functions (`psp_encode_*_resp`) — encode specific `Response` sub-messages from Q values into a byte vector

The nanopb generator creates `perspective.pb.c` / `perspective.pb.h` from the real `perspective.proto` schema in the repository. Static buffer sizes are controlled by `perspective.nanopb.options`.

### Component 3 — VirtualServer (`q/vs.q`)

Mirrors the Rust `VirtualServer` in `rust/perspective-client/src/rust/virtual_server/server.rs`:
- Dispatches on `req_type` (protobuf oneof field number)
- Maintains per-session view state: `viewToTable`, `viewConfigs`, `viewSchemas`
- Handles 18 request types; stubs the remainder

### Component 4 — Q Query Engine (`q/engine.q`)

Translates Perspective `ViewConfig` to functional Q selects:
- Column projection, filter (`where` clause), sort (`xasc`/`xdesc`)
- Filter operators: `==`, `!=`, `>`, `>=`, `<`, `<=`, `contains`, `begins with`, `ends with`, `is null`, `is not null`, `in`, `not in`

**v1 feature set** (reported via `GetFeaturesResp`):
- ✅ Column selection
- ✅ Filter (all basic operators)
- ✅ Sort
- ❌ Group-by / split-by (disabled; set feature flag false)
- ❌ Expressions (disabled)

### Component 5 — Type Mapping (`q/type_map.q`)

Maps KDB+ type characters to Perspective `ColumnType` enum values using the `meta` table output and `.Q.t` lookup.

### Component 6 — Arrow IPC (`q/arrow.q`)

**arrowkdb** (Option A): Uses `arrowkdb.kx.writeArrow[options; table]` to produce Arrow IPC stream bytes from a Q table. The schema is inferred automatically from KDB+ column types.

### Component 7 — Real-Time Push (`q/realtime.q`)

Integrates with the standard KDB+ tickerplant protocol:
1. `upd[tbl; data]` appends new rows and calls `.psp.rt.notifyTable[tbl]`
2. All WebSocket clients subscribed to views over that table receive a `ViewOnUpdateResp`
3. The Perspective JS client re-fetches data via `ViewToArrowReq`

---

## Limitations (v1)

| Feature | Status | Notes |
|---------|--------|-------|
| Group-by / pivoting | ❌ Disabled | Implement `split_by` with Q `exec … by` for v2 |
| Computed expressions | ❌ Disabled | Add Q expression parsing for v2 |
| Multi-client concurrency | ⚠️ Sequential | Q is single-threaded; use secondary threads (`-s N`) for v2 |
| Large Arrow payloads | ⚠️ Heap allocated | `psp_encode_view_to_arrow_resp` uses `malloc`; monitor for large views |
| TLS / WSS | ⚠️ Not configured | Enable with KDB+ `-E 1` flag and certificates |
| Authentication | ❌ None | Add `.z.pw` handler for username/password checking |

---

## File Structure

```
examples/kdb-perspective/
├── Makefile                       # Build system
├── perspective.nanopb.options     # nanopb static buffer size constraints
├── index.html                     # Browser demo
├── README.md                      # This file
├── src/
│   └── nanopb_bridge.c            # C protobuf bridge (nanopb FFI)
└── q/
    ├── psp.q                      # Entrypoint — loads modules, starts server
    ├── wss.q                      # WebSocket server (.z.ws / .z.wo / .z.wc)
    ├── pb.q                       # Q wrappers around nanopb FFI functions
    ├── vs.q                       # VirtualServer dispatch
    ├── engine.q                   # ViewConfig → functional Q select
    ├── type_map.q                 # KDB+ type ↔ Perspective ColumnType
    ├── arrow.q                    # Arrow IPC serialization via arrowkdb
    └── realtime.q                 # Tickerplant upd[] → ViewOnUpdateResp push
```
