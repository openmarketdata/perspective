// vs.q — VirtualServer dispatch layer
//
// Decodes each incoming protobuf Request, dispatches to the appropriate
// handler, encodes the Response, and manages per-session view state.
//
// Session state (keyed by WebSocket handle):
//   .psp.wss.sessions[h]: `viewToTable`viewConfigs`viewSchemas!
//                             (()!(); ()!(); ()!())
//
// The view registry maps view_id (string) to table_id (string), view config
// and cached schema, mirroring the Rust VirtualServer implementation.

// ---------------------------------------------------------------------------
// Session management
// ---------------------------------------------------------------------------

.psp.vs.newSession: {[]
    `viewToTable`viewConfigs`viewSchemas!(()!(); ()!(); ()!())
 };

.psp.vs.closeSession: {[h]
    // Nothing to do: session dict cleaned up by wss.wc caller
 };

// ---------------------------------------------------------------------------
// Main dispatch entry point
// Called by .z.ws for each incoming binary WebSocket frame.
// Returns: byte vector to send back, or () to send nothing.
// ---------------------------------------------------------------------------
.psp.vs.handle: {[h; bytes]
    // Decode outer envelope
    r: .[.psp.pb.decodeRequest; enlist bytes; {.psp.log.error"[vs] decode error: ",x; ()}];
    if[0 = count r; :()];

    msg_id:    r 0;
    entity_id: r 1;
    req_type:  r 2;
    payload:   r 3;

    st: .psp.wss.sessions[h];

    // Dispatch to handler — errors are caught and returned as ServerError
    res: .[.psp.vs.dispatch; (msg_id; entity_id; req_type; payload; h; st);
           {[m; e; i; p; h2; st2; err]
               .psp.log.error"[vs] handler error req=",string[i]," err=",err;
               .psp.pb.encodeError[m; e; err]
           }[msg_id; entity_id; req_type; payload; h; st]];
    res
 };

// ---------------------------------------------------------------------------
// Dispatcher: (msg_id; entity_id; req_type; payload; handle; session) -> bytes
// req_type field numbers match perspective.proto Request oneof tags.
// ---------------------------------------------------------------------------
.psp.vs.dispatch: {[msg_id; entity_id; req_type; payload; h; st]

    // 3 — GetFeatures
    $[req_type = 3;
        .psp.vs.getFeatures[msg_id; entity_id];

    // 4 — GetHostedTables
    req_type = 4;
        .psp.vs.getHostedTables[msg_id; entity_id];

    // 5 — TableMakePort
    req_type = 5;
        .psp.pb.encodeTableMakePort[msg_id; entity_id; 0];

    // 6 — TableMakeView
    req_type = 6;
        .psp.vs.tableMakeView[msg_id; entity_id; payload; h; st];

    // 7 — TableSchema
    req_type = 7;
        .psp.vs.tableSchema[msg_id; entity_id];

    // 8 — TableSize
    req_type = 8;
        .psp.vs.tableSize[msg_id; entity_id];

    // 9 — TableValidateExpr (stub: always return float type)
    req_type = 9;
        .psp.vs.tableValidateExpr[msg_id; entity_id; payload];

    // 10 — ViewColumnPaths
    req_type = 10;
        .psp.vs.viewColumnPaths[msg_id; entity_id; h; payload];

    // 11 — ViewDelete
    req_type = 11;
        .psp.vs.viewDelete[msg_id; entity_id; h; st];

    // 12 — ViewDimensions
    req_type = 12;
        .psp.vs.viewDimensions[msg_id; entity_id; h; st];

    // 13 — ViewExpressionSchema (stub: empty map)
    req_type = 13;
        .psp.pb.encodeViewSchema[msg_id; entity_id; (); "i"$()];

    // 14 — ViewGetConfig
    req_type = 14;
        .psp.vs.viewGetConfig[msg_id; entity_id; h; st];

    // 15 — ViewSchema
    req_type = 15;
        .psp.vs.viewSchema[msg_id; entity_id; h; st];

    // 16 — ViewToArrow
    req_type = 16;
        .psp.vs.viewToArrow[msg_id; entity_id; payload; h; st];

    // 17 — ServerSystemInfo (stub)
    req_type = 17;
        .psp.pb.encodeEmpty[msg_id; entity_id; 17];

    // 21 — ViewOnUpdate (register update callback — stub: immediate empty ack)
    req_type = 21;
        .psp.vs.viewOnUpdate[msg_id; entity_id; h; st];

    // 22 — ViewRemoveOnUpdate (stub)
    req_type = 22;
        .psp.pb.encodeEmpty[msg_id; entity_id; 22];

    // 29 — TableOnDelete (stub)
    req_type = 29;
        .psp.pb.encodeEmpty[msg_id; entity_id; 29];

    // 30 — TableRemoveDelete (stub)
    req_type = 30;
        .psp.pb.encodeEmpty[msg_id; entity_id; 30];

    // 34 — ViewOnDelete (stub)
    req_type = 34;
        .psp.pb.encodeEmpty[msg_id; entity_id; 34];

    // 35 — ViewRemoveDelete (stub)
    req_type = 35;
        .psp.pb.encodeEmpty[msg_id; entity_id; 35];

    // 37 — RemoveHostedTablesUpdate (stub)
    req_type = 37;
        .psp.pb.encodeEmpty[msg_id; entity_id; 37];

    // Unknown / unimplemented
    [.psp.log.warn"[vs] unhandled req_type=",string req_type;
     .psp.pb.encodeError[msg_id; entity_id;
         "unhandled request type: ",string req_type]]
    ]
 };

// ---------------------------------------------------------------------------
// Handler implementations
// ---------------------------------------------------------------------------

// 3 — GetFeatures
// Advertise which optional features are enabled.
// flags bitmask: bit0=group_by, bit1=split_by, bit2=expressions,
//                bit3=on_update, bit4=sort
// For v1: sort only (bit4 set = 16i)
.psp.vs.getFeatures: {[msg_id; entity_id]
    .psp.pb.encodeGetFeatures[msg_id; entity_id; 16i]   // sort=true only
 };

// 4 — GetHostedTables
// Return all tables visible across the configured namespaces.
.psp.vs.getHostedTables: {[msg_id; entity_id]
    tbl_ids: .psp.engine.listTables[];
    strs: {x} each string each tbl_ids;
    .psp.pb.encodeGetHostedTables[msg_id; entity_id; strs]
 };

// 7 — TableSchema
.psp.vs.tableSchema: {[msg_id; entity_id]
    tbl: `$entity_id;
    schema: .psp.engine.tableSchema[tbl];
    names: {x} each string each key schema;
    types: "i"$value schema;
    .psp.pb.encodeTableSchema[msg_id; entity_id; names; types]
 };

// 8 — TableSize
.psp.vs.tableSize: {[msg_id; entity_id]
    n: .psp.engine.tableSize[`$entity_id];
    .psp.pb.encodeTableSize[msg_id; entity_id; n]
 };

// 9 — TableValidateExpr (stub: always reports float)
.psp.vs.tableValidateExpr: {[msg_id; entity_id; payload]
    // payload[0] is a dict of col_name!expression strings
    // Return all as ColumnType FLOAT (4) for v1
    expr_dict: payload 0;
    names: {x} each string each key expr_dict;
    types: (count names)#4i;
    .psp.pb.encodeViewSchema[msg_id; entity_id; names; types]
 };

// 6 — TableMakeView
// Creates a logical view over a table and stores its config in session state.
.psp.vs.tableMakeView: {[msg_id; entity_id; payload; h; st]
    view_id: payload 0;
    vc: payload 1;
    tbl_id: entity_id;

    // Register view in session state
    st[`viewToTable;  view_id]: tbl_id;
    st[`viewConfigs;  view_id]: vc;
    // Invalidate any cached schema
    st[`viewSchemas]: (view_id) _ st[`viewSchemas];
    .psp.wss.sessions[h]: st;

    // Create the functional Q view (a named select result stored in .psp.v)
    .psp.engine.makeView[`$tbl_id; view_id; vc];

    .psp.pb.encodeTableMakeView[msg_id; entity_id; view_id]
 };

// 11 — ViewDelete
.psp.vs.viewDelete: {[msg_id; entity_id; h; st]
    view_id: entity_id;
    .psp.engine.deleteView[view_id];
    // Remove from session
    st[`viewToTable]:  view_id _ st[`viewToTable];
    st[`viewConfigs]:  view_id _ st[`viewConfigs];
    st[`viewSchemas]:  view_id _ st[`viewSchemas];
    .psp.wss.sessions[h]: st;
    .psp.pb.encodeEmpty[msg_id; entity_id; 11]
 };

// 12 — ViewDimensions
.psp.vs.viewDimensions: {[msg_id; entity_id; h; st]
    view_id: entity_id;
    tbl_id: st[`viewToTable;view_id];
    num_table_rows:  .psp.engine.tableSize[`$tbl_id];
    tbl_schema:      .psp.engine.tableSchema[`$tbl_id];
    num_table_cols:  count tbl_schema;
    view_schema:     .psp.vs.getCachedViewSchema[view_id; h; st];
    num_view_cols:   count view_schema;
    num_view_rows:   .psp.engine.viewSize[view_id];
    dims: num_table_rows,num_table_cols,num_view_rows,num_view_cols;
    .psp.pb.encodeViewDimensions[msg_id; entity_id; "j"$dims]
 };

// 14 — ViewGetConfig
.psp.vs.viewGetConfig: {[msg_id; entity_id; h; st]
    view_id: entity_id;
    vc: st[`viewConfigs;view_id];
    gb: $[0 < count vc[`group_by]; vc[`group_by]; ()];
    sb: $[0 < count vc[`split_by]; vc[`split_by]; ()];
    cl: $[0 < count vc[`columns]; vc[`columns]; ()];
    fo: vc[`filter_op];
    .psp.pb.encodeViewGetConfig[msg_id; entity_id; gb; sb; cl; fo]
 };

// 15 — ViewSchema
.psp.vs.viewSchema: {[msg_id; entity_id; h; st]
    view_id: entity_id;
    schema: .psp.vs.getCachedViewSchema[view_id; h; st];
    names: {x} each string each key schema;
    types: "i"$value schema;
    .psp.pb.encodeViewSchema[msg_id; entity_id; names; types]
 };

// 10 — ViewColumnPaths
.psp.vs.viewColumnPaths: {[msg_id; entity_id; h; payload]
    view_id: entity_id;
    st: .psp.wss.sessions[h];
    schema: .psp.vs.getCachedViewSchema[view_id; h; st];
    paths: {x} each string each key schema;
    // Apply start_col/end_col viewport if specified
    sc: payload 0; ec: payload 1;
    if[not sc = 0x80000000i; paths: sc _ paths];
    if[not ec = 0x80000000i;
        rem: $[sc = 0x80000000i; 0; sc];
        paths: (ec - rem) # paths];
    .psp.pb.encodeViewColumnPaths[msg_id; entity_id; paths]
 };

// 16 — ViewToArrow
.psp.vs.viewToArrow: {[msg_id; entity_id; payload; h; st]
    view_id: entity_id;
    vc: st[`viewConfigs;view_id];
    // payload = (start_row; start_col; end_row; end_col) — all ints
    viewport: `start_row`start_col`end_row`end_col ! payload;

    // Fetch result table from view engine
    result_tbl: .psp.engine.viewGetData[view_id; vc; viewport];

    // Serialise to Arrow IPC using arrowkdb
    arrow_bytes: .psp.arrow.encode[result_tbl];

    .psp.pb.encodeViewToArrow[msg_id; entity_id; arrow_bytes]
 };

// 21 — ViewOnUpdate
// Register handle as a subscriber for real-time push on this view.
// The actual push is done by realtime.q when table data changes.
.psp.vs.viewOnUpdate: {[msg_id; entity_id; h; st]
    view_id: entity_id;
    .psp.rt.subscribe[h; view_id; msg_id];
    // Ack with an empty ViewOnUpdateResp (no delta, port_id=0)
    .psp.pb.encodeEmpty[msg_id; entity_id; 21]
 };

// ---------------------------------------------------------------------------
// View schema cache (session-level, invalidated on view create/delete)
// ---------------------------------------------------------------------------
.psp.vs.getCachedViewSchema: {[view_id; h; st]
    if[view_id in key st[`viewSchemas];
        :st[`viewSchemas;view_id]];
    // Compute and cache
    tbl_id: `$st[`viewToTable;view_id];
    vc: st[`viewConfigs;view_id];
    schema: .psp.engine.viewSchema[view_id; tbl_id; vc];
    st[`viewSchemas;view_id]: schema;
    .psp.wss.sessions[h]: st;
    schema
 };
