// realtime.q — Real-time push via Perspective ViewOnUpdateResp
//
// Integrates with a KDB+ tickerplant or any table-update mechanism.
//
// Architecture:
//   1. The tickerplant calls `upd[tbl; data]` on this process (standard TP protocol).
//   2. realtime.q intercepts upd[], identifies which views depend on that table,
//      and pushes a ViewOnUpdateResp to each subscribed WebSocket client.
//   3. The Perspective JS client receives the notification, then issues a new
//      ViewToArrowReq to fetch the updated data.
//
// This avoids maintaining a separate "delta" computation — the client always
// re-fetches the current view data after an update notification.

// ---------------------------------------------------------------------------
// Subscription registry
//   .psp.rt.subs: table with columns:
//     handle   (long)   — WebSocket client handle
//     view_id  (string) — view entity_id
//     msg_id   (long)   — msg_id echoed back in ViewOnUpdateResp (matches subscription request)
//     table_id (string) — table the view is over (populated lazily on first upd call)
// ---------------------------------------------------------------------------
.psp.rt.subs: ([] handle:0h#0j; view_id:0h#""; msg_id:0h#0j; table_id:0h#"");

// ---------------------------------------------------------------------------
// .psp.rt.subscribe[handle; view_id; msg_id]
// Register a WebSocket client for real-time updates on a given view.
// Called from vs.q when ViewOnUpdateReq is received.
// ---------------------------------------------------------------------------
.psp.rt.subscribe: {[h; view_id; msg_id]
    // Remove any existing subscription for this (handle, view_id) pair
    .psp.rt.subs: delete from .psp.rt.subs
        where (handle = h) and (view_id ~ \: .psp.rt.subs`view_id);
    // Look up the table this view covers
    st: .psp.wss.sessions[h];
    tbl_id: $[(view_id in key st[`viewToTable]); st[`viewToTable;view_id]; ""];
    // Insert new subscription
    `.psp.rt.subs upsert (h; view_id; msg_id; tbl_id);
 };

// ---------------------------------------------------------------------------
// .psp.rt.unsubscribe[handle; view_id]
// Remove a subscription (called on ViewRemoveOnUpdateReq or view delete).
// ---------------------------------------------------------------------------
.psp.rt.unsubscribe: {[h; view_id]
    .psp.rt.subs: delete from .psp.rt.subs
        where (handle = h) and (view_id ~ \: .psp.rt.subs`view_id)
 };

// ---------------------------------------------------------------------------
// .psp.rt.notifyTable[tbl_sym]
// Push ViewOnUpdateResp to all clients subscribed on views of the given table.
// Called from upd[] after new data has been appended.
// ---------------------------------------------------------------------------
.psp.rt.notifyTable: {[tbl]
    tbl_str: string tbl;
    // Find all subscriptions on views over this table
    subs: select from .psp.rt.subs where table_id ~ \: tbl_str;
    if[0 = count subs; :()];

    // For each subscription, send an update notification
    {[row]
        h:       row[`handle];
        view_id: row[`view_id];
        msg_id:  row[`msg_id];
        // Use a fresh msg_id (client tracks by view_id, not msg_id, for updates)
        frame: .psp.pb.encodeViewOnUpdate[msg_id; view_id; 0; `byte$()];
        @[(neg h); frame;
          {.psp.log.warn"[rt] failed to push to handle ",string[x],": ",y}[h]]
    } each subs;
 };

// ---------------------------------------------------------------------------
// .psp.rt.cleanupHandle[handle]
// Remove all subscriptions for a disconnected client handle.
// Called from .z.wc (via wss.q) when a client disconnects.
// ---------------------------------------------------------------------------
.psp.rt.cleanupHandle: {[h]
    .psp.rt.subs: delete from .psp.rt.subs where handle = h
 };

// ---------------------------------------------------------------------------
// upd[] — Standard KDB+ tickerplant update handler
//
// Replace any existing upd with one that:
//   1. Appends data to the target table (standard RDB behaviour)
//   2. Notifies all subscribed Perspective clients
//
// If you already have a custom upd[], integrate .psp.rt.notifyTable[tbl]
// into your existing handler instead of using this definition.
// ---------------------------------------------------------------------------
upd: {[tbl; data]
    // Standard append (RDB-style): insert into global table
    tbl insert data;
    // Notify Perspective subscribers
    .psp.rt.notifyTable[tbl]
 };

// ---------------------------------------------------------------------------
// Hook into .z.wc to clean up subscriptions on disconnect
// (wss.q sets .z.wc; we extend it here)
// ---------------------------------------------------------------------------
.psp.rt._prevWc: @[value; `.z.wc; {[h] }];
.z.wc: {[h]
    .psp.rt.cleanupHandle[h];
    .psp.rt._prevWc h
 };
