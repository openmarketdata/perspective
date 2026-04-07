// arrow.q — Arrow IPC serialization via arrowkdb
//
// Requires arrowkdb 3.x installed in $QHOME.
// https://github.com/KxSystems/arrowkdb
//
// arrowkdb provides two APIs:
//   - High-level: arrowkdb.kx.*  (native kdb+ table ↔ Arrow conversion)
//   - Low-level:  arrowkdb.dt.* / arrowkdb.fd.* / arrowkdb.sc.* (explicit schema)
//
// We use the high-level arrowkdb.kx.writeArrow API which infers the Arrow
// schema automatically from the kdb+ column types.  This produces Arrow IPC
// stream format, which is exactly what the Perspective JS client expects.

// ---------------------------------------------------------------------------
// Load arrowkdb
// ---------------------------------------------------------------------------
@[system; "l ",.psp.pb.toStr .psp.cfg.arrowkdb;
    {.psp.log.error"[arrow] failed to load arrowkdb: ",x,
        " — set .psp.cfg.arrowkdb to the correct path"; '`arrowkdb_load_failed}];

// Verify the high-level API is available
if[not `kx in key arrowkdb;
    '`arrowkdb_kx_api_missing];

// ---------------------------------------------------------------------------
// Column-type coercions required before serialising to Arrow
//
// arrowkdb.kx.writeArrow handles most type conversions automatically, but
// a few KDB+ types need preprocessing:
//   - symbol  (11h): arrowkdb maps to utf8 string (handled automatically)
//   - guid    (2h):  arrowkdb maps to fixedsizebinary[16]
//   - char    (10h): arrowkdb maps to uint8; cast to string for Perspective
//   - month   (13h): arrowkdb maps as int32; leave as-is (DATE mapping)
// ---------------------------------------------------------------------------

// Pre-process a single column before Arrow serialization.
// Returns the column with any necessary type coercions applied.
.psp.arrow.prepareCol: {[col_sym; col_data]
    t: type col_data;
    $[t = 10h;  // char vector → string (symbol then string)
        string `$col_data;
      t = 2h;   // guid vector — leave as-is (arrowkdb handles it)
        col_data;
      col_data]  // all other types: pass through
 };

// Pre-process an entire table before encoding.
.psp.arrow.prepareTable: {[tbl]
    col_syms: cols tbl;
    new_cols: .psp.arrow.prepareCol'[col_syms; flip tbl];
    flip col_syms ! new_cols
 };

// ---------------------------------------------------------------------------
// .psp.arrow.encode[tbl]
// Serialize a Q table to Arrow IPC stream bytes.
// Returns a byte vector (0x...) suitable for embedding in ViewToArrowResp.
// ---------------------------------------------------------------------------
.psp.arrow.encode: {[tbl]
    if[0 = count cols tbl; :`byte$()];    // empty schema → empty bytes

    // Prepare table (type coercions)
    prepared: .psp.arrow.prepareTable[tbl];

    // arrowkdb options dict: empty for default settings
    opts: ()!();

    // arrowkdb.kx.writeArrow[options; table] → byte vector
    bytes: .[arrowkdb.kx.writeArrow; (opts; prepared);
             {.psp.log.error"[arrow] encode failed: ",x; `byte$()}];
    bytes
 };

// ---------------------------------------------------------------------------
// .psp.arrow.encodeWithSchema[schema_dict; tbl]
// Low-level path: use explicit arrowkdb schema when fine-grained control is
// needed (e.g. overriding the inferred Arrow type for a column).
//
// schema_dict: sym!int dict of col_name→ColumnType (Perspective int enum)
// tbl:         Q table
// ---------------------------------------------------------------------------
.psp.arrow.colTypeToArrow: {[ct]
    // Perspective ColumnType → arrowkdb DataType id
    $[ct = 0i; arrowkdb.dt.utf8[];         // STRING  → Arrow Utf8
      ct = 1i; arrowkdb.dt.date32[];       // DATE    → Arrow Date32
      ct = 2i; arrowkdb.dt.timestamp[`milli; ""];  // DATETIME → Timestamp<ms>
      ct = 3i; arrowkdb.dt.int32[];        // INTEGER → Arrow Int32
      ct = 4i; arrowkdb.dt.float64[];      // FLOAT   → Arrow Float64
      ct = 5i; arrowkdb.dt.boolean[];      // BOOLEAN → Arrow Boolean
      arrowkdb.dt.utf8[]]                  // fallback → Utf8
 };

.psp.arrow.encodeWithSchema: {[schema_dict; tbl]
    if[0 = count schema_dict; :`byte$()];

    col_names: key schema_dict;
    col_types: value schema_dict;

    // Build arrowkdb field IDs
    fields: arrowkdb.fd.field'[col_names; .psp.arrow.colTypeToArrow each col_types];

    // Build arrowkdb schema ID
    schema_id: arrowkdb.sc.schema[fields];

    // Build array data: list of column arrays matching schema order
    arrays: {[c; tbl2] tbl2[c]} each col_names cross enlist tbl;

    opts: ()!();
    bytes: .[arrowkdb.ipc.writeArrow; (schema_id; arrays; opts);
             {.psp.log.error"[arrow] encodeWithSchema failed: ",x; `byte$()}];
    bytes
 };
