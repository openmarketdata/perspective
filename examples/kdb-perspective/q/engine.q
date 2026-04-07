// engine.q — Q query engine: translates Perspective ViewConfig to functional Q
//
// Core responsibilities:
//   1. List exposed tables (.psp.engine.listTables)
//   2. Return table schema (.psp.engine.tableSchema)
//   3. Create / delete named views in the .psp.v namespace
//   4. Execute a view with viewport slicing for ViewToArrow
//
// Views are stored lazily as a function in `.psp.v` that, when called with
// a viewport dict, returns the result table.  This avoids re-computing the
// full result on every schema/dimension query.

// Namespace that holds live view results.
\d .psp.v
\d .

// ---------------------------------------------------------------------------
// .psp.engine.listTables[]
// Return a list of table symbols visible from the configured namespaces.
// ---------------------------------------------------------------------------
.psp.engine.listTables: {[]
    ns: .psp.cfg.namespaces;
    tbls: raze {[n]
        $[n = `.;
            // Default namespace: use tables[] builtin
            tables[];
            // Named namespace: list variables that are tables
            {x where {98h = type value x} each x} key[n]
        ]
    } each ns;
    tbls
 };

// ---------------------------------------------------------------------------
// .psp.engine.tableSchema[tbl_sym]
// Returns a sym!int dict mapping column name → Perspective ColumnType int.
// ---------------------------------------------------------------------------
.psp.engine.tableSchema: {[tbl]
    m: meta tbl;
    .psp.t.schemaFromMeta[m]
 };

// ---------------------------------------------------------------------------
// .psp.engine.tableSize[tbl_sym]
// Return row count as a long.
// ---------------------------------------------------------------------------
.psp.engine.tableSize: {[tbl]
    count value tbl
 };

// ---------------------------------------------------------------------------
// .psp.engine.makeView[tbl_sym; view_id_str; view_config_dict]
// Register a named view.  Stores a parameterised select closure in .psp.v.
// ---------------------------------------------------------------------------
.psp.engine.makeView: {[tbl; view_id; vc]
    // Store a closure: vc and tbl are captured, viewport is the parameter
    .psp.v[`$view_id]: (tbl; vc)
 };

// ---------------------------------------------------------------------------
// .psp.engine.deleteView[view_id_str]
// Remove a view registration.
// ---------------------------------------------------------------------------
.psp.engine.deleteView: {[view_id]
    .psp.v _: `$view_id
 };

// ---------------------------------------------------------------------------
// .psp.engine.viewSize[view_id_str]
// Return the number of rows in the view (full result, no viewport).
// ---------------------------------------------------------------------------
.psp.engine.viewSize: {[view_id]
    entry: .psp.v[`$view_id];
    if[() ~ entry; :0];
    tbl: entry 0;
    vc:  entry 1;
    // Execute without viewport to get total row count
    full: .psp.engine.execSelect[tbl; vc];
    count full
 };

// ---------------------------------------------------------------------------
// .psp.engine.viewSchema[view_id_str; tbl_sym; vc_dict]
// Return the schema of the view result (col→ColumnType dict).
// For v1 (no group_by/split_by), schema = subset of table schema.
// ---------------------------------------------------------------------------
.psp.engine.viewSchema: {[view_id; tbl; vc]
    tbl_schema: .psp.engine.tableSchema[tbl];
    sel_cols: vc[`columns];
    $[0 = count sel_cols;
        tbl_schema;                        // all columns
        sel_cols_sym: `$each sel_cols;
        (sel_cols_sym inter key tbl_schema) ! tbl_schema[sel_cols_sym inter key tbl_schema]
    ]
 };

// ---------------------------------------------------------------------------
// .psp.engine.viewGetData[view_id_str; vc_dict; viewport_dict]
// Execute the view and apply the viewport slice.
// viewport_dict: `start_row`start_col`end_row`end_col ! (int;int;int;int)
//   (0x80000000i = null / not set)
// Returns: Q table (may be keyed if group_by; unkeyed with 0!)
// ---------------------------------------------------------------------------
.psp.engine.viewGetData: {[view_id; vc; vp]
    entry: .psp.v[`$view_id];
    if[() ~ entry; :([] )];
    tbl: entry 0;
    vc2: entry 1;

    // Execute the Q select with filters, sorts, column selection
    full: .psp.engine.execSelect[tbl; vc2];

    // Apply row viewport
    sr: vp[`start_row]; er: vp[`end_row];
    null_int: 0x80000000i;
    sr: $[sr = null_int; 0j; "j"$sr];
    er: $[er = null_int; count full; "j"$er];
    rows: (er - sr) sublist sr _ full;

    // Apply column viewport (start_col/end_col)
    sc: vp[`start_col]; ec: vp[`end_col];
    if[(not sc = null_int) and not ec = null_int;
        all_cols: cols rows;
        sub_cols: (ec - sc) # sc _ all_cols;
        rows: sub_cols # rows];

    rows
 };

// ---------------------------------------------------------------------------
// .psp.engine.execSelect[tbl_sym; vc_dict]
// Build and execute a functional Q select from a ViewConfig dict.
// Returns an unkeyed table.
// ---------------------------------------------------------------------------
.psp.engine.execSelect: {[tbl; vc]
    // --- Column selection ---
    sel_cols_raw: vc[`columns];
    sel_cols: $[0 = count sel_cols_raw;
        cols value tbl;                // all columns
        `$each sel_cols_raw];          // specified columns (as syms)

    // Build select dict: col_sym → col_sym (identity projection)
    sel_dict: sel_cols ! sel_cols;

    // --- Where clause ---
    where_list: .psp.engine.buildWhere[vc[`filter]; vc[`filter_op]];

    // --- Execute flat select (group_by disabled in v1) ---
    result: ?[tbl; where_list; 0b; sel_dict];

    // --- Apply sorts ---
    result: .psp.engine.applySort[result; vc[`sort]];

    result
 };

// ---------------------------------------------------------------------------
// .psp.engine.buildWhere[filter_list; filter_op]
// Translate a list of filter dicts to a functional Q where clause list.
// filter_op: 0i = AND (default), 1i = OR
// Returns: list of parse-tree triples suitable for ?[tbl; where_list; ...]
// ---------------------------------------------------------------------------
.psp.engine.buildWhere: {[filters; filter_op]
    if[0 = count filters; :()];

    conditions: .psp.engine.buildFilter each filters;
    // Remove null conditions (unsupported operators)
    conditions: conditions where not conditions ~\: (::);

    $[0 = count conditions; ();
      1 = count conditions; conditions;
      // AND: return list of conditions (Q evaluates all with implicit AND)
      // OR:  wrap in (|/) applied to boolean columns — complex; for v1 AND only
      conditions]
 };

// ---------------------------------------------------------------------------
// .psp.engine.buildFilter[filter_dict]
// Build a single functional Q where clause expression from one filter dict.
// filter_dict: `column`op`value!(string; string; value)
// Returns a Q parse tree triple or (::) if unsupported.
// ---------------------------------------------------------------------------
.psp.engine.buildFilter: {[f]
    col: `$f[`column];
    op:  f[`op];
    raw: f[`value];

    // Scalar value coercion (raw may be a Q mixed list, boolean, float, or string)
    val: $[10h = type raw; // char vector (string)
               $[11h = type col; `$raw; raw];   // sym column → cast to sym
           -9h = type raw; raw;           // float atom
           -1h = type raw; raw;           // boolean atom
           0h = type raw;                 // mixed list → take first element
               $[0 < count raw; first raw; (::)];
           raw];

    $[op ~ "==";         (=; col; enlist val);
      op ~ "!=";         (<>; col; enlist val);
      op ~ ">";          (>; col; val);
      op ~ ">=";         (>=; col; val);
      op ~ "<";          (<; col; val);
      op ~ "<=";         (<=; col; val);
      op ~ "contains";   (like; col; "*",($[10h = type val; val; string val]),"*");
      op ~ "not contains";(not; (like; col; "*",($[10h = type val; val; string val]),"*"));
      op ~ "begins with";(like; col; ($[10h = type val; val; string val]),"*");
      op ~ "ends with";  (like; col; "*",$[10h = type val; val; string val]);
      op ~ "is null";    (null; col);
      op ~ "is not null";(not; (null; col));
      op ~ "in";         (in; col; $[0h = type raw; raw; enlist raw]);
      op ~ "not in";     (not; (in; col; $[0h = type raw; raw; enlist raw]));
      (::)]             // unsupported operator
 };

// ---------------------------------------------------------------------------
// .psp.engine.applySort[tbl; sort_list]
// Apply a list of sort dicts to the result table.
// sort_dict: `column`op!(string; int)
//   SortOp: 0=SORT_NONE, 1=SORT_ASC, 2=SORT_DESC
// Q applies sorts in reverse order (last sort = primary key).
// ---------------------------------------------------------------------------
.psp.engine.applySort: {[tbl; sorts]
    if[0 = count sorts; :tbl];

    // Process sorts in reverse (so first sort in list has highest priority)
    rev: reverse sorts;
    {[t; s]
        col: `$s[`column];
        op:  s[`op];
        $[op = 1i; col xasc t;
          op = 2i; col xdesc t;
          t]   // SORT_NONE — no sort
    }[tbl] / rev   // fold over reversed sort list
 };
