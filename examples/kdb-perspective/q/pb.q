// pb.q — Protobuf FFI bindings via nanopb_bridge.so
//
// Loads the compiled C bridge and exposes typed Q wrappers for each
// encode/decode function.  All encode functions return a byte vector (0x...)
// suitable for sending directly over a WebSocket frame.

// ---------------------------------------------------------------------------
// Load the shared library
// ---------------------------------------------------------------------------
.psp.pb.lib: .psp.cfg.libpath 2: (`psp_init; 0);

// Decode one function handle per exported C symbol.
.psp.pb._decodeRequest:             .psp.cfg.libpath 2: (`psp_decode_request; 1);
.psp.pb._encodeEmptyResp:           .psp.cfg.libpath 2: (`psp_encode_empty_resp; 3);
.psp.pb._encodeServerError:         .psp.cfg.libpath 2: (`psp_encode_server_error; 3);
.psp.pb._encodeGetFeaturesResp:     .psp.cfg.libpath 2: (`psp_encode_get_features_resp; 3);
.psp.pb._encodeGetHostedTablesResp: .psp.cfg.libpath 2: (`psp_encode_get_hosted_tables_resp; 3);
.psp.pb._encodeTableSchemaResp:     .psp.cfg.libpath 2: (`psp_encode_table_schema_resp; 4);
.psp.pb._encodeTableSizeResp:       .psp.cfg.libpath 2: (`psp_encode_table_size_resp; 3);
.psp.pb._encodeTableMakePortResp:   .psp.cfg.libpath 2: (`psp_encode_table_make_port_resp; 3);
.psp.pb._encodeTableMakeViewResp:   .psp.cfg.libpath 2: (`psp_encode_table_make_view_resp; 3);
.psp.pb._encodeViewSchemaResp:      .psp.cfg.libpath 2: (`psp_encode_view_schema_resp; 4);
.psp.pb._encodeViewDimensionsResp:  .psp.cfg.libpath 2: (`psp_encode_view_dimensions_resp; 3);
.psp.pb._encodeViewGetConfigResp:   .psp.cfg.libpath 2: (`psp_encode_view_get_config_resp; 6);
.psp.pb._encodeViewColumnPathsResp: .psp.cfg.libpath 2: (`psp_encode_view_column_paths_resp; 3);
.psp.pb._encodeViewToArrowResp:     .psp.cfg.libpath 2: (`psp_encode_view_to_arrow_resp; 3);
.psp.pb._encodeViewOnUpdateResp:    .psp.cfg.libpath 2: (`psp_encode_view_on_update_resp; 4);

// ---------------------------------------------------------------------------
// Public wrappers with Q-idiomatic naming and argument validation
// ---------------------------------------------------------------------------

// Decode a raw protobuf Request byte vector.
// Returns: (msg_id:long; entity_id:string; req_type:int; payload:mixed)
.psp.pb.decodeRequest: {[bytes]
    .psp.pb._decodeRequest[bytes]
 };

// Encode an empty response (for void operations like view_delete, stubs).
// resp_type: int — field number of the response oneof (e.g. 11 for view_delete)
.psp.pb.encodeEmpty: {[msg_id; entity_id; resp_type]
    .psp.pb._encodeEmptyResp[msg_id; .psp.pb.toStr entity_id; "i"$resp_type]
 };

// Encode a server-error response.
.psp.pb.encodeError: {[msg_id; entity_id; message]
    .psp.pb._encodeServerError[msg_id; .psp.pb.toStr entity_id; .psp.pb.toStr message]
 };

// Encode GetFeaturesResp.
// flags: int bitmask  bit0=group_by, bit1=split_by, bit2=expressions,
//                      bit3=on_update, bit4=sort
.psp.pb.encodeGetFeatures: {[msg_id; entity_id; flags]
    .psp.pb._encodeGetFeaturesResp[msg_id; .psp.pb.toStr entity_id; "i"$flags]
 };

// Encode GetHostedTablesResp.
// table_ids: mixed list of char vectors (table name strings)
.psp.pb.encodeGetHostedTables: {[msg_id; entity_id; table_ids]
    .psp.pb._encodeGetHostedTablesResp[msg_id; .psp.pb.toStr entity_id; table_ids]
 };

// Encode TableSchemaResp.
// col_names: mixed list of char vectors
// col_types: int vector of ColumnType enum values (0..5)
.psp.pb.encodeTableSchema: {[msg_id; entity_id; col_names; col_types]
    .psp.pb._encodeTableSchemaResp[msg_id; .psp.pb.toStr entity_id; col_names; "i"$col_types]
 };

// Encode TableSizeResp.
.psp.pb.encodeTableSize: {[msg_id; entity_id; n]
    .psp.pb._encodeTableSizeResp[msg_id; .psp.pb.toStr entity_id; "j"$n]
 };

// Encode TableMakePortResp.
.psp.pb.encodeTableMakePort: {[msg_id; entity_id; port_id]
    .psp.pb._encodeTableMakePortResp[msg_id; .psp.pb.toStr entity_id; "i"$port_id]
 };

// Encode TableMakeViewResp.
.psp.pb.encodeTableMakeView: {[msg_id; entity_id; view_id]
    .psp.pb._encodeTableMakeViewResp[msg_id; .psp.pb.toStr entity_id; .psp.pb.toStr view_id]
 };

// Encode ViewSchemaResp.
// col_names: mixed list of char vectors
// col_types: int vector of ColumnType enum values
.psp.pb.encodeViewSchema: {[msg_id; entity_id; col_names; col_types]
    .psp.pb._encodeViewSchemaResp[msg_id; .psp.pb.toStr entity_id; col_names; "i"$col_types]
 };

// Encode ViewDimensionsResp.
// dims: long vector [num_table_rows; num_table_cols; num_view_rows; num_view_cols]
.psp.pb.encodeViewDimensions: {[msg_id; entity_id; dims]
    .psp.pb._encodeViewDimensionsResp[msg_id; .psp.pb.toStr entity_id; "j"$dims]
 };

// Encode ViewGetConfigResp.
.psp.pb.encodeViewGetConfig: {[msg_id; entity_id; group_by; split_by; columns; filter_op]
    .psp.pb._encodeViewGetConfigResp[msg_id; .psp.pb.toStr entity_id;
        group_by; split_by; columns; "i"$filter_op]
 };

// Encode ViewColumnPathsResp.
// paths: mixed list of char vectors
.psp.pb.encodeViewColumnPaths: {[msg_id; entity_id; paths]
    .psp.pb._encodeViewColumnPathsResp[msg_id; .psp.pb.toStr entity_id; paths]
 };

// Encode ViewToArrowResp.
// arrow_bytes: Q byte vector (raw Arrow IPC stream bytes)
.psp.pb.encodeViewToArrow: {[msg_id; entity_id; arrow_bytes]
    .psp.pb._encodeViewToArrowResp[msg_id; .psp.pb.toStr entity_id; arrow_bytes]
 };

// Encode ViewOnUpdateResp (real-time push notification).
.psp.pb.encodeViewOnUpdate: {[msg_id; entity_id; port_id; delta]
    .psp.pb._encodeViewOnUpdateResp[msg_id; .psp.pb.toStr entity_id; "i"$port_id; delta]
 };

// ---------------------------------------------------------------------------
// Utility: normalise entity_id to a Q char vector (string)
// Accepts: char vector, symbol, or string
// ---------------------------------------------------------------------------
.psp.pb.toStr: {[x]
    $[-11h = type x; string x;        // symbol
      10h  = type x; x;               // char vector (already correct)
      -10h = type x; enlist x;        // char atom
      string x]                       // fallback
 };
