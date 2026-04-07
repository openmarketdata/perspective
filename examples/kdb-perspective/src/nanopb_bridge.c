/*
 * nanopb_bridge.c
 *
 * KDB+/Q ↔ Perspective protobuf codec bridge.
 *
 * Compiled as a shared library and loaded by Q via the 2: FFI:
 *
 *   lib: `:/path/to/nanopb_bridge.so 2:
 *   pspDecodeRequest: lib (`psp_decode_request; 1)
 *
 * Each WebSocket binary frame from the Perspective JS client is a raw
 * protobuf-encoded Request message (NO length prefix).  The bridge:
 *   1. Decodes a Request byte vector → Q mixed list
 *   2. Encodes a Response from Q data → Q byte vector
 *
 * Build:  see Makefile
 *
 * K API reference: https://code.kx.com/q/interfaces/c-client-for-q/
 */

/* ---- nanopb + protobuf generated header ---------------------------------- */
#include "perspective.pb.h"
#include "pb_encode.h"
#include "pb_decode.h"

/* ---- KDB+ C API ---------------------------------------------------------- */
#define KXVER 3
#include "k.h"

#include <stdint.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>

/* =========================================================================
 * Internal helpers
 * ========================================================================= */

/* Create a Q string (char vector, type 10) from a C string. */
static K kdb_string(const char *s) {
    return s ? kpn((S)s, (J)strlen(s)) : kpn("", 0);
}

/* Create a Q byte vector (type 4) from a raw buffer. */
static K kdb_bytes(const uint8_t *buf, size_t len) {
    K r = ktn(4, (J)len);
    memcpy(kG(r), buf, len);
    return r;
}

/* Append an encoded message to a K byte vector (in-place realloc). */
static K encode_to_kbytes(const pb_msgdesc_t *fields, const void *msg) {
    uint8_t buf[65536];
    pb_ostream_t stream = pb_ostream_from_buffer(buf, sizeof(buf));
    if (!pb_encode(&stream, fields, msg)) {
        return krr((S)"nanopb encode failed");
    }
    return kdb_bytes(buf, stream.bytes_written);
}

/* Convert a ColumnType enum (proto int32) to the Q symbol used in type_map.q */
static const char *coltype_sym(int32_t ct) {
    switch (ct) {
        case 0:  return "string";
        case 1:  return "date";
        case 2:  return "datetime";
        case 3:  return "integer";
        case 4:  return "float";
        case 5:  return "boolean";
        default: return "string";
    }
}

/* =========================================================================
 * psp_decode_request
 *
 * Signature (Q):  psp_decode_request[bytes]
 * Returns a Q mixed list: (msg_id; entity_id; req_type; payload)
 *   msg_id      -7h  (long)
 *   entity_id   10h  (char vector / string)
 *   req_type    -6h  (int)  — the oneof field number (3..38)
 *   payload     0    (mixed list) — type-specific decoded sub-fields
 *                                   (empty list () for request types
 *                                    with no sub-fields)
 * ========================================================================= */
K psp_decode_request(K bytes) {
    if (!bytes || bytes->t != 4) return krr((S)"psp_decode_request: expected byte vector");

    perspective_proto_Request req = perspective_proto_Request_init_zero;

    pb_istream_t stream = pb_istream_from_buffer(kG(bytes), (size_t)bytes->n);
    if (!pb_decode(&stream, perspective_proto_Request_fields, &req)) {
        return krr((S)"psp_decode_request: protobuf decode failed");
    }

    /* Determine which oneof field is set and build the payload mixed list.
     * For simple (empty) sub-messages, payload is `()`.
     * For sub-messages with fields, payload is a Q mixed list of those fields.
     */
    int req_type = 0;
    K payload = knk(0);  /* default: empty list */

    switch (req.which_client_req) {

        /* ---- Empty sub-messages (no fields needed in Q) ---- */
        case perspective_proto_Request_get_features_req_tag:
            req_type = 3; break;

        case perspective_proto_Request_get_hosted_tables_req_tag:
            req_type = 4;
            r0(payload);
            payload = knk(1, kb(req.client_req.get_hosted_tables_req.subscribe ? 1 : 0));
            break;

        case perspective_proto_Request_table_make_port_req_tag:
            req_type = 5; break;

        case perspective_proto_Request_table_make_view_req_tag: {
            req_type = 6;
            r0(payload);

            /* view_id */
            K view_id = kdb_string(req.client_req.table_make_view_req.view_id);

            /* ViewConfig — decode into a Q dict */
            perspective_proto_ViewConfig *vc = &req.client_req.table_make_view_req.config;

            /* group_by: list of strings */
            K group_by = ktn(0, vc->group_by_count);
            for (int i = 0; i < (int)vc->group_by_count; i++)
                kK(group_by)[i] = kdb_string(vc->group_by[i]);

            /* split_by: list of strings */
            K split_by = ktn(0, vc->split_by_count);
            for (int i = 0; i < (int)vc->split_by_count; i++)
                kK(split_by)[i] = kdb_string(vc->split_by[i]);

            /* columns (ColumnsUpdate) */
            K columns;
            if (vc->has_columns &&
                vc->columns.which_opt_columns ==
                    perspective_proto_ColumnsUpdate_columns_tag) {
                columns = ktn(0, vc->columns.opt_columns.columns.columns_count);
                for (int i = 0;
                     i < (int)vc->columns.opt_columns.columns.columns_count; i++) {
                    kK(columns)[i] = kdb_string(
                        vc->columns.opt_columns.columns.columns[i]);
                }
            } else {
                columns = ktn(0, 0);  /* empty → use all columns */
            }

            /* filter: list of filter dicts (each: `column`op`value!(str;str;mixed)) */
            K filters = ktn(0, vc->filter_count);
            for (int fi = 0; fi < (int)vc->filter_count; fi++) {
                perspective_proto_ViewConfig_Filter *f = &vc->filter[fi];

                /* value: first scalar value (simplified: we take value[0]) */
                K val;
                if (f->value_count > 0) {
                    perspective_proto_Scalar *s = &f->value[0];
                    switch (s->which_scalar) {
                        case perspective_proto_Scalar_bool_tag:
                            val = kb(s->scalar.bool_); break;
                        case perspective_proto_Scalar_float_tag:
                            val = kf(s->scalar.float_); break;
                        case perspective_proto_Scalar_string_tag:
                            val = kdb_string(s->scalar.string_); break;
                        default:
                            val = ktn(0, 0); /* null */
                    }
                } else {
                    val = ktn(0, 0);
                }

                K keys   = ktn(11, 3);  /* sym list */
                kS(keys)[0] = ss("column");
                kS(keys)[1] = ss("op");
                kS(keys)[2] = ss("value");
                K vals = knk(3,
                    kdb_string(f->column),
                    kdb_string(f->op),
                    val);
                kK(filters)[fi] = xD(keys, vals);
            }

            /* sort: list of sort dicts (`column`op!(str;int)) */
            K sorts = ktn(0, vc->sort_count);
            for (int si = 0; si < (int)vc->sort_count; si++) {
                perspective_proto_ViewConfig_Sort *s = &vc->sort[si];
                K keys = ktn(11, 2);
                kS(keys)[0] = ss("column");
                kS(keys)[1] = ss("op");
                K vals = knk(2,
                    kdb_string(s->column),
                    ki((I)s->op));
                kK(sorts)[si] = xD(keys, vals);
            }

            /* expressions: Q dict of string!string (col_name → expression) */
            K expr_keys = ktn(0, vc->expressions_count);
            K expr_vals = ktn(0, vc->expressions_count);
            for (int ei = 0; ei < (int)vc->expressions_count; ei++) {
                kK(expr_keys)[ei] = kdb_string(vc->expressions[ei].key);
                kK(expr_vals)[ei] = kdb_string(vc->expressions[ei].value);
            }
            K expressions = xD(expr_keys, expr_vals);

            /* aggregates: Q dict of string!(list of string) */
            K agg_keys = ktn(0, vc->aggregates_count);
            K agg_vals = ktn(0, vc->aggregates_count);
            for (int ai = 0; ai < (int)vc->aggregates_count; ai++) {
                kK(agg_keys)[ai] = kdb_string(vc->aggregates[ai].key);
                /* list of aggregation strings */
                K agg_list = ktn(0, vc->aggregates[ai].value.aggregations_count);
                for (int ali = 0;
                     ali < (int)vc->aggregates[ai].value.aggregations_count; ali++) {
                    kK(agg_list)[ali] = kdb_string(
                        vc->aggregates[ai].value.aggregations[ali]);
                }
                kK(agg_vals)[ai] = agg_list;
            }
            K aggregates = xD(agg_keys, agg_vals);

            /* filter_op (0=AND, 1=OR), group_by_depth */
            K filter_op = ki((I)vc->filter_op);
            K gbdepth   = vc->has_group_by_depth
                        ? ki((I)vc->group_by_depth)
                        : ki(0x80000000);  /* Q int null */

            /* Pack ViewConfig as a Q dict keyed on symbols */
            K vc_keys = ktn(11, 9);
            kS(vc_keys)[0] = ss("group_by");
            kS(vc_keys)[1] = ss("split_by");
            kS(vc_keys)[2] = ss("columns");
            kS(vc_keys)[3] = ss("filter");
            kS(vc_keys)[4] = ss("sort");
            kS(vc_keys)[5] = ss("expressions");
            kS(vc_keys)[6] = ss("aggregates");
            kS(vc_keys)[7] = ss("filter_op");
            kS(vc_keys)[8] = ss("group_by_depth");

            K vc_vals = knk(9,
                group_by, split_by, columns, filters, sorts,
                expressions, aggregates, filter_op, gbdepth);

            K vc_dict = xD(vc_keys, vc_vals);

            payload = knk(2, view_id, vc_dict);
            break;
        }

        case perspective_proto_Request_table_schema_req_tag:
            req_type = 7; break;

        case perspective_proto_Request_table_size_req_tag:
            req_type = 8; break;

        case perspective_proto_Request_table_validate_expr_req_tag: {
            req_type = 9;
            r0(payload);
            perspective_proto_TableValidateExprReq *tvr =
                &req.client_req.table_validate_expr_req;
            K k = ktn(0, tvr->column_to_expr_count);
            K v = ktn(0, tvr->column_to_expr_count);
            for (int i = 0; i < (int)tvr->column_to_expr_count; i++) {
                kK(k)[i] = kdb_string(tvr->column_to_expr[i].key);
                kK(v)[i] = kdb_string(tvr->column_to_expr[i].value);
            }
            payload = knk(1, xD(k, v));
            break;
        }

        case perspective_proto_Request_view_column_paths_req_tag: {
            req_type = 10;
            r0(payload);
            perspective_proto_ViewColumnPathsReq *vcp =
                &req.client_req.view_column_paths_req;
            K start_col = vcp->has_start_col ? ki((I)vcp->start_col)
                                             : ki(0x80000000);
            K end_col   = vcp->has_end_col   ? ki((I)vcp->end_col)
                                             : ki(0x80000000);
            payload = knk(2, start_col, end_col);
            break;
        }

        case perspective_proto_Request_view_delete_req_tag:
            req_type = 11; break;

        case perspective_proto_Request_view_dimensions_req_tag:
            req_type = 12; break;

        case perspective_proto_Request_view_expression_schema_req_tag:
            req_type = 13; break;

        case perspective_proto_Request_view_get_config_req_tag:
            req_type = 14; break;

        case perspective_proto_Request_view_schema_req_tag:
            req_type = 15; break;

        case perspective_proto_Request_view_to_arrow_req_tag: {
            req_type = 16;
            r0(payload);
            perspective_proto_ViewToArrowReq *vta =
                &req.client_req.view_to_arrow_req;
            perspective_proto_ViewPort *vp = &vta->viewport;
            /* viewport: `start_row`start_col`end_row`end_col */
            K sr = vp->has_start_row ? ki((I)vp->start_row) : ki(0x80000000);
            K sc = vp->has_start_col ? ki((I)vp->start_col) : ki(0x80000000);
            K er = vp->has_end_row   ? ki((I)vp->end_row)   : ki(0x80000000);
            K ec = vp->has_end_col   ? ki((I)vp->end_col)   : ki(0x80000000);
            payload = knk(4, sr, sc, er, ec);
            break;
        }

        case perspective_proto_Request_server_system_info_req_tag:
            req_type = 17; break;

        case perspective_proto_Request_view_get_min_max_req_tag: {
            req_type = 20;
            r0(payload);
            K col = kdb_string(
                req.client_req.view_get_min_max_req.column_name);
            payload = knk(1, col);
            break;
        }

        case perspective_proto_Request_view_on_update_req_tag:
            req_type = 21; break;

        case perspective_proto_Request_view_remove_on_update_req_tag:
            req_type = 22;
            r0(payload);
            payload = knk(1, ki((I)req.client_req.view_remove_on_update_req.id));
            break;

        case perspective_proto_Request_view_to_columns_string_req_tag:
            req_type = 24; break;

        case perspective_proto_Request_view_to_csv_req_tag:
            req_type = 25; break;

        case perspective_proto_Request_view_to_rows_string_req_tag:
            req_type = 26; break;

        case perspective_proto_Request_make_table_req_tag:
            req_type = 27; break;

        case perspective_proto_Request_table_delete_req_tag:
            req_type = 28; break;

        case perspective_proto_Request_table_on_delete_req_tag:
            req_type = 29; break;

        case perspective_proto_Request_table_remove_delete_req_tag:
            req_type = 30; break;

        case perspective_proto_Request_view_on_delete_req_tag:
            req_type = 34; break;

        case perspective_proto_Request_view_remove_delete_req_tag:
            req_type = 35; break;

        case perspective_proto_Request_view_to_ndjson_string_req_tag:
            req_type = 36; break;

        case perspective_proto_Request_remove_hosted_tables_update_req_tag:
            req_type = 37; break;

        default:
            req_type = -1;  /* unknown */
            break;
    }

    return knk(4,
        kj((J)req.msg_id),
        kdb_string(req.entity_id),
        ki(req_type),
        payload);
}

/* =========================================================================
 * Encode helpers — one function per response type.
 * All functions return a Q byte vector (type 4).
 *
 * Convention:  psp_encode_<name>(msg_id_long, entity_id_str, ...)
 *              where msg_id_long is -7h and entity_id_str is 10h.
 * ========================================================================= */

/* Copy a Q char vector (or sym) into a fixed C char array safely. */
static void qstr_to_cbuf(K qs, char *buf, size_t bufsz) {
    buf[0] = '\0';
    if (!qs) return;
    if (qs->t == -11) {              /* symbol atom */
        strncpy(buf, qs->s, bufsz - 1);
    } else if (qs->t == 10) {        /* char vector */
        size_t n = (size_t)qs->n < bufsz - 1 ? (size_t)qs->n : bufsz - 1;
        memcpy(buf, kC(qs), n);
        buf[n] = '\0';
    }
}

static void fill_envelope(perspective_proto_Response *resp, K msg_id, K entity_id) {
    resp->msg_id = (uint32_t)(msg_id->j);
    qstr_to_cbuf(entity_id, resp->entity_id, sizeof(resp->entity_id));
}

/* ---- Empty responses (ViewDelete, ViewOnUpdate stubs, etc.) -------------- */

/*
 * psp_encode_empty_resp(msg_id, entity_id, resp_type_int)
 * resp_type_int: Q int (-6h) containing the response field number.
 */
K psp_encode_empty_resp(K msg_id, K entity_id, K resp_type) {
    perspective_proto_Response resp = perspective_proto_Response_init_zero;
    fill_envelope(&resp, msg_id, entity_id);

    int rt = resp_type->i;
    switch (rt) {
        case 5:  resp.which_client_resp = perspective_proto_Response_table_make_port_resp_tag; break;
        case 6:  resp.which_client_resp = perspective_proto_Response_table_make_view_resp_tag; break;
        case 11: resp.which_client_resp = perspective_proto_Response_view_delete_resp_tag; break;
        case 17: resp.which_client_resp = perspective_proto_Response_server_system_info_resp_tag; break;
        case 21: resp.which_client_resp = perspective_proto_Response_view_on_update_resp_tag; break;
        case 22: resp.which_client_resp = perspective_proto_Response_view_remove_on_update_resp_tag; break;
        case 27: resp.which_client_resp = perspective_proto_Response_make_table_resp_tag; break;
        case 29: resp.which_client_resp = perspective_proto_Response_table_on_delete_resp_tag; break;
        case 30: resp.which_client_resp = perspective_proto_Response_table_remove_delete_resp_tag; break;
        case 34: resp.which_client_resp = perspective_proto_Response_view_on_delete_resp_tag; break;
        case 35: resp.which_client_resp = perspective_proto_Response_view_remove_delete_resp_tag; break;
        case 37: resp.which_client_resp = perspective_proto_Response_remove_hosted_tables_update_resp_tag; break;
        default: break;
    }
    return encode_to_kbytes(perspective_proto_Response_fields, &resp);
}

/* ---- Server error -------------------------------------------------------- */

/* psp_encode_server_error(msg_id, entity_id, message_str) */
K psp_encode_server_error(K msg_id, K entity_id, K message) {
    perspective_proto_Response resp = perspective_proto_Response_init_zero;
    fill_envelope(&resp, msg_id, entity_id);
    resp.which_client_resp = perspective_proto_Response_server_error_tag;
    qstr_to_cbuf(message, resp.client_resp.server_error.message,
                 sizeof(resp.client_resp.server_error.message));
    return encode_to_kbytes(perspective_proto_Response_fields, &resp);
}

/* ---- GetFeatures --------------------------------------------------------- */

/*
 * psp_encode_get_features_resp(msg_id, entity_id, flags_int)
 *   flags_int: -6h bitmask  bit0=group_by, bit1=split_by, bit2=expressions,
 *                            bit3=on_update, bit4=sort
 */
K psp_encode_get_features_resp(K msg_id, K entity_id, K flags) {
    perspective_proto_Response resp = perspective_proto_Response_init_zero;
    fill_envelope(&resp, msg_id, entity_id);
    resp.which_client_resp = perspective_proto_Response_get_features_resp_tag;

    perspective_proto_GetFeaturesResp *f = &resp.client_resp.get_features_resp;
    int fl = flags->i;
    f->group_by   = (fl >> 0) & 1;
    f->split_by   = (fl >> 1) & 1;
    f->expressions = (fl >> 2) & 1;
    f->on_update  = (fl >> 3) & 1;
    f->sort       = (fl >> 4) & 1;

    /* Supported filter ops: == != > >= < <= (for all types) */
    const char *ops[] = {"==","!=",">",">=","<","<="};
    int nops = 6;
    /* ColumnType enum values: 0=STRING 1=DATE 2=DATETIME 3=INTEGER 4=FLOAT 5=BOOLEAN */
    for (int ct = 0; ct < 6; ct++) {
        f->filter_ops[ct].key = ct;
        for (int oi = 0; oi < nops; oi++) {
            strncpy(f->filter_ops[ct].value.options[oi], ops[oi], 63);
        }
        f->filter_ops[ct].value.options_count = nops;
    }
    f->filter_ops_count = 6;

    /* Aggregate functions by column type */
    const char *num_aggs[]  = {"sum","count","avg","min","max","first","last"};
    const char *str_aggs[]  = {"count","first","last"};
    int n_num = 7, n_str = 3;

    for (int ct = 0; ct < 6; ct++) {
        f->aggregates[ct].key = ct;
        perspective_proto_GetFeaturesResp_AggregateOptions *ao =
            &f->aggregates[ct].value;
        if (ct == 0 /* STRING */ || ct == 1 /* DATE */ || ct == 2 /* DATETIME */ ||
            ct == 5 /* BOOLEAN */) {
            for (int i = 0; i < n_str; i++) {
                strncpy(ao->aggregates[i].name, str_aggs[i], 63);
                ao->aggregates[i].args_count = 0;
            }
            ao->aggregates_count = n_str;
        } else {
            for (int i = 0; i < n_num; i++) {
                strncpy(ao->aggregates[i].name, num_aggs[i], 63);
                ao->aggregates[i].args_count = 0;
            }
            ao->aggregates_count = n_num;
        }
    }
    f->aggregates_count = 6;

    /* GroupRollupMode: ROLLUP=0, FLAT=1, TOTAL=2 */
    f->group_rollup_mode[0] = 0;
    f->group_rollup_mode[1] = 1;
    f->group_rollup_mode[2] = 2;
    f->group_rollup_mode_count = 3;

    return encode_to_kbytes(perspective_proto_Response_fields, &resp);
}

/* ---- GetHostedTables ----------------------------------------------------- */

/*
 * psp_encode_get_hosted_tables_resp(msg_id, entity_id, table_ids)
 *   table_ids: Q mixed list of char vectors (table name strings)
 */
K psp_encode_get_hosted_tables_resp(K msg_id, K entity_id, K table_ids) {
    perspective_proto_Response resp = perspective_proto_Response_init_zero;
    fill_envelope(&resp, msg_id, entity_id);
    resp.which_client_resp = perspective_proto_Response_get_hosted_tables_resp_tag;

    perspective_proto_GetHostedTablesResp *r = &resp.client_resp.get_hosted_tables_resp;
    int n = (int)table_ids->n;
    if (n > (int)(sizeof(r->table_infos) / sizeof(r->table_infos[0])))
        n = (int)(sizeof(r->table_infos) / sizeof(r->table_infos[0]));

    for (int i = 0; i < n; i++) {
        qstr_to_cbuf(kK(table_ids)[i], r->table_infos[i].entity_id,
                     sizeof(r->table_infos[i].entity_id));
    }
    r->table_infos_count = n;

    return encode_to_kbytes(perspective_proto_Response_fields, &resp);
}

/* ---- TableSchema --------------------------------------------------------- */

/*
 * psp_encode_table_schema_resp(msg_id, entity_id, col_names, col_types)
 *   col_names: Q mixed list of char vectors
 *   col_types: Q int vector (-6h list), values = ColumnType enum (0..5)
 */
K psp_encode_table_schema_resp(K msg_id, K entity_id, K col_names, K col_types) {
    perspective_proto_Response resp = perspective_proto_Response_init_zero;
    fill_envelope(&resp, msg_id, entity_id);
    resp.which_client_resp = perspective_proto_Response_table_schema_resp_tag;

    perspective_proto_TableSchemaResp *r = &resp.client_resp.table_schema_resp;
    r->has_schema = true;
    int n = (int)col_names->n;
    int maxn = (int)(sizeof(r->schema.schema) / sizeof(r->schema.schema[0]));
    if (n > maxn) n = maxn;

    for (int i = 0; i < n; i++) {
        qstr_to_cbuf(kK(col_names)[i], r->schema.schema[i].name,
                     sizeof(r->schema.schema[i].name));
        r->schema.schema[i].type = kI(col_types)[i];
    }
    r->schema.schema_count = n;

    return encode_to_kbytes(perspective_proto_Response_fields, &resp);
}

/* ---- TableSize ----------------------------------------------------------- */

/* psp_encode_table_size_resp(msg_id, entity_id, size_long) */
K psp_encode_table_size_resp(K msg_id, K entity_id, K size) {
    perspective_proto_Response resp = perspective_proto_Response_init_zero;
    fill_envelope(&resp, msg_id, entity_id);
    resp.which_client_resp = perspective_proto_Response_table_size_resp_tag;
    resp.client_resp.table_size_resp.size = (uint32_t)(size->j);
    return encode_to_kbytes(perspective_proto_Response_fields, &resp);
}

/* ---- TableMakePort ------------------------------------------------------- */

/* psp_encode_table_make_port_resp(msg_id, entity_id, port_id_int) */
K psp_encode_table_make_port_resp(K msg_id, K entity_id, K port_id) {
    perspective_proto_Response resp = perspective_proto_Response_init_zero;
    fill_envelope(&resp, msg_id, entity_id);
    resp.which_client_resp = perspective_proto_Response_table_make_port_resp_tag;
    resp.client_resp.table_make_port_resp.port_id = (uint32_t)(port_id->i);
    return encode_to_kbytes(perspective_proto_Response_fields, &resp);
}

/* ---- TableMakeView ------------------------------------------------------- */

/* psp_encode_table_make_view_resp(msg_id, entity_id, view_id_str) */
K psp_encode_table_make_view_resp(K msg_id, K entity_id, K view_id) {
    perspective_proto_Response resp = perspective_proto_Response_init_zero;
    fill_envelope(&resp, msg_id, entity_id);
    resp.which_client_resp = perspective_proto_Response_table_make_view_resp_tag;
    qstr_to_cbuf(view_id, resp.client_resp.table_make_view_resp.view_id,
                 sizeof(resp.client_resp.table_make_view_resp.view_id));
    return encode_to_kbytes(perspective_proto_Response_fields, &resp);
}

/* ---- ViewSchema ---------------------------------------------------------- */

/*
 * psp_encode_view_schema_resp(msg_id, entity_id, col_names, col_types)
 * Same signature as table_schema_resp but uses ViewSchemaResp with
 * a map<string, ColumnType> instead of a Schema message.
 */
K psp_encode_view_schema_resp(K msg_id, K entity_id, K col_names, K col_types) {
    perspective_proto_Response resp = perspective_proto_Response_init_zero;
    fill_envelope(&resp, msg_id, entity_id);
    resp.which_client_resp = perspective_proto_Response_view_schema_resp_tag;

    perspective_proto_ViewSchemaResp *r = &resp.client_resp.view_schema_resp;
    int n = (int)col_names->n;
    int maxn = (int)(sizeof(r->schema) / sizeof(r->schema[0]));
    if (n > maxn) n = maxn;
    for (int i = 0; i < n; i++) {
        qstr_to_cbuf(kK(col_names)[i], r->schema[i].key, sizeof(r->schema[i].key));
        r->schema[i].value = kI(col_types)[i];
    }
    r->schema_count = n;
    return encode_to_kbytes(perspective_proto_Response_fields, &resp);
}

/* ---- ViewDimensions ------------------------------------------------------ */

/*
 * psp_encode_view_dimensions_resp(msg_id, entity_id, dims)
 *   dims: Q int vector [num_table_rows; num_table_cols; num_view_rows; num_view_cols]
 */
K psp_encode_view_dimensions_resp(K msg_id, K entity_id, K dims) {
    perspective_proto_Response resp = perspective_proto_Response_init_zero;
    fill_envelope(&resp, msg_id, entity_id);
    resp.which_client_resp = perspective_proto_Response_view_dimensions_resp_tag;
    perspective_proto_ViewDimensionsResp *r = &resp.client_resp.view_dimensions_resp;
    r->num_table_rows    = (uint32_t)kJ(dims)[0];
    r->num_table_columns = (uint32_t)kJ(dims)[1];
    r->num_view_rows     = (uint32_t)kJ(dims)[2];
    r->num_view_columns  = (uint32_t)kJ(dims)[3];
    return encode_to_kbytes(perspective_proto_Response_fields, &resp);
}

/* ---- ViewGetConfig ------------------------------------------------------- */

/*
 * psp_encode_view_get_config_resp(msg_id, entity_id, group_by, split_by,
 *                                  columns, filter_op)
 *   group_by: Q mixed list of strings
 *   split_by: Q mixed list of strings
 *   columns:  Q mixed list of strings
 *   filter_op: Q int (0=AND, 1=OR)
 */
K psp_encode_view_get_config_resp(K msg_id, K entity_id,
                                   K group_by, K split_by,
                                   K columns, K filter_op) {
    perspective_proto_Response resp = perspective_proto_Response_init_zero;
    fill_envelope(&resp, msg_id, entity_id);
    resp.which_client_resp = perspective_proto_Response_view_get_config_resp_tag;

    perspective_proto_ViewGetConfigResp *r = &resp.client_resp.view_get_config_resp;
    r->has_config = true;
    perspective_proto_ViewConfig *vc = &r->config;

    /* group_by */
    int n = (int)group_by->n;
    if (n > 64) n = 64;
    for (int i = 0; i < n; i++)
        qstr_to_cbuf(kK(group_by)[i], vc->group_by[i], 256);
    vc->group_by_count = n;

    /* split_by */
    n = (int)split_by->n;
    if (n > 64) n = 64;
    for (int i = 0; i < n; i++)
        qstr_to_cbuf(kK(split_by)[i], vc->split_by[i], 256);
    vc->split_by_count = n;

    /* columns */
    n = (int)columns->n;
    if (n > 0) {
        vc->has_columns = true;
        vc->columns.which_opt_columns = perspective_proto_ColumnsUpdate_columns_tag;
        if (n > 512) n = 512;
        for (int i = 0; i < n; i++)
            qstr_to_cbuf(kK(columns)[i], vc->columns.opt_columns.columns.columns[i], 256);
        vc->columns.opt_columns.columns.columns_count = n;
    }

    vc->filter_op = (perspective_proto_ViewConfig_FilterReducer)filter_op->i;
    return encode_to_kbytes(perspective_proto_Response_fields, &resp);
}

/* ---- ViewColumnPaths ----------------------------------------------------- */

/*
 * psp_encode_view_column_paths_resp(msg_id, entity_id, paths)
 *   paths: Q mixed list of char vectors
 */
K psp_encode_view_column_paths_resp(K msg_id, K entity_id, K paths) {
    perspective_proto_Response resp = perspective_proto_Response_init_zero;
    fill_envelope(&resp, msg_id, entity_id);
    resp.which_client_resp = perspective_proto_Response_view_column_paths_resp_tag;

    perspective_proto_ViewColumnPathsResp *r = &resp.client_resp.view_column_paths_resp;
    int n = (int)paths->n;
    int maxn = (int)(sizeof(r->paths) / sizeof(r->paths[0]));
    if (n > maxn) n = maxn;
    for (int i = 0; i < n; i++)
        qstr_to_cbuf(kK(paths)[i], r->paths[i], sizeof(r->paths[i]));
    r->paths_count = n;
    return encode_to_kbytes(perspective_proto_Response_fields, &resp);
}

/* ---- ViewToArrow --------------------------------------------------------- */

/*
 * psp_encode_view_to_arrow_resp(msg_id, entity_id, arrow_bytes)
 *   arrow_bytes: Q byte vector (type 4) — raw Arrow IPC stream
 */
K psp_encode_view_to_arrow_resp(K msg_id, K entity_id, K arrow_bytes) {
    /* Arrow IPC can be very large; use a heap-allocated encode buffer.       */
    perspective_proto_Response resp = perspective_proto_Response_init_zero;
    fill_envelope(&resp, msg_id, entity_id);
    resp.which_client_resp = perspective_proto_Response_view_to_arrow_resp_tag;

    /* nanopb callback for the `arrow` bytes field (field 1 in ViewToArrowResp) */
    size_t arrow_len = (size_t)arrow_bytes->n;

    /* Compute required buffer size: envelope overhead + arrow payload + varint */
    /* Overhead: Response envelope ≈ 32 bytes + entity_id ≈ 256 bytes + field tags + lengths */
    size_t buf_size = arrow_len + 1024;
    uint8_t *buf = (uint8_t *)malloc(buf_size);
    if (!buf) return krr((S)"psp_encode_view_to_arrow_resp: out of memory");

    pb_ostream_t stream = pb_ostream_from_buffer(buf, buf_size);

    /* We encode the ViewToArrowResp.arrow field manually via a callback      */
    /* because the generated struct uses a fixed array which would be huge.   */
    /* Instead, write the Response envelope fields manually using pb_encode.  */

    /* Field 1 (msg_id, uint32, varint): tag = (1 << 3) | 0 = 0x08 */
    pb_encode_tag(&stream, PB_WT_VARINT, 1);
    pb_encode_varint(&stream, resp.msg_id);

    /* Field 2 (entity_id, string): tag = (2 << 3) | 2 = 0x12 */
    pb_encode_tag(&stream, PB_WT_STRING, 2);
    pb_encode_string(&stream, (const pb_byte_t *)resp.entity_id,
                     strlen(resp.entity_id));

    /* Field 16 (view_to_arrow_resp, embedded message): tag = (16 << 3) | 2 = 0x82 0x01 */
    pb_encode_tag(&stream, PB_WT_STRING, 16);
    /* ViewToArrowResp: field 1 (arrow bytes): tag = (1 << 3) | 2 = 0x0a */
    /* Size of sub-message: 1 (tag) + varint(len) + len */
    size_t submsg_size = 1 + pb_varint_size(arrow_len) + arrow_len;
    pb_encode_varint(&stream, submsg_size);
    pb_encode_tag(&stream, PB_WT_STRING, 1);
    pb_encode_string(&stream, (const pb_byte_t *)kG(arrow_bytes), arrow_len);

    K result = kdb_bytes(buf, stream.bytes_written);
    free(buf);
    return result;
}

/* ---- ViewOnUpdate push --------------------------------------------------- */

/*
 * psp_encode_view_on_update_resp(msg_id, entity_id, port_id_int, delta_bytes)
 *   port_id_int:  Q int (-6h)
 *   delta_bytes:  Q byte vector (type 4), or () for no delta
 */
K psp_encode_view_on_update_resp(K msg_id, K entity_id, K port_id, K delta_bytes) {
    perspective_proto_Response resp = perspective_proto_Response_init_zero;
    fill_envelope(&resp, msg_id, entity_id);
    resp.which_client_resp = perspective_proto_Response_view_on_update_resp_tag;

    perspective_proto_ViewOnUpdateResp *r = &resp.client_resp.view_on_update_resp;
    r->port_id = (uint32_t)(port_id->i);
    /* delta is optional; skip if empty */
    (void)delta_bytes;

    return encode_to_kbytes(perspective_proto_Response_fields, &resp);
}

/* =========================================================================
 * Module initialisation — called once when Q loads the library.
 * ========================================================================= */

/* Not strictly required for KDB+ 4.1, but good practice. */
void psp_init(void) {}
