/*
 * decoderbufs - PostgreSQL output plug-in for logical replication to Protocol
 * Buffers
 *
 * Copyright (c) 2014 Xavier Stevens
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

#if defined(__linux__)
#include <endian.h>
#elif defined(__APPLE__)
#include <machine/endian.h>
#include <libkern/OSByteOrder.h>
#define htobe64(x) OSSwapHostToBigInt64(x)
#endif
#include <inttypes.h>

#include "postgres.h"
#include "funcapi.h"
#include "catalog/pg_class.h"
#include "catalog/pg_type.h"
#include "catalog/namespace.h"
#include "executor/spi.h"
#include "replication/output_plugin.h"
#include "replication/logical.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/geo_decls.h"
#include "utils/json.h"
#include "utils/memutils.h"
#include "utils/numeric.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/syscache.h"
#include "utils/typcache.h"
#include "utils/uuid.h"
#include "proto/pg_logicaldec.pb-c.h"

/* POSTGIS version define so it doesn't redef macros */
#define POSTGIS_PGSQL_VERSION 94
#include "liblwgeom.h"

PG_MODULE_MAGIC;

/* define a time macro to convert TimestampTz into something more sane,
 * which in this case is microseconds since epoch
 */
#ifdef HAVE_INT64_TIMESTAMP
#define TIMESTAMPTZ_TO_USEC_SINCE_EPOCH(t)                                     \
  t + ((POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * SECS_PER_DAY *USECS_PER_SEC);
#else
#define TIMESTAMPTZ_TO_USEC_SINCE_EPOCH(t)                                     \
  (t + ((POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * SECS_PER_DAY)) * 1000.0;
#endif

typedef struct {
  MemoryContext context;
  bool debug_mode;
} DecoderData;

/* GLOBALs for PostGIS dynamic OIDs */
Oid geometry_oid = InvalidOid;
Oid geography_oid = InvalidOid;

/* these must be available to pg_dlsym() */
extern void _PG_init(void);
extern void _PG_output_plugin_init(OutputPluginCallbacks *cb);

static void pg_decode_startup(LogicalDecodingContext *ctx,
                              OutputPluginOptions *opt, bool is_init);
static void pg_decode_shutdown(LogicalDecodingContext *ctx);
static void pg_decode_begin_txn(LogicalDecodingContext *ctx,
                                ReorderBufferTXN *txn);
static void pg_decode_commit_txn(LogicalDecodingContext *ctx,
                                 ReorderBufferTXN *txn, XLogRecPtr commit_lsn);
static void pg_decode_change(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
                             Relation rel, ReorderBufferChange *change);

void _PG_init(void) {}

/* specify output plugin callbacks */
void _PG_output_plugin_init(OutputPluginCallbacks *cb) {
  AssertVariableIsOfType(&_PG_output_plugin_init, LogicalOutputPluginInit);
  cb->startup_cb = pg_decode_startup;
  cb->begin_cb = pg_decode_begin_txn;
  cb->change_cb = pg_decode_change;
  cb->commit_cb = pg_decode_commit_txn;
  cb->shutdown_cb = pg_decode_shutdown;
}

/* initialize this plugin */
static void pg_decode_startup(LogicalDecodingContext *ctx,
                              OutputPluginOptions *opt, bool is_init) {
  elog(INFO, "Entering startup callback");

  ListCell *option;
  DecoderData *data;

  data = palloc(sizeof(DecoderData));
  data->context = AllocSetContextCreate(
      ctx->context, "decoderbufs context", ALLOCSET_DEFAULT_MINSIZE,
      ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);
  data->debug_mode = false;
  opt->output_type = OUTPUT_PLUGIN_BINARY_OUTPUT;

  foreach(option, ctx->output_plugin_options) {
    DefElem *elem = lfirst(option);
    Assert(elem->arg == NULL || IsA(elem->arg, String));

    if (strcmp(elem->defname, "debug-mode") == 0) {
      if (elem->arg == NULL) {
        data->debug_mode = false;
      } else if (!parse_bool(strVal(elem->arg), &data->debug_mode)) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("could not parse value \"%s\" for parameter \"%s\"",
                        strVal(elem->arg), elem->defname)));
      }

      if (data->debug_mode) {
        elog(NOTICE, "Decoderbufs DEBUG MODE is ON.");
        opt->output_type = OUTPUT_PLUGIN_TEXTUAL_OUTPUT;
      } else {
        elog(NOTICE, "Decoderbufs DEBUG MODE is OFF.");
      }
    } else {
      ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                      errmsg("option \"%s\" = \"%s\" is unknown", elem->defname,
                             elem->arg ? strVal(elem->arg) : "(null)")));
    }
  }

  ctx->output_plugin_private = data;

  elog(INFO, "Exiting startup callback");
}

/* cleanup this plugin's resources */
static void pg_decode_shutdown(LogicalDecodingContext *ctx) {
  DecoderData *data = ctx->output_plugin_private;

  /* cleanup our own resources via memory context reset */
  MemoryContextDelete(data->context);
}

/* BEGIN callback */
static void pg_decode_begin_txn(LogicalDecodingContext *ctx,
                                ReorderBufferTXN *txn) {
  // set PostGIS geometry type id (these are dynamic)
  // TODO: Figure out how to make sure we get the typid's from postgis extension namespace
  if (geometry_oid == InvalidOid) {
    geometry_oid = TypenameGetTypid("geometry");
    if (geometry_oid != InvalidOid) {
      elog(DEBUG1, "PostGIS geometry type detected: %u", geometry_oid);
    }
  }
  if (geography_oid == InvalidOid) {
    geography_oid = TypenameGetTypid("geography");
    if (geography_oid != InvalidOid) {
      elog(DEBUG1, "PostGIS geometry type detected: %u", geography_oid);
    }
  }
}

/* COMMIT callback */
static void pg_decode_commit_txn(LogicalDecodingContext *ctx,
                                 ReorderBufferTXN *txn, XLogRecPtr commit_lsn) {
}

/* convenience method to free up sub-messages */
static void row_message_destroy(Decoderbufs__RowMessage *msg) {
  if (!msg) {
    return;
  }

  if (msg->table) {
    pfree(msg->table);
  }

  if (msg->n_new_tuple > 0) {
    for (int i = 0; i < msg->n_new_tuple; i++) {
      if (msg->new_tuple[i]) {
        switch (msg->new_tuple[i]->datum_case) {
          case DECODERBUFS__DATUM_MESSAGE__DATUM_DATUM_STRING:
            if (msg->new_tuple[i]->datum_string) {
              pfree(msg->new_tuple[i]->datum_string);
            }
            break;
          case DECODERBUFS__DATUM_MESSAGE__DATUM_DATUM_BYTES:
            if (msg->new_tuple[i]->datum_bytes.data) {
              pfree(msg->new_tuple[i]->datum_bytes.data);
              msg->new_tuple[i]->datum_bytes.data = NULL;
              msg->new_tuple[i]->datum_bytes.len = 0;
            }
            break;
          case DECODERBUFS__DATUM_MESSAGE__DATUM_DATUM_POINT:
            if (msg->new_tuple[i]->datum_point) {
              pfree(msg->new_tuple[i]->datum_point);
            }
            break;
          default:
            break;
        }
        pfree(msg->new_tuple[i]);
      }
    }
    pfree(msg->new_tuple);
  }
  if (msg->n_old_tuple > 0) {
    for (int i = 0; i < msg->n_old_tuple; i++) {
      if (msg->old_tuple[i]) {
        switch (msg->old_tuple[i]->datum_case) {
          case DECODERBUFS__DATUM_MESSAGE__DATUM_DATUM_STRING:
            if (msg->old_tuple[i]->datum_string) {
              pfree(msg->old_tuple[i]->datum_string);
            }
            break;
          case DECODERBUFS__DATUM_MESSAGE__DATUM_DATUM_BYTES:
            if (msg->old_tuple[i]->datum_bytes.data) {
              pfree(msg->old_tuple[i]->datum_bytes.data);
              msg->old_tuple[i]->datum_bytes.data = NULL;
              msg->old_tuple[i]->datum_bytes.len = 0;
            }
            break;
          case DECODERBUFS__DATUM_MESSAGE__DATUM_DATUM_POINT:
            if (msg->old_tuple[i]->datum_point) {
              pfree(msg->old_tuple[i]->datum_point);
            }
            break;
          default:
            break;
        }
        pfree(msg->old_tuple[i]);
      }
    }
    pfree(msg->old_tuple);
  }
}

/* only used for debug-mode (currently not all OIDs are currently supported) */
static void print_tuple_msg(StringInfo out, Decoderbufs__DatumMessage **tup,
                            size_t n) {
  if (tup) {
    for (int i = 0; i < n; i++) {
      Decoderbufs__DatumMessage *dmsg = tup[i];
      if (dmsg->column_name)
        appendStringInfo(out, "column_name[%s]", dmsg->column_name);
      if (dmsg->has_column_type) {
        appendStringInfo(out, ", column_type[%" PRId64 "]", dmsg->column_type);
        switch (dmsg->column_type) {
          case INT2OID:
          case INT4OID:
            appendStringInfo(out, ", datum[%d]", dmsg->datum_int32);
            break;
          case INT8OID:
            appendStringInfo(out, ", datum[%" PRId64 "]", dmsg->datum_int64);
            break;
          case FLOAT4OID:
            appendStringInfo(out, ", datum[%f]", dmsg->datum_float);
            break;
          case FLOAT8OID:
          case NUMERICOID:
            appendStringInfo(out, ", datum[%f]", dmsg->datum_double);
            break;
          case CHAROID:
          case VARCHAROID:
          case BPCHAROID:
          case TEXTOID:
          case JSONOID:
          case XMLOID:
          case UUIDOID:
          case TIMESTAMPOID:
          case TIMESTAMPTZOID:
            appendStringInfo(out, ", datum[%s]", dmsg->datum_string);
            break;
          case POINTOID:
            appendStringInfo(out, ", datum[POINT(%f, %f)]",
                             dmsg->datum_point->x, dmsg->datum_point->y);
            break;
          default:
            if (dmsg->column_type == geometry_oid &&
                dmsg->datum_point != NULL) {
              appendStringInfo(out, ", datum[GEOMETRY(POINT(%f,%f))]",
                               dmsg->datum_point->x, dmsg->datum_point->y);
            } else if (dmsg->column_type == geography_oid &&
                       dmsg->datum_point != NULL) {
              appendStringInfo(out, ", datum[GEOGRAPHY(POINT(%f,%f))]",
                               dmsg->datum_point->x, dmsg->datum_point->y);
            }
            break;
        }
        appendStringInfo(out, "\n");
      }
    }
  }
}

/* this doesn't seem to be available in the public api (unfortunate) */
static double numeric_to_double_no_overflow(Numeric num) {
  char *tmp;
  double val;
  char *endptr;

  tmp = DatumGetCString(DirectFunctionCall1(numeric_out, NumericGetDatum(num)));

  /* unlike float8in, we ignore ERANGE from strtod */
  val = strtod(tmp, &endptr);
  if (*endptr != '\0') {
    /* shouldn't happen ... */
    ereport(ERROR,
            (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
             errmsg("invalid input syntax for type double precision: \"%s\"",
                    tmp)));
  }

  pfree(tmp);

  return val;
}

static bool geography_point_as_decoderbufs_point(Datum datum,
                                                 Decoderbufs__Point *p) {
  GSERIALIZED *geom;
  LWGEOM *lwgeom;
  LWPOINT *point = NULL;
  POINT2D p2d;

  geom = (GSERIALIZED *)PG_DETOAST_DATUM(datum);
  if (gserialized_get_type(geom) != POINTTYPE) {
    return false;
  }

  lwgeom = lwgeom_from_gserialized(geom);
  point = lwgeom_as_lwpoint(lwgeom);
  if (lwgeom_is_empty(lwgeom)) {
    return false;
  }

  getPoint2d_p(point->point, 0, &p2d);

  if (p != NULL) {
    Decoderbufs__Point dp = DECODERBUFS__POINT__INIT;
    dp.x = p2d.x;
    dp.y = p2d.y;
    memcpy(p, &dp, sizeof(dp));
    elog(DEBUG1, "Translating geography to point: (x,y) = (%f,%f)", p->x, p->y);
  }

  return true;
}

/* set a datum value based on its OID specified by typid */
static void set_datum_value(Decoderbufs__DatumMessage *datum_msg, Oid typid,
                            Oid typoutput, Datum datum) {
  Numeric num;
  bytea *valptr;
  const char *output;
  Point *p;
  int size = 0;

  switch (typid) {
    case BOOLOID:
      datum_msg->datum_bool = DatumGetBool(datum);
      datum_msg->datum_case = DECODERBUFS__DATUM_MESSAGE__DATUM_DATUM_BOOL;
      break;
    case INT2OID:
      datum_msg->datum_int32 = DatumGetInt16(datum);
      datum_msg->datum_case = DECODERBUFS__DATUM_MESSAGE__DATUM_DATUM_INT32;
      break;
    case INT4OID:
      datum_msg->datum_int32 = DatumGetInt32(datum);
      datum_msg->datum_case = DECODERBUFS__DATUM_MESSAGE__DATUM_DATUM_INT32;
      break;
    case INT8OID:
    case OIDOID:
      datum_msg->datum_int64 = DatumGetInt64(datum);
      datum_msg->datum_case = DECODERBUFS__DATUM_MESSAGE__DATUM_DATUM_INT64;
      break;
    case FLOAT4OID:
      datum_msg->datum_float = DatumGetFloat4(datum);
      datum_msg->datum_case = DECODERBUFS__DATUM_MESSAGE__DATUM_DATUM_FLOAT;
      break;
    case FLOAT8OID:
      datum_msg->datum_double = DatumGetFloat8(datum);
      datum_msg->datum_case = DECODERBUFS__DATUM_MESSAGE__DATUM_DATUM_DOUBLE;
      break;
    case NUMERICOID:
      num = DatumGetNumeric(datum);
      if (!numeric_is_nan(num)) {
        datum_msg->datum_double = numeric_to_double_no_overflow(num);
        datum_msg->datum_case = DECODERBUFS__DATUM_MESSAGE__DATUM_DATUM_DOUBLE;
      }
      break;
    case CHAROID:
    case VARCHAROID:
    case BPCHAROID:
    case TEXTOID:
    case JSONOID:
    case XMLOID:
    case UUIDOID:
      output = OidOutputFunctionCall(typoutput, datum);
      datum_msg->datum_string = pnstrdup(output, strlen(output));
      datum_msg->datum_case = DECODERBUFS__DATUM_MESSAGE__DATUM_DATUM_STRING;
      break;
    case TIMESTAMPOID:
    /*
     * THIS FALLTHROUGH IS MAKING THE ASSUMPTION WE ARE ON UTC
     */
    case TIMESTAMPTZOID:
      output = timestamptz_to_str(DatumGetTimestampTz(datum));
      datum_msg->datum_string = pnstrdup(output, strlen(output));
      datum_msg->datum_case = DECODERBUFS__DATUM_MESSAGE__DATUM_DATUM_STRING;
      break;
    case BYTEAOID:
      valptr = DatumGetByteaPCopy(datum);
      size = VARSIZE(valptr) - VARHDRSZ;
      datum_msg->datum_bytes.data = palloc(size);
      datum_msg->datum_case = DECODERBUFS__DATUM_MESSAGE__DATUM_DATUM_BYTES;
      memcpy(datum_msg->datum_bytes.data, (uint8_t *)VARDATA(valptr), size);
      datum_msg->datum_bytes.len = size;
      break;
    case POINTOID:
      p = DatumGetPointP(datum);
      Decoderbufs__Point dp = DECODERBUFS__POINT__INIT;
      dp.x = p->x;
      dp.y = p->y;
      datum_msg->datum_point = palloc(sizeof(Decoderbufs__Point));
      datum_msg->datum_case = DECODERBUFS__DATUM_MESSAGE__DATUM_DATUM_POINT;
      memcpy(datum_msg->datum_point, &dp, sizeof(dp));
      break;
    default:
      // PostGIS uses dynamic OIDs so we need to check the type again here
      if (typid == geometry_oid || typid == geography_oid) {
        elog(DEBUG1, "Converting geography point to datum_point");
        datum_msg->datum_point = palloc(sizeof(Decoderbufs__Point));
        datum_msg->datum_case = DECODERBUFS__DATUM_MESSAGE__DATUM_DATUM_POINT;
        geography_point_as_decoderbufs_point(datum, datum_msg->datum_point);
      } else {
        elog(WARNING, "Encountered unknown typid: %d, using bytes", typid);
        output = OidOutputFunctionCall(typoutput, datum);
        int len = strlen(output);
        size = sizeof(char) * len;
        datum_msg->datum_bytes.data = palloc(size);
        datum_msg->datum_case = DECODERBUFS__DATUM_MESSAGE__DATUM_DATUM_BYTES;
        memcpy(datum_msg->datum_bytes.data, (uint8_t *)output, size);
        datum_msg->datum_bytes.len = len;
      }
      break;
  }
}

/* convert a PG tuple to an array of DatumMessage(s) */
static void tuple_to_tuple_msg(Decoderbufs__DatumMessage **tmsg,
                               Relation relation, HeapTuple tuple,
                               TupleDesc tupdesc) {
  int natt;

  /* build column names and values */
  for (natt = 0; natt < tupdesc->natts; natt++) {
    Form_pg_attribute attr;
    Datum origval;
    bool isnull;

    attr = tupdesc->attrs[natt];

    /* skip dropped columns and system columns */
    if (attr->attisdropped || attr->attnum < 0) {
      continue;
    }

    Decoderbufs__DatumMessage datum_msg = DECODERBUFS__DATUM_MESSAGE__INIT;

    /* set the column name */
    datum_msg.column_name = quote_identifier(NameStr(attr->attname));

    /* set datum from tuple */
    origval = fastgetattr(tuple, natt + 1, tupdesc, &isnull);

    /* get output function */
    datum_msg.column_type = attr->atttypid;
    datum_msg.has_column_type = true;

    Oid typoutput;
    bool typisvarlena;
    /* query output function */
    getTypeOutputInfo(attr->atttypid, &typoutput, &typisvarlena);
    if (!isnull) {
      if (typisvarlena && VARATT_IS_EXTERNAL_ONDISK(origval)) {
        // TODO: Is there a way we can handle this?
        elog(WARNING, "Not handling external on disk varlena at the moment.");
      } else if (!typisvarlena) {
        set_datum_value(&datum_msg, attr->atttypid, typoutput, origval);
      } else {
        Datum val = PointerGetDatum(PG_DETOAST_DATUM(origval));
        set_datum_value(&datum_msg, attr->atttypid, typoutput, val);
      }
    }

    tmsg[natt] = palloc(sizeof(datum_msg));
    memcpy(tmsg[natt], &datum_msg, sizeof(datum_msg));
  }
}

/* callback for individual changed tuples */
static void pg_decode_change(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
                             Relation relation, ReorderBufferChange *change) {
  DecoderData *data;
  MemoryContext old;

  Form_pg_class class_form;
  char replident = relation->rd_rel->relreplident;
  bool is_rel_non_selective;
  Decoderbufs__RowMessage rmsg = DECODERBUFS__ROW_MESSAGE__INIT;
  class_form = RelationGetForm(relation);

  data = ctx->output_plugin_private;

  /* Avoid leaking memory by using and resetting our own context */
  old = MemoryContextSwitchTo(data->context);

  RelationGetIndexList(relation);
  is_rel_non_selective = (replident == REPLICA_IDENTITY_NOTHING ||
                          (replident == REPLICA_IDENTITY_DEFAULT &&
                           !OidIsValid(relation->rd_replidindex)));

  /* set common fields */
  rmsg.transaction_id = txn->xid;
  rmsg.has_transaction_id = true;
  rmsg.commit_time = TIMESTAMPTZ_TO_USEC_SINCE_EPOCH(txn->commit_time);
  rmsg.has_commit_time = true;
  rmsg.table = pstrdup(NameStr(class_form->relname));

  /* decode different operation types */
  switch (change->action) {
    case REORDER_BUFFER_CHANGE_INSERT:
      rmsg.op = DECODERBUFS__OP__INSERT;
      rmsg.has_op = true;
      if (change->data.tp.newtuple != NULL) {
        TupleDesc tupdesc = RelationGetDescr(relation);
        rmsg.n_new_tuple = tupdesc->natts;
        rmsg.new_tuple =
            palloc(sizeof(Decoderbufs__DatumMessage*) * tupdesc->natts);
        tuple_to_tuple_msg(rmsg.new_tuple, relation,
                           &change->data.tp.newtuple->tuple, tupdesc);
      }
      break;
    case REORDER_BUFFER_CHANGE_UPDATE:
      rmsg.op = DECODERBUFS__OP__UPDATE;
      rmsg.has_op = true;
      if (!is_rel_non_selective) {
        if (change->data.tp.oldtuple != NULL) {
          TupleDesc tupdesc = RelationGetDescr(relation);
          rmsg.n_old_tuple = tupdesc->natts;
          rmsg.old_tuple =
              palloc(sizeof(Decoderbufs__DatumMessage*) * tupdesc->natts);
          tuple_to_tuple_msg(rmsg.old_tuple, relation,
                             &change->data.tp.oldtuple->tuple, tupdesc);
        }
        if (change->data.tp.newtuple != NULL) {
          TupleDesc tupdesc = RelationGetDescr(relation);
          rmsg.n_new_tuple = tupdesc->natts;
          rmsg.new_tuple =
              palloc(sizeof(Decoderbufs__DatumMessage*) * tupdesc->natts);
          tuple_to_tuple_msg(rmsg.new_tuple, relation,
                             &change->data.tp.newtuple->tuple, tupdesc);
        }
      }
      break;
    case REORDER_BUFFER_CHANGE_DELETE:
      rmsg.op = DECODERBUFS__OP__DELETE;
      rmsg.has_op = true;
      /* if there was no PK, we only know that a delete happened */
      if (!is_rel_non_selective && change->data.tp.oldtuple != NULL) {
        TupleDesc tupdesc = RelationGetDescr(relation);
        rmsg.n_old_tuple = tupdesc->natts;
        rmsg.old_tuple =
            palloc(sizeof(Decoderbufs__DatumMessage*) * tupdesc->natts);
        tuple_to_tuple_msg(rmsg.old_tuple, relation,
                           &change->data.tp.oldtuple->tuple, tupdesc);
      }
      break;
    default:
      Assert(0);
      break;
  }

  if (data->debug_mode) {
    OutputPluginPrepareWrite(ctx, true);
    if (rmsg.has_commit_time)
      appendStringInfo(ctx->out, "commit_time[%" PRId64 "]", rmsg.commit_time);
    if (rmsg.table)
      appendStringInfo(ctx->out, ", table[%s]", rmsg.table);
    if (rmsg.has_op)
      appendStringInfo(ctx->out, ", op[%d]", rmsg.op);
    if (rmsg.old_tuple) {
      appendStringInfo(ctx->out, "\nOLD TUPLE: \n");
      print_tuple_msg(ctx->out, rmsg.old_tuple, rmsg.n_old_tuple);
      appendStringInfo(ctx->out, "\n");
    }
    if (rmsg.new_tuple) {
      appendStringInfo(ctx->out, "\nNEW TUPLE: \n");
      print_tuple_msg(ctx->out, rmsg.new_tuple, rmsg.n_new_tuple);
      appendStringInfo(ctx->out, "\n");
    }
    OutputPluginWrite(ctx, true);
  } else {
    size_t psize = decoderbufs__row_message__get_packed_size(&rmsg);
    void *packed = palloc(psize);
    size_t ssize = decoderbufs__row_message__pack(&rmsg, packed);

    OutputPluginPrepareWrite(ctx, true);
    appendBinaryStringInfo(ctx->out, packed, ssize);
    OutputPluginWrite(ctx, true);

    /* free packed buffer */
    pfree(packed);
  }

  /* cleanup msg */
  row_message_destroy(&rmsg);

  MemoryContextSwitchTo(old);
  MemoryContextReset(data->context);
}
