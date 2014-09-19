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
 *all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

#include "postgres.h"
#include "funcapi.h"
#include "catalog/pg_class.h"
#include "catalog/pg_type.h"
#include "replication/output_plugin.h"
#include "replication/logical.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/json.h"
#include "utils/memutils.h"
#include "utils/numeric.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/syscache.h"
#include "utils/typcache.h"
#include "proto/pg_logicaldec.pb-c.h"

PG_MODULE_MAGIC;

typedef struct {
  MemoryContext context;
  Decoderbufs__TxnMessage *txn_msg;
  bool debug_mode;
} DecoderData;

/* These must be available to pg_dlsym() */
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
  ListCell *option;
  DecoderData *data;

  data = palloc(sizeof(DecoderData));
  data->context = AllocSetContextCreate(ctx->context, "decoderbufs context",
		  ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE,
		  ALLOCSET_DEFAULT_MAXSIZE);

  ctx->output_plugin_private = data;

  opt->output_type = OUTPUT_PLUGIN_BINARY_OUTPUT;

  foreach(option, ctx->output_plugin_options) {
    DefElem *elem = lfirst(option);
    Assert(elem->arg == NULL || IsA(elem->arg, String));

    if (strcmp(elem->defname, "debug-mode") == 0) {
      bool debug_mode;
      if (elem->arg == NULL)
    	debug_mode = false;
      else if (!parse_bool(strVal(elem->arg), &debug_mode))
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("could not parse value \"%s\" for parameter \"%s\"",
                        strVal(elem->arg), elem->defname)));

      if (debug_mode)
    	  opt->output_type = OUTPUT_PLUGIN_TEXTUAL_OUTPUT;
    } else {
      ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                      errmsg("option \"%s\" = \"%s\" is unknown", elem->defname,
                             elem->arg ? strVal(elem->arg) : "(null)")));
    }
  }
}

static void free_txn_msg_datums(Decoderbufs__TxnMessage *msg) {
	if (msg->new_datum) {
		if (msg->new_datum->has_datum_bytes) {
			pfree(msg->new_datum->datum_bytes.data);
			msg->new_datum->datum_bytes.data = NULL;
			msg->new_datum->datum_bytes.len = 0;
		}
		pfree(msg->new_datum);
	}
	if (msg->old_datum) {
		if (msg->old_datum->has_datum_bytes) {
			pfree(msg->old_datum->datum_bytes.data);
			msg->old_datum->datum_bytes.data = NULL;
			msg->old_datum->datum_bytes.len = 0;
		}
		pfree(msg->old_datum);
	}
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
  DecoderData *data = ctx->output_plugin_private;
  Decoderbufs__TxnMessage msg = DECODERBUFS__TXN_MESSAGE__INIT;
  data->txn_msg = &msg;
  data->txn_msg->xid = txn->xid;
}

/* COMMIT callback */
static void pg_decode_commit_txn(LogicalDecodingContext *ctx,
                                 ReorderBufferTXN *txn, XLogRecPtr commit_lsn) {
  DecoderData *data = ctx->output_plugin_private;
  Decoderbufs__TxnMessage *msg = data->txn_msg;
  msg->timestamp = txn->commit_time;

  OutputPluginPrepareWrite(ctx, true);
  size_t psize = decoderbufs__txn_message__get_packed_size(msg);
  void *packed = palloc(psize);
  size_t ssize = decoderbufs__txn_message__pack(msg, packed);
  appendBinaryStringInfo(ctx->out, packed, ssize);
  OutputPluginWrite(ctx, true);

  pfree(packed);
  free_txn_msg_datums(msg);
}

/* this doesn't seem to be available in the public api (unfortunate) */
static double numeric_to_double_no_overflow(Numeric num) {
	char	   *tmp;
	double		val;
	char	   *endptr;

	tmp = DatumGetCString(DirectFunctionCall1(numeric_out,
											  NumericGetDatum(num)));

	/* unlike float8in, we ignore ERANGE from strtod */
	val = strtod(tmp, &endptr);
	if (*endptr != '\0')
	{
		/* shouldn't happen ... */
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("invalid input syntax for type double precision: \"%s\"",
						tmp)));
	}

	pfree(tmp);

	return val;
}

static void set_datum_value(Decoderbufs__DatumMessage *datum_msg, Oid typid, Oid typoutput, Datum datum) {
	Numeric num;
	char c;
	bytea *valptr;
	char *output;
	switch (typid) {
		case BOOLOID:
			datum_msg->datum_bool = DatumGetBool(datum);
			break;
		case INT2OID:
			datum_msg->datum_int32 = DatumGetInt16(datum);
			break;
		case INT4OID:
			datum_msg->datum_int32 = DatumGetInt32(datum);
			break;
		case INT8OID:
			datum_msg->datum_int64 = DatumGetInt64(datum);
			break;
		case OIDOID:
			break;
		case FLOAT4OID:
			datum_msg->datum_float = DatumGetFloat4(datum);
			break;
		case FLOAT8OID:
			datum_msg->datum_double = DatumGetFloat8(datum);
			break;
		case NUMERICOID:
			num = DatumGetNumeric(datum);
			if (!numeric_is_nan(num)) {
				datum_msg->datum_double = numeric_to_double_no_overflow(num);
			}
			break;
		case CHAROID:
			c = DatumGetChar(datum);
			datum_msg->datum_string = &c;
			break;
		case VARCHAROID:
		case TEXTOID:
			datum_msg->datum_string = DatumGetCString(datum);
			break;
		case BYTEAOID:
			valptr = DatumGetByteaPCopy(datum);
			int size = VARSIZE(valptr);
			datum_msg->datum_bytes = *((ProtobufCBinaryData *)palloc(sizeof(ProtobufCBinaryData)));
			datum_msg->datum_bytes.data = (uint8_t *)VARDATA(valptr);
			datum_msg->datum_bytes.len = size - VARHDRSZ;
			break;
		default:
			output = OidOutputFunctionCall(typoutput, datum);
			datum_msg->datum_bytes = *((ProtobufCBinaryData *)palloc(sizeof(ProtobufCBinaryData)));
			datum_msg->datum_bytes.data = (uint8_t *)output;
			datum_msg->datum_bytes.len = sizeof(output);
			break;
	}
}

static Decoderbufs__DatumMessage tuple_to_datum_msg(Relation relation, HeapTuple tuple) {
	TupleDesc tupdesc = RelationGetDescr(relation);
	int	natt;
	Decoderbufs__DatumMessage datum_msg = DECODERBUFS__DATUM_MESSAGE__INIT;

	/* Build column names and values */
	for (natt = 0; natt < tupdesc->natts; natt++) {
		Form_pg_attribute attr;
		Datum origval;
		bool isnull;

		attr = tupdesc->attrs[natt];

		/* Skip dropped columns and system columns */
		if (attr->attisdropped || attr->attnum < 0)
			continue;

		/* Set the column name */
		datum_msg.column_name = quote_identifier(NameStr(attr->attname));

		/* Get Datum from tuple */
		origval = fastgetattr(tuple, natt + 1, tupdesc, &isnull);

		/* Get output function */
		datum_msg.column_type = attr->atttypid;

		Oid	typoutput;
		bool typisvarlena;
		/* Query output function */
		getTypeOutputInfo(attr->atttypid, &typoutput, &typisvarlena);
		if (!isnull) {
			if (typisvarlena && VARATT_IS_EXTERNAL_ONDISK(origval)) {
				// what to do if anything?
			} else if (!typisvarlena) {
				set_datum_value(&datum_msg, attr->atttypid, typoutput, origval);
			} else {
				Datum val = PointerGetDatum(PG_DETOAST_DATUM(origval));
				set_datum_value(&datum_msg, attr->atttypid, typoutput, val);
			}
		}
	}

	return datum_msg;
}

/*
 * callback for individual changed tuples
 */
static void pg_decode_change(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
                             Relation relation, ReorderBufferChange *change) {
  DecoderData *data;
  MemoryContext old;

  Form_pg_class class_form;
  char replident = relation->rd_rel->relreplident;
  bool is_rel_non_selective;
  class_form = RelationGetForm(relation);

  data = ctx->output_plugin_private;

  /* Avoid leaking memory by using and resetting our own context */
  old = MemoryContextSwitchTo(data->context);

  RelationGetIndexList(relation);
  is_rel_non_selective = (replident == REPLICA_IDENTITY_NOTHING ||
  							(replident == REPLICA_IDENTITY_DEFAULT &&
  							 !OidIsValid(relation->rd_replidindex)));

  /* set common fields */
  data->txn_msg->table = quote_qualified_identifier(
			get_namespace_name(
					   get_rel_namespace(RelationGetRelid(relation))),
		NameStr(class_form->relname));

  /* decode different operation types */
  switch (change->action) {
    case REORDER_BUFFER_CHANGE_INSERT:
      data->txn_msg->op = DECODERBUFS__OP__INSERT;
      if (change->data.tp.newtuple != NULL) {
    	HeapTupleGetDatum(&change->data.tp.newtuple->tuple);
    	Decoderbufs__DatumMessage new_datum = tuple_to_datum_msg(relation,
    			&change->data.tp.newtuple->tuple);
    	data->txn_msg->new_datum = &new_datum;
      }
      break;
    case REORDER_BUFFER_CHANGE_UPDATE:
      data->txn_msg->op = DECODERBUFS__OP__UPDATE;
      if (is_rel_non_selective) {
    	  if (change->data.tp.oldtuple != NULL) {
    		  Decoderbufs__DatumMessage old_datum = tuple_to_datum_msg(relation,
    		      			&change->data.tp.oldtuple->tuple);
    		  data->txn_msg->old_datum = &old_datum;
    	  }
    	  if (change->data.tp.newtuple != NULL) {
    		  Decoderbufs__DatumMessage new_datum = tuple_to_datum_msg(relation,
    		      			&change->data.tp.newtuple->tuple);
    		  data->txn_msg->new_datum = &new_datum;
    	  }
      }
      break;
    case REORDER_BUFFER_CHANGE_DELETE:
      data->txn_msg->op = DECODERBUFS__OP__DELETE;
      /* if there was no PK, we only know that a delete happened */
      if (is_rel_non_selective && change->data.tp.oldtuple != NULL) {
    	  Decoderbufs__DatumMessage old_datum = tuple_to_datum_msg(relation,
    	      		      			&change->data.tp.oldtuple->tuple);
    	  data->txn_msg->old_datum = &old_datum;
      }
      break;
    default:
      Assert(0);
      break;
  }

  MemoryContextSwitchTo(old);
  MemoryContextReset(data->context);
}
