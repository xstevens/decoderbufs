#include "protobuf-c-text.h"

/** Escape string.
 *
 * Add escape characters to strings for problematic characters.
 *
 * \param[in] src The unescaped string to process.
 * \param[in] len Length of \c src. Note that \c src might have ASCII
 *                \c NULs so strlen() isn't good enough here.
 * \return The fully escaped string, or \c NULL if there has been an
 *         allocation error.
 */
static char* esc_str(char *src, int len) {
  int i, escapes = 0, dst_len = 0;
  unsigned char *dst;

  for (i = 0; i < len; i++) {
    if (!isprint(src[i])) {
      escapes++;
    }
  }
  dst = palloc((escapes * 4) + ((len - escapes) * 2) + 1);
  if (!dst) {
    return NULL;
  }

  for (i = 0; i < len; i++) {
    switch (src[i]) {
      /* Special cases. */
      case '\'':
        dst[dst_len++] = '\\';
        dst[dst_len++] = '\'';
        break;
      case '\"':
        dst[dst_len++] = '\\';
        dst[dst_len++] = '\"';
        break;
      case '\\':
        dst[dst_len++] = '\\';
        dst[dst_len++] = '\\';
        break;
      case '\n':
        dst[dst_len++] = '\\';
        dst[dst_len++] = 'n';
        break;
      case '\r':
        dst[dst_len++] = '\\';
        dst[dst_len++] = 'r';
        break;
      case '\t':
        dst[dst_len++] = '\\';
        dst[dst_len++] = 't';
        break;

      /* Escape with octal if !isprint. */
      default:
        if (!isprint(src[i])) {
          dst_len += snprintf("\\%03o", dst + dst_len, src[i]);
        } else {
          dst[dst_len++] = src[i];
        }
        break;
    }
  }
  dst[dst_len] = '\0';

  return dst;
}


/** Internal function to back API function.
 *
 * Has a few extra params to better enable recursion.  This function gets
 * called for each nested message as the \c ProtobufCMessage struct is
 * traversed.
 *
 * \param[in,out] out The string being built up for the text format protobuf.
 * \param[in] level Indent level - increments in 2's.
 * \param[in] m The \c ProtobufCMessage being serialised.
 * \param[in] d The descriptor for the \c ProtobufCMessage.
 */
static void protobuf_c_text_to_string_internal(StringInfo out,
                                               int level,
                                               ProtobufCMessage *m,
                                               const ProtobufCMessageDescriptor *d) {
  int i;
  size_t j, quantifier_offset;
  double float_var;
  const ProtobufCFieldDescriptor *f;
  ProtobufCEnumDescriptor *enumd;
  const ProtobufCEnumValue *enumv;

  f = d->fields;
  for (i = 0; i < d->n_fields; i++) {
    /* Decide if something needs to be done for this field. */
    switch (f[i].label) {
      case PROTOBUF_C_LABEL_OPTIONAL:
        if (f[i].type == PROTOBUF_C_TYPE_STRING) {
          if (!STRUCT_MEMBER(char *, m, f[i].offset) || (STRUCT_MEMBER(char *, m, f[i].offset) == (char *)f[i].default_value)) {
            continue;
          }
        } else if (f[i].type == PROTOBUF_C_TYPE_MESSAGE) {
          if (!STRUCT_MEMBER(char *, m, f[i].offset)) {
            continue;
          }
        } else {
          if (!STRUCT_MEMBER(protobuf_c_boolean, m, f[i].quantifier_offset)) {
            continue;
          }
        }
        break;
      case PROTOBUF_C_LABEL_REPEATED:
        if (!STRUCT_MEMBER(size_t, m, f[i].quantifier_offset)) {
          continue;
        }
        break;
    }

    quantifier_offset = STRUCT_MEMBER(size_t, m, f[i].quantifier_offset);
    /* Field exists and has data, dump it. */
    switch (f[i].type) {
      case PROTOBUF_C_TYPE_INT32:
      case PROTOBUF_C_TYPE_UINT32:
      case PROTOBUF_C_TYPE_FIXED32:
        if (f[i].label == PROTOBUF_C_LABEL_REPEATED) {
          for (j = 0; j < quantifier_offset; j++) {
            appendStringInfo(out,
                "%*s%s: %u\n",
                level, "", f[i].name,
                STRUCT_MEMBER(uint32_t *, m, f[i].offset)[j]);
          }
        } else {
          appendStringInfo(out,
              "%*s%s: %u\n",
              level, "", f[i].name,
              STRUCT_MEMBER(uint32_t, m, f[i].offset));
        }
        break;
      case PROTOBUF_C_TYPE_SINT32:
      case PROTOBUF_C_TYPE_SFIXED32:
        if (f[i].label == PROTOBUF_C_LABEL_REPEATED) {
          for (j = 0; j < quantifier_offset; j++) {
            appendStringInfo(out,
                "%*s%s: %d\n",
                level, "", f[i].name,
                STRUCT_MEMBER(int32_t *, m, f[i].offset)[j]);
          }
        } else {
          appendStringInfo(out,
              "%*s%s: %d\n",
              level, "", f[i].name,
              STRUCT_MEMBER(int32_t, m, f[i].offset));
        }
        break;
      case PROTOBUF_C_TYPE_INT64:
      case PROTOBUF_C_TYPE_UINT64:
      case PROTOBUF_C_TYPE_FIXED64:
        if (f[i].label == PROTOBUF_C_LABEL_REPEATED) {
          for (j = 0; j < quantifier_offset; j++) {
            appendStringInfo(out,
                "%*s%s: %lu\n",
                level, "", f[i].name,
                STRUCT_MEMBER(uint64_t *, m, f[i].offset)[j]);
          }
        } else {
          appendStringInfo(out,
              "%*s%s: %lu\n",
              level, "", f[i].name,
              STRUCT_MEMBER(uint64_t, m, f[i].offset));
        }
        break;
      case PROTOBUF_C_TYPE_SINT64:
      case PROTOBUF_C_TYPE_SFIXED64:
        if (f[i].label == PROTOBUF_C_LABEL_REPEATED) {
          for (j = 0; j < quantifier_offset; j++) {
            appendStringInfo(out,
                "%*s%s: %ld\n",
                level, "", f[i].name,
                STRUCT_MEMBER(int64_t *, m, f[i].offset)[j]);
          }
        } else {
          appendStringInfo(out,
              "%*s%s: %ld\n",
              level, "", f[i].name,
              STRUCT_MEMBER(int64_t, m, f[i].offset));
        }
        break;
      case PROTOBUF_C_TYPE_FLOAT:
        if (f[i].label == PROTOBUF_C_LABEL_REPEATED) {
          for (j = 0; j < quantifier_offset; j++) {
            float_var = STRUCT_MEMBER(float *, m, f[i].offset)[j];
            appendStringInfo(out,
                "%*s%s: %g\n",
                level, "", f[i].name,
                float_var);
          }
        } else {
          float_var = STRUCT_MEMBER(float, m, f[i].offset);
          appendStringInfo(out,
              "%*s%s: %g\n",
              level, "", f[i].name,
              float_var);
        }
        break;
      case PROTOBUF_C_TYPE_DOUBLE:
        if (f[i].label == PROTOBUF_C_LABEL_REPEATED) {
          for (j = 0; j < quantifier_offset; j++) {
            appendStringInfo(out,
                "%*s%s: %g\n",
                level, "", f[i].name,
                STRUCT_MEMBER(double *, m, f[i].offset)[j]);
          }
        } else {
          appendStringInfo(out,
              "%*s%s: %g\n",
              level, "", f[i].name,
              STRUCT_MEMBER(double, m, f[i].offset));
        }
        break;
      case PROTOBUF_C_TYPE_BOOL:
        if (f[i].label == PROTOBUF_C_LABEL_REPEATED) {
          for (j = 0; j < quantifier_offset; j++) {
            appendStringInfo(out,
                "%*s%s: %s\n",
                level, "", f[i].name,
                STRUCT_MEMBER(protobuf_c_boolean *, m, f[i].offset)[j]?
                "true": "false");
          }
        } else {
          appendStringInfo(out,
              "%*s%s: %s\n",
              level, "", f[i].name,
              STRUCT_MEMBER(protobuf_c_boolean, m, f[i].offset)?
              "true": "false");
        }
        break;
      case PROTOBUF_C_TYPE_ENUM:
        enumd = (ProtobufCEnumDescriptor *)f[i].descriptor;
        if (f[i].label == PROTOBUF_C_LABEL_REPEATED) {
          for (j = 0; j < quantifier_offset; j++) {
            enumv = protobuf_c_enum_descriptor_get_value(
                enumd, STRUCT_MEMBER(int *, m, f[i].offset)[j]);
            appendStringInfo(out,
                "%*s%s: %s\n",
                level, "", f[i].name,
                enumv? enumv->name: "unknown");
          }
        } else {
          enumv = protobuf_c_enum_descriptor_get_value(
              enumd, STRUCT_MEMBER(int, m, f[i].offset));
          appendStringInfo(out,
              "%*s%s: %s\n",
              level, "", f[i].name,
              enumv? enumv->name: "unknown");
        }
        break;
      case PROTOBUF_C_TYPE_STRING:
        if (f[i].label == PROTOBUF_C_LABEL_REPEATED) {
          for (j = 0; j < quantifier_offset; j++) {
            unsigned char *escaped;

            escaped = esc_str(
                STRUCT_MEMBER(unsigned char **, m, f[i].offset)[j],
                strlen(STRUCT_MEMBER(unsigned char **, m, f[i].offset)[j]));
            if (!escaped) {
              return;
            }
            appendStringInfo(out,
                "%*s%s: \"%s\"\n", level, "", f[i].name, escaped);
            pfree(escaped);
          }
        } else {
          unsigned char *escaped;

          escaped = esc_str(STRUCT_MEMBER(unsigned char *, m, f[i].offset),
              strlen(STRUCT_MEMBER(unsigned char *, m, f[i].offset)));
          if (!escaped) {
            return;
          }
          appendStringInfo(out,
              "%*s%s: \"%s\"\n", level, "", f[i].name, escaped);
          pfree(escaped);
        }
        break;
      case PROTOBUF_C_TYPE_BYTES:
        if (f[i].label == PROTOBUF_C_LABEL_REPEATED) {
          for (j = 0; j < quantifier_offset; j++) {
            unsigned char *escaped;

            escaped = esc_str(
                STRUCT_MEMBER(ProtobufCBinaryData *, m, f[i].offset)[j].data,
                STRUCT_MEMBER(ProtobufCBinaryData *, m, f[i].offset)[j].len);
            if (!escaped) {
              return;
            }
            appendStringInfo(out,
                "%*s%s: \"%s\"\n", level, "", f[i].name, escaped);
            pfree(escaped);
          }
        } else {
          unsigned char *escaped;

          escaped = esc_str(
              STRUCT_MEMBER(ProtobufCBinaryData, m, f[i].offset).data,
              STRUCT_MEMBER(ProtobufCBinaryData, m, f[i].offset).len);
          if (!escaped) {
            return;
          }
          appendStringInfo(out,
              "%*s%s: \"%s\"\n", level, "", f[i].name, escaped);
          pfree(escaped);
        }
        break;

      case PROTOBUF_C_TYPE_MESSAGE:
        if (f[i].label == PROTOBUF_C_LABEL_REPEATED) {
          for (j = 0;
              j < STRUCT_MEMBER(size_t, m, f[i].quantifier_offset);
              j++) {
            appendStringInfo(out,
                "%*s%s {\n", level, "", f[i].name);
            protobuf_c_text_to_string_internal(out, level + 2,
                STRUCT_MEMBER(ProtobufCMessage **, m, f[i].offset)[j],
                (ProtobufCMessageDescriptor *)f[i].descriptor);
            appendStringInfo(out,
                "%*s}\n", level, "");
          }
        } else {
          appendStringInfo(out,
              "%*s%s {\n", level, "", f[i].name);
          protobuf_c_text_to_string_internal(out, level + 2,
              STRUCT_MEMBER(ProtobufCMessage *, m, f[i].offset),
              (ProtobufCMessageDescriptor *)f[i].descriptor);
          appendStringInfo(out,
              "%*s}\n", level, "");
        }
        break;
      default:
        return;
        break;
    }
  }
}


static void protobuf_c_text_to_string(StringInfo out, ProtobufCMessage *m) {
  protobuf_c_text_to_string_internal(out, 0, m, m->descriptor);
}
