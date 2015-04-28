#ifndef PROTOBUF_C_TEXT_H
#define PROTOBUF_C_TEXT_H

#include "postgres.h"
#include "lib/stringinfo.h"
#include "protobuf-c/protobuf-c.h"


/* BEGIN MACROS TAKEN FROM protobuf-c/protobuf-c.c */
/**
 * Internal `ProtobufCMessage` manipulation macro.
 *
 * Base macro for manipulating a `ProtobufCMessage`. Used by STRUCT_MEMBER() and
 * STRUCT_MEMBER_PTR().
 */
#define STRUCT_MEMBER_P(struct_p, struct_offset) \
    ((void *) ((uint8_t *) (struct_p) + (struct_offset)))

/**
 * Return field in a `ProtobufCMessage` based on offset.
 *
 * Take a pointer to a `ProtobufCMessage` and find the field at the offset.
 * Cast it to the passed type.
 */
#define STRUCT_MEMBER(member_type, struct_p, struct_offset) \
    (*(member_type *) STRUCT_MEMBER_P((struct_p), (struct_offset)))

/**
 * Return field in a `ProtobufCMessage` based on offset.
 *
 * Take a pointer to a `ProtobufCMessage` and find the field at the offset. Cast
 * it to a pointer to the passed type.
 */
#define STRUCT_MEMBER_PTR(member_type, struct_p, struct_offset) \
    ((member_type *) STRUCT_MEMBER_P((struct_p), (struct_offset)))
/* END MACROS FROM protobuf-c/protobuf-c.c */


/** Internal function to back API function.
 *
 * Has a few extra params to better enable recursion.  This function gets
 * called for each nested message as the \c ProtobufCMessage struct is
 * traversed.
 *
 * \param[in,out] out The string being used for the text format protobuf.
 * \param[in] level Indent level - increments in 2's.
 * \param[in] m The \c ProtobufCMessage being serialised.
 * \param[in] d The descriptor for the \c ProtobufCMessage.
 */
extern void protobuf_c_text_to_string_internal(StringInfo out,
                                               int level,
                                               ProtobufCMessage *m,
                                               const ProtobufCMessageDescriptor *d);

extern void protobuf_c_text_to_string(StringInfo out, ProtobufCMessage *m);

#endif /* PROTOBUF_C_TEXT_H */
