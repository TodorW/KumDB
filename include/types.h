#ifndef KUMDB_TYPES_H
#define KUMDB_TYPES_H

#include "internal.h"

/* ============================================================
 * KumDB type engine
 * Handles type inference from raw strings, value construction,
 * comparison, serialization helpers, and display.
 * ============================================================ */

/* ------------------------------------------------------------
 * Type inference
 * Attempt to infer the type of a raw string value.
 * Returns KDB_TYPE_UNKNOWN if it can't figure it out.
 * ------------------------------------------------------------ */
KdbType kdb_type_infer(const char *raw);

/* ------------------------------------------------------------
 * Value construction from raw strings
 * Allocates heap memory for STRING/BLOB types.
 * Returns KDB_OK or KDB_ERR_BAD_TYPE / KDB_ERR_OOM.
 * ------------------------------------------------------------ */
KdbStatus kdb_value_from_string(const char *raw, KdbType hint, KdbValue *out);
KdbStatus kdb_value_from_int   (int64_t v,   KdbValue *out);
KdbStatus kdb_value_from_float (double v,    KdbValue *out);
KdbStatus kdb_value_from_bool  (uint8_t v,   KdbValue *out);
KdbStatus kdb_value_from_null  (KdbValue *out);

/* ------------------------------------------------------------
 * Value deep copy & free
 * kdb_value_copy allocates fresh heap memory for string/blob.
 * kdb_value_free releases any heap memory owned by the value.
 * ------------------------------------------------------------ */
KdbStatus kdb_value_copy(const KdbValue *src, KdbValue *dst);
void      kdb_value_free(KdbValue *v);

/* ------------------------------------------------------------
 * Comparison
 * Returns:
 *   < 0  if a < b
 *     0  if a == b
 *   > 0  if a > b
 * Returns INT32_MIN if types are incompatible (not comparable).
 * ------------------------------------------------------------ */
int kdb_value_compare(const KdbValue *a, const KdbValue *b);

/* ------------------------------------------------------------
 * Predicate evaluation
 * Applies operator op between field value and filter value(s).
 * value2 is only used for KDB_OP_BETWEEN.
 * Returns 1 if the predicate is satisfied, 0 otherwise.
 * ------------------------------------------------------------ */
int kdb_value_matches(const KdbValue *field,
                      KdbOperator     op,
                      const KdbValue *filter_value,
                      const KdbValue *filter_value2);

/* ------------------------------------------------------------
 * Type name helpers
 * ------------------------------------------------------------ */
const char *kdb_type_name(KdbType type);
const char *kdb_op_name  (KdbOperator op);

/* ------------------------------------------------------------
 * Parse a filter key string into column name + operator.
 * Input:  "age__gt"  →  col_name="age",  op=KDB_OP_GT
 * Input:  "name"     →  col_name="name", op=KDB_OP_EQ  (default)
 * Returns KDB_OK or KDB_ERR_BAD_FILTER.
 * ------------------------------------------------------------ */
KdbStatus kdb_parse_filter_key(const char  *key,
                               char         col_name_out[KDB_MAX_NAME_LEN],
                               KdbOperator *op_out);

/* ------------------------------------------------------------
 * Value display (for CLI / dump tool)
 * Writes a human-readable representation of v into buf.
 * Always null-terminates. Returns number of bytes written.
 * ------------------------------------------------------------ */
int kdb_value_to_str(const KdbValue *v, char *buf, size_t buf_size);

/* ------------------------------------------------------------
 * Type casting
 * Attempt to cast src to target_type and write into dst.
 * Returns KDB_OK or KDB_ERR_BAD_TYPE if cast is impossible.
 * ------------------------------------------------------------ */
KdbStatus kdb_value_cast(const KdbValue *src, KdbType target_type, KdbValue *dst);

/* ------------------------------------------------------------
 * Numeric helpers used internally
 * ------------------------------------------------------------ */
int kdb_str_is_int  (const char *s);
int kdb_str_is_float(const char *s);
int kdb_str_is_bool (const char *s);
int kdb_str_is_null (const char *s);

#endif /* KUMDB_TYPES_H */