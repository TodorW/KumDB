#ifndef KUMDB_RECORD_H
#define KUMDB_RECORD_H

#include "internal.h"
#include "types.h"

/* ============================================================
 * KumDB record layer
 * Construction, field access, deep copy, free, and
 * binary serialization / deserialization of rows.
 * ============================================================ */

/* ------------------------------------------------------------
 * Lifecycle
 * ------------------------------------------------------------ */

/* Allocate a new empty record with capacity for field_count fields.
 * id is assigned automatically from the table's next_id counter;
 * pass 0 to leave it unset (the storage layer will assign it).
 * Returns NULL on OOM. */
KdbRecord *kdb_record_new(uint32_t field_count);

/* Deep copy src into a freshly allocated record.
 * Returns NULL on OOM. */
KdbRecord *kdb_record_copy(const KdbRecord *src);

/* Free all memory owned by a record (fields + string/blob data).
 * Safe to call with NULL. */
void kdb_record_free(KdbRecord *r);

/* Free an array of records (e.g. a result set).
 * Also frees the array pointer itself. */
void kdb_record_free_array(KdbRecord *arr, size_t count);

/* ------------------------------------------------------------
 * Field mutation
 * ------------------------------------------------------------ */

/* Add a field to a record.  The record must have been created with
 * enough capacity (field_count passed to kdb_record_new).
 * Performs a deep copy of value.
 * Returns KDB_OK or KDB_ERR_OOM / KDB_ERR_FULL. */
KdbStatus kdb_record_set_field(KdbRecord  *r,
                               const char *col_name,
                               const KdbValue *value);

/* Convenience wrappers for common types */
KdbStatus kdb_record_set_int   (KdbRecord *r, const char *col, int64_t  v);
KdbStatus kdb_record_set_float (KdbRecord *r, const char *col, double   v);
KdbStatus kdb_record_set_bool  (KdbRecord *r, const char *col, uint8_t  v);
KdbStatus kdb_record_set_string(KdbRecord *r, const char *col, const char *v);
KdbStatus kdb_record_set_null  (KdbRecord *r, const char *col);

/* Update an existing field value in-place.
 * Returns KDB_ERR_NOT_FOUND if col_name doesn't exist in the record. */
KdbStatus kdb_record_update_field(KdbRecord      *r,
                                  const char     *col_name,
                                  const KdbValue *new_value);

/* ------------------------------------------------------------
 * Field access
 * ------------------------------------------------------------ */

/* Get a pointer to the KdbField for col_name.
 * Returns NULL if not found. */
const KdbField *kdb_record_get_field(const KdbRecord *r, const char *col_name);

/* Typed getters — populate *out and return KDB_OK,
 * or return KDB_ERR_NOT_FOUND / KDB_ERR_BAD_TYPE. */
KdbStatus kdb_record_get_int   (const KdbRecord *r, const char *col, int64_t  *out);
KdbStatus kdb_record_get_float (const KdbRecord *r, const char *col, double   *out);
KdbStatus kdb_record_get_bool  (const KdbRecord *r, const char *col, uint8_t  *out);
KdbStatus kdb_record_get_string(const KdbRecord *r, const char *col, const char **out);

/* Returns 1 if the field exists and is NULL, 0 otherwise. */
int kdb_record_is_null(const KdbRecord *r, const char *col_name);

/* Returns 1 if the field exists (regardless of type), 0 otherwise. */
int kdb_record_has_field(const KdbRecord *r, const char *col_name);

/* ------------------------------------------------------------
 * Serialization (binary wire format)
 * See internal.h for the exact byte layout.
 * ------------------------------------------------------------ */

/* Calculate the exact serialized size in bytes for record r. */
size_t kdb_record_serial_size(const KdbRecord *r);

/* Serialize record r into buf (must be >= kdb_record_serial_size bytes).
 * Returns number of bytes written, or 0 on error. */
size_t kdb_record_serialize(const KdbRecord *r, uint8_t *buf, size_t buf_size);

/* Deserialize a record from buf.
 * Allocates a new KdbRecord on the heap.
 * bytes_read is set to the number of bytes consumed.
 * Returns NULL on error (check kdb_last_error). */
KdbRecord *kdb_record_deserialize(const uint8_t *buf,
                                  size_t         buf_size,
                                  size_t        *bytes_read);

/* Write record r to file fp at its current position.
 * Returns KDB_OK or KDB_ERR_IO. */
KdbStatus kdb_record_write(const KdbRecord *r, FILE *fp);

/* Read a record from file fp at its current position.
 * Allocates and returns a new KdbRecord.
 * Returns NULL on error or EOF. */
KdbRecord *kdb_record_read(FILE *fp);

/* ------------------------------------------------------------
 * Utility
 * ------------------------------------------------------------ */

/* Print a human-readable representation of the record to fp.
 * Used by the dump tool and CLI. */
void kdb_record_print(const KdbRecord *r, FILE *fp);

/* Returns 1 if the record is soft-deleted, 0 otherwise. */
int kdb_record_is_deleted(const KdbRecord *r);

/* Mark a record as soft-deleted (does not remove from disk). */
void kdb_record_mark_deleted(KdbRecord *r);

/* Compare two records by id (for sorting). */
int kdb_record_cmp_id(const void *a, const void *b);

#endif /* KUMDB_RECORD_H */