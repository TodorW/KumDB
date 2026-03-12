#ifndef KUMDB_TABLE_H
#define KUMDB_TABLE_H

#include "internal.h"
#include "storage.h"
#include "index.h"

/* ============================================================
 * KumDB table layer
 * Table lifecycle management: create, open, close, drop,
 * schema introspection, and column management.
 * The table layer sits above storage and below the query/API layer.
 * ============================================================ */

/* ------------------------------------------------------------
 * Table lifecycle
 * ------------------------------------------------------------ */

/* Create a new table in data_dir with the given column schema.
 * If columns is NULL / column_count is 0, the schema is inferred
 * lazily on first insert (schema-free mode).
 * Returns KDB_OK or an error.
 * Fails with KDB_ERR_EXISTS if the table already exists. */
KdbStatus kdb_table_create(const char      *data_dir,
                           const char      *table_name,
                           const KdbColumn *columns,
                           uint32_t         column_count);

/* Open an existing table and populate tbl.
 * Also builds in-memory indices for indexed columns.
 * Returns KDB_OK or KDB_ERR_NOT_FOUND / KDB_ERR_CORRUPT. */
KdbStatus kdb_table_open(KdbTable   *tbl,
                         const char *data_dir,
                         const char *table_name);

/* Flush header, release file handles, free indices.
 * Does NOT free tbl itself. */
void kdb_table_close(KdbTable *tbl);

/* Drop a table: close it (if open) and delete its .kdb + .lock files.
 * Returns KDB_OK or KDB_ERR_IO. */
KdbStatus kdb_table_drop(KdbTable   *tbl,
                         const char *data_dir,
                         const char *table_name);

/* Returns 1 if a table named table_name exists in data_dir. */
int kdb_table_exists(const char *data_dir, const char *table_name);

/* ------------------------------------------------------------
 * Schema management
 * KumDB infers schema from the first insert if none is provided.
 * You can also add columns explicitly after creation.
 * ------------------------------------------------------------ */

/* Add a column to an existing table's schema.
 * Existing records will return NULL for the new column.
 * Returns KDB_OK or KDB_ERR_EXISTS (column already defined)
 * or KDB_ERR_FULL (KDB_MAX_COLUMNS reached). */
KdbStatus kdb_table_add_column(KdbTable       *tbl,
                               const char     *col_name,
                               KdbType         type,
                               uint8_t         nullable,
                               uint8_t         indexed);

/* Remove a column from the schema.
 * Triggers a full table rewrite to drop the column data.
 * Returns KDB_OK or KDB_ERR_NOT_FOUND / KDB_ERR_IO. */
KdbStatus kdb_table_drop_column(KdbTable   *tbl,
                                const char *col_name);

/* Look up a column by name.
 * Returns a pointer into tbl->header.columns or NULL. */
const KdbColumn *kdb_table_get_column(const KdbTable *tbl,
                                      const char     *col_name);

/* Returns 1 if col_name is defined in the table schema. */
int kdb_table_has_column(const KdbTable *tbl, const char *col_name);

/* Infer and register schema columns from a record's fields.
 * Used on first insert when no schema was provided.
 * Returns KDB_OK or KDB_ERR_FULL. */
KdbStatus kdb_table_infer_schema(KdbTable        *tbl,
                                 const KdbRecord *r);

/* ------------------------------------------------------------
 * Record insert / update / delete (table-level, above storage)
 * These functions handle schema validation, index maintenance,
 * and locking. The raw I/O is delegated to storage.c.
 * ------------------------------------------------------------ */

/* Insert a record into tbl.
 * Validates against schema (if defined).
 * Updates all relevant indices.
 * Acquires the table lock for the duration.
 * Returns KDB_OK or an error. */
KdbStatus kdb_table_insert(KdbTable  *tbl,
                           KdbRecord *r);

/* Insert a batch of records in a single locked, buffered pass.
 * Returns KDB_OK or an error. On partial failure, the number of
 * successfully inserted records is written to *inserted_out. */
KdbStatus kdb_table_insert_batch(KdbTable  *tbl,
                                 KdbRecord *records,
                                 size_t     count,
                                 size_t    *inserted_out);

/* Update all records matching query using the fields in patch.
 * patch is a record whose fields are the new values to apply.
 * Only fields present in patch are modified.
 * Returns KDB_OK or an error. Sets *updated_out to # of rows changed. */
KdbStatus kdb_table_update(KdbTable        *tbl,
                           const KdbQuery  *query,
                           const KdbRecord *patch,
                           size_t          *updated_out);

/* Soft-delete all records matching query.
 * Returns KDB_OK or an error. Sets *deleted_out to # of rows removed. */
KdbStatus kdb_table_delete(KdbTable       *tbl,
                           const KdbQuery *query,
                           size_t         *deleted_out);

/* ------------------------------------------------------------
 * Utility
 * ------------------------------------------------------------ */

/* Compact the table file (remove soft-deleted records).
 * Rebuilds all indices after compaction.
 * Returns KDB_OK or KDB_ERR_IO. */
KdbStatus kdb_table_compact(KdbTable *tbl);

/* Print a human-readable schema description to fp. */
void kdb_table_print_schema(const KdbTable *tbl, FILE *fp);

/* Print table stats (record count, file size, etc.) to fp. */
void kdb_table_print_stats(KdbTable *tbl, FILE *fp);

/* Returns the number of live (non-deleted) records in the table. */
uint64_t kdb_table_count(KdbTable *tbl);

#endif /* KUMDB_TABLE_H */