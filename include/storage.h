#ifndef KUMDB_STORAGE_H
#define KUMDB_STORAGE_H

#include "internal.h"
#include "record.h"
#include "lock.h"

/* ============================================================
 * KumDB storage layer
 * All disk I/O goes through here. Handles the .kdb binary
 * format, header reads/writes, record scanning, and atomic
 * full-file rewrites for updates and deletes.
 * ============================================================ */

/* ------------------------------------------------------------
 * Table file lifecycle
 * ------------------------------------------------------------ */

/* Create a new .kdb file for table_name inside data_dir.
 * Writes the initial header with column schema.
 * Returns KDB_OK or an error code.
 * Fails with KDB_ERR_EXISTS if the file already exists. */
KdbStatus kdb_storage_create(const char       *data_dir,
                             const char       *table_name,
                             const KdbColumn  *columns,
                             uint32_t          column_count);

/* Open an existing .kdb file and populate tbl.
 * Validates magic and version.
 * Returns KDB_OK or KDB_ERR_NOT_FOUND / KDB_ERR_CORRUPT. */
KdbStatus kdb_storage_open(KdbTable   *tbl,
                           const char *data_dir,
                           const char *table_name);

/* Flush the in-memory header back to disk (updates record_count,
 * next_id, updated_at, etc.).
 * Returns KDB_OK or KDB_ERR_IO. */
KdbStatus kdb_storage_flush_header(KdbTable *tbl);

/* Close the file handle and release resources held by tbl.
 * Does NOT free the KdbTable pointer itself. */
void kdb_storage_close(KdbTable *tbl);

/* Permanently delete the .kdb file and its lock file.
 * Returns KDB_OK or KDB_ERR_IO. */
KdbStatus kdb_storage_drop(const char *data_dir, const char *table_name);

/* Returns 1 if a .kdb file exists for table_name in data_dir. */
int kdb_storage_exists(const char *data_dir, const char *table_name);

/* ------------------------------------------------------------
 * Single record I/O
 * ------------------------------------------------------------ */

/* Append a new record to the end of the data section.
 * Assigns r->id from tbl->header.next_id and increments it.
 * Updates tbl->header.record_count and tbl->dirty.
 * Returns KDB_OK or KDB_ERR_IO / KDB_ERR_FULL. */
KdbStatus kdb_storage_append(KdbTable *tbl, KdbRecord *r);

/* Read the record at byte offset file_offset from tbl.
 * Returns a newly allocated KdbRecord or NULL on error. */
KdbRecord *kdb_storage_read_at(KdbTable *tbl, uint64_t file_offset);

/* ------------------------------------------------------------
 * Full-table scan
 * Iterates every non-deleted record in the table file.
 * Calls callback(record, user_data) for each one.
 * If callback returns 0, the scan stops early (like break).
 * Returns KDB_OK or KDB_ERR_IO.
 * ------------------------------------------------------------ */
typedef int (*KdbScanCallback)(const KdbRecord *r, void *user_data);

KdbStatus kdb_storage_scan(KdbTable      *tbl,
                           KdbScanCallback callback,
                           void           *user_data);

/* ------------------------------------------------------------
 * Rewrite (used for updates and deletes)
 * Reads every record, applies transform_fn to each one, and
 * atomically rewrites the entire file.
 *
 * transform_fn receives a mutable record and user_data.
 * It should modify the record in-place (or set r->deleted=1).
 * Return 1 to keep the record, 0 to drop it from the output.
 *
 * The rewrite is atomic: the old file is only replaced after
 * the new file is fully written and fsync'd.
 *
 * Returns KDB_OK or KDB_ERR_IO / KDB_ERR_LOCKED.
 * ------------------------------------------------------------ */
typedef int (*KdbTransformFn)(KdbRecord *r, void *user_data);

KdbStatus kdb_storage_rewrite(KdbTable      *tbl,
                              KdbTransformFn transform_fn,
                              void          *user_data);

/* ------------------------------------------------------------
 * Compaction
 * Rewrites the table file removing all soft-deleted records.
 * Equivalent to calling kdb_storage_rewrite with an identity
 * transform (deleted records are naturally excluded).
 * Returns KDB_OK or KDB_ERR_IO.
 * ------------------------------------------------------------ */
KdbStatus kdb_storage_compact(KdbTable *tbl);

/* ------------------------------------------------------------
 * Batch append
 * Writes an array of records in a single buffered pass.
 * More efficient than calling kdb_storage_append in a loop.
 * Returns KDB_OK or KDB_ERR_IO / KDB_ERR_FULL.
 * ------------------------------------------------------------ */
KdbStatus kdb_storage_append_batch(KdbTable        *tbl,
                                   KdbRecord       *records,
                                   size_t           count);

/* ------------------------------------------------------------
 * Path helpers
 * ------------------------------------------------------------ */

/* Build the full .kdb file path into out_buf (>= 4096 bytes). */
void kdb_storage_path(const char *data_dir,
                      const char *table_name,
                      char       *out_buf,
                      size_t      out_size);

/* List all table names in data_dir.
 * Populates names_out (array of KDB_MAX_NAME_LEN strings).
 * Sets *count_out to number of tables found.
 * Returns KDB_OK or KDB_ERR_IO. */
KdbStatus kdb_storage_list_tables(const char *data_dir,
                                  char        names_out[][KDB_MAX_NAME_LEN],
                                  uint32_t   *count_out);

/* ------------------------------------------------------------
 * Header validation
 * ------------------------------------------------------------ */

/* Validate the magic bytes and version of a header read from disk.
 * Returns KDB_OK or KDB_ERR_CORRUPT. */
KdbStatus kdb_storage_validate_header(const KdbTableHeader *hdr,
                                      const char           *path);

/* ------------------------------------------------------------
 * Stats
 * ------------------------------------------------------------ */
typedef struct {
    uint64_t file_size_bytes;
    uint64_t record_count;
    uint64_t deleted_count;
    uint64_t live_count;
    double   fragmentation_ratio;  /* deleted / total */
} KdbStorageStats;

KdbStatus kdb_storage_stats(KdbTable *tbl, KdbStorageStats *out);

#endif /* KUMDB_STORAGE_H */