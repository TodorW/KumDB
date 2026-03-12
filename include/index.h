#ifndef KUMDB_INDEX_H
#define KUMDB_INDEX_H

#include "internal.h"
#include "record.h"

/* ============================================================
 * KumDB index layer
 * Per-column hash index for O(1) average-case lookups.
 * Built on top of a chained hash table with KDB_INDEX_BUCKETS
 * buckets. Indices are held in memory and rebuilt from the
 * table file on open.
 * ============================================================ */

/* ------------------------------------------------------------
 * Index lifecycle
 * ------------------------------------------------------------ */

/* Allocate and initialize a new empty index for col_name.
 * Returns NULL on OOM. */
KdbIndex *kdb_index_new(const char *col_name);

/* Free all memory owned by the index (nodes + bucket arrays).
 * Safe to call with NULL. */
void kdb_index_free(KdbIndex *idx);

/* Free an array of index pointers (and each index within it).
 * Also frees the array pointer itself. */
void kdb_index_free_array(KdbIndex **indices, uint32_t count);

/* ------------------------------------------------------------
 * Index population
 * ------------------------------------------------------------ */

/* Insert a record into the index.
 * file_offset is the byte position of the record in the .kdb file.
 * Returns KDB_OK or KDB_ERR_OOM. */
KdbStatus kdb_index_insert(KdbIndex       *idx,
                           const KdbRecord *r,
                           uint64_t         file_offset);

/* Remove a record from the index by its id.
 * Returns KDB_OK or KDB_ERR_NOT_FOUND. */
KdbStatus kdb_index_remove(KdbIndex *idx, uint64_t record_id);

/* Rebuild the entire index by scanning tbl from disk.
 * Clears all existing entries first.
 * Returns KDB_OK or KDB_ERR_IO. */
KdbStatus kdb_index_rebuild(KdbIndex *idx, struct KdbTable *tbl);

/* ------------------------------------------------------------
 * Index lookup
 * ------------------------------------------------------------ */

/* Look up a record by the value of the indexed column.
 * Populates file_offsets_out (caller-supplied array of size max_results).
 * Sets *count_out to the number of matching offsets found.
 * Returns KDB_OK or KDB_ERR_NOT_FOUND. */
KdbStatus kdb_index_lookup(const KdbIndex *idx,
                           const KdbValue *value,
                           uint64_t       *file_offsets_out,
                           size_t          max_results,
                           size_t         *count_out);

/* Convenience: returns the file offset of the first match,
 * or UINT64_MAX if not found. */
uint64_t kdb_index_lookup_one(const KdbIndex *idx, const KdbValue *value);

/* ------------------------------------------------------------
 * Index management helpers (used by table layer)
 * ------------------------------------------------------------ */

/* Given an array of KdbColumn, build an index for each column
 * with indexed=1.  Populates indices_out (caller-allocated array
 * of KDB_MAX_COLUMNS pointers).  Sets *count_out.
 * Returns KDB_OK or KDB_ERR_OOM. */
KdbStatus kdb_index_build_for_table(const KdbColumn *columns,
                                    uint32_t         column_count,
                                    KdbIndex       **indices_out,
                                    uint32_t        *count_out);

/* Find the index for col_name in an array of indices.
 * Returns the KdbIndex pointer or NULL if not indexed. */
KdbIndex *kdb_index_find(KdbIndex **indices,
                         uint32_t   count,
                         const char *col_name);

/* ------------------------------------------------------------
 * Hash function
 * Public so that query.c can hash filter values consistently
 * with the index.
 * ------------------------------------------------------------ */
uint32_t kdb_index_hash(const KdbValue *v);

/* ------------------------------------------------------------
 * Stats
 * ------------------------------------------------------------ */
typedef struct {
    size_t   entry_count;
    size_t   bucket_count;
    size_t   collision_count;
    double   load_factor;
    size_t   longest_chain;
} KdbIndexStats;

void kdb_index_stats(const KdbIndex *idx, KdbIndexStats *out);

#endif /* KUMDB_INDEX_H */