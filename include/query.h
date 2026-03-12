#ifndef KUMDB_QUERY_H
#define KUMDB_QUERY_H

#include "internal.h"
#include "record.h"
#include "types.h"
#include "table.h"

/* ============================================================
 * KumDB query engine
 * Parses filter key strings (e.g. "age__gt"), builds KdbQuery
 * objects, evaluates them against records, and manages result sets.
 * ============================================================ */

/* ------------------------------------------------------------
 * Query construction
 * ------------------------------------------------------------ */

/* Initialize an empty query (zero filters). */
void kdb_query_init(KdbQuery *q);

/* Add a filter to a query.
 * key is the raw filter string e.g. "age__gt", "name__contains".
 * raw_value is the string representation of the filter value.
 * raw_value2 is used only for KDB_OP_BETWEEN (the upper bound).
 * Returns KDB_OK or KDB_ERR_BAD_FILTER / KDB_ERR_FULL. */
KdbStatus kdb_query_add_filter(KdbQuery   *q,
                               const char *key,
                               const char *raw_value,
                               const char *raw_value2);

/* Convenience: add a typed filter directly (no string parsing). */
KdbStatus kdb_query_add_filter_value(KdbQuery       *q,
                                     const char     *col_name,
                                     KdbOperator     op,
                                     const KdbValue *value,
                                     const KdbValue *value2);

/* Free any heap memory owned by filter values inside q.
 * Does NOT free q itself (it may be stack-allocated). */
void kdb_query_free(KdbQuery *q);

/* Returns 1 if q has zero filters (match-all query). */
int kdb_query_is_empty(const KdbQuery *q);

/* ------------------------------------------------------------
 * Record matching
 * ------------------------------------------------------------ */

/* Returns 1 if record r satisfies ALL filters in q, 0 otherwise. */
int kdb_query_matches(const KdbQuery *q, const KdbRecord *r);

/* ------------------------------------------------------------
 * Result set
 * ------------------------------------------------------------ */

/* Initialize an empty result set with initial capacity. */
KdbStatus kdb_result_init(KdbResult *res, size_t initial_capacity);

/* Append a deep copy of record r to res.
 * Grows the result set automatically.
 * Returns KDB_OK or KDB_ERR_OOM. */
KdbStatus kdb_result_append(KdbResult *res, const KdbRecord *r);

/* Free all memory owned by a result set.
 * Does NOT free res itself. */
void kdb_result_free(KdbResult *res);

/* Sort the result set by the given column name.
 * ascending: 1 = ASC, 0 = DESC.
 * Returns KDB_OK or KDB_ERR_NOT_FOUND. */
KdbStatus kdb_result_sort(KdbResult  *res,
                          const char *col_name,
                          int         ascending);

/* Limit the result set to at most max_rows entries.
 * Frees excess records. */
void kdb_result_limit(KdbResult *res, size_t max_rows);

/* Skip the first offset rows.
 * Frees the skipped records and shifts the array. */
void kdb_result_offset(KdbResult *res, size_t offset);

/* Print the full result set as a table to fp (for CLI / dump). */
void kdb_result_print(const KdbResult *res, FILE *fp);

/* ------------------------------------------------------------
 * Full query execution
 * Runs q against tbl and returns a populated result set.
 * Uses the index if one exists for a filter column, otherwise
 * falls back to a full table scan.
 *
 * The caller owns the returned result and must call
 * kdb_result_free when done.
 *
 * Returns KDB_OK or an error.
 * ------------------------------------------------------------ */
KdbStatus kdb_query_execute(KdbTable        *tbl,
                            const KdbQuery  *q,
                            KdbResult       *res_out);

/* Like kdb_query_execute but stops after the first match.
 * Returns the record in res_out (count will be 0 or 1).
 * Returns KDB_OK or KDB_ERR_NOT_FOUND. */
KdbStatus kdb_query_execute_one(KdbTable       *tbl,
                                const KdbQuery *q,
                                KdbResult      *res_out);

/* Count matching records without building a result set.
 * More efficient than kdb_query_execute when you only need count.
 * Returns KDB_OK, sets *count_out. */
KdbStatus kdb_query_count(KdbTable       *tbl,
                          const KdbQuery *q,
                          size_t         *count_out);

/* ------------------------------------------------------------
 * Query display (for CLI / debug)
 * ------------------------------------------------------------ */
void kdb_query_print(const KdbQuery *q, FILE *fp);

#endif /* KUMDB_QUERY_H */