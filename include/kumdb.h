#ifndef KUMDB_H
#define KUMDB_H

/*
 * ╔═══════════════════════════════════════════════════════════╗
 * ║                        KumDB                             ║
 * ║       The Database That Doesn't Waste Your Time          ║
 * ║                                                          ║
 * ║  This is the only header you need to include.            ║
 * ║  Everything else is internal. Don't touch it.            ║
 * ╚═══════════════════════════════════════════════════════════╝
 */

#include <stdint.h>
#include <stddef.h>
#include <stdio.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ============================================================
 * Opaque handle types (users never touch the internals)
 * ============================================================ */
typedef struct KumDB    KumDB;
typedef struct KdbTable KdbTable;

/* ============================================================
 * Status codes
 * ============================================================ */
typedef enum {
    KDB_OK               =  0,
    KDB_ERR_OOM          = -1,
    KDB_ERR_IO           = -2,
    KDB_ERR_NOT_FOUND    = -3,
    KDB_ERR_EXISTS       = -4,
    KDB_ERR_BAD_TYPE     = -5,
    KDB_ERR_BAD_FILTER   = -6,
    KDB_ERR_BAD_ARG      = -7,
    KDB_ERR_LOCKED       = -8,
    KDB_ERR_CORRUPT      = -9,
    KDB_ERR_FULL         = -10,
    KDB_ERR_VALIDATION   = -11,
    KDB_ERR_READ_ONLY    = -12,
    KDB_ERR_UNKNOWN      = -99
} KdbStatus;

/* ============================================================
 * Field types (used when building records manually)
 * ============================================================ */
typedef enum {
    KDB_TYPE_NULL    = 0,
    KDB_TYPE_INT     = 1,
    KDB_TYPE_FLOAT   = 2,
    KDB_TYPE_BOOL    = 3,
    KDB_TYPE_STRING  = 4,
    KDB_TYPE_BLOB    = 5
} KdbFieldType;

/* ============================================================
 * A single field value (used in KdbRow)
 * ============================================================ */
typedef struct {
    const char  *name;        /* column name                    */
    KdbFieldType type;
    union {
        int64_t      as_int;
        double       as_float;
        int          as_bool;
        const char  *as_string;
        struct { const void *data; size_t len; } as_blob;
    } v;
} KdbField;

/* ============================================================
 * A row (returned from find / find_one)
 * ============================================================ */
typedef struct {
    uint64_t    id;
    uint64_t    created_at;   /* unix timestamp */
    uint64_t    updated_at;
    uint32_t    field_count;
    KdbField   *fields;       /* heap-allocated — free with kdb_row_free */
} KdbRow;

/* ============================================================
 * A result set (returned from find)
 * ============================================================ */
typedef struct {
    KdbRow  *rows;            /* heap-allocated array           */
    size_t   count;
} KdbRows;

/* ============================================================
 * Validator callback type
 * Receives the row about to be inserted/updated.
 * Return KDB_OK to allow it, any other KdbStatus to reject it.
 * The error message you set via kdb_set_error() will be surfaced.
 * ============================================================ */
typedef KdbStatus (*KdbValidator)(const KdbRow *row, void *user_data);

/* ============================================================
 * Database lifecycle
 * ============================================================ */

/* Open (or create) a KumDB database in data_dir.
 * Creates the directory if it doesn't exist.
 * Returns NULL on failure — call kdb_last_error() to find out why. */
KumDB *kdb_open(const char *data_dir);

/* Open in read-only mode (no writes allowed). */
KumDB *kdb_open_readonly(const char *data_dir);

/* Flush pending writes and close all open tables.
 * Safe to call with NULL. */
void kdb_close(KumDB *db);

/* ============================================================
 * Core CRUD API
 * ============================================================ */

/* --- INSERT ---
 * Add a row to table_name.
 * Fields are supplied as a NULL-terminated array of KdbField.
 * Schema is inferred from the fields on first insert.
 * Returns KDB_OK or an error code.
 *
 * Example:
 *   KdbField fields[] = {
 *       { "name",     KDB_TYPE_STRING, .v.as_string = "John"  },
 *       { "age",      KDB_TYPE_INT,    .v.as_int    = 30      },
 *       { "is_admin", KDB_TYPE_BOOL,   .v.as_bool   = 1       },
 *       { NULL }
 *   };
 *   kdb_add(db, "users", fields);
 */
KdbStatus kdb_add(KumDB *db, const char *table_name, const KdbField *fields);

/* Convenience: add with a validator callback.
 * validator may be NULL (no validation).
 * user_data is passed through to the validator as-is. */
KdbStatus kdb_add_validated(KumDB            *db,
                            const char       *table_name,
                            const KdbField   *fields,
                            KdbValidator      validator,
                            void             *user_data);

/* --- BATCH INSERT ---
 * Insert multiple rows in a single atomic, buffered pass.
 * rows is an array of KdbField* (each NULL-terminated), count long.
 * Returns KDB_OK or an error. *inserted_out = # rows that made it in. */
KdbStatus kdb_batch_import(KumDB             *db,
                           const char        *table_name,
                           const KdbField   **rows,
                           size_t             count,
                           size_t            *inserted_out);

/* --- FIND ---
 * Query rows from table_name.
 * filters is a NULL-terminated array of "key=value" strings.
 * Filter operators: name, name__eq, name__neq, name__gt,
 *   name__gte, name__lt, name__lte, name__contains,
 *   name__startswith, name__endswith, name__between (use two values).
 *
 * Returns a heap-allocated KdbRows. Call kdb_rows_free when done.
 * Returns NULL on error (check kdb_last_error).
 *
 * Example:
 *   const char *filters[] = { "age__gt=21", "name__contains=Savage", NULL };
 *   KdbRows *result = kdb_find(db, "users", filters);
 */
KdbRows *kdb_find(KumDB *db, const char *table_name, const char **filters);

/* Like kdb_find but returns at most one row (the first match).
 * Returns NULL if no match or on error. */
KdbRow *kdb_find_one(KumDB *db, const char *table_name, const char **filters);

/* Find by the auto-assigned row ID (fastest possible lookup). */
KdbRow *kdb_find_by_id(KumDB *db, const char *table_name, uint64_t id);

/* Count matching rows without loading them.
 * Returns -1 on error. */
int64_t kdb_count(KumDB *db, const char *table_name, const char **filters);

/* --- UPDATE ---
 * Update all rows matching where_filters.
 * set_fields is a NULL-terminated array of KdbField with new values.
 * Only fields listed in set_fields are modified; others are untouched.
 * Returns KDB_OK or an error. *updated_out = # rows changed. */
KdbStatus kdb_update(KumDB            *db,
                     const char       *table_name,
                     const char      **where_filters,
                     const KdbField   *set_fields,
                     size_t           *updated_out);

/* --- DELETE ---
 * Soft-delete all rows matching filters.
 * Returns KDB_OK or an error. *deleted_out = # rows removed. */
KdbStatus kdb_delete(KumDB       *db,
                     const char  *table_name,
                     const char **filters,
                     size_t      *deleted_out);

/* ============================================================
 * Table management
 * ============================================================ */

/* Drop a table and delete its file. Permanent, no undo.
 * Returns KDB_OK or KDB_ERR_NOT_FOUND / KDB_ERR_IO. */
KdbStatus kdb_drop_table(KumDB *db, const char *table_name);

/* Compact a table (remove soft-deleted rows from the file).
 * Returns KDB_OK or KDB_ERR_IO. */
KdbStatus kdb_compact(KumDB *db, const char *table_name);

/* Returns 1 if the table exists, 0 if not. */
int kdb_table_exists(KumDB *db, const char *table_name);

/* List all table names in the database.
 * Populates names_out (array of at least max_tables char* pointers).
 * Sets *count_out. Returns KDB_OK or KDB_ERR_IO. */
KdbStatus kdb_list_tables(KumDB       *db,
                          const char **names_out,
                          size_t       max_tables,
                          size_t      *count_out);

/* ============================================================
 * Result set helpers
 * ============================================================ */

/* Free a KdbRows result set (including all row data). */
void kdb_rows_free(KdbRows *rows);

/* Free a single KdbRow. */
void kdb_row_free(KdbRow *row);

/* Get a field value from a row by column name.
 * Returns NULL if not found. */
const KdbField *kdb_row_get(const KdbRow *row, const char *col_name);

/* Typed getters — populate *out and return KDB_OK,
 * or return KDB_ERR_NOT_FOUND / KDB_ERR_BAD_TYPE. */
KdbStatus kdb_row_get_int   (const KdbRow *row, const char *col, int64_t    *out);
KdbStatus kdb_row_get_float (const KdbRow *row, const char *col, double     *out);
KdbStatus kdb_row_get_bool  (const KdbRow *row, const char *col, int        *out);
KdbStatus kdb_row_get_string(const KdbRow *row, const char *col, const char **out);

/* ============================================================
 * Error handling
 * ============================================================ */

/* Get the last error message for the current thread.
 * Always returns a valid string (never NULL). */
const char *kdb_last_error(void);

/* Get the last KdbStatus code. */
KdbStatus kdb_last_status(void);

/* Clear the last error. */
void kdb_clear_error(void);

/* ============================================================
 * Utility / debug
 * ============================================================ */

/* Print a row to fp in a human-readable format. */
void kdb_row_print(const KdbRow *row, FILE *fp);

/* Print all rows in a KdbRows result to fp as a table. */
void kdb_rows_print(const KdbRows *rows, FILE *fp);

/* Print schema information for table_name to fp. */
KdbStatus kdb_print_schema(KumDB *db, const char *table_name, FILE *fp);

/* Returns the KumDB version string e.g. "1.0.0". */
const char *kdb_version(void);

/* ============================================================
 * Field construction helpers
 * Make it easy to build KdbField arrays without memorising
 * the union syntax.
 * ============================================================ */
static inline KdbField kdb_field_int   (const char *n, int64_t     v) { KdbField f = {n, KDB_TYPE_INT,    .v.as_int    = v}; return f; }
static inline KdbField kdb_field_float (const char *n, double      v) { KdbField f = {n, KDB_TYPE_FLOAT,  .v.as_float  = v}; return f; }
static inline KdbField kdb_field_bool  (const char *n, int         v) { KdbField f = {n, KDB_TYPE_BOOL,   .v.as_bool   = v}; return f; }
static inline KdbField kdb_field_string(const char *n, const char *v) { KdbField f = {n, KDB_TYPE_STRING, .v.as_string = v}; return f; }
static inline KdbField kdb_field_null  (const char *n)                { KdbField f = {n, KDB_TYPE_NULL,   {0}           }; return f; }
/* Sentinel — use as the last element of a KdbField array */
static inline KdbField kdb_field_end   (void)                         { KdbField f = {NULL, KDB_TYPE_NULL, {0}           }; return f; }

#ifdef __cplusplus
}
#endif

#endif /* KUMDB_H */