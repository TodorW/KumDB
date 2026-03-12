#ifndef KUMDB_INTERNAL_H
#define KUMDB_INTERNAL_H

#include <stdint.h>
#include <stddef.h>
#include <stdio.h>
#include <time.h>

/* ============================================================
 * KumDB internal header — not for library users
 * All structs, constants, and macros shared across source files
 * ============================================================ */

/* ------------------------------------------------------------
 * Version & magic
 * ------------------------------------------------------------ */
#define KDB_MAGIC              0x4B554D44  /* "KUMD" */
#define KDB_VERSION_MAJOR      1
#define KDB_VERSION_MINOR      0
#define KDB_VERSION_PATCH      0

/* ------------------------------------------------------------
 * Limits
 * ------------------------------------------------------------ */
#define KDB_MAX_TABLES         256
#define KDB_MAX_COLUMNS        64
#define KDB_MAX_NAME_LEN       128
#define KDB_MAX_STRING_LEN     4096
#define KDB_MAX_RECORDS        (1 << 24)   /* ~16 million per table */
#define KDB_MAX_FILTER_KEYS    32
#define KDB_MAX_BATCH_SIZE     65536
#define KDB_PAGE_SIZE          4096
#define KDB_INDEX_BUCKETS      1024

/* ------------------------------------------------------------
 * Field types
 * ------------------------------------------------------------ */
typedef enum {
    KDB_TYPE_NULL    = 0,
    KDB_TYPE_INT     = 1,   /* int64_t          */
    KDB_TYPE_FLOAT   = 2,   /* double           */
    KDB_TYPE_BOOL    = 3,   /* uint8_t (0 or 1) */
    KDB_TYPE_STRING  = 4,   /* char[]           */
    KDB_TYPE_BLOB    = 5,   /* raw bytes        */
    KDB_TYPE_UNKNOWN = 255
} KdbType;

/* ------------------------------------------------------------
 * A single typed value
 * ------------------------------------------------------------ */
typedef struct {
    KdbType type;
    union {
        int64_t  as_int;
        double   as_float;
        uint8_t  as_bool;
        struct {
            char    *data;
            size_t   len;
        } as_string;
        struct {
            uint8_t *data;
            size_t   len;
        } as_blob;
    } v;
} KdbValue;

/* ------------------------------------------------------------
 * Column schema (one per column in a table)
 * ------------------------------------------------------------ */
typedef struct {
    char     name[KDB_MAX_NAME_LEN];
    KdbType  type;
    uint8_t  nullable;       /* 1 = nullable, 0 = required   */
    uint8_t  indexed;        /* 1 = has an index             */
    uint8_t  _pad[6];
} KdbColumn;

/* ------------------------------------------------------------
 * Table schema header (stored at the top of each .kdb file)
 * ------------------------------------------------------------ */
typedef struct {
    uint32_t magic;                          /* KDB_MAGIC                 */
    uint8_t  version_major;
    uint8_t  version_minor;
    uint8_t  version_patch;
    uint8_t  _pad0;
    char     table_name[KDB_MAX_NAME_LEN];
    uint32_t column_count;
    uint64_t record_count;
    uint64_t next_id;                        /* auto-increment row ID     */
    uint64_t created_at;                     /* unix timestamp            */
    uint64_t updated_at;
    uint64_t data_offset;                    /* byte offset to first row  */
    uint64_t index_offset;                   /* byte offset to index area */
    uint8_t  _pad1[64];                      /* reserved for future use   */
    KdbColumn columns[KDB_MAX_COLUMNS];
} KdbTableHeader;

/* ------------------------------------------------------------
 * A single field value inside a record
 * ------------------------------------------------------------ */
typedef struct {
    char     col_name[KDB_MAX_NAME_LEN];
    KdbValue value;
} KdbField;

/* ------------------------------------------------------------
 * A record (one row)
 * ------------------------------------------------------------ */
typedef struct {
    uint64_t  id;            /* auto-assigned row ID         */
    uint64_t  created_at;    /* unix timestamp               */
    uint64_t  updated_at;
    uint32_t  field_count;
    uint8_t   deleted;       /* soft-delete flag             */
    uint8_t   _pad[3];
    KdbField *fields;        /* heap-allocated array         */
} KdbRecord;

/* ------------------------------------------------------------
 * Filter operator (used by query engine)
 * ------------------------------------------------------------ */
typedef enum {
    KDB_OP_EQ          = 0,   /* field == value              */
    KDB_OP_NEQ         = 1,   /* field != value              */
    KDB_OP_GT          = 2,   /* field >  value              */
    KDB_OP_GTE         = 3,   /* field >= value              */
    KDB_OP_LT          = 4,   /* field <  value              */
    KDB_OP_LTE         = 5,   /* field <= value              */
    KDB_OP_CONTAINS    = 6,   /* string contains substring   */
    KDB_OP_STARTSWITH  = 7,   /* string starts with prefix   */
    KDB_OP_ENDSWITH    = 8,   /* string ends with suffix     */
    KDB_OP_IN          = 9,   /* field in value list         */
    KDB_OP_BETWEEN     = 10,  /* value_lo <= field <= value_hi */
    KDB_OP_IS_NULL     = 11,  /* field is null               */
    KDB_OP_IS_NOT_NULL = 12   /* field is not null           */
} KdbOperator;

/* ------------------------------------------------------------
 * A single filter predicate  (e.g. age__gt=21)
 * ------------------------------------------------------------ */
typedef struct {
    char        col_name[KDB_MAX_NAME_LEN];
    KdbOperator op;
    KdbValue    value;
    KdbValue    value2;   /* used by KDB_OP_BETWEEN for upper bound */
} KdbFilter;

/* ------------------------------------------------------------
 * A set of filters (AND semantics — all must match)
 * ------------------------------------------------------------ */
typedef struct {
    KdbFilter filters[KDB_MAX_FILTER_KEYS];
    uint32_t  count;
} KdbQuery;

/* ------------------------------------------------------------
 * Result set returned from kdb_find / kdb_find_one
 * ------------------------------------------------------------ */
typedef struct {
    KdbRecord *rows;       /* heap-allocated array of records  */
    size_t     count;
    size_t     capacity;
} KdbResult;

/* ------------------------------------------------------------
 * Index entry (hash-based, chained collision)
 * ------------------------------------------------------------ */
typedef struct KdbIndexNode {
    uint64_t            record_id;
    uint64_t            file_offset;    /* byte position of record in file */
    struct KdbIndexNode *next;           /* collision chain                 */
} KdbIndexNode;

typedef struct {
    char          col_name[KDB_MAX_NAME_LEN];
    KdbIndexNode *buckets[KDB_INDEX_BUCKETS];
} KdbIndex;

/* ------------------------------------------------------------
 * Open table handle (in-memory representation of a .kdb file)
 * ------------------------------------------------------------ */
typedef struct {
    char           name[KDB_MAX_NAME_LEN];
    char           path[4096];
    FILE          *fp;
    KdbTableHeader header;
    KdbIndex      *indices;         /* array of per-column indices    */
    uint32_t       index_count;
    uint8_t        dirty;           /* 1 = header needs flush         */
    uint8_t        read_only;
    uint8_t        _pad[6];
    int            lock_fd;         /* file descriptor for lock file  */
} KdbTable;

/* ------------------------------------------------------------
 * Top-level database handle  (returned by kdb_open)
 * ------------------------------------------------------------ */
typedef struct {
    char      data_dir[4096];       /* path to data folder            */
    KdbTable *tables[KDB_MAX_TABLES];
    uint32_t  table_count;
    uint8_t   read_only;
    uint8_t   _pad[7];
} KumDB;

/* ------------------------------------------------------------
 * Error codes
 * ------------------------------------------------------------ */
typedef enum {
    KDB_OK               =  0,
    KDB_ERR_OOM          = -1,   /* out of memory                  */
    KDB_ERR_IO           = -2,   /* file I/O failure               */
    KDB_ERR_NOT_FOUND    = -3,   /* table or record not found      */
    KDB_ERR_EXISTS       = -4,   /* table or record already exists */
    KDB_ERR_BAD_TYPE     = -5,   /* type mismatch or inference fail*/
    KDB_ERR_BAD_FILTER   = -6,   /* malformed filter key           */
    KDB_ERR_BAD_ARG      = -7,   /* NULL or invalid argument       */
    KDB_ERR_LOCKED       = -8,   /* file is locked by another proc */
    KDB_ERR_CORRUPT      = -9,   /* magic / version mismatch       */
    KDB_ERR_FULL         = -10,  /* table / batch limit hit        */
    KDB_ERR_VALIDATION   = -11,  /* user validator rejected record */
    KDB_ERR_READ_ONLY    = -12,  /* write attempted on RO handle   */
    KDB_ERR_UNKNOWN      = -99
} KdbStatus;

/* ------------------------------------------------------------
 * Serialization sizes (on-disk record layout)
 *
 * Record wire format (binary, little-endian):
 *   [uint64 id]
 *   [uint64 created_at]
 *   [uint64 updated_at]
 *   [uint32 field_count]
 *   [uint8  deleted]
 *   [uint8  _pad x3]
 *   for each field:
 *     [char[128] col_name]
 *     [uint8     type]
 *     [uint8     _pad x7]
 *     [payload based on type]:
 *       INT:    int64_t
 *       FLOAT:  double
 *       BOOL:   uint8_t
 *       STRING: uint32_t len + char[len]
 *       BLOB:   uint32_t len + uint8_t[len]
 *       NULL:   (nothing)
 * ------------------------------------------------------------ */
#define KDB_RECORD_FIXED_SIZE  (8 + 8 + 8 + 4 + 1 + 3)   /* 32 bytes */
#define KDB_FIELD_HEADER_SIZE  (KDB_MAX_NAME_LEN + 1 + 7) /* 136 bytes */

/* ------------------------------------------------------------
 * Utility macros
 * ------------------------------------------------------------ */
#define KDB_UNUSED(x)       ((void)(x))
#define KDB_ARRAY_LEN(a)    (sizeof(a) / sizeof((a)[0]))
#define KDB_MIN(a, b)       ((a) < (b) ? (a) : (b))
#define KDB_MAX(a, b)       ((a) > (b) ? (a) : (b))
#define KDB_CLAMP(v, lo, hi) KDB_MIN(KDB_MAX((v), (lo)), (hi))

/* Safe string copy that always null-terminates */
#define KDB_STRLCPY(dst, src, size) \
    do { \
        strncpy((dst), (src), (size) - 1); \
        (dst)[(size) - 1] = '\0'; \
    } while (0)

/* Allocate and zero a single struct */
#define KDB_ALLOC(type)     ((type *)calloc(1, sizeof(type)))

/* Free and NULL a pointer */
#define KDB_FREE(ptr) \
    do { \
        free(ptr); \
        (ptr) = NULL; \
    } while (0)

/* ------------------------------------------------------------
 * Forward declarations (for cross-file use without full headers)
 * ------------------------------------------------------------ */
struct KumDB;
struct KdbTable;
struct KdbRecord;
struct KdbQuery;
struct KdbResult;

#endif /* KUMDB_INTERNAL_H */