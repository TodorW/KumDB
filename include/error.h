#ifndef KUMDB_ERROR_H
#define KUMDB_ERROR_H

#include "internal.h"
#include <stdarg.h>

/* ============================================================
 * KumDB error system
 * Centralized, opinionated, brutally honest error messages.
 * ============================================================ */

/* ------------------------------------------------------------
 * Thread-local last error storage
 * Call kdb_last_error() to get the last error message for the
 * current thread.
 * ------------------------------------------------------------ */
const char *kdb_last_error(void);

/* ------------------------------------------------------------
 * Set the last error with a formatted message.
 * Used internally by all KumDB source files.
 * ------------------------------------------------------------ */
void kdb_set_error(KdbStatus status, const char *fmt, ...);

/* ------------------------------------------------------------
 * Clear the last error.
 * ------------------------------------------------------------ */
void kdb_clear_error(void);

/* ------------------------------------------------------------
 * Get the last KdbStatus code (not just the message).
 * ------------------------------------------------------------ */
KdbStatus kdb_last_status(void);

/* ------------------------------------------------------------
 * Human-readable status code name
 * e.g. KDB_ERR_OOM → "KDB_ERR_OOM"
 * ------------------------------------------------------------ */
const char *kdb_status_name(KdbStatus status);

/* ------------------------------------------------------------
 * Pre-baked savage error setters
 * Each one calls kdb_set_error internally with a spicy message.
 * ------------------------------------------------------------ */

/* OOM errors */
void kdb_err_oom(const char *what);

/* I/O errors */
void kdb_err_io           (const char *path, const char *op);
void kdb_err_io_corrupt   (const char *path);
void kdb_err_io_locked    (const char *path);

/* Argument errors */
void kdb_err_null_arg     (const char *arg_name, const char *fn_name);
void kdb_err_bad_arg      (const char *arg_name, const char *reason);

/* Table errors */
void kdb_err_table_not_found (const char *table_name);
void kdb_err_table_exists    (const char *table_name);
void kdb_err_table_full      (const char *table_name);
void kdb_err_table_read_only (const char *table_name);

/* Record / field errors */
void kdb_err_record_not_found(uint64_t id, const char *table_name);
void kdb_err_field_not_found (const char *col_name, const char *table_name);
void kdb_err_bad_type        (const char *col_name, KdbType expected, KdbType got);
void kdb_err_bad_filter      (const char *filter_key, const char *reason);

/* Validation errors */
void kdb_err_validation      (const char *table_name, const char *reason);

/* Batch errors */
void kdb_err_batch_too_large (size_t count, size_t limit);

/* ------------------------------------------------------------
 * Debug / assert macro (compiled out in release builds)
 * ------------------------------------------------------------ */
#ifdef KDB_DEBUG
  #include <stdio.h>
  #define KDB_ASSERT(cond, msg) \
      do { \
          if (!(cond)) { \
              fprintf(stderr, "[KumDB ASSERT] %s:%d — %s\n", \
                      __FILE__, __LINE__, (msg)); \
              __builtin_trap(); \
          } \
      } while (0)
#else
  #define KDB_ASSERT(cond, msg)  ((void)0)
#endif

/* ------------------------------------------------------------
 * Convenience macro: set error and return a status code.
 * Usage:  KDB_FAIL(KDB_ERR_BAD_ARG, kdb_err_null_arg("db", "kdb_open"));
 * ------------------------------------------------------------ */
#define KDB_FAIL(status, error_call) \
    do { \
        error_call; \
        return (status); \
    } while (0)

/* Variant that returns NULL instead of a status */
#define KDB_FAIL_NULL(error_call) \
    do { \
        error_call; \
        return NULL; \
    } while (0)

#endif /* KUMDB_ERROR_H */