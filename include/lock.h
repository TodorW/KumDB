#ifndef KUMDB_LOCK_H
#define KUMDB_LOCK_H

#include "internal.h"

/* ============================================================
 * KumDB file locking
 * Uses fcntl advisory locks on Linux (perfect for KumOS).
 * Lock files live next to the .kdb file as  <name>.kdb.lock
 * ============================================================ */

/* ------------------------------------------------------------
 * Lock handle
 * ------------------------------------------------------------ */
typedef struct {
    int  fd;                    /* lock file descriptor (-1 = not held) */
    char path[4096];            /* path to the .lock file               */
} KdbLock;

/* ------------------------------------------------------------
 * Acquire / release
 * ------------------------------------------------------------ */

/* Open and exclusively lock the lock file for table_path.
 * Blocks until the lock is acquired if wait=1.
 * Returns immediately with KDB_ERR_LOCKED if wait=0 and lock is taken.
 * Returns KDB_OK on success.
 * The caller must call kdb_lock_release when done. */
KdbStatus kdb_lock_acquire(KdbLock *lock, const char *table_path, int wait);

/* Release and close the lock.
 * Safe to call on an already-released lock. */
void kdb_lock_release(KdbLock *lock);

/* Returns 1 if lock is currently held by this handle. */
int kdb_lock_is_held(const KdbLock *lock);

/* ------------------------------------------------------------
 * Atomic write helper
 * Write data to a temp file then rename into place.
 * This guarantees that a crash mid-write never leaves a
 * half-written .kdb file.
 *
 * Steps:
 *   1. Write data to <dest_path>.tmp
 *   2. fsync the temp file
 *   3. rename(<dest_path>.tmp, dest_path)   ← atomic on POSIX
 *   4. fsync the parent directory
 *
 * Returns KDB_OK or KDB_ERR_IO.
 * ------------------------------------------------------------ */
KdbStatus kdb_atomic_write(const char    *dest_path,
                           const uint8_t *data,
                           size_t         len);

/* ------------------------------------------------------------
 * Scoped lock macros
 * Acquire a lock at the top of a block and auto-release on
 * every return path via a cleanup label.
 *
 * Usage:
 *   KDB_LOCK_SCOPE(lk, table->path, 1) {
 *       ... do work ...
 *   }
 *   KDB_LOCK_END(lk);
 *
 * Note: KDB_LOCK_SCOPE sets 'kdb__lock_status'. Check it
 * immediately after the macro if you need to handle failure.
 * ------------------------------------------------------------ */
#define KDB_LOCK_SCOPE(lk, path, wait) \
    KdbLock lk = { .fd = -1 }; \
    KdbStatus kdb__lock_status = kdb_lock_acquire(&(lk), (path), (wait)); \
    if (kdb__lock_status == KDB_OK)

#define KDB_LOCK_END(lk) \
    kdb_lock_release(&(lk))

/* ------------------------------------------------------------
 * Lock file path helper
 * Writes the lock file path for table_path into out_buf.
 * out_buf must be at least 4096 bytes.
 * ------------------------------------------------------------ */
void kdb_lock_path(const char *table_path, char *out_buf, size_t out_size);

#endif /* KUMDB_LOCK_H */