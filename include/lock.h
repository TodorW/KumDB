#ifndef KUMDB_LOCK_H
#define KUMDB_LOCK_H

#include "internal.h"

typedef struct {
    int  fd;                    
    char path[4096];            
} KdbLock;

KdbStatus kdb_lock_acquire(KdbLock *lock, const char *table_path, int wait);

void kdb_lock_release(KdbLock *lock);

int kdb_lock_is_held(const KdbLock *lock);

KdbStatus kdb_atomic_write(const char    *dest_path,
                           const uint8_t *data,
                           size_t         len);

#define KDB_LOCK_SCOPE(lk, path, wait) \
    KdbLock lk = { .fd = -1 }; \
    KdbStatus kdb__lock_status = kdb_lock_acquire(&(lk), (path), (wait)); \
    if (kdb__lock_status == KDB_OK)

#define KDB_LOCK_END(lk) \
    kdb_lock_release(&(lk))

void kdb_lock_path(const char *table_path, char *out_buf, size_t out_size);

#endif 