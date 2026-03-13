#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "../include/index.h"
#include "../include/storage.h"
#include "../include/error.h"
#include "../include/types.h"


uint32_t kdb_index_hash(const KdbValue *v) {
    if (!v) return 0;

    uint32_t hash = 2166136261u; 
#define FNV_MIX(byte) hash ^= (uint8_t)(byte); hash *= 16777619u

    switch (v->type) {
        case KDB_TYPE_INT: {
            uint64_t n = (uint64_t)v->v.as_int;
            for (int i = 0; i < 8; i++) { FNV_MIX(n); n >>= 8; }
            break;
        }
        case KDB_TYPE_FLOAT: {
            uint64_t bits;
            memcpy(&bits, &v->v.as_float, 8);
            for (int i = 0; i < 8; i++) { FNV_MIX(bits); bits >>= 8; }
            break;
        }
        case KDB_TYPE_BOOL:
            FNV_MIX(v->v.as_bool);
            break;
        case KDB_TYPE_STRING:
            if (v->v.as_string.data) {
                const char *p = v->v.as_string.data;
                while (*p) { FNV_MIX(*p); p++; }
            }
            break;
        case KDB_TYPE_BLOB:
            if (v->v.as_blob.data) {
                const uint8_t *p = v->v.as_blob.data;
                for (size_t i = 0; i < v->v.as_blob.len; i++) { FNV_MIX(p[i]); }
            }
            break;
        case KDB_TYPE_NULL:
        default:
            FNV_MIX(0xff);
            break;
    }
#undef FNV_MIX

    return hash % KDB_INDEX_BUCKETS;
}


KdbIndex *kdb_index_new(const char *col_name) {
    KdbIndex *idx = (KdbIndex *)calloc(1, sizeof(KdbIndex));
    if (!idx) { kdb_err_oom("KdbIndex"); return NULL; }
    if (col_name)
        KDB_STRLCPY(idx->col_name, col_name, KDB_MAX_NAME_LEN);
    
    return idx;
}

void kdb_index_free(KdbIndex *idx) {
    if (!idx) return;
    for (uint32_t i = 0; i < KDB_INDEX_BUCKETS; i++) {
        KdbIndexNode *node = idx->buckets[i];
        while (node) {
            KdbIndexNode *next = node->next;
            free(node);
            node = next;
        }
        idx->buckets[i] = NULL;
    }
    free(idx);
}

void kdb_index_free_array(KdbIndex **indices, uint32_t count) {
    if (!indices) return;
    for (uint32_t i = 0; i < count; i++)
        kdb_index_free(indices[i]);
    free(indices);
}


KdbStatus kdb_index_insert(KdbIndex       *idx,
                           const KdbRecord *r,
                           uint64_t         file_offset) {
    if (!idx || !r) {
        kdb_err_null_arg("idx/r", "kdb_index_insert");
        return KDB_ERR_BAD_ARG;
    }

    
    const KdbRecordField *f = kdb_record_get_field(r, idx->col_name);
    if (!f) return KDB_OK; 

    uint32_t bucket = kdb_index_hash(&f->value);

    KdbIndexNode *node = (KdbIndexNode *)calloc(1, sizeof(KdbIndexNode));
    if (!node) { kdb_err_oom("KdbIndexNode"); return KDB_ERR_OOM; }

    node->record_id   = r->id;
    node->file_offset = file_offset;

    
    node->next         = idx->buckets[bucket];
    idx->buckets[bucket] = node;

    return KDB_OK;
}


KdbStatus kdb_index_remove(KdbIndex *idx, uint64_t record_id) {
    if (!idx) {
        kdb_err_null_arg("idx", "kdb_index_remove");
        return KDB_ERR_BAD_ARG;
    }

    for (uint32_t i = 0; i < KDB_INDEX_BUCKETS; i++) {
        KdbIndexNode **pp = &idx->buckets[i];
        while (*pp) {
            if ((*pp)->record_id == record_id) {
                KdbIndexNode *to_free = *pp;
                *pp = to_free->next;
                free(to_free);
                return KDB_OK;
            }
            pp = &(*pp)->next;
        }
    }

    kdb_err_record_not_found(record_id, idx->col_name);
    return KDB_ERR_NOT_FOUND;
}


typedef struct {
    KdbIndex *idx;
    uint64_t  file_offset;
    FILE     *fp;
    uint64_t  data_offset;
} KdbRebuildCtx;

static int kdb__rebuild_cb(const KdbRecord *r, void *ud) {
    KdbRebuildCtx *ctx = (KdbRebuildCtx *)ud;
    

    kdb_index_insert(ctx->idx, r, ctx->file_offset);
    

    ctx->file_offset += 4 + kdb_record_serial_size(r);
    return 1;
}

KdbStatus kdb_index_rebuild(KdbIndex *idx, KdbTable *tbl) {
    if (!idx || !tbl) {
        kdb_err_null_arg("idx/tbl", "kdb_index_rebuild");
        return KDB_ERR_BAD_ARG;
    }

    
    for (uint32_t i = 0; i < KDB_INDEX_BUCKETS; i++) {
        KdbIndexNode *node = idx->buckets[i];
        while (node) {
            KdbIndexNode *next = node->next;
            free(node);
            node = next;
        }
        idx->buckets[i] = NULL;
    }

    KdbRebuildCtx ctx = {
        .idx         = idx,
        .file_offset = tbl->header.data_offset,
        .fp          = tbl->fp,
        .data_offset = tbl->header.data_offset
    };

    return kdb_storage_scan(tbl, kdb__rebuild_cb, &ctx);
}


KdbStatus kdb_index_lookup(const KdbIndex *idx,
                           const KdbValue *value,
                           uint64_t       *file_offsets_out,
                           size_t          max_results,
                           size_t         *count_out) {
    if (!idx || !value || !file_offsets_out || !count_out) {
        kdb_err_null_arg("idx/value/file_offsets_out/count_out", "kdb_index_lookup");
        return KDB_ERR_BAD_ARG;
    }

    *count_out = 0;
    uint32_t bucket = kdb_index_hash(value);

    const KdbIndexNode *node = idx->buckets[bucket];
    while (node && *count_out < max_results) {
        

        file_offsets_out[(*count_out)++] = node->file_offset;
        node = node->next;
    }

    if (*count_out == 0) {
        kdb_set_error(KDB_ERR_NOT_FOUND, "No index entries found for value in column '%s'.",
                      idx->col_name);
        return KDB_ERR_NOT_FOUND;
    }
    return KDB_OK;
}

uint64_t kdb_index_lookup_one(const KdbIndex *idx, const KdbValue *value) {
    if (!idx || !value) return UINT64_MAX;
    uint64_t offset = 0;
    size_t   count  = 0;
    KdbStatus st = kdb_index_lookup(idx, value, &offset, 1, &count);
    return (st == KDB_OK && count > 0) ? offset : UINT64_MAX;
}


KdbStatus kdb_index_build_for_table(const KdbColumn *columns,
                                    uint32_t         column_count,
                                    KdbIndex       **indices_out,
                                    uint32_t        *count_out) {
    if (!columns || !indices_out || !count_out) {
        kdb_err_null_arg("columns/indices_out/count_out", "kdb_index_build_for_table");
        return KDB_ERR_BAD_ARG;
    }

    *count_out = 0;
    for (uint32_t i = 0; i < column_count; i++) {
        if (!columns[i].indexed) continue;
        KdbIndex *idx = kdb_index_new(columns[i].name);
        if (!idx) return KDB_ERR_OOM;
        indices_out[(*count_out)++] = idx;
    }
    return KDB_OK;
}

KdbIndex *kdb_index_find(KdbIndex **indices,
                         uint32_t   count,
                         const char *col_name) {
    if (!indices || !col_name) return NULL;
    for (uint32_t i = 0; i < count; i++) {
        if (indices[i] && strcmp(indices[i]->col_name, col_name) == 0)
            return indices[i];
    }
    return NULL;
}


void kdb_index_stats(const KdbIndex *idx, KdbIndexStats *out) {
    if (!idx || !out) return;
    memset(out, 0, sizeof(*out));
    out->bucket_count = KDB_INDEX_BUCKETS;

    for (uint32_t i = 0; i < KDB_INDEX_BUCKETS; i++) {
        const KdbIndexNode *node = idx->buckets[i];
        if (!node) continue;
        size_t chain_len = 0;
        while (node) {
            out->entry_count++;
            chain_len++;
            node = node->next;
        }
        if (chain_len > 1)
            out->collision_count += chain_len - 1;
        if (chain_len > out->longest_chain)
            out->longest_chain = chain_len;
    }

    out->load_factor = (double)out->entry_count / (double)KDB_INDEX_BUCKETS;
}