#ifndef KUMDB_INDEX_H
#define KUMDB_INDEX_H

#include "internal.h"
#include "record.h"


KdbIndex *kdb_index_new(const char *col_name);


void kdb_index_free(KdbIndex *idx);


void kdb_index_free_array(KdbIndex **indices, uint32_t count);


KdbStatus kdb_index_insert(KdbIndex       *idx,
                           const KdbRecord *r,
                           uint64_t         file_offset);


KdbStatus kdb_index_remove(KdbIndex *idx, uint64_t record_id);


KdbStatus kdb_index_rebuild(KdbIndex *idx, KdbTable *tbl);


KdbStatus kdb_index_lookup(const KdbIndex *idx,
                           const KdbValue *value,
                           uint64_t       *file_offsets_out,
                           size_t          max_results,
                           size_t         *count_out);


uint64_t kdb_index_lookup_one(const KdbIndex *idx, const KdbValue *value);


KdbStatus kdb_index_build_for_table(const KdbColumn *columns,
                                    uint32_t         column_count,
                                    KdbIndex       **indices_out,
                                    uint32_t        *count_out);


KdbIndex *kdb_index_find(KdbIndex **indices,
                         uint32_t   count,
                         const char *col_name);


uint32_t kdb_index_hash(const KdbValue *v);


typedef struct {
    size_t   entry_count;
    size_t   bucket_count;
    size_t   collision_count;
    double   load_factor;
    size_t   longest_chain;
} KdbIndexStats;

void kdb_index_stats(const KdbIndex *idx, KdbIndexStats *out);

#endif