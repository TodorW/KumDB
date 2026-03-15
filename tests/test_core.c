#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>

#include "../include/kumdb.h"

static int passed = 0;
static int failed = 0;

#define ASSERT(expr) do { \
    if (expr) { passed++; } \
    else { \
        fprintf(stderr, "FAIL [%s:%d] %s\n", __FILE__, __LINE__, #expr); \
        failed++; \
    } \
} while (0)

#define ASSERT_EQ(a, b)  ASSERT((a) == (b))
#define ASSERT_STR(a, b) ASSERT(strcmp((a), (b)) == 0)
#define ASSERT_OK(st)    ASSERT((st) == KDB_OK)

#define TEST_DIR   "/tmp/kumdb_test_core"
#define TABLE      "users"

static void setup(KumDB **db_out) {
    system("rm -rf " TEST_DIR);
    mkdir(TEST_DIR, 0755);
    KumDB *db = kdb_open(TEST_DIR);
    ASSERT(db != NULL);
    *db_out = db;
}

static void teardown(KumDB *db) {
    kdb_close(db);
    system("rm -rf " TEST_DIR);
}

static void test_insert_and_find(void) {
    KumDB *db;
    setup(&db);

    KdbField row1[] = {
        kdb_field_string("name",   "Alice"),
        kdb_field_int   ("age",    30),
        kdb_field_bool  ("active", 1),
        kdb_field_end   ()
    };
    KdbField row2[] = {
        kdb_field_string("name",   "Bob"),
        kdb_field_int   ("age",    25),
        kdb_field_bool  ("active", 0),
        kdb_field_end   ()
    };

    ASSERT_OK(kdb_add(db, TABLE, row1));
    ASSERT_OK(kdb_add(db, TABLE, row2));

    KdbRows *rows = kdb_find(db, TABLE, NULL);
    ASSERT(rows != NULL);
    ASSERT_EQ(rows->count, 2u);
    kdb_rows_free(rows);

    teardown(db);
}

static void test_find_with_filter(void) {
    KumDB *db;
    setup(&db);

    for (int i = 0; i < 10; i++) {
        char name[32];
        snprintf(name, sizeof(name), "user%d", i);
        KdbField f[] = {
            kdb_field_string("name",  name),
            kdb_field_int   ("score", i * 10),
            kdb_field_end   ()
        };
        ASSERT_OK(kdb_add(db, TABLE, f));
    }

    const char *filters[] = { "score__gte=50", NULL };
    KdbRows *rows = kdb_find(db, TABLE, filters);
    ASSERT(rows != NULL);
    ASSERT_EQ(rows->count, 5u);
    kdb_rows_free(rows);

    const char *between[] = { "score__between=20,60", NULL };
    rows = kdb_find(db, TABLE, between);
    ASSERT(rows != NULL);
    ASSERT_EQ(rows->count, 5u);
    kdb_rows_free(rows);

    teardown(db);
}

static void test_find_one(void) {
    KumDB *db;
    setup(&db);

    KdbField f[] = {
        kdb_field_string("name", "Charlie"),
        kdb_field_int   ("age",  40),
        kdb_field_end   ()
    };
    ASSERT_OK(kdb_add(db, TABLE, f));

    const char *filters[] = { "name=Charlie", NULL };
    KdbRow *row = kdb_find_one(db, TABLE, filters);
    ASSERT(row != NULL);

    const char *name = NULL;
    ASSERT_OK(kdb_row_get_string(row, "name", &name));
    ASSERT_STR(name, "Charlie");

    int64_t age = 0;
    ASSERT_OK(kdb_row_get_int(row, "age", &age));
    ASSERT_EQ(age, 40);

    kdb_row_free(row);
    teardown(db);
}

static void test_count(void) {
    KumDB *db;
    setup(&db);

    for (int i = 0; i < 5; i++) {
        KdbField f[] = {
            kdb_field_int("val", i),
            kdb_field_end()
        };
        ASSERT_OK(kdb_add(db, TABLE, f));
    }

    ASSERT_EQ(kdb_count(db, TABLE, NULL), 5);

    const char *filters[] = { "val__lt=3", NULL };
    ASSERT_EQ(kdb_count(db, TABLE, filters), 3);

    teardown(db);
}

static void test_update(void) {
    KumDB *db;
    setup(&db);

    KdbField f[] = {
        kdb_field_string("name",   "Dave"),
        kdb_field_int   ("age",    22),
        kdb_field_bool  ("active", 0),
        kdb_field_end   ()
    };
    ASSERT_OK(kdb_add(db, TABLE, f));

    const char *where[] = { "name=Dave", NULL };
    KdbField patch[] = {
        kdb_field_bool("active", 1),
        kdb_field_int ("age",    23),
        kdb_field_end ()
    };
    size_t updated = 0;
    ASSERT_OK(kdb_update(db, TABLE, where, patch, &updated));
    ASSERT_EQ(updated, 1u);

    KdbRow *row = kdb_find_one(db, TABLE, where);
    ASSERT(row != NULL);
    int64_t age = 0;
    ASSERT_OK(kdb_row_get_int(row, "age", &age));
    ASSERT_EQ(age, 23);
    int active = 0;
    ASSERT_OK(kdb_row_get_bool(row, "active", &active));
    ASSERT_EQ(active, 1);
    kdb_row_free(row);

    teardown(db);
}

static void test_delete(void) {
    KumDB *db;
    setup(&db);

    for (int i = 0; i < 6; i++) {
        KdbField f[] = {
            kdb_field_int("val", i),
            kdb_field_end()
        };
        ASSERT_OK(kdb_add(db, TABLE, f));
    }
    ASSERT_EQ(kdb_count(db, TABLE, NULL), 6);

    const char *where[] = { "val__gte=3", NULL };
    size_t deleted = 0;
    ASSERT_OK(kdb_delete(db, TABLE, where, &deleted));
    ASSERT_EQ(deleted, 3u);
    ASSERT_EQ(kdb_count(db, TABLE, NULL), 3);

    teardown(db);
}

static void test_compact(void) {
    KumDB *db;
    setup(&db);

    for (int i = 0; i < 10; i++) {
        KdbField f[] = { kdb_field_int("n", i), kdb_field_end() };
        ASSERT_OK(kdb_add(db, TABLE, f));
    }

    const char *where[] = { "n__lt=5", NULL };
    size_t deleted = 0;
    ASSERT_OK(kdb_delete(db, TABLE, where, &deleted));
    ASSERT_EQ(deleted, 5u);

    ASSERT_OK(kdb_compact(db, TABLE));
    ASSERT_EQ(kdb_count(db, TABLE, NULL), 5);

    teardown(db);
}

static void test_table_exists_and_drop(void) {
    KumDB *db;
    setup(&db);

    ASSERT(!kdb_table_exists(db, TABLE));

    KdbField f[] = { kdb_field_int("x", 1), kdb_field_end() };
    ASSERT_OK(kdb_add(db, TABLE, f));
    ASSERT(kdb_table_exists(db, TABLE));

    ASSERT_OK(kdb_drop_table(db, TABLE));
    ASSERT(!kdb_table_exists(db, TABLE));

    teardown(db);
}

static void test_reopen(void) {
    system("rm -rf " TEST_DIR);
    mkdir(TEST_DIR, 0755);

    KumDB *db = kdb_open(TEST_DIR);
    ASSERT(db != NULL);

    for (int i = 0; i < 5; i++) {
        char name[32];
        snprintf(name, sizeof(name), "item%d", i);
        KdbField f[] = {
            kdb_field_string("name", name),
            kdb_field_int   ("idx",  i),
            kdb_field_end   ()
        };
        ASSERT_OK(kdb_add(db, TABLE, f));
    }
    kdb_close(db);

    db = kdb_open(TEST_DIR);
    ASSERT(db != NULL);
    ASSERT_EQ(kdb_count(db, TABLE, NULL), 5);

    kdb_close(db);
    system("rm -rf " TEST_DIR);
}

static void test_row_accessors(void) {
    KumDB *db;
    setup(&db);

    KdbField f[] = {
        kdb_field_string("name",  "Eve"),
        kdb_field_int   ("age",   29),
        kdb_field_float ("score", 9.5),
        kdb_field_bool  ("vip",   1),
        kdb_field_null  ("notes"),
        kdb_field_end   ()
    };
    ASSERT_OK(kdb_add(db, TABLE, f));

    const char *filters[] = { "name=Eve", NULL };
    KdbRow *row = kdb_find_one(db, TABLE, filters);
    ASSERT(row != NULL);

    const char *name = NULL;
    int64_t age = 0;
    double score = 0.0;
    int vip = 0;

    ASSERT_OK(kdb_row_get_string(row, "name",  &name));
    ASSERT_STR(name, "Eve");
    ASSERT_OK(kdb_row_get_int   (row, "age",   &age));
    ASSERT_EQ(age, 29);
    ASSERT_OK(kdb_row_get_float (row, "score", &score));
    ASSERT(score > 9.4 && score < 9.6);
    ASSERT_OK(kdb_row_get_bool  (row, "vip",   &vip));
    ASSERT_EQ(vip, 1);

    kdb_row_free(row);
    teardown(db);
}

int main(void) {
    printf("=== test_core ===\n");

    test_insert_and_find();
    test_find_with_filter();
    test_find_one();
    test_count();
    test_update();
    test_delete();
    test_compact();
    test_table_exists_and_drop();
    test_reopen();
    test_row_accessors();

    printf("passed=%d  failed=%d\n", passed, failed);
    return failed > 0 ? 1 : 0;
}