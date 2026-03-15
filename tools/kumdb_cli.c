#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>

#include "../include/kumdb.h"
#include "../include/types.h"

#define CLI_VERSION  "1.0.0"
#define MAX_LINE     4096
#define MAX_TOKENS   128
#define MAX_FIELDS   64

static KumDB *db = NULL;

static void print_banner(void) {
    printf("KumDB CLI v%s  (engine v%s)\n", CLI_VERSION, kdb_version());
    printf("Type 'help' for commands.\n\n");
}

static void print_help(void) {
    printf("Commands:\n");
    printf("  open <dir>                         Open a database directory\n");
    printf("  close                              Close the current database\n");
    printf("  tables                             List all tables\n");
    printf("  schema <table>                     Show schema for a table\n");
    printf("  add <table> <k=v> [k=v ...]        Insert a record\n");
    printf("  find <table> [k=v ...]             Find records (empty = all)\n");
    printf("  count <table> [k=v ...]            Count matching records\n");
    printf("  delete <table> <k=v> [k=v ...]     Delete matching records\n");
    printf("  update <table> where <k=v> [...] set <k=v> [...]  Update records\n");
    printf("  drop <table>                       Drop a table\n");
    printf("  compact <table>                    Compact a table\n");
    printf("  help                               Show this help\n");
    printf("  quit / exit                        Exit\n\n");
}

static void ensure_db(void) {
    if (!db) printf("No database open. Use: open <dir>\n");
}

static int tokenize(char *line, char **tokens, int max) {
    int count = 0;
    char *p = line;
    while (*p && count < max) {
        while (*p && isspace((unsigned char)*p)) p++;
        if (!*p) break;
        tokens[count++] = p;
        while (*p && !isspace((unsigned char)*p)) p++;
        if (*p) *p++ = '\0';
    }
    return count;
}

static void cmd_open(int argc, char **argv) {
    if (argc < 2) { printf("Usage: open <dir>\n"); return; }
    if (db) { kdb_close(db); db = NULL; }
    db = kdb_open(argv[1]);
    if (!db) printf("Error: %s\n", kdb_last_error());
    else     printf("Opened database at '%s'\n", argv[1]);
}

static void cmd_close(void) {
    if (!db) { printf("No database open.\n"); return; }
    kdb_close(db);
    db = NULL;
    printf("Database closed.\n");
}

static void cmd_tables(void) {
    ensure_db(); if (!db) return;
    const char *names[256];
    size_t count = 0;
    if (kdb_list_tables(db, names, 256, &count) != KDB_OK) {
        printf("Error: %s\n", kdb_last_error());
        return;
    }
    if (count == 0) { printf("No tables.\n"); return; }
    for (size_t i = 0; i < count; i++) printf("  %s\n", names[i]);
}

static void cmd_schema(int argc, char **argv) {
    ensure_db(); if (!db) return;
    if (argc < 2) { printf("Usage: schema <table>\n"); return; }
    kdb_print_schema(db, argv[1], stdout);
}

static void cmd_add(int argc, char **argv) {
    ensure_db(); if (!db) return;
    if (argc < 3) { printf("Usage: add <table> <key=value> [...]\n"); return; }

    KdbField fields[MAX_FIELDS + 1];
    int nf = 0;
    char names[MAX_FIELDS][128];
    char values[MAX_FIELDS][512];

    for (int i = 2; i < argc && nf < MAX_FIELDS; i++) {
        char *eq = strchr(argv[i], '=');
        if (!eq) { printf("Bad field '%s' — use key=value\n", argv[i]); return; }
        *eq = '\0';
        strncpy(names[nf],  argv[i], 127); names[nf][127]  = '\0';
        strncpy(values[nf], eq + 1,  511); values[nf][511] = '\0';

        KdbType t = kdb_type_infer(values[nf]);
        switch (t) {
            case KDB_TYPE_INT:    fields[nf] = kdb_field_int   (names[nf], (int64_t)atoll(values[nf])); break;
            case KDB_TYPE_FLOAT:  fields[nf] = kdb_field_float (names[nf], atof(values[nf]));           break;
            case KDB_TYPE_BOOL:   fields[nf] = kdb_field_bool  (names[nf], strcasecmp(values[nf], "true") == 0 || strcmp(values[nf], "1") == 0); break;
            case KDB_TYPE_NULL:   fields[nf] = kdb_field_null  (names[nf]);                             break;
            default:              fields[nf] = kdb_field_string(names[nf], values[nf]);                 break;
        }
        nf++;
    }
    fields[nf] = kdb_field_end();

    KdbStatus st = kdb_add(db, argv[1], fields);
    if (st != KDB_OK) printf("Error: %s\n", kdb_last_error());
    else              printf("Inserted.\n");
}

static void cmd_find(int argc, char **argv) {
    ensure_db(); if (!db) return;
    if (argc < 2) { printf("Usage: find <table> [filter ...]\n"); return; }

    const char *filters[MAX_FIELDS + 1];
    int nf = 0;
    for (int i = 2; i < argc && nf < MAX_FIELDS; i++)
        filters[nf++] = argv[i];
    filters[nf] = NULL;

    KdbRows *rows = kdb_find(db, argv[1], nf > 0 ? filters : NULL);
    if (!rows) { printf("Error: %s\n", kdb_last_error()); return; }
    if (rows->count == 0) { printf("No results.\n"); kdb_rows_free(rows); return; }
    kdb_rows_print(rows, stdout);
    kdb_rows_free(rows);
}

static void cmd_count(int argc, char **argv) {
    ensure_db(); if (!db) return;
    if (argc < 2) { printf("Usage: count <table> [filter ...]\n"); return; }

    const char *filters[MAX_FIELDS + 1];
    int nf = 0;
    for (int i = 2; i < argc && nf < MAX_FIELDS; i++)
        filters[nf++] = argv[i];
    filters[nf] = NULL;

    int64_t n = kdb_count(db, argv[1], nf > 0 ? filters : NULL);
    if (n < 0) printf("Error: %s\n", kdb_last_error());
    else       printf("%lld\n", (long long)n);
}

static void cmd_delete(int argc, char **argv) {
    ensure_db(); if (!db) return;
    if (argc < 3) { printf("Usage: delete <table> <filter> [...]\n"); return; }

    const char *filters[MAX_FIELDS + 1];
    int nf = 0;
    for (int i = 2; i < argc && nf < MAX_FIELDS; i++)
        filters[nf++] = argv[i];
    filters[nf] = NULL;

    size_t deleted = 0;
    KdbStatus st = kdb_delete(db, argv[1], filters, &deleted);
    if (st != KDB_OK) printf("Error: %s\n", kdb_last_error());
    else              printf("Deleted %zu record(s).\n", deleted);
}

static void cmd_update(int argc, char **argv) {
    ensure_db(); if (!db) return;
    if (argc < 5) {
        printf("Usage: update <table> where <k=v> [...] set <k=v> [...]\n");
        return;
    }

    const char *where_filters[MAX_FIELDS + 1];
    int nw = 0;
    char set_names[MAX_FIELDS][128];
    char set_values[MAX_FIELDS][512];
    KdbField set_fields[MAX_FIELDS + 1];
    int ns = 0;

    int in_set = 0;
    for (int i = 2; i < argc; i++) {
        if (strcmp(argv[i], "where") == 0) { in_set = 0; continue; }
        if (strcmp(argv[i], "set")   == 0) { in_set = 1; continue; }
        if (!in_set) {
            if (nw < MAX_FIELDS) where_filters[nw++] = argv[i];
        } else {
            char *eq = strchr(argv[i], '=');
            if (!eq || ns >= MAX_FIELDS) continue;
            *eq = '\0';
            strncpy(set_names[ns],  argv[i], 127); set_names[ns][127]  = '\0';
            strncpy(set_values[ns], eq + 1,  511); set_values[ns][511] = '\0';
            KdbType t = kdb_type_infer(set_values[ns]);
            switch (t) {
                case KDB_TYPE_INT:   set_fields[ns] = kdb_field_int   (set_names[ns], (int64_t)atoll(set_values[ns])); break;
                case KDB_TYPE_FLOAT: set_fields[ns] = kdb_field_float (set_names[ns], atof(set_values[ns]));           break;
                case KDB_TYPE_BOOL:  set_fields[ns] = kdb_field_bool  (set_names[ns], strcasecmp(set_values[ns], "true") == 0); break;
                case KDB_TYPE_NULL:  set_fields[ns] = kdb_field_null  (set_names[ns]);                                 break;
                default:             set_fields[ns] = kdb_field_string(set_names[ns], set_values[ns]);                 break;
            }
            ns++;
        }
    }
    where_filters[nw] = NULL;
    set_fields[ns]    = kdb_field_end();

    if (ns == 0) { printf("No set fields specified.\n"); return; }

    size_t updated = 0;
    KdbStatus st = kdb_update(db, argv[1], nw > 0 ? where_filters : NULL, set_fields, &updated);
    if (st != KDB_OK) printf("Error: %s\n", kdb_last_error());
    else              printf("Updated %zu record(s).\n", updated);
}

static void cmd_drop(int argc, char **argv) {
    ensure_db(); if (!db) return;
    if (argc < 2) { printf("Usage: drop <table>\n"); return; }
    KdbStatus st = kdb_drop_table(db, argv[1]);
    if (st != KDB_OK) printf("Error: %s\n", kdb_last_error());
    else              printf("Dropped '%s'.\n", argv[1]);
}

static void cmd_compact(int argc, char **argv) {
    ensure_db(); if (!db) return;
    if (argc < 2) { printf("Usage: compact <table>\n"); return; }
    KdbStatus st = kdb_compact(db, argv[1]);
    if (st != KDB_OK) printf("Error: %s\n", kdb_last_error());
    else              printf("Compacted '%s'.\n", argv[1]);
}

int main(int argc, char **argv) {
    print_banner();

    if (argc >= 2) {
        char *open_argv[] = { "open", argv[1] };
        cmd_open(2, open_argv);
    }

    char line[MAX_LINE];
    char *tokens[MAX_TOKENS];

    while (1) {
        printf("kumdb> ");
        fflush(stdout);

        if (!fgets(line, sizeof(line), stdin)) break;

        line[strcspn(line, "\n")] = '\0';
        if (!*line) continue;

        int ntok = tokenize(line, tokens, MAX_TOKENS);
        if (ntok == 0) continue;

        char *cmd = tokens[0];

        if (strcmp(cmd, "quit") == 0 || strcmp(cmd, "exit") == 0) break;
        else if (strcmp(cmd, "help")    == 0) print_help();
        else if (strcmp(cmd, "open")    == 0) cmd_open(ntok, tokens);
        else if (strcmp(cmd, "close")   == 0) cmd_close();
        else if (strcmp(cmd, "tables")  == 0) cmd_tables();
        else if (strcmp(cmd, "schema")  == 0) cmd_schema(ntok, tokens);
        else if (strcmp(cmd, "add")     == 0) cmd_add(ntok, tokens);
        else if (strcmp(cmd, "find")    == 0) cmd_find(ntok, tokens);
        else if (strcmp(cmd, "count")   == 0) cmd_count(ntok, tokens);
        else if (strcmp(cmd, "delete")  == 0) cmd_delete(ntok, tokens);
        else if (strcmp(cmd, "update")  == 0) cmd_update(ntok, tokens);
        else if (strcmp(cmd, "drop")    == 0) cmd_drop(ntok, tokens);
        else if (strcmp(cmd, "compact") == 0) cmd_compact(ntok, tokens);
        else printf("Unknown command '%s'. Type 'help'.\n", cmd);
    }

    if (db) kdb_close(db);
    printf("Bye.\n");
    return 0;
}