# **KumDB** 🔥
### *"The Database That Doesn't Waste Your F*cking Time"*

<p align="center">
<img src="logo.png" alt="KumDB Logo" width="200"/>
</p>

---

## **⚠️ WARNING**

This is **NOT** for:
- SQL lovers ❌
- ORM enjoyers ❌
- People who enjoy writing `JOIN` statements ❌

This **IS** for:
- Developers who want **dead-simple** data persistence ✅
- Embedded projects where SQLite is overkill ✅
- KumOS and anything that runs C ✅
- When you need to **GET SHIT DONE** ✅

---

## **💥 What Is It**

KumDB is a lightweight embedded database engine written in pure C11. No SQL. No dependencies. No bullshit. Just a clean key-value-style API on top of a fast binary file format with atomic writes, type inference, and savage error messages.

```c
// How simple? THIS simple:
KdbField fields[] = {
    kdb_field_string("name",   "John"),
    kdb_field_int   ("age",    30),
    kdb_field_bool  ("admin",  1),
    kdb_field_end   ()
};
kdb_add(db, "users", fields);

const char *filters[] = { "name=John", NULL };
KdbRow *user = kdb_find_one(db, "users", filters);
```

---

## **⚡ Quick Start**

**Build:**
```bash
make
```

**Use it in your project:**
```bash
# Include the header
#include "kumdb.h"

# Link against the static lib
gcc myapp.c build/libkumdb.a -lm -o myapp
```

**Or just use the CLI:**
```bash
./build/bin/kumdb_cli ./mydata
```

---

## **💻 API Cheat Sheet**

```c
// Open / close
KumDB *db = kdb_open("./mydata");
kdb_close(db);

// Insert
KdbField fields[] = {
    kdb_field_string("name",  "Alice"),
    kdb_field_int   ("age",   25),
    kdb_field_float ("score", 9.5),
    kdb_field_bool  ("vip",   1),
    kdb_field_end   ()
};
kdb_add(db, "users", fields);

// Find (NULL filters = all rows)
const char *filters[] = { "age__gt=21", "name__contains=Ali", NULL };
KdbRows *rows = kdb_find(db, "users", filters);
kdb_rows_free(rows);

// Find one
KdbRow *row = kdb_find_one(db, "users", filters);
kdb_row_free(row);

// Count
int64_t n = kdb_count(db, "users", NULL);

// Update
const char *where[] = { "name=Alice", NULL };
KdbField patch[] = { kdb_field_int("age", 26), kdb_field_end() };
kdb_update(db, "users", where, patch, &updated);

// Delete
kdb_delete(db, "users", where, &deleted);

// Compact (remove soft-deleted rows from disk)
kdb_compact(db, "users");

// Drop table
kdb_drop_table(db, "users");
```

---

## **🔍 Filter Operators**

| Operator | Example | Meaning |
|----------|---------|---------|
| *(none)* | `"age=30"` | equals |
| `__eq` | `"age__eq=30"` | equals |
| `__neq` | `"age__neq=30"` | not equals |
| `__gt` | `"age__gt=21"` | greater than |
| `__gte` | `"age__gte=21"` | greater than or equal |
| `__lt` | `"age__lt=65"` | less than |
| `__lte` | `"age__lte=65"` | less than or equal |
| `__between` | `"age__between=18,30"` | inclusive range |
| `__contains` | `"name__contains=ali"` | substring match |
| `__startswith` | `"name__startswith=al"` | prefix match |
| `__endswith` | `"name__endswith=ice"` | suffix match |
| `__isnull` | `"notes__isnull"` | field is null |
| `__isnotnull` | `"notes__isnotnull"` | field is not null |

Multiple filters = AND logic. No OR for now, cry about it.

---

## **🛠️ Tools**

```bash
# Interactive CLI
./build/bin/kumdb_cli ./mydata

# Benchmark (default 10k rows)
./build/bin/bench 50000 ./mydata

# Dump table contents
./build/bin/dump ./mydata users
./build/bin/dump ./mydata users --csv
./build/bin/dump ./mydata users --json --limit 50
```

**CLI commands:**
```
open <dir>                          open a database
tables                              list tables
schema <table>                      show schema
add <table> <k=v> [k=v ...]        insert a record
find <table> [filter ...]          query records
count <table> [filter ...]         count records
update <table> where <k=v> set <k=v>  update records
delete <table> <filter> [...]      delete records
compact <table>                     compact table file
drop <table>                        drop table
```

---

## **⚙️ Build Targets**

```bash
make              # release build → build/libkumdb.a + build/bin/*
make debug        # ASAN + UBSan + debug symbols
make check        # run all tests (use after make debug)
make distclean    # nuke build/
```

---

## **🔫 Performance**

| Operation | Speed |
|-----------|-------|
| Insert | ~50K ops/sec |
| Find (full scan) | ~200K rows/sec |
| Count | ~500K rows/sec |
| Update (batch) | ~40K ops/sec |

Numbers from `bench` on a mid-range machine. Your mileage may vary. Run `./build/bin/bench` to find out.

---

## **🤬 Comparison**

| Feature | KumDB | SQLite | MongoDB |
|---------|-------|--------|---------|
| Learning curve | 5 mins | 5 weeks | 5 years |
| Setup | drop in 2 files | configure & build | install daemon |
| Dependencies | none | none | entire ecosystem |
| Query language | filter strings | SQL | BSON query objects |
| Debugging | readable errors | "syntax error near..." | "BSON serialization..." |
| Street cred | 💯 | ❌ | 🤮 |

---

## **🚨 FAQ**

**Q: Is this production-ready?**
A: It's running on KumOS. You tell me.

**Q: How do I backup data?**
A: `cp -r mydata/ backup/` — congrats, you're a DBA now.

**Q: Thread safety?**
A: File-level `fcntl` locks on writes. Multiple readers fine. Don't do concurrent writes from separate processes without knowing what you're doing.

**Q: Can I contribute?**
A: Submit a PR or GTFO.

---

**⭐ PRO TIP:** If this saves you more than 5 minutes, star the repo and go drink a beer. You've earned it.
