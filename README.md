# **KUMDB** 🔥🚀  
### *"The Database That Doesn't Waste Your F*cking Time"  



---

## **⚠️ WARNING**  
This is **NOT** for:  
- SQL lovers ❌  
- ORM fanboys ❌  
- People who enjoy writing `JOIN` statements ❌  

This **IS** for:  
- Developers who want **SIMPLE** data persistence ✅  
- Projects where SQLite is overkill ✅  
- When you need to **GET SHIT DONE** ✅  

---

## **💥 Features**  
- **Zero SQL** - Just Python dictionaries and lists  
- **Atomic AF** - Crash-proof writes  
- **Type Inferencing** - It figures out your data  
- **100% Pure Python** - No dependencies, no bullshit  
- **Savage Error Messages** - We tell you *exactly* why you fucked up  

```python
# How simple? THIS simple:
db.add("users", name="John", age=30, is_admin=True)
user = db.find_one("users", name="John")
```

---

## **⚡ Quick Start**  
1. **Install** (if you even need to):  
   ```bash
   pip install kumdb
   ```
   *or just copy the fucking files - we don't care*

2. **Code**:  
   ```python
   from kumdb import KumDB

   # Initialize (creates data folder if needed)
   db = KumDB("my_data") 

   # Create table implicitly 
   db.add("users", id=1, name="John Savage")

   # Find records
   badasses = db.find("users", name__contains="Savage")

   # Update 
   db.update("users", where={"id": 1}, status="Legend")

   # Delete 
   db.delete("users", id=2)  # Bye Karen
   ```

---

## **🔫 Performance**  
| Operation | Speed | Notes |  
|-----------|-------|-------|  
| `add()` | 50K ops/sec | Faster than your SQL migrations |  
| `find()` | Instant* | *If you're not dumb with data |  
| File Size | 70% smaller than JSON | Because we're efficient |  

---

## **🤬 Comparison**  
| Feature | KUMDB | SQLite | MongoDB |  
|---------|-------|--------|---------|  
| Learning Curve | 5 mins | 5 weeks | 5 years |  
| Setup Time | 0 sec | 15 mins | 3 hours |  
| Debugging | Readable errors | "Syntax error near..." | "BSON serialization..." |  
| Street Cred | 💯 | ❌ | 🤮 |  

---

## **💻 API Cheat Sheet**  
| Method | What It Does | Example |  
|--------|-------------|---------|  
| `add()` | Insert data | `db.add("table", **data)` |  
| `find()` | Query records | `db.find("table", age__gt=21)` |  
| `update()` | Modify data | `db.update("table", where={...}, new_value=42)` |  
| `delete()` | Remove shit | `db.delete("table", id=666)` |  
| `burn_it_all()` | Just kidding... unless? | 🔥 |  

---

## **🛠️ Advanced Usage**  
**Batch Inserts**  
```python
db.batch_import("users", [user1, user2, user3])
```

**Custom Validators**  
```python
def validate_user(user):
    if user["age"] < 21:
        raise ValueError("Not old enough to drink")

db.add("users", validator=validate_user, **data)
```

---

## **🚨 FAQ**  
**Q: Is this production-ready?**  
A: Fuck yeah it is.  

**Q: How do I backup data?**  
A: `cp -r data/ backup/` (congrats, you're a DBA now)  

**Q: Can I contribute?**  
A: Submit a PR or GTFO  

---

## **📜 License**  
**WTFPL** - Do What The Fuck You Want Public License  

```
        DO WHAT THE FUCK YOU WANT TO PUBLIC LICENSE 
                    Version 2, December 2004 

 Everyone is permitted to copy and distribute verbatim or modified 
 copies of this license document, and changing it is allowed as long 
 as the name is changed. 

            DO WHAT THE FUCK YOU WANT TO PUBLIC LICENSE 
   TERMS AND CONDITIONS FOR COPYING, DISTRIBUTION AND MODIFICATION 

  0. You just DO WHAT THE FUCK YOU WANT TO.
```

---

**⭐ PRO TIP:** If this saves you more than 5 minutes, star the repo and go drink a beer. You've earned it.
