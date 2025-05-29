import csv
import io
import json
import os
import pickle
import re
import shutil
import threading
import uuid
import zlib
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from functools import lru_cache
from hashlib import sha256
from pathlib import Path
from typing import (Any, Callable, Dict, Generator, Generic, Iterable, List,
                    Optional, Set, Tuple, TypeVar, Union)

# ===== OPTIONAL DEPENDENCIES =====
try:
    import msgpack
    HAS_MSGPACK = True
except ImportError:
    msgpack = None
    HAS_MSGPACK = False

# ========== CUSTOM EXCEPTIONS ==========
class KumDBError(Exception):
    """Base exception for all KumDB errors."""
    def __init__(self, message: str = "KUMDB operation failed"):
        self.message = message
        super().__init__(message)

class TableNotFoundError(KumDBError):
    """Raised when a table doesn't exist."""
    def __init__(self, table_name: str):
        super().__init__(f"Table '{table_name}' not found. Create it first.")

class InvalidDataError(KumDBError):
    """Raised for invalid data operations."""
    def __init__(self, message: str = "Invalid data operation"):
        super().__init__(f"Data Error: {message}")

class FeatureNotAvailableError(KumDBError):
    """Raised when trying to use an unavailable feature."""
    def __init__(self, feature: str):
        super().__init__(f"Feature '{feature}' requires additional packages")

class TransactionError(KumDBError):
    """Raised for transaction-related failures."""
    def __init__(self, message: str = "Transaction failed"):
        super().__init__(f"Transaction Error: {message}")

class LockTimeoutError(KumDBError):
    """Raised when a lock can't be acquired."""
    def __init__(self, resource: str, timeout: float):
        super().__init__(f"Couldn't lock '{resource}' after {timeout} seconds")

# ========== ENUMS & TYPES ==========
class IndexType(Enum):
    HASH = "hash"
    BTREE = "btree"
    FULLTEXT = "fulltext"

T = TypeVar('T')
Record = Dict[str, Any]
ResultSet = List[Record]
QueryFunc = Callable[[Record], bool]

# ========== CORE DATABASE CLASS ==========
class KumDB:
    """The no-bullshit database system for Python."""
    
    def __init__(self, 
                 folder: str = "data", 
                 file_format: str = "json",
                 auto_commit: bool = True,
                 cache_size: int = 1000):
        """
        Initialize a new KUMDB instance.
        
        Args:
            folder: Directory to store data files
            file_format: Storage format ('json', 'csv', or 'msgpack')
            auto_commit: Whether to save changes immediately
            cache_size: Maximum number of cached tables
        """
        self.folder = Path(folder).absolute()
        self.file_format = file_format.lower()
        
        if self.file_format == "msgpack" and not HAS_MSGPACK:
            raise FeatureNotAvailableError(
                "msgpack support requires 'pip install msgpack'"
            )
            
        self.auto_commit = auto_commit
        self.cache_size = cache_size
        self._lock = threading.RLock()
        self._transaction_stack = []
        self._cache = {}
        self._indexes = defaultdict(dict)
        
        self.folder.mkdir(exist_ok=True)
        self.tables = self._discover_tables()
        self._writer_executor = ThreadPoolExecutor(max_workers=1)
        self._pending_writes = {}

    def __repr__(self) -> str:
        return f"<KumDB: {len(self.tables)} tables | {self.file_format} | {self.folder}>"

    def __enter__(self):
        """Enter transaction context."""
        self.begin_transaction()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit transaction context."""
        if exc_type is None:
            self.commit_transaction()
        else:
            self.rollback_transaction()

    # ===== CORE CRUD OPERATIONS =====
    @lru_cache(maxsize=100)
    def find(self, table: str, **conditions) -> ResultSet:
        """Find records matching conditions."""
        with self._lock:
            data = self._load_table(table)
            return [row for row in data if self._matches(row, conditions)]

    def find_one(self, table: str, **conditions) -> Optional[Record]:
        """Find first matching record or None."""
        results = self.find(table, **conditions)
        return results[0] if results else None

    def add(self, table: str, **data) -> Record:
        """Add a new record to a table."""
        with self._lock:
            if 'id' not in data:
                data['id'] = str(uuid.uuid4())
                
            records = self._load_table(table)
            records.append(data)
            
            if self.auto_commit:
                self._schedule_write(table, records)
                
            self._update_indexes(table, data)
            return data

    def update(self, table: str, where: Dict[str, Any], **new_data) -> int:
        """Update records matching conditions."""
        with self._lock:
            records = self._load_table(table)
            updated = 0
            
            for record in records:
                if self._matches(record, where):
                    old_values = {k: record[k] for k in new_data.keys() if k in record}
                    record.update(new_data)
                    updated += 1
                    self._update_indexes(table, record, old_values)
            
            if updated and self.auto_commit:
                self._schedule_write(table, records)
                
            return updated

    def delete(self, table: str, **conditions) -> int:
        """Delete records matching conditions."""
        with self._lock:
            records = self._load_table(table)
            remaining = [r for r in records if not self._matches(r, conditions)]
            deleted = len(records) - len(remaining)
            
            if deleted:
                if self.auto_commit:
                    self._schedule_write(table, remaining)
                    
                for record in records:
                    if not self._matches(record, conditions):
                        continue
                    if 'id' in record:
                        self._remove_from_indexes(table, record['id'])
            
            return deleted

    # ===== TABLE OPERATIONS =====
    def create_table(self, table: str, schema: Optional[Dict[str, type]] = None,
                    indexes: Optional[Dict[str, IndexType]] = None) -> None:
        """Create a new table with optional schema and indexes."""
        with self._lock:
            if table in self.tables:
                raise InvalidDataError(f"Table '{table}' already exists")
                
            self._save_table(table, [])
            self.tables.add(table)
            
            if indexes:
                for field, index_type in indexes.items():
                    self.create_index(table, field, index_type)

    def drop_table(self, table: str, confirm: bool = False) -> None:
        """Permanently delete a table and all its data."""
        if not confirm:
            raise KumDBError("Must pass confirm=True to drop table")
            
        with self._lock:
            try:
                os.remove(self._table_path(table))
                self.tables.remove(table)
                
                if table in self._indexes:
                    del self._indexes[table]
                    
                if table in self._cache:
                    del self._cache[table]
                    
            except FileNotFoundError:
                raise TableNotFoundError(f"Table '{table}' not found")

    def truncate_table(self, table: str, confirm: bool = False) -> None:
        """Delete all records in a table while keeping the structure."""
        if not confirm:
            raise KumDBError("Must pass confirm=True to truncate table")
            
        with self._lock:
            self._save_table(table, [])
            
            if table in self._indexes:
                self._indexes[table].clear()

    # ===== INDEXING =====
    def create_index(self, table: str, field: str,
                    index_type: IndexType = IndexType.HASH) -> None:
        """Create an index on a field for faster queries."""
        with self._lock:
            if table not in self._indexes:
                self._indexes[table] = {}
                
            if field in self._indexes[table]:
                raise InvalidDataError(f"Index already exists on {table}.{field}")
                
            records = self._load_table(table)
            index = {}
            
            for record in records:
                if field not in record:
                    continue
                    
                value = record[field]
                if index_type == IndexType.HASH:
                    if value not in index:
                        index[value] = []
                    index[value].append(record['id'])
                    
            self._indexes[table][field] = {
                'type': index_type,
                'data': index
            }

    def drop_index(self, table: str, field: str) -> None:
        """Remove an index from a field."""
        with self._lock:
            if table not in self._indexes or field not in self._indexes[table]:
                raise InvalidDataError(f"No index exists on {table}.{field}")
                
            del self._indexes[table][field]

    # ===== TRANSACTIONS =====
    def begin_transaction(self) -> None:
        """Start a new transaction."""
        self._transaction_stack.append({
            'tables': set(),
            'original_data': {}
        })

    def commit_transaction(self) -> None:
        """Commit all changes in the current transaction."""
        if not self._transaction_stack:
            raise TransactionError("No active transaction")
            
        transaction = self._transaction_stack.pop()
        
        with self._lock:
            for table in transaction['tables']:
                self._save_table(table, self._load_table(table))

    def rollback_transaction(self) -> None:
        """Rollback all changes in the current transaction."""
        if not self._transaction_stack:
            raise TransactionError("No active transaction")
            
        transaction = self._transaction_stack.pop()
        
        with self._lock:
            for table, data in transaction['original_data'].items():
                self._save_table(table, data)
                
            for table in transaction['tables']:
                if table in self._cache:
                    del self._cache[table]

    # ===== ADVANCED FEATURES =====
    def batch_import(self, table: str, data: List[Record],
                    chunk_size: int = 1000) -> None:
        """Import multiple records efficiently in chunks."""
        with self._lock:
            records = self._load_table(table)
            records.extend(data)
            
            if self.auto_commit:
                self._schedule_write(table, records)
                
            for record in data:
                self._update_indexes(table, record)

    def export_table(self, table: str, output_format: str = "json",
                    compress: bool = False) -> Union[str, bytes, List[Record]]:
        """Export table data in specified format."""
        data = self._load_table(table)
        
        try:
            if output_format == "json":
                result = json.dumps(data, indent=2).encode('utf-8')
            elif output_format == "csv":
                output = io.StringIO()
                writer = csv.DictWriter(output, fieldnames=data[0].keys() if data else [])
                writer.writeheader()
                writer.writerows(data)
                result = output.getvalue().encode('utf-8')
            elif output_format == "msgpack":
                if not HAS_MSGPACK:
                    raise FeatureNotAvailableError("msgpack")
                result = msgpack.packb(data)
            elif output_format == "dict":
                return data
            else:
                raise InvalidDataError(f"Unsupported format: {output_format}")
            
            return zlib.compress(result) if compress else result
        except Exception as e:
            raise InvalidDataError(f"Export failed: {str(e)}")

    def query(self, table: str, func: QueryFunc) -> ResultSet:
        """Flexible query using a filter function."""
        return [row for row in self._load_table(table) if func(row)]

    def backup(self, backup_folder: str, compress: bool = True) -> str:
        """Create a complete backup of the database."""
        backup_path = Path(backup_folder) / f"kumdb_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.kum"
        
        with self._lock:
            backup_data = {
                'meta': {
                    'version': '2.0',
                    'created_at': datetime.now().isoformat(),
                    'tables': list(self.tables)
                },
                'data': {
                    table: self._load_table(table)
                    for table in self.tables
                }
            }
            
            try:
                serialized = msgpack.packb(backup_data) if HAS_MSGPACK else json.dumps(backup_data).encode('utf-8')
                
                if compress:
                    serialized = zlib.compress(serialized)
                    
                with open(backup_path, 'wb') as f:
                    f.write(serialized)
                    
                return str(backup_path)
            except Exception as e:
                raise InvalidDataError(f"Backup failed: {str(e)}")

    def restore(self, backup_file: str, confirm: bool = False) -> None:
        """Restore database from backup."""
        if not confirm:
            raise KumDBError("Must pass confirm=True to restore from backup")
            
        with self._lock, open(backup_file, 'rb') as f:
            try:
                data = f.read()
                
                try:
                    data = zlib.decompress(data)
                except zlib.error:
                    pass
                    
                try:
                    backup = msgpack.unpackb(data) if HAS_MSGPACK else json.loads(data.decode('utf-8'))
                except Exception:
                    raise InvalidDataError("Invalid backup file format")
                
                for table in list(self.tables):
                    self.drop_table(table, confirm=True)
                    
                for table, records in backup['data'].items():
                    self._save_table(table, records)
                    self.tables.add(table)
            except Exception as e:
                raise InvalidDataError(f"Restore failed: {str(e)}")

    # ===== PRIVATE METHODS =====
    def _discover_tables(self) -> Set[str]:
        """Find all existing tables in the data folder."""
        if not self.folder.exists():
            return set()
            
        tables = set()
        for f in self.folder.iterdir():
            if f.suffix == f".{self.file_format}":
                tables.add(f.stem)
        return tables
    
    def _table_path(self, table: str) -> Path:
        """Get full path to table file."""
        return self.folder / f"{table}.{self.file_format}"
    
    def _load_table(self, table: str) -> List[Record]:
        """Load table data from file with caching."""
        if table not in self.tables:
            raise TableNotFoundError(table)
            
        if table in self._cache:
            return self._cache[table]
            
        path = self._table_path(table)
        
        try:
            if self.file_format == "csv":
                with open(path, 'r', newline='') as f:
                    data = list(csv.DictReader(f))
            elif self.file_format == "json":
                with open(path, 'r') as f:
                    data = json.load(f)
            elif self.file_format == "msgpack":
                if not HAS_MSGPACK:
                    raise FeatureNotAvailableError("msgpack")
                with open(path, 'rb') as f:
                    data = msgpack.unpack(f, raw=False)
            else:
                raise InvalidDataError(f"Unsupported format: {self.file_format}")
                
            self._cache[table] = data
            return data
        except Exception as e:
            raise InvalidDataError(f"Failed to load table '{table}': {str(e)}")
    
    def _save_table(self, table: str, data: List[Record]) -> None:
        """Save table data to file."""
        path = self._table_path(table)
        
        try:
            if self.file_format == "csv":
                fieldnames = data[0].keys() if data else []
                with open(path, 'w', newline='') as f:
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(data)
            elif self.file_format == "json":
                with open(path, 'w') as f:
                    json.dump(data, f, indent=2)
            elif self.file_format == "msgpack":
                if not HAS_MSGPACK:
                    raise FeatureNotAvailableError("msgpack")
                with open(path, 'wb') as f:
                    msgpack.pack(data, f)
                    
            self._cache[table] = data
        except Exception as e:
            raise InvalidDataError(f"Failed to save table '{table}': {str(e)}")
    
    def _schedule_write(self, table: str, data: List[Record]) -> None:
        """Schedule a write operation in background."""
        self._pending_writes[table] = data
        self._writer_executor.submit(self._process_pending_writes)
    
    def _process_pending_writes(self) -> None:
        """Process all pending writes."""
        with self._lock:
            for table, data in self._pending_writes.items():
                self._save_table(table, data)
            self._pending_writes.clear()
    
    def _matches(self, row: Record, conditions: Dict[str, Any]) -> bool:
        """Check if row matches all conditions with advanced operators."""
        for key, value in conditions.items():
            if '__' in key:
                field, op = key.split('__')
                if field not in row:
                    return False
                    
                field_value = row[field]
                
                if op == 'eq':
                    if field_value != value:
                        return False
                elif op == 'ne':
                    if field_value == value:
                        return False
                elif op == 'gt':
                    if field_value <= value:
                        return False
                elif op == 'lt':
                    if field_value >= value:
                        return False
                elif op == 'gte':
                    if field_value < value:
                        return False
                elif op == 'lte':
                    if field_value > value:
                        return False
                elif op == 'in':
                    if field_value not in value:
                        return False
                elif op == 'nin':
                    if field_value in value:
                        return False
                elif op == 'contains':
                    if value not in str(field_value):
                        return False
                elif op == 'startswith':
                    if not str(field_value).startswith(value):
                        return False
                elif op == 'endswith':
                    if not str(field_value).endswith(value):
                        return False
                elif op == 'regex':
                    if not re.search(value, str(field_value)):
                        return False
                else:
                    raise InvalidDataError(f"Unknown operator: {op}")
            else:
                if row.get(key) != value:
                    return False
        return True
    
    def _update_indexes(self, table: str, record: Record,
                       old_values: Optional[Dict[str, Any]] = None) -> None:
        """Update all indexes for a record."""
        if table not in self._indexes:
            return
            
        record_id = record.get('id')
        if not record_id:
            return
            
        for field, index in self._indexes[table].items():
            if field not in record:
                continue
                
            current_value = record[field]
            
            if old_values and field in old_values:
                old_value = old_values[field]
                if old_value in index['data'] and record_id in index['data'][old_value]:
                    index['data'][old_value].remove(record_id)
                    if not index['data'][old_value]:
                        del index['data'][old_value]
            
            if current_value not in index['data']:
                index['data'][current_value] = []
            if record_id not in index['data'][current_value]:
                index['data'][current_value].append(record_id)
    
    def _remove_from_indexes(self, table: str, record_id: str) -> None:
        """Remove a record from all indexes."""
        if table not in self._indexes:
            return
            
        for field, index in self._indexes[table].items():
            for value, ids in list(index['data'].items()):
                if record_id in ids:
                    ids.remove(record_id)
                    if not ids:
                        del index['data'][value]