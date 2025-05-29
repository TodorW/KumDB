"""
KUMDB Type System - Strict typing for maximum code safety and clarity.
"""

from enum import Enum, auto
from typing import (
    Any,
    Dict,
    List,
    Optional,
    Union,
    Tuple,
    Callable,
    TypeVar,
    Generic,
    Set,
    Iterable
)
from pathlib import Path
from datetime import datetime

# ===== CORE TYPE ALIASES =====
Record = Dict[str, Any]
"""A single database record (dictionary of field-value pairs)"""

ResultSet = List[Record]
"""Collection of records returned by queries"""

QueryFunc = Callable[[Record], bool]
"""Type for lambda query functions (e.g., lambda r: r['age'] > 25)"""

# ===== INDEXING =====
class IndexType(Enum):
    """Supported index types for optimized queries."""
    HASH = auto()      # O(1) lookups (exact matches)
    BTREE = auto()     # O(log n) range queries
    FULLTEXT = auto()  # Text search optimization

# ===== TRANSACTION TYPES =====
TransactionId = str
"""Unique identifier for transactions (UUID-based)"""

IsolationLevel = Enum('IsolationLevel', [
    'READ_UNCOMMITTED',
    'READ_COMMITTED',
    'REPEATABLE_READ', 
    'SERIALIZABLE'
])
"""Transaction isolation levels"""

# ===== STORAGE TYPES =====
class StorageEngine(Enum):
    """Supported storage backends."""
    JSON = auto()
    MSGPACK = auto()
    CSV = auto()
    MEMORY = auto()  # In-memory only

# ===== QUERY OPERATORS =====
class Operator(Enum):
    """Comparison operators for query conditions."""
    EQ = '='          # Equal
    NE = '!='         # Not equal
    GT = '>'          # Greater than
    LT = '<'          # Less than
    GTE = '>='        # Greater or equal
    LTE = '<='        # Less or equal
    IN = 'in'         # Contained in list
    LIKE = 'like'     # String pattern match
    CONTAINS = 'contains'  # Field contains value

# ===== GENERIC TYPES =====
T = TypeVar('T')
"""Generic type variable for reusable type hints"""

class PaginatedResults(Generic[T]):
    """Type for paginated query results."""
    def __init__(
        self,
        items: List[T],
        total: int,
        page: int,
        per_page: int
    ):
        self.items = items
        self.total = total
        self.page = page
        self.per_page = per_page

# ===== COMPLEX FIELD TYPES =====
class FieldType(Enum):
    """Supported field data types."""
    STRING = auto()
    INTEGER = auto()
    FLOAT = auto()
    BOOLEAN = auto()
    DATETIME = auto()
    BINARY = auto()
    JSON = auto()  # Nested structures

# ===== FUNCTION TYPES =====
MigrationFunc = Callable[[List[Record]], List[Record]]
"""Type for data migration functions"""

HookFunc = Callable[[str, Record], Optional[Record]]
"""Type for database hooks (pre/post operations)"""

# ===== CONFIGURATION =====
@dataclass
class KumDBConfig:
    """Database configuration options."""
    cache_size: int = 1000
    auto_compact: bool = True
    default_isolation: IsolationLevel = IsolationLevel.READ_COMMITTED
    strict_schema: bool = False

# ===== ERROR TYPES =====
class ValidationError(TypedDict):
    """Type for data validation errors."""
    field: str
    message: str
    code: int