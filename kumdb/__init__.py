"""
KUMDB - The Database That Doesn't Waste Your Time.
No SQL. No Schemas. No ORM. Just Tables and Speed.
"""

from .core import KumDB
from .exceptions import (
    KumDBError,
    TableNotFoundError,
    InvalidDataError,
    TransactionError,
    LockTimeoutError
)
from .types import IndexType

# Public API - what users get when they do `from kumdb import *`
__all__ = [
    'KumDB',
    'IndexType',
    'KumDBError',
    'TableNotFoundError',
    'InvalidDataError',
    'TransactionError',
    'LockTimeoutError'
]

# Versioning
__version__ = '3.0.0'
__author__ = 'TodorW'
__license__ = 'MIT'

# Optional: Enable rich logging if installed
try:
    from rich import print
    def __print_hello():
        print("[bold green]KumDB initialized![/bold green] :rocket:")
    __print_hello()
except ImportError:
    pass