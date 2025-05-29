"""
KUMDB Exceptions - When shit goes wrong, we tell you exactly why.
"""

class KumDBError(Exception):
    """Base exception class for all KUMDB errors."""
    def __init__(self, message: str = "KUMDB fucked up"):
        super().__init__(message)
        self.message = message

    def __str__(self):
        return f"🔥 KUMDB Error: {self.message}"

# ===== CORE EXCEPTIONS =====
class TableNotFoundError(KumDBError):
    """Raised when a table doesn't exist."""
    def __init__(self, table_name: str):
        super().__init__(f"Table '{table_name}' not found. Create it first, dumbass.")

class InvalidDataError(KumDBError):
    """Raised for invalid data operations."""
    def __init__(self, message: str = "Invalid data operation"):
        super().__init__(f"Data Error: {message}")

class TransactionError(KumDBError):
    """Raised for transaction-related failures."""
    def __init__(self, message: str = "Transaction failed"):
        super().__init__(f"Transaction Error: {message}")

# ===== LOCKING/CONCURRENCY =====
class LockTimeoutError(KumDBError):
    """Raised when a lock can't be acquired."""
    def __init__(self, resource: str, timeout: float):
        super().__init__(f"Couldn't lock '{resource}' after {timeout} seconds. Try again when less busy.")

class DeadlockError(KumDBError):
    """Raised when a deadlock is detected."""
    def __init__(self, threads: list):
        thread_ids = ", ".join(str(t) for t in threads)
        super().__init__(f"Deadlock detected between threads: {thread_ids}. Grow some patience.")

# ===== QUERY EXCEPTIONS =====
class QuerySyntaxError(KumDBError):
    """Raised for invalid query syntax."""
    def __init__(self, bad_query: str):
        super().__init__(f"Invalid query: '{bad_query}'. Learn to type, moron.")

class IndexNotFoundError(KumDBError):
    """Raised when a requested index doesn't exist."""
    def __init__(self, index_name: str):
        super().__init__(f"Index '{index_name}' not found. Did you create it first?")

# ===== STORAGE EXCEPTIONS =====
class StorageCorruptionError(KumDBError):
    """Raised when data corruption is detected."""
    def __init__(self, file_path: str):
        super().__init__(f"Data corruption detected in '{file_path}'. Did you edit this manually? Don't.")

class DiskFullError(KumDBError):
    """Raised when the database runs out of storage."""
    def __init__(self, needed: int, available: int):
        super().__init__(
            f"Need {needed} bytes but only {available} available. "
            "Delete some cat videos and try again."
        )

# ===== PERMISSIONS =====
class PermissionDeniedError(KumDBError):
    """Raised for unauthorized operations."""
    def __init__(self, operation: str):
        super().__init__(f"Permission denied for '{operation}'. Are you root?")

# ===== CONNECTION EXCEPTIONS =====
class ConnectionFailedError(KumDBError):
    """Raised when database connection fails."""
    def __init__(self, reason: str):
        super().__init__(f"Connection failed: {reason}. Check your cables, n00b.")

class ConnectionPoolExhaustedError(KumDBError):
    """Raised when no connections are available."""
    def __init__(self, max_connections: int):
        super().__init__(
            f"All {max_connections} connections are busy. "
            "Stop spamming requests and learn some patience."
        )

# ===== MIGRATION EXCEPTIONS =====
class MigrationError(KumDBError):
    """Base exception for migration failures."""
    def __init__(self, message: str):
        super().__init__(f"Migration failed: {message}")

class SchemaDriftError(MigrationError):
    """Raised when schema doesn't match expected state."""
    def __init__(self, table: str, diff: str):
        super().__init__(f"Schema drift in '{table}':\n{diff}")

# ===== BACKUP/RESTORE =====
class BackupFailedError(KumDBError):
    """Raised when backup operations fail."""
    def __init__(self, reason: str):
        super().__init__(f"Backup failed: {reason}")

class RestoreFailedError(KumDBError):
    """Raised when restore operations fail."""
    def __init__(self, reason: str):
        super().__init__(f"Restore failed: {reason}. Hope you have another backup.")

# ===== VALIDATION EXCEPTIONS =====
class ConstraintViolationError(KumDBError):
    """Raised when data constraints are violated."""
    def __init__(self, constraint: str, value: Any):
        super().__init__(f"Violated constraint '{constraint}' with value: {value}")

class TypeValidationError(KumDBError):
    """Raised when data types don't match expectations."""
    def __init__(self, field: str, expected: type, got: type):
        super().__init__(
            f"Field '{field}' expects {expected.__name__}, "
            f"but got {got.__name__}. Learn to type."
        )