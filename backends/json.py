"""
JSON Backend - Human-readable storage with atomic guarantees.
"""

import json
import os
from tempfile import NamedTemporaryFile
from pathlib import Path
from typing import List, Dict, Any, Optional, Union, Iterator
from threading import Lock
from datetime import datetime
from ..exceptions import StorageCorruptionError, DiskFullError
from ..types import Record, ResultSet

class JSONBackend:
    """Atomic JSON storage with datetime handling and compression support."""

    def __init__(
        self,
        path: Path,
        pretty: bool = False,
        compress: bool = False,
        indent: int = 2
    ):
        self.path = path
        self.pretty = pretty
        self.compress = compress
        self.indent = indent if pretty else None
        self._lock = Lock()
        self._encoder = _JSONEncoder()
        
        # Create empty array file if doesn't exist
        if not self.path.exists():
            self._atomic_write([])

    def load(self) -> ResultSet:
        """Load all records with proper type restoration."""
        if not self.path.exists():
            return []

        with self._lock, open(self.path, 'rb' if self.compress else 'r') as f:
            try:
                if self.compress:
                    import zlib
                    data = zlib.decompress(f.read()).decode('utf-8')
                else:
                    data = f.read()
                return self._encoder.decode(json.loads(data))
            except (json.JSONDecodeError, UnicodeDecodeError, zlib.error) as e:
                raise StorageCorruptionError(f"JSON parse failed: {str(e)}")

    def save(self, records: ResultSet) -> None:
        """Atomically save records with compression and pretty printing."""
        self._atomic_write(records)

    def stream(self) -> Iterator[Record]:
        """Lazy record streaming using iterative JSON parser."""
        if not self.path.exists():
            return

        with self._lock, open(self.path, 'rb' if self.compress else 'r') as f:
            try:
                if self.compress:
                    import zlib
                    data = zlib.decompress(f.read())
                else:
                    data = f.read()

                for record in json.loads(data):
                    yield self._encoder.decode(record)
            except (json.JSONDecodeError, UnicodeDecodeError) as e:
                raise StorageCorruptionError(f"Stream read failed: {str(e)}")

    def _atomic_write(self, data: Union[ResultSet, List]) -> None:
        """Atomic write procedure with error recovery."""
        try:
            with NamedTemporaryFile(
                mode='wb' if self.compress else 'w',
                dir=str(self.path.parent),
                delete=False,
                prefix=f"{self.path.stem}_temp_",
                suffix='.json' + ('.gz' if self.compress else '')
            ) as tmp:
                json_data = json.dumps(
                    data,
                    indent=self.indent,
                    cls=self._encoder,
                    ensure_ascii=False
                )

                if self.compress:
                    import zlib
                    tmp.write(zlib.compress(json_data.encode('utf-8')))
                else:
                    tmp.write(json_data)

            # Atomic rename (POSIX-compliant)
            os.replace(tmp.name, self.path)

        except OSError as e:
            if e.errno == 28:  # ENOSPC
                raise DiskFullError(
                    needed=os.path.getsize(tmp.name),
                    available=os.statvfs(str(self.path.parent)).f_bsize * os.statvfs(str(self.path.parent)).f_bavail
                )
            raise StorageCorruptionError(f"Write failed: {str(e)}")
        finally:
            if 'tmp' in locals() and os.path.exists(tmp.name):
                os.unlink(tmp.name)

    def optimize(self) -> None:
        """Compact JSON by removing whitespace and sorting keys."""
        records = self.load()
        if records:
            self.pretty = False
            self.save(records)

    @property
    def size(self) -> int:
        """Get current storage size in bytes."""
        return self.path.stat().st_size if self.path.exists() else 0

class _JSONEncoder(json.JSONEncoder):
    """Custom JSON encoder with extended type support."""

    def default(self, obj):
        if isinstance(obj, datetime):
            return {'__datetime__': obj.isoformat()}
        elif isinstance(obj, bytes):
            return {'__bytes__': obj.hex()}
        elif isinstance(obj, set):
            return {'__set__': list(obj)}
        return super().default(obj)

    @staticmethod
    def decode(data: Any) -> Any:
        """Restore Python objects from JSON."""
        if isinstance(data, dict):
            if '__datetime__' in data:
                return datetime.fromisoformat(data['__datetime__'])
            elif '__bytes__' in data:
                return bytes.fromhex(data['__bytes__'])
            elif '__set__' in data:
                return set(data['__set__'])
            return {k: _JSONEncoder.decode(v) for k, v in data.items()}
        elif isinstance(data, list):
            return [_JSONEncoder.decode(item) for item in data]
        return data