"""
MessagePack Backend - Blazing-fast binary storage with full atomicity.
"""

import os
import msgpack
from tempfile import NamedTemporaryFile
from pathlib import Path
from typing import List, Dict, Any, Optional, Union, Iterator
from threading import Lock
from datetime import datetime
from ..exceptions import StorageCorruptionError, DiskFullError
from ..types import Record, ResultSet

class MsgpackBackend:
    """Atomic MessagePack storage with extended type support."""

    def __init__(
        self,
        path: Path,
        compress: bool = True,
        compression_level: int = 3
    ):
        self.path = path
        self.compress = compress
        self.compression_level = compression_level
        self._lock = Lock()
        self._packer = msgpack.Packer(
            use_bin_type=True,
            datetime=True,
            default=self._encode_custom_types
        )

        # Create empty file if doesn't exist
        if not self.path.exists():
            self._atomic_write([])

    def load(self) -> ResultSet:
        """Load all records with full type restoration."""
        if not self.path.exists():
            return []

        with self._lock, open(self.path, 'rb') as f:
            try:
                data = f.read()
                if self.compress:
                    import zlib
                    data = zlib.decompress(data)
                return [
                    self._decode_custom_types(record)
                    for record in msgpack.Unpacker(
                        data,
                        raw=False,
                        object_hook=self._decode_custom_types
                    )
                ]
            except (msgpack.ExtraData, ValueError, zlib.error) as e:
                raise StorageCorruptionError(f"Msgpack parse failed: {str(e)}")

    def save(self, records: ResultSet) -> None:
        """Atomically save records with compression."""
        self._atomic_write(records)

    def stream(self) -> Iterator[Record]:
        """Lazy record streaming with memory efficiency."""
        if not self.path.exists():
            return

        with self._lock, open(self.path, 'rb') as f:
            try:
                data = f.read()
                if self.compress:
                    import zlib
                    data = zlib.decompress(data)

                for record in msgpack.Unpacker(
                    data,
                    raw=False,
                    object_hook=self._decode_custom_types
                ):
                    yield record
            except (msgpack.ExtraData, ValueError) as e:
                raise StorageCorruptionError(f"Stream read failed: {str(e)}")

    def _atomic_write(self, data: Union[ResultSet, List]) -> None:
        """Atomic write procedure with error recovery."""
        try:
            with NamedTemporaryFile(
                mode='wb',
                dir=str(self.path.parent),
                delete=False,
                prefix=f"{self.path.stem}_temp_",
                suffix='.mp'
            ) as tmp:
                packed = b''.join(
                    self._packer.pack(record) for record in data
                )

                if self.compress:
                    import zlib
                    packed = zlib.compress(packed, level=self.compression_level)

                tmp.write(packed)

            # Atomic rename
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

    @staticmethod
    def _encode_custom_types(obj: Any) -> Any:
        """Handle non-native MessagePack types."""
        if isinstance(obj, set):
            return {'__set__': list(obj)}
        elif isinstance(obj, Path):
            return {'__path__': str(obj)}
        return obj

    @staticmethod
    def _decode_custom_types(obj: Dict) -> Any:
        """Restore custom types during unpacking."""
        if '__set__' in obj:
            return set(obj['__set__'])
        elif '__path__' in obj:
            return Path(obj['__path__'])
        return obj

    def optimize(self) -> None:
        """Recompress and defragment the data file."""
        records = list(self.stream())
        if records:
            self.save(records)

    @property
    def size(self) -> int:
        """Get current storage size in bytes."""
        return self.path.stat().st_size if self.path.exists() else 0

    def create_snapshot(self, target_path: Path) -> None:
        """Create a point-in-time snapshot."""
        with self._lock:
            if self.path.exists():
                shutil.copy2(self.path, target_path)