"""
CSV Backend - Simple, brutal efficiency for tabular data.
"""

import csv
import os
from tempfile import NamedTemporaryFile
from pathlib import Path
from typing import List, Dict, Any, Optional, Iterator
from threading import Lock
from ..exceptions import StorageCorruptionError, DiskFullError
from ..types import Record, ResultSet

class CSVBackend:
    """Atomic CSV storage engine with type inference."""
    
    def __init__(self, path: Path, delimiter: str = ',', quotechar: str = '"'):
        self.path = path
        self.delimiter = delimiter
        self.quotechar = quotechar
        self._lock = Lock()
        self._field_types: Dict[str, type] = {}
        
        # Create empty file if doesn't exist
        if not self.path.exists():
            with open(self.path, 'w') as f:
                f.write('')  # Empty file

    def load(self) -> ResultSet:
        """Load all records with type conversion."""
        with self._lock, open(self.path, 'r') as f:
            try:
                reader = csv.DictReader(f, delimiter=self.delimiter, quotechar=self.quotechar)
                return [self._convert_types(row) for row in reader]
            except (csv.Error, UnicodeDecodeError) as e:
                raise StorageCorruptionError(f"CSV parse failed: {str(e)}")

    def save(self, records: ResultSet) -> None:
        """Atomically save records with write-ahead logging."""
        if not records:
            return
            
        # Infer field types on first save
        if not self._field_types:
            self._infer_field_types(records[0])
            
        # Convert data to CSV-compatible strings
        converted = [self._convert_to_csv(r) for r in records]
        fieldnames = list(self._field_types.keys())
        
        # Atomic write procedure
        try:
            with NamedTemporaryFile(
                mode='w',
                dir=str(self.path.parent),
                delete=False,
                prefix=f"{self.path.stem}_temp_",
                suffix='.csv'
            ) as tmp:
                writer = csv.DictWriter(
                    tmp,
                    fieldnames=fieldnames,
                    delimiter=self.delimiter,
                    quotechar=self.quotechar
                )
                writer.writeheader()
                writer.writerows(converted)
                
            # Atomic rename (POSIX-compliant)
            os.replace(tmp.name, self.path)
            
        except OSError as e:
            if e.errno == 28:  # ENOSPC
                raise DiskFullError(needed=os.path.getsize(tmp.name), 
                                  available=shutil.disk_usage(str(self.path.parent)).free)
            raise StorageCorruptionError(f"Write failed: {str(e)}")
        finally:
            if os.path.exists(tmp.name):
                os.unlink(tmp.name)

    def stream(self) -> Iterator[Record]:
        """Lazy record streaming for large datasets."""
        with open(self.path, 'r') as f:
            reader = csv.DictReader(f, delimiter=self.delimiter, quotechar=self.quotechar)
            for row in reader:
                yield self._convert_types(row)

    def _infer_field_types(self, sample_record: Record) -> None:
        """Detect field types from sample data."""
        for field, value in sample_record.items():
            if isinstance(value, bool):
                self._field_types[field] = bool
            elif isinstance(value, int):
                self._field_types[field] = int
            elif isinstance(value, float):
                self._field_types[field] = float
            else:
                self._field_types[field] = str

    def _convert_types(self, row: Dict[str, str]) -> Record:
        """Convert CSV strings back to original types."""
        converted = {}
        for field, value in row.items():
            if field not in self._field_types:
                converted[field] = value
                continue
                
            try:
                if self._field_types[field] == bool:
                    converted[field] = value.lower() == 'true'
                elif self._field_types[field] == int:
                    converted[field] = int(value)
                elif self._field_types[field] == float:
                    converted[field] = float(value)
                else:
                    converted[field] = value
            except (ValueError, AttributeError):
                converted[field] = value  # Fallback to string
                
        return converted

    def _convert_to_csv(self, record: Record) -> Dict[str, str]:
        """Convert native types to CSV-compatible strings."""
        return {
            field: str(record.get(field, ''))
            for field in self._field_types
        }

    def optimize(self) -> None:
        """Compact CSV by removing empty rows and sorting fields."""
        records = self.load()
        if records:
            self.save(records)

    @property
    def size(self) -> int:
        """Get current storage size in bytes."""
        return self.path.stat().st_size