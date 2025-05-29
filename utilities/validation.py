"""
KUMDB Validation - Data integrity enforcement with an iron fist.
"""

from typing import Dict, List, Any, Optional, Callable, Union, Type
from datetime import datetime
from pathlib import Path
from enum import Enum
import re
from ..exceptions import ValidationError
from ..types import Record, FieldType

class Constraint:
    """Base class for all field constraints."""
    def validate(self, value: Any, field: str) -> None:
        raise NotImplementedError

class Required(Constraint):
    """Ensures field exists and is not None."""
    def validate(self, value: Any, field: str) -> None:
        if value is None:
            raise ValidationError(f"Field '{field}' is required")

class TypeCheck(Constraint):
    """Validates value type matches expected."""
    def __init__(self, expected_type: Union[Type, List[Type]]):
        self.expected_type = expected_type

    def validate(self, value: Any, field: str) -> None:
        if not isinstance(value, self.expected_type):
            expected = (
                " or ".join(t.__name__ for t in self.expected_type)
                if isinstance(self.expected_type, list)
                else self.expected_type.__name__
            )
            raise ValidationError(
                f"Field '{field}' must be {expected}, got {type(value).__name__}"
            )

class Range(Constraint):
    """Validates numeric ranges."""
    def __init__(self, min_val: Optional[float] = None, max_val: Optional[float] = None):
        self.min = min_val
        self.max = max_val

    def validate(self, value: Any, field: str) -> None:
        if not isinstance(value, (int, float)):
            raise ValidationError(f"Field '{field}' must be numeric for range validation")
        if self.min is not None and value < self.min:
            raise ValidationError(f"Field '{field}' must be ≥ {self.min}")
        if self.max is not None and value > self.max:
            raise ValidationError(f"Field '{field}' must be ≤ {self.max}")

class Length(Constraint):
    """Validates string/collection lengths."""
    def __init__(self, min_len: Optional[int] = None, max_len: Optional[int] = None):
        self.min = min_len
        self.max = max_len

    def validate(self, value: Any, field: str) -> None:
        length = len(value) if hasattr(value, '__len__') else 0
        if self.min is not None and length < self.min:
            raise ValidationError(f"Field '{field}' length must be ≥ {self.min}")
        if self.max is not None and length > self.max:
            raise ValidationError(f"Field '{field}' length must be ≤ {self.max}")

class Regex(Constraint):
    """Validates string patterns."""
    def __init__(self, pattern: str, flags: int = 0):
        self.pattern = re.compile(pattern, flags)

    def validate(self, value: Any, field: str) -> None:
        if not isinstance(value, str):
            raise ValidationError(f"Field '{field}' must be string for regex validation")
        if not self.pattern.search(value):
            raise ValidationError(f"Field '{field}' must match pattern: {self.pattern.pattern}")

class EnumCheck(Constraint):
    """Validates against allowed values."""
    def __init__(self, allowed_values: List[Any]):
        self.allowed = allowed_values

    def validate(self, value: Any, field: str) -> None:
        if value not in self.allowed:
            allowed = ", ".join(str(v) for v in self.allowed)
            raise ValidationError(f"Field '{field}' must be one of: {allowed}")

class Unique(Constraint):
    """Ensures value uniqueness across records."""
    def __init__(self, table_name: str):
        self.table = table_name

    def validate(self, value: Any, field: str) -> None:
        # Implementation would check against existing records
        pass

class ForeignKey(Constraint):
    """Validates references to other tables."""
    def __init__(self, table: str, field: str):
        self.table = table
        self.field = field

    def validate(self, value: Any, field: str) -> None:
        # Implementation would verify the foreign key exists
        pass

class Validator:
    """Main validation engine for KUMDB schemas."""
    
    def __init__(self, schema: Dict[str, List[Constraint]]):
        """
        Initialize with validation schema.
        Example schema:
        {
            "username": [Required(), Length(min=3, max=20), Regex(r'^[a-z0-9_]+$')],
            "age": [Required(), Range(min=18, max=120)],
            "email": [Required(), TypeCheck(str), Regex(r'^[^@]+@[^@]+\.[^@]+$')]
        }
        """
        self.schema = schema

    def validate_record(self, record: Record) -> List[ValidationError]:
        """Validate a single record against schema."""
        errors = []
        for field, constraints in self.schema.items():
            value = record.get(field)
            for constraint in constraints:
                try:
                    constraint.validate(value, field)
                except ValidationError as e:
                    errors.append(e)
        return errors

    def validate_table(self, records: List[Record]) -> Dict[str, List[ValidationError]]:
        """Validate all records in a table."""
        results = {}
        for i, record in enumerate(records):
            errors = self.validate_record(record)
            if errors:
                results[f"record_{i}"] = errors
        return results

    @staticmethod
    def infer_schema(records: List[Record]) -> Dict[str, List[Constraint]]:
        """Generate schema constraints from sample data."""
        if not records:
            return {}

        # Get field types from first record
        sample = records[0]
        schema = {}
        
        for field, value in sample.items():
            constraints = [Required()]
            
            if isinstance(value, bool):
                constraints.append(TypeCheck(bool))
            elif isinstance(value, int):
                constraints.append(TypeCheck(int))
            elif isinstance(value, float):
                constraints.append(TypeCheck(float))
            elif isinstance(value, str):
                constraints.append(TypeCheck(str))
                if '@' in value and '.' in value:
                    constraints.append(Regex(r'^[^@]+@[^@]+\.[^@]+$'))
            elif isinstance(value, (list, set)):
                constraints.append(TypeCheck((list, set)))
            
            schema[field] = constraints
        
        return schema

# Pre-built validators for common patterns
CommonValidators = {
    "email": [TypeCheck(str), Regex(r'^[^@]+@[^@]+\.[^@]+$')],
    "url": [TypeCheck(str), Regex(r'^https?://[^\s/$.?#].[^\s]*$')],
    "iso_date": [TypeCheck(str), Regex(r'^\d{4}-\d{2}-\d{2}$')],
    "positive_int": [TypeCheck(int), Range(min=0)],
    "password": [TypeCheck(str), Length(min=8)]
}