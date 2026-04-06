"""Writer base class, stats, and shared utilities."""
from __future__ import annotations

import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field


@dataclass
class WriterStats:
    records_written: int = 0
    elapsed_sec: float = 0.0
    errors: int = 0


class Writer(ABC):
    """Abstract base class for all ztract output writers."""

    batch_size: int = 1000

    @abstractmethod
    def open(self, schema: dict) -> None:
        """Open the writer and prepare for writing using the given schema."""

    @abstractmethod
    def write_batch(self, records: list[dict]) -> int:
        """Write a batch of records. Returns number of records written."""

    @abstractmethod
    def close(self) -> WriterStats:
        """Flush, close, and return statistics."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Human-readable writer name."""


def sanitize_column_name(name: str) -> str:
    """Replace hyphens with underscores in column names.

    Example: CUST-ID → CUST_ID
    """
    return name.replace("-", "_")


def flatten_occurs(record: dict, schema_fields: list[dict]) -> dict:
    """Flatten OCCURS (repeating group) arrays into individual columns.

    OCCURS arrays become indexed columns:
        {"ITEM": [{"CODE": "A"}, {"CODE": "B"}]}
        → {"ITEM_1_CODE": "A", "ITEM_2_CODE": "B"}

    Non-OCCURS fields pass through with sanitized names.
    A field is identified as an OCCURS group via field_def.get("occurs").
    """
    result: dict = {}
    for field_def in schema_fields:
        field_name = field_def["name"]
        sanitized = sanitize_column_name(field_name)

        if field_def.get("occurs"):
            # It's a repeating group — flatten with 1-based index
            children = field_def.get("children", [])
            raw_value = record.get(field_name, record.get(sanitized, []))
            if not isinstance(raw_value, list):
                raw_value = []
            for i, occurrence in enumerate(raw_value, start=1):
                for child in children:
                    child_name = child["name"]
                    child_sanitized = sanitize_column_name(child_name)
                    key = f"{sanitized}_{i}_{child_sanitized}"
                    result[key] = occurrence.get(child_name, occurrence.get(child_sanitized))
        else:
            # Flat field — pass through with sanitized name
            value = record.get(field_name, record.get(sanitized))
            result[sanitized] = value

    return result
