"""Database writer for ztract (SQLAlchemy-based)."""
from __future__ import annotations

import time
from typing import Any

from sqlalchemy import (
    Column,
    Float,
    Integer,
    MetaData,
    Numeric,
    String,
    Table,
    create_engine,
)
from sqlalchemy.engine import Engine

from ztract.writers.base import Writer, WriterStats, sanitize_column_name


def _cobol_to_sqla_type(field_def: dict) -> Any:
    """Map a COBOL field type to a SQLAlchemy column type."""
    ftype = field_def.get("type", "ALPHANUMERIC").upper()
    size = field_def.get("size", 255)
    scale = field_def.get("scale", 0)

    if ftype == "ALPHANUMERIC":
        return String(size)

    if ftype in ("NUMERIC", "DISPLAY"):
        if scale and scale > 0:
            return Numeric(precision=size, scale=scale)
        return Integer()

    if ftype in ("PACKED_DECIMAL", "COMP-3"):
        return Numeric(precision=size, scale=scale or 0)

    if ftype in ("COMP-1", "COMP-2"):
        return Float()

    if ftype in ("COMP", "COMP-4", "BINARY"):
        if scale and scale > 0:
            return Numeric(precision=size, scale=scale)
        return Integer()

    return String(size)


class DatabaseWriter(Writer):
    """Write records to a relational database table via SQLAlchemy."""

    def __init__(
        self,
        connection_url: str,
        table_name: str,
        mode: str = "append",
        batch_size: int = 1000,
    ) -> None:
        self.connection_url = connection_url
        self.table_name = table_name
        self.mode = mode  # "append" or "truncate"
        self.batch_size = batch_size

        self._engine: Engine | None = None
        self._table: Table | None = None
        self._field_defs: list[dict] = []
        self._columns: list[str] = []        # sanitized column names
        self._original_names: list[str] = [] # original schema names
        self._records_written = 0
        self._start_time: float = 0.0

    @property
    def name(self) -> str:
        return f"DB → {self.table_name}"

    def open(self, schema: dict) -> None:
        self._start_time = time.monotonic()
        fields = schema.get("fields", [])

        # Skip FILLER
        self._field_defs = [f for f in fields if f["name"].upper() != "FILLER"]
        self._columns = [sanitize_column_name(f["name"]) for f in self._field_defs]
        self._original_names = [f["name"] for f in self._field_defs]

        self._engine = create_engine(self.connection_url)
        metadata = MetaData()

        columns = [
            Column(col, _cobol_to_sqla_type(field_def), nullable=True)
            for col, field_def in zip(self._columns, self._field_defs)
        ]
        self._table = Table(self.table_name, metadata, *columns)
        metadata.create_all(self._engine)

        if self.mode == "truncate":
            with self._engine.connect() as conn:
                conn.execute(self._table.delete())
                conn.commit()

    def write_batch(self, records: list[dict]) -> int:
        if self._engine is None or self._table is None:
            raise RuntimeError("DatabaseWriter.open() must be called before write_batch()")

        rows = []
        for record in records:
            row = {}
            for original, sanitized in zip(self._original_names, self._columns):
                if sanitized in record:
                    value = record[sanitized]
                elif original in record:
                    value = record[original]
                else:
                    value = None
                row[sanitized] = value
            rows.append(row)

        with self._engine.connect() as conn:
            conn.execute(self._table.insert(), rows)
            conn.commit()

        self._records_written += len(rows)
        return len(rows)

    def close(self) -> WriterStats:
        if self._engine is not None:
            self._engine.dispose()
            self._engine = None
        elapsed = time.monotonic() - self._start_time
        return WriterStats(records_written=self._records_written, elapsed_sec=elapsed)
