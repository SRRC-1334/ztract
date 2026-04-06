"""Parquet writer for ztract."""
from __future__ import annotations

import time
from decimal import Decimal, InvalidOperation
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from ztract.writers.base import Writer, WriterStats, sanitize_column_name


def _cobol_to_arrow_type(field_def: dict) -> pa.DataType:
    """Map a COBOL field type to an Apache Arrow data type."""
    ftype = field_def.get("type", "ALPHANUMERIC").upper()
    size = field_def.get("size", 18)
    scale = field_def.get("scale", 0)

    if ftype == "ALPHANUMERIC":
        return pa.string()

    if ftype in ("NUMERIC", "DISPLAY"):
        if scale and scale > 0:
            return pa.decimal128(size, scale)
        if size <= 9:
            return pa.int32()
        return pa.int64()

    if ftype in ("PACKED_DECIMAL", "COMP-3"):
        return pa.decimal128(size, scale)

    if ftype == "COMP-1":
        return pa.float32()

    if ftype == "COMP-2":
        return pa.float64()

    if ftype in ("COMP", "COMP-4", "BINARY"):
        if scale and scale > 0:
            return pa.decimal128(size, scale)
        if size <= 9:
            return pa.int32()
        return pa.int64()

    # Default fallback
    return pa.string()


def build_arrow_schema(fields: list[dict]) -> pa.Schema:
    """Build a PyArrow schema from a list of COBOL field definitions.

    Skips FILLER fields and sanitizes column names.
    """
    arrow_fields = []
    for f in fields:
        if f["name"].upper() == "FILLER":
            continue
        col_name = sanitize_column_name(f["name"])
        arrow_type = _cobol_to_arrow_type(f)
        arrow_fields.append(pa.field(col_name, arrow_type, nullable=True))
    return pa.schema(arrow_fields)


def _coerce_value(value, arrow_type: pa.DataType):
    """Coerce a Python value to the target Arrow type."""
    if value is None:
        return None

    if pa.types.is_decimal(arrow_type):
        try:
            return Decimal(str(value))
        except InvalidOperation:
            return None

    if pa.types.is_integer(arrow_type):
        try:
            return int(value)
        except (ValueError, TypeError):
            return None

    if pa.types.is_floating(arrow_type):
        try:
            return float(value)
        except (ValueError, TypeError):
            return None

    return value


class ParquetWriter(Writer):
    """Write records to an Apache Parquet file."""

    def __init__(
        self,
        output_path: str,
        row_group_size: int = 10_000,
        compression: str = "snappy",
    ) -> None:
        self.output_path = Path(output_path)
        self.row_group_size = row_group_size
        self.compression = compression

        self._schema: pa.Schema | None = None
        self._field_defs: list[dict] = []
        self._columns: list[str] = []          # sanitized names
        self._original_names: list[str] = []   # original names from schema
        self._pq_writer: pq.ParquetWriter | None = None
        self._buffer: list[dict] = []
        self._records_written = 0
        self._start_time: float = 0.0

    @property
    def name(self) -> str:
        return f"Parquet → {self.output_path.name}"

    def open(self, schema: dict) -> None:
        self._start_time = time.monotonic()
        fields = schema.get("fields", [])
        self._field_defs = [f for f in fields if f["name"].upper() != "FILLER"]
        self._columns = [sanitize_column_name(f["name"]) for f in self._field_defs]
        self._original_names = [f["name"] for f in self._field_defs]
        self._schema = build_arrow_schema(fields)
        self._pq_writer = pq.ParquetWriter(
            str(self.output_path),
            self._schema,
            compression=self.compression,
        )
        self._buffer = []

    def _flush_buffer(self) -> None:
        if not self._buffer:
            return
        # Build columnar data
        col_data: dict[str, list] = {col: [] for col in self._columns}
        for record in self._buffer:
            for original, sanitized, field_def in zip(
                self._original_names, self._columns, self._field_defs
            ):
                if sanitized in record:
                    raw = record[sanitized]
                elif original in record:
                    raw = record[original]
                else:
                    raw = None
                arrow_type = _cobol_to_arrow_type(field_def)
                col_data[sanitized].append(_coerce_value(raw, arrow_type))

        arrays = []
        for col, field_def in zip(self._columns, self._field_defs):
            arrow_type = _cobol_to_arrow_type(field_def)
            arrays.append(pa.array(col_data[col], type=arrow_type))

        table = pa.table(dict(zip(self._columns, arrays)), schema=self._schema)
        self._pq_writer.write_table(table)
        self._records_written += len(self._buffer)
        self._buffer = []

    def write_batch(self, records: list[dict]) -> int:
        if self._pq_writer is None:
            raise RuntimeError("ParquetWriter.open() must be called before write_batch()")
        self._buffer.extend(records)
        # Flush complete row groups
        while len(self._buffer) >= self.row_group_size:
            chunk, self._buffer = (
                self._buffer[: self.row_group_size],
                self._buffer[self.row_group_size :],
            )
            orig_buffer = self._buffer
            self._buffer = chunk
            self._flush_buffer()
            self._buffer = orig_buffer
        return len(records)

    def close(self) -> WriterStats:
        self._flush_buffer()  # flush remaining
        if self._pq_writer is not None:
            self._pq_writer.close()
            self._pq_writer = None
        elapsed = time.monotonic() - self._start_time
        return WriterStats(records_written=self._records_written, elapsed_sec=elapsed)
