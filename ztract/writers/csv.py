"""CSV writer for ztract."""
from __future__ import annotations

import csv
import time
from io import TextIOWrapper
from pathlib import Path
from typing import Any, Optional

import fsspec

from ztract.connectors.base import is_cloud_path
from ztract.writers.base import Writer, WriterStats, sanitize_column_name


class CSVWriter(Writer):
    """Write records to a delimited text file."""

    def __init__(
        self,
        output_path: str,
        delimiter: str = ",",
        null_value: str = "",
        encoding: str = "utf-8",
        bom: bool = False,
        storage_options: dict | None = None,
    ) -> None:
        self._output_str = str(output_path)
        self.output_path = Path(output_path) if not is_cloud_path(self._output_str) else output_path
        self.delimiter = delimiter
        self.null_value = null_value
        self.encoding = encoding
        self.bom = bom
        self._storage_options = storage_options or {}

        self._file: Optional[TextIOWrapper] = None
        self._writer: Optional[Any] = None
        self._columns: list[str] = []  # sanitized column names to write (no FILLER)
        self._field_names: list[str] = []  # original names from schema
        self._records_written = 0
        self._start_time: float = 0.0

    @property
    def name(self) -> str:
        return f"CSV → {self.output_path.name}"

    def open(self, schema: dict) -> None:
        self._start_time = time.monotonic()
        fields = schema.get("fields", [])
        # Build column list, skipping FILLER
        self._columns = []
        self._field_names = []
        for f in fields:
            if f["name"].upper() == "FILLER":
                continue
            self._field_names.append(f["name"])
            self._columns.append(sanitize_column_name(f["name"]))

        enc = f"{self.encoding}-sig" if self.bom and self.encoding == "utf-8" else self.encoding

        if is_cloud_path(self._output_str):
            self._file = fsspec.open(
                self._output_str, mode="w", encoding=enc,
                newline="", **self._storage_options,
            ).open()
        else:
            Path(self._output_str).parent.mkdir(parents=True, exist_ok=True)
            self._file = open(self._output_str, "w", newline="", encoding=enc)

        self._writer = csv.writer(self._file, delimiter=self.delimiter)
        self._writer.writerow(self._columns)

    def write_batch(self, records: list[dict]) -> int:
        if self._writer is None:
            raise RuntimeError("CSVWriter.open() must be called before write_batch()")
        written = 0
        for record in records:
            row = []
            for original, sanitized in zip(self._field_names, self._columns):
                # Try sanitized first, then original
                if sanitized in record:
                    value = record[sanitized]
                elif original in record:
                    value = record[original]
                else:
                    value = None

                if value is None:
                    row.append(self.null_value)
                else:
                    row.append(str(value))
            self._writer.writerow(row)
            written += 1
        if self._file is not None:
            self._file.flush()
        self._records_written += written
        return written

    def close(self) -> WriterStats:
        if self._file is not None:
            self._file.close()
            self._file = None
        elapsed = time.monotonic() - self._start_time
        return WriterStats(records_written=self._records_written, elapsed_sec=elapsed)
