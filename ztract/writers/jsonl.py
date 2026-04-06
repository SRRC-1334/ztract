"""JSON Lines writer for ztract."""
from __future__ import annotations

import json
import time
from pathlib import Path

from ztract.writers.base import Writer, WriterStats


class JSONLWriter(Writer):
    """Write records as JSON Lines (one JSON object per line)."""

    def __init__(self, output_path: str) -> None:
        self.output_path = Path(output_path)
        self._file = None
        self._records_written = 0
        self._start_time: float = 0.0

    @property
    def name(self) -> str:
        return f"JSONL → {self.output_path.name}"

    def open(self, schema: dict) -> None:
        self._start_time = time.monotonic()
        self._file = open(self.output_path, "w", encoding="utf-8")

    def write_batch(self, records: list[dict]) -> int:
        if self._file is None:
            raise RuntimeError("JSONLWriter.open() must be called before write_batch()")
        written = 0
        for record in records:
            self._file.write(json.dumps(record, ensure_ascii=False))
            self._file.write("\n")
            written += 1
        self._file.flush()
        self._records_written += written
        return written

    def close(self) -> WriterStats:
        if self._file is not None:
            self._file.close()
            self._file = None
        elapsed = time.monotonic() - self._start_time
        return WriterStats(records_written=self._records_written, elapsed_sec=elapsed)
