"""EBCDIC writer — encodes mock records to a mainframe binary file.

Buffers generated records in memory then calls ``ZtractBridge.encode()``
during ``close()`` so the Java engine receives a complete stream.
"""
from __future__ import annotations

import time
from pathlib import Path
from typing import TYPE_CHECKING

from ztract.writers.base import Writer, WriterStats

if TYPE_CHECKING:
    from ztract.engine.bridge import ZtractBridge


class EBCDICWriter(Writer):
    """Write records to an EBCDIC binary file via the Java engine bridge."""

    def __init__(
        self,
        output_path: str | Path,
        bridge: "ZtractBridge",
        copybook: Path,
        recfm: str,
        lrecl: int | None,
        codepage: str = "cp037",
    ) -> None:
        self.output_path = Path(output_path)
        self._bridge = bridge
        self._copybook = Path(copybook)
        self._recfm = recfm
        self._lrecl = lrecl if lrecl is not None else 0
        self._codepage = codepage

        self._records: list[dict] = []
        self._start_time: float = 0.0

    # ------------------------------------------------------------------
    # Writer interface
    # ------------------------------------------------------------------

    @property
    def name(self) -> str:
        return f"EBCDIC → {self.output_path.name}"

    def open(self, schema: dict) -> None:
        """Record the start time (no file handle needed; bridge does I/O)."""
        self._start_time = time.monotonic()

    def write_batch(self, records: list[dict]) -> int:
        """Buffer *records* for later encoding."""
        self._records.extend(records)
        return len(records)

    def close(self) -> WriterStats:
        """Encode all buffered records and return statistics.

        Calls ``ZtractBridge.encode()`` with the accumulated records, then
        clears the buffer and returns a :class:`WriterStats` instance.
        """
        records_snapshot = list(self._records)
        self._records.clear()
        count = self._bridge.encode(
            copybook=self._copybook,
            output_path=self.output_path,
            recfm=self._recfm,
            lrecl=self._lrecl,
            codepage=self._codepage,
            records=records_snapshot,
        )
        elapsed = time.monotonic() - self._start_time
        return WriterStats(records_written=count, elapsed_sec=elapsed)
