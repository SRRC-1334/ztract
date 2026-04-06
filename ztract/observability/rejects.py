"""Reject handler — writes rejected records to a JSONL file."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


class RejectHandler:
    """Writes rejected records to a JSONL file, lazily creating it on first reject."""

    def __init__(self, file_path: Path) -> None:
        self._file_path = Path(file_path)
        self._count: int = 0
        self._fh = None  # opened lazily on first reject

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def open(self) -> None:
        """Prepare the handler. The file is created lazily on first reject."""
        # Nothing to do eagerly — kept for symmetry with close().

    def close(self) -> None:
        """Flush and close the underlying file handle if it was opened."""
        if self._fh is not None:
            self._fh.flush()
            self._fh.close()
            self._fh = None

    # ------------------------------------------------------------------
    # Core operation
    # ------------------------------------------------------------------

    def reject(
        self,
        record_num: int,
        byte_offset: int,
        step: str,
        error_type: str,
        error_msg: str,
        target: str,
        decoded: Any | None = None,
        raw_hex: str | None = None,
    ) -> None:
        """Write a JSONL entry for a rejected record.

        Parent directories and the file itself are created on the first call.
        The entry is flushed immediately so partial runs are recoverable.
        """
        if self._fh is None:
            self._file_path.parent.mkdir(parents=True, exist_ok=True)
            self._fh = self._file_path.open("a", encoding="utf-8")

        entry: dict[str, Any] = {
            "timestamp": datetime.now(tz=timezone.utc).isoformat(),
            "record_num": record_num,
            "byte_offset": byte_offset,
            "step": step,
            "error_type": error_type,
            "error_msg": error_msg,
            "target": target,
        }
        if decoded is not None:
            entry["decoded"] = decoded
        if raw_hex is not None:
            entry["raw_hex"] = raw_hex

        self._fh.write(json.dumps(entry) + "\n")
        self._fh.flush()
        self._count += 1

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def count(self) -> int:
        """Number of records rejected so far."""
        return self._count

    # ------------------------------------------------------------------
    # Context manager
    # ------------------------------------------------------------------

    def __enter__(self) -> "RejectHandler":
        self.open()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()
