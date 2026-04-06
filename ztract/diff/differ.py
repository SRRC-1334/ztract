"""ztract.diff.differ — field-level JSONL differ.

Compares two JSONL files record-by-record, either by key fields or by
ordinal position, and returns a structured DiffResult.
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from pathlib import Path

logger = logging.getLogger("ztract.diff.differ")


@dataclass
class DiffResult:
    """Aggregated result of a JSONL diff operation."""

    added: int = 0
    deleted: int = 0
    changed: int = 0
    unchanged: int = 0
    total_before: int = 0
    total_after: int = 0
    changes: list[dict] = field(default_factory=list)
    additions: list[dict] = field(default_factory=list)
    deletions: list[dict] = field(default_factory=list)


class Differ:
    """Compare two JSONL files field-by-field.

    Parameters
    ----------
    key_fields:
        One or more field names whose combined value uniquely identifies a
        record.  When ``None`` (default) records are compared by ordinal
        position.
    show_unchanged:
        If *True*, unchanged records will be included in a separate list on
        the result (not yet surfaced in the CLI summary but available for
        programmatic use).
    """

    def __init__(
        self,
        key_fields: list[str] | None = None,
        show_unchanged: bool = False,
    ) -> None:
        self.key_fields = list(key_fields) if key_fields else None
        self.show_unchanged = show_unchanged

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def diff_jsonl(self, before_path: Path, after_path: Path) -> DiffResult:
        """Diff two JSONL files and return a :class:`DiffResult`."""
        before = self._load_jsonl(before_path)
        after  = self._load_jsonl(after_path)

        result = DiffResult(
            total_before=len(before),
            total_after=len(after),
        )

        if self.key_fields:
            self._diff_by_key(before, after, result)
        else:
            self._diff_by_ordinal(before, after, result)

        return result

    # ------------------------------------------------------------------
    # Internal — key-based diff
    # ------------------------------------------------------------------

    def _diff_by_key(
        self,
        before: list[dict],
        after: list[dict],
        result: DiffResult,
    ) -> None:
        """Compare records using key field(s) as the identity."""
        before_map: dict[tuple, dict] = {self._key(r): r for r in before}
        after_map:  dict[tuple, dict] = {self._key(r): r for r in after}

        all_keys = set(before_map) | set(after_map)

        for key in all_keys:
            in_before = key in before_map
            in_after  = key in after_map

            if in_before and not in_after:
                result.deleted += 1
                result.deletions.append(before_map[key])

            elif in_after and not in_before:
                result.added += 1
                result.additions.append(after_map[key])

            else:
                rec_before = before_map[key]
                rec_after  = after_map[key]
                diff_fields = self._field_diff(rec_before, rec_after)

                if diff_fields:
                    result.changed += 1
                    change_entry = {k: v for k, v in zip(self.key_fields, key)}  # type: ignore[arg-type]
                    change_entry["_before"] = {f: rec_before.get(f) for f in diff_fields}
                    change_entry["_after"]  = {f: rec_after.get(f)  for f in diff_fields}
                    result.changes.append(change_entry)
                else:
                    result.unchanged += 1

    # ------------------------------------------------------------------
    # Internal — ordinal diff
    # ------------------------------------------------------------------

    def _diff_by_ordinal(
        self,
        before: list[dict],
        after: list[dict],
        result: DiffResult,
    ) -> None:
        """Compare records by position; extra records count as added/deleted."""
        if len(before) != len(after):
            logger.warning(
                "Record count mismatch: before=%d, after=%d. "
                "Diffing by ordinal position; trailing records will be "
                "counted as added or deleted.",
                len(before),
                len(after),
            )

        common = min(len(before), len(after))

        for i in range(common):
            diff_fields = self._field_diff(before[i], after[i])
            if diff_fields:
                result.changed += 1
                change_entry: dict = {"_index": i}
                change_entry["_before"] = {f: before[i].get(f) for f in diff_fields}
                change_entry["_after"]  = {f: after[i].get(f)  for f in diff_fields}
                result.changes.append(change_entry)
            else:
                result.unchanged += 1

        # Extra records in *before* → deleted
        for rec in before[common:]:
            result.deleted += 1
            result.deletions.append(rec)

        # Extra records in *after* → added
        for rec in after[common:]:
            result.added += 1
            result.additions.append(rec)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _key(self, record: dict) -> tuple:
        """Return the key tuple for *record* based on ``self.key_fields``."""
        return tuple(record.get(k) for k in (self.key_fields or []))

    @staticmethod
    def _field_diff(rec_before: dict, rec_after: dict) -> list[str]:
        """Return a list of field names that differ between the two records."""
        all_fields = set(rec_before) | set(rec_after)
        return [f for f in all_fields if rec_before.get(f) != rec_after.get(f)]

    @staticmethod
    def _load_jsonl(path: Path) -> list[dict]:
        """Load all records from a JSONL file into a list of dicts."""
        records: list[dict] = []
        with open(path, encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if line:
                    records.append(json.loads(line))
        return records
