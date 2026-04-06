"""Tests for ztract.diff.differ — field-level JSONL differ."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from ztract.diff.differ import Differ, DiffResult


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def write_jsonl(path: Path, records: list[dict]) -> None:
    with open(path, "w", encoding="utf-8") as fh:
        for rec in records:
            fh.write(json.dumps(rec) + "\n")


# ---------------------------------------------------------------------------
# Key-based diff tests
# ---------------------------------------------------------------------------

class TestDiffByKey:
    def test_identical_files(self, tmp_path):
        records = [
            {"id": "1", "name": "Alice", "score": 100},
            {"id": "2", "name": "Bob",   "score": 200},
        ]
        before = tmp_path / "before.jsonl"
        after  = tmp_path / "after.jsonl"
        write_jsonl(before, records)
        write_jsonl(after,  records)

        result = Differ(key_fields=["id"]).diff_jsonl(before, after)

        assert result.added     == 0
        assert result.deleted   == 0
        assert result.changed   == 0
        assert result.unchanged == 2
        assert result.changes   == []
        assert result.additions == []
        assert result.deletions == []

    def test_added_record(self, tmp_path):
        before_recs = [{"id": "1", "name": "Alice"}]
        after_recs  = [{"id": "1", "name": "Alice"}, {"id": "2", "name": "Bob"}]

        before = tmp_path / "before.jsonl"
        after  = tmp_path / "after.jsonl"
        write_jsonl(before, before_recs)
        write_jsonl(after,  after_recs)

        result = Differ(key_fields=["id"]).diff_jsonl(before, after)

        assert result.added     == 1
        assert result.deleted   == 0
        assert result.changed   == 0
        assert result.unchanged == 1
        assert len(result.additions) == 1
        assert result.additions[0]["id"] == "2"

    def test_deleted_record(self, tmp_path):
        before_recs = [{"id": "1", "name": "Alice"}, {"id": "2", "name": "Bob"}]
        after_recs  = [{"id": "1", "name": "Alice"}]

        before = tmp_path / "before.jsonl"
        after  = tmp_path / "after.jsonl"
        write_jsonl(before, before_recs)
        write_jsonl(after,  after_recs)

        result = Differ(key_fields=["id"]).diff_jsonl(before, after)

        assert result.deleted   == 1
        assert result.added     == 0
        assert result.changed   == 0
        assert len(result.deletions) == 1
        assert result.deletions[0]["id"] == "2"

    def test_changed_record(self, tmp_path):
        before_recs = [{"id": "1", "name": "Alice", "score": 100}]
        after_recs  = [{"id": "1", "name": "Alice", "score": 999}]

        before = tmp_path / "before.jsonl"
        after  = tmp_path / "after.jsonl"
        write_jsonl(before, before_recs)
        write_jsonl(after,  after_recs)

        result = Differ(key_fields=["id"]).diff_jsonl(before, after)

        assert result.changed   == 1
        assert result.added     == 0
        assert result.deleted   == 0
        assert result.unchanged == 0
        assert len(result.changes) == 1

        change = result.changes[0]
        assert change["id"] == "1"
        assert change["_before"]["score"] == 100
        assert change["_after"]["score"]  == 999

    def test_changed_record_captures_only_differing_fields(self, tmp_path):
        before_recs = [{"id": "1", "name": "Alice", "score": 100, "rank": 5}]
        after_recs  = [{"id": "1", "name": "Alice", "score": 200, "rank": 5}]

        before = tmp_path / "before.jsonl"
        after  = tmp_path / "after.jsonl"
        write_jsonl(before, before_recs)
        write_jsonl(after,  after_recs)

        result = Differ(key_fields=["id"]).diff_jsonl(before, after)

        change = result.changes[0]
        assert "score" in change["_before"]
        assert "rank" not in change["_before"]

    def test_composite_key(self, tmp_path):
        before_recs = [
            {"region": "US", "dept": "eng", "headcount": 10},
            {"region": "EU", "dept": "eng", "headcount": 20},
        ]
        after_recs = [
            {"region": "US", "dept": "eng", "headcount": 15},
            {"region": "EU", "dept": "eng", "headcount": 20},
        ]

        before = tmp_path / "before.jsonl"
        after  = tmp_path / "after.jsonl"
        write_jsonl(before, before_recs)
        write_jsonl(after,  after_recs)

        result = Differ(key_fields=["region", "dept"]).diff_jsonl(before, after)

        assert result.changed   == 1
        assert result.unchanged == 1

    def test_total_counts(self, tmp_path):
        before_recs = [{"id": "1"}, {"id": "2"}]
        after_recs  = [{"id": "2"}, {"id": "3"}]

        before = tmp_path / "before.jsonl"
        after  = tmp_path / "after.jsonl"
        write_jsonl(before, before_recs)
        write_jsonl(after,  after_recs)

        result = Differ(key_fields=["id"]).diff_jsonl(before, after)

        assert result.total_before == 2
        assert result.total_after  == 2
        assert result.added   == 1
        assert result.deleted == 1


# ---------------------------------------------------------------------------
# Ordinal diff tests
# ---------------------------------------------------------------------------

class TestDiffByOrdinal:
    def test_identical_files(self, tmp_path):
        records = [{"name": "Alice"}, {"name": "Bob"}]
        before = tmp_path / "before.jsonl"
        after  = tmp_path / "after.jsonl"
        write_jsonl(before, records)
        write_jsonl(after,  records)

        result = Differ().diff_jsonl(before, after)

        assert result.added     == 0
        assert result.deleted   == 0
        assert result.changed   == 0
        assert result.unchanged == 2

    def test_changed_record_ordinal(self, tmp_path):
        before_recs = [{"name": "Alice"}, {"name": "Bob"}]
        after_recs  = [{"name": "Alice"}, {"name": "Charlie"}]

        before = tmp_path / "before.jsonl"
        after  = tmp_path / "after.jsonl"
        write_jsonl(before, before_recs)
        write_jsonl(after,  after_recs)

        result = Differ().diff_jsonl(before, after)

        assert result.changed == 1
        assert result.unchanged == 1

    def test_added_record_ordinal(self, tmp_path):
        before_recs = [{"name": "Alice"}]
        after_recs  = [{"name": "Alice"}, {"name": "Bob"}]

        before = tmp_path / "before.jsonl"
        after  = tmp_path / "after.jsonl"
        write_jsonl(before, before_recs)
        write_jsonl(after,  after_recs)

        result = Differ().diff_jsonl(before, after)

        assert result.added  == 1
        assert result.changed == 0

    def test_deleted_record_ordinal(self, tmp_path):
        before_recs = [{"name": "Alice"}, {"name": "Bob"}]
        after_recs  = [{"name": "Alice"}]

        before = tmp_path / "before.jsonl"
        after  = tmp_path / "after.jsonl"
        write_jsonl(before, before_recs)
        write_jsonl(after,  after_recs)

        result = Differ().diff_jsonl(before, after)

        assert result.deleted == 1
        assert result.changed  == 0

    def test_warns_on_different_counts(self, tmp_path, caplog):
        import logging
        before_recs = [{"name": "Alice"}, {"name": "Bob"}]
        after_recs  = [{"name": "Alice"}]

        before = tmp_path / "before.jsonl"
        after  = tmp_path / "after.jsonl"
        write_jsonl(before, before_recs)
        write_jsonl(after,  after_recs)

        with caplog.at_level(logging.WARNING, logger="ztract.diff.differ"):
            Differ().diff_jsonl(before, after)

        assert any("count" in msg.lower() or "record" in msg.lower() for msg in caplog.messages)
