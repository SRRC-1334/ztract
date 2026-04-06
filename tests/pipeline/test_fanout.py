"""Tests for pipeline/fanout.py - FanOut queue."""
import pytest
from ztract.pipeline.fanout import FanOut
from ztract.writers.base import Writer, WriterStats


SIMPLE_SCHEMA = {
    "fields": [
        {"name": "ID", "type": "ALPHANUMERIC", "size": 3},
        {"name": "VALUE", "type": "NUMERIC", "size": 5, "scale": 0},
    ]
}


class FakeWriter(Writer):
    """In-memory test double that records all written batches."""

    def __init__(self, name_str: str = "fake") -> None:
        self._name = name_str
        self.opened = False
        self.closed = False
        self.batches: list[list[dict]] = []
        self.records: list[dict] = []
        self.batch_size = 1000

    @property
    def name(self) -> str:
        return self._name

    def open(self, schema: dict) -> None:
        self.opened = True

    def write_batch(self, records: list[dict]) -> int:
        self.batches.append(list(records))
        self.records.extend(records)
        return len(records)

    def close(self) -> WriterStats:
        self.closed = True
        return WriterStats(records_written=len(self.records))


def make_records(n: int) -> list[dict]:
    return [{"ID": str(i), "VALUE": i} for i in range(n)]


class TestFanOut:
    def test_broadcasts_to_all_writers(self):
        """All writers receive all records."""
        w1 = FakeWriter("w1")
        w2 = FakeWriter("w2")
        records = make_records(5)
        fan = FanOut([w1, w2], SIMPLE_SCHEMA, batch_size=10)
        total = fan.run(iter(records))
        assert total == 5
        assert len(w1.records) == 5
        assert len(w2.records) == 5
        assert w1.records == w2.records

    def test_batches_records_correctly(self):
        """25 records with batch_size=10 should produce 3 batches (10+10+5)."""
        w = FakeWriter()
        records = make_records(25)
        fan = FanOut([w], SIMPLE_SCHEMA, batch_size=10)
        fan.run(iter(records))
        assert len(w.batches) == 3
        assert len(w.batches[0]) == 10
        assert len(w.batches[1]) == 10
        assert len(w.batches[2]) == 5

    def test_returns_total_count(self):
        w = FakeWriter()
        records = make_records(42)
        fan = FanOut([w], SIMPLE_SCHEMA, batch_size=10)
        total = fan.run(iter(records))
        assert total == 42

    def test_empty_iterator(self):
        w = FakeWriter()
        fan = FanOut([w], SIMPLE_SCHEMA, batch_size=10)
        total = fan.run(iter([]))
        assert total == 0
        assert w.records == []

    def test_opens_and_closes_all_writers(self):
        w1 = FakeWriter("w1")
        w2 = FakeWriter("w2")
        fan = FanOut([w1, w2], SIMPLE_SCHEMA)
        fan.run(iter(make_records(3)))
        assert w1.opened and w1.closed
        assert w2.opened and w2.closed

    def test_single_writer_no_threads(self):
        """Single writer should still work correctly (optimized path)."""
        w = FakeWriter()
        records = make_records(100)
        fan = FanOut([w], SIMPLE_SCHEMA, batch_size=20)
        total = fan.run(iter(records))
        assert total == 100
        assert len(w.records) == 100

    def test_multi_writer_correct_data(self):
        """Multi-writer threaded path delivers correct data."""
        w1 = FakeWriter("w1")
        w2 = FakeWriter("w2")
        w3 = FakeWriter("w3")
        records = make_records(50)
        fan = FanOut([w1, w2, w3], SIMPLE_SCHEMA, batch_size=10)
        total = fan.run(iter(records))
        assert total == 50
        assert len(w1.records) == 50
        assert len(w2.records) == 50
        assert len(w3.records) == 50

    def test_record_order_preserved(self):
        """Records arrive at writers in the same order they were read."""
        w = FakeWriter()
        records = make_records(30)
        fan = FanOut([w], SIMPLE_SCHEMA, batch_size=10)
        fan.run(iter(records))
        ids = [r["ID"] for r in w.records]
        assert ids == [str(i) for i in range(30)]
