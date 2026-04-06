"""Tests for writers/database.py - DatabaseWriter (SQLite)."""
import pytest
from sqlalchemy import create_engine, text

from ztract.writers.database import DatabaseWriter


SIMPLE_SCHEMA = {
    "fields": [
        {"name": "CUST-ID", "type": "ALPHANUMERIC", "size": 10},
        {"name": "NAME", "type": "ALPHANUMERIC", "size": 30},
        {"name": "AMOUNT", "type": "NUMERIC", "size": 9, "scale": 0},
        {"name": "FILLER", "type": "ALPHANUMERIC", "size": 5},
    ]
}

RECORDS = [
    {"CUST-ID": "001", "NAME": "Alice", "AMOUNT": 100, "FILLER": ""},
    {"CUST-ID": "002", "NAME": "Bob", "AMOUNT": 200, "FILLER": ""},
]


def db_url(tmp_path):
    return f"sqlite:///{tmp_path}/test.db"


class TestDatabaseWriter:
    def test_creates_table_and_inserts(self, tmp_path):
        url = db_url(tmp_path)
        w = DatabaseWriter(url, "customers")
        w.open(SIMPLE_SCHEMA)
        w.write_batch(RECORDS)
        w.close()

        engine = create_engine(url)
        with engine.connect() as conn:
            rows = conn.execute(text("SELECT * FROM customers")).fetchall()
        assert len(rows) == 2
        engine.dispose()

    def test_column_names_sanitized(self, tmp_path):
        """CUST-ID should become CUST_ID and be queryable."""
        url = db_url(tmp_path)
        w = DatabaseWriter(url, "customers")
        w.open(SIMPLE_SCHEMA)
        w.write_batch(RECORDS)
        w.close()

        engine = create_engine(url)
        with engine.connect() as conn:
            rows = conn.execute(text("SELECT CUST_ID FROM customers")).fetchall()
        assert rows[0][0] == "001"
        engine.dispose()

    def test_filler_skipped(self, tmp_path):
        url = db_url(tmp_path)
        w = DatabaseWriter(url, "customers")
        w.open(SIMPLE_SCHEMA)
        w.write_batch(RECORDS)
        w.close()

        engine = create_engine(url)
        with engine.connect() as conn:
            cols = conn.execute(text("PRAGMA table_info(customers)")).fetchall()
        col_names = [c[1] for c in cols]
        assert "FILLER" not in col_names
        engine.dispose()

    def test_append_mode_keeps_existing_rows(self, tmp_path):
        url = db_url(tmp_path)

        w1 = DatabaseWriter(url, "customers", mode="append")
        w1.open(SIMPLE_SCHEMA)
        w1.write_batch(RECORDS)
        w1.close()

        w2 = DatabaseWriter(url, "customers", mode="append")
        w2.open(SIMPLE_SCHEMA)
        w2.write_batch([{"CUST-ID": "003", "NAME": "Carol", "AMOUNT": 300, "FILLER": ""}])
        w2.close()

        engine = create_engine(url)
        with engine.connect() as conn:
            count = conn.execute(text("SELECT COUNT(*) FROM customers")).scalar()
        assert count == 3
        engine.dispose()

    def test_truncate_mode_removes_existing_rows(self, tmp_path):
        url = db_url(tmp_path)

        w1 = DatabaseWriter(url, "customers", mode="append")
        w1.open(SIMPLE_SCHEMA)
        w1.write_batch(RECORDS)
        w1.close()

        w2 = DatabaseWriter(url, "customers", mode="truncate")
        w2.open(SIMPLE_SCHEMA)
        w2.write_batch([{"CUST-ID": "099", "NAME": "Lone", "AMOUNT": 1, "FILLER": ""}])
        w2.close()

        engine = create_engine(url)
        with engine.connect() as conn:
            count = conn.execute(text("SELECT COUNT(*) FROM customers")).scalar()
        assert count == 1
        engine.dispose()

    def test_null_values_inserted(self, tmp_path):
        url = db_url(tmp_path)
        w = DatabaseWriter(url, "customers")
        w.open(SIMPLE_SCHEMA)
        w.write_batch([{"CUST-ID": None, "NAME": None, "AMOUNT": None, "FILLER": ""}])
        w.close()

        engine = create_engine(url)
        with engine.connect() as conn:
            row = conn.execute(text("SELECT CUST_ID, NAME, AMOUNT FROM customers")).fetchone()
        assert row[0] is None
        assert row[1] is None
        assert row[2] is None
        engine.dispose()

    def test_returns_writer_stats(self, tmp_path):
        url = db_url(tmp_path)
        w = DatabaseWriter(url, "customers")
        w.open(SIMPLE_SCHEMA)
        w.write_batch(RECORDS)
        stats = w.close()
        assert stats.records_written == 2

    def test_writer_name_contains_table(self, tmp_path):
        url = db_url(tmp_path)
        w = DatabaseWriter(url, "customers")
        assert "customers" in w.name
