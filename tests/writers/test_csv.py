"""Tests for writers/csv.py - CSVWriter."""
import csv
from pathlib import Path

import pytest

from ztract.writers.csv import CSVWriter


SIMPLE_SCHEMA = {
    "fields": [
        {"name": "CUST-ID", "type": "ALPHANUMERIC"},
        {"name": "NAME", "type": "ALPHANUMERIC"},
        {"name": "AMOUNT", "type": "NUMERIC"},
    ]
}

RECORDS = [
    {"CUST-ID": "001", "NAME": "Alice", "AMOUNT": 100},
    {"CUST-ID": "002", "NAME": "Bob", "AMOUNT": 200},
]


class TestCSVWriter:
    def test_writes_header_and_rows(self, tmp_path):
        out = tmp_path / "out.csv"
        w = CSVWriter(str(out))
        w.open(SIMPLE_SCHEMA)
        w.write_batch(RECORDS)
        w.close()

        rows = list(csv.reader(out.read_text(encoding="utf-8").splitlines()))
        assert rows[0] == ["CUST_ID", "NAME", "AMOUNT"]
        assert rows[1] == ["001", "Alice", "100"]
        assert rows[2] == ["002", "Bob", "200"]

    def test_custom_delimiter_pipe(self, tmp_path):
        out = tmp_path / "out.csv"
        w = CSVWriter(str(out), delimiter="|")
        w.open(SIMPLE_SCHEMA)
        w.write_batch(RECORDS)
        w.close()

        lines = out.read_text(encoding="utf-8").splitlines()
        assert "|" in lines[0]
        assert lines[0] == "CUST_ID|NAME|AMOUNT"

    def test_null_becomes_empty_string_by_default(self, tmp_path):
        out = tmp_path / "out.csv"
        w = CSVWriter(str(out))
        w.open(SIMPLE_SCHEMA)
        w.write_batch([{"CUST-ID": None, "NAME": "Alice", "AMOUNT": None}])
        w.close()

        rows = list(csv.reader(out.read_text(encoding="utf-8").splitlines()))
        assert rows[1] == ["", "Alice", ""]

    def test_null_custom_value(self, tmp_path):
        out = tmp_path / "out.csv"
        w = CSVWriter(str(out), null_value="NULL")
        w.open(SIMPLE_SCHEMA)
        w.write_batch([{"CUST-ID": None, "NAME": "Alice", "AMOUNT": None}])
        w.close()

        rows = list(csv.reader(out.read_text(encoding="utf-8").splitlines()))
        assert rows[1] == ["NULL", "Alice", "NULL"]

    def test_norwegian_chars_preserved(self, tmp_path):
        out = tmp_path / "out.csv"
        w = CSVWriter(str(out), encoding="utf-8")
        schema = {"fields": [{"name": "NAME", "type": "ALPHANUMERIC"}]}
        w.open(schema)
        w.write_batch([{"NAME": "Ærlig Øst Åpent"}])
        w.close()

        content = out.read_text(encoding="utf-8")
        assert "Ærlig Øst Åpent" in content

    def test_writer_name_contains_csv(self, tmp_path):
        out = tmp_path / "output.csv"
        w = CSVWriter(str(out))
        assert "csv" in w.name.lower() or "CSV" in w.name

    def test_filler_fields_skipped(self, tmp_path):
        out = tmp_path / "out.csv"
        schema = {
            "fields": [
                {"name": "CUST-ID", "type": "ALPHANUMERIC"},
                {"name": "FILLER", "type": "ALPHANUMERIC"},
                {"name": "NAME", "type": "ALPHANUMERIC"},
            ]
        }
        w = CSVWriter(str(out))
        w.open(schema)
        w.write_batch([{"CUST-ID": "001", "FILLER": "xxx", "NAME": "Alice"}])
        w.close()

        rows = list(csv.reader(out.read_text(encoding="utf-8").splitlines()))
        assert "FILLER" not in rows[0]
        assert rows[0] == ["CUST_ID", "NAME"]

    def test_returns_writer_stats(self, tmp_path):
        out = tmp_path / "out.csv"
        w = CSVWriter(str(out))
        w.open(SIMPLE_SCHEMA)
        w.write_batch(RECORDS)
        stats = w.close()
        assert stats.records_written == 2
        assert stats.elapsed_sec >= 0

    def test_sanitized_name_lookup(self, tmp_path):
        """Records may have sanitized keys (underscores) already."""
        out = tmp_path / "out.csv"
        w = CSVWriter(str(out))
        w.open(SIMPLE_SCHEMA)
        # Keys already sanitized
        w.write_batch([{"CUST_ID": "001", "NAME": "Alice", "AMOUNT": 42}])
        w.close()

        rows = list(csv.reader(out.read_text(encoding="utf-8").splitlines()))
        assert rows[1][0] == "001"
