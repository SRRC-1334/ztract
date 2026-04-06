"""Tests for writers/jsonl.py - JSONLWriter."""
import json


from ztract.writers.jsonl import JSONLWriter


SIMPLE_SCHEMA = {
    "fields": [
        {"name": "CUST-ID", "type": "ALPHANUMERIC"},
        {"name": "NAME", "type": "ALPHANUMERIC"},
    ]
}


class TestJSONLWriter:
    def test_one_json_per_line(self, tmp_path):
        out = tmp_path / "out.jsonl"
        w = JSONLWriter(str(out))
        w.open(SIMPLE_SCHEMA)
        w.write_batch([
            {"CUST-ID": "001", "NAME": "Alice"},
            {"CUST-ID": "002", "NAME": "Bob"},
        ])
        w.close()

        lines = out.read_text(encoding="utf-8").strip().splitlines()
        assert len(lines) == 2
        r1 = json.loads(lines[0])
        r2 = json.loads(lines[1])
        assert r1["CUST-ID"] == "001"
        assert r2["NAME"] == "Bob"

    def test_null_as_json_null(self, tmp_path):
        out = tmp_path / "out.jsonl"
        w = JSONLWriter(str(out))
        w.open(SIMPLE_SCHEMA)
        w.write_batch([{"CUST-ID": None, "NAME": "Alice"}])
        w.close()

        line = out.read_text(encoding="utf-8").strip()
        record = json.loads(line)
        assert record["CUST-ID"] is None

    def test_norwegian_chars_not_escaped(self, tmp_path):
        out = tmp_path / "out.jsonl"
        w = JSONLWriter(str(out))
        w.open({"fields": [{"name": "NAME", "type": "ALPHANUMERIC"}]})
        w.write_batch([{"NAME": "Ærlig Øst Åpent"}])
        w.close()

        content = out.read_text(encoding="utf-8")
        # Characters must appear literally, not as \u escapes
        assert "Ærlig Øst Åpent" in content
        assert r"\u" not in content

    def test_returns_writer_stats(self, tmp_path):
        out = tmp_path / "out.jsonl"
        w = JSONLWriter(str(out))
        w.open(SIMPLE_SCHEMA)
        w.write_batch([{"CUST-ID": "001", "NAME": "Alice"}])
        stats = w.close()
        assert stats.records_written == 1

    def test_writer_name_contains_jsonl(self, tmp_path):
        out = tmp_path / "data.jsonl"
        w = JSONLWriter(str(out))
        assert "jsonl" in w.name.lower() or "JSONL" in w.name

    def test_multiple_batches(self, tmp_path):
        out = tmp_path / "out.jsonl"
        w = JSONLWriter(str(out))
        w.open(SIMPLE_SCHEMA)
        w.write_batch([{"CUST-ID": "001", "NAME": "Alice"}])
        w.write_batch([{"CUST-ID": "002", "NAME": "Bob"}])
        w.close()

        lines = out.read_text(encoding="utf-8").strip().splitlines()
        assert len(lines) == 2
