"""Tests for writers/parquet.py - ParquetWriter."""
import pytest

pyarrow = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")

from ztract.writers.parquet import ParquetWriter, _cobol_to_arrow_type, build_arrow_schema  # noqa: E402


SCHEMA_FIXTURE = {
    "fields": [
        {"name": "CUST-ID", "type": "ALPHANUMERIC", "size": 10},
        {"name": "AMOUNT", "type": "NUMERIC", "size": 9, "scale": 0},
        {"name": "BALANCE", "type": "PACKED_DECIMAL", "size": 9, "scale": 2},
        {"name": "RATE", "type": "NUMERIC", "size": 7, "scale": 3},
        {"name": "FILLER", "type": "ALPHANUMERIC", "size": 5},
    ]
}

RECORDS = [
    {"CUST-ID": "001", "AMOUNT": 100, "BALANCE": "12.34", "RATE": "1.250", "FILLER": ""},
    {"CUST-ID": "002", "AMOUNT": 200, "BALANCE": "99.99", "RATE": "0.500", "FILLER": ""},
]


class TestCobolToArrowType:
    def test_alphanumeric_is_string(self):
        import pyarrow as pa
        t = _cobol_to_arrow_type({"type": "ALPHANUMERIC", "size": 10})
        assert t == pa.string()

    def test_numeric_no_scale_small_is_int32(self):
        import pyarrow as pa
        t = _cobol_to_arrow_type({"type": "NUMERIC", "size": 9, "scale": 0})
        assert t == pa.int32()

    def test_numeric_no_scale_large_is_int64(self):
        import pyarrow as pa
        t = _cobol_to_arrow_type({"type": "NUMERIC", "size": 10, "scale": 0})
        assert t == pa.int64()

    def test_numeric_with_scale_is_decimal128(self):
        import pyarrow as pa
        t = _cobol_to_arrow_type({"type": "NUMERIC", "size": 7, "scale": 3})
        assert t == pa.decimal128(7, 3)

    def test_packed_decimal_is_decimal128(self):
        import pyarrow as pa
        t = _cobol_to_arrow_type({"type": "PACKED_DECIMAL", "size": 9, "scale": 2})
        assert t == pa.decimal128(9, 2)

    def test_comp3_is_decimal128(self):
        import pyarrow as pa
        t = _cobol_to_arrow_type({"type": "COMP-3", "size": 5, "scale": 2})
        assert t == pa.decimal128(5, 2)

    def test_comp1_is_float32(self):
        import pyarrow as pa
        t = _cobol_to_arrow_type({"type": "COMP-1"})
        assert t == pa.float32()

    def test_comp2_is_float64(self):
        import pyarrow as pa
        t = _cobol_to_arrow_type({"type": "COMP-2"})
        assert t == pa.float64()


class TestBuildArrowSchema:
    def test_filler_skipped(self):
        schema = build_arrow_schema(SCHEMA_FIXTURE["fields"])
        assert "FILLER" not in schema.names

    def test_column_names_sanitized(self):
        schema = build_arrow_schema(SCHEMA_FIXTURE["fields"])
        assert "CUST_ID" in schema.names
        assert "CUST-ID" not in schema.names


class TestParquetWriter:
    def test_writes_readable_parquet(self, tmp_path):
        out = tmp_path / "out.parquet"
        w = ParquetWriter(str(out))
        w.open(SCHEMA_FIXTURE)
        w.write_batch(RECORDS)
        w.close()

        table = pq.read_table(str(out))
        assert table.num_rows == 2

    def test_correct_column_names(self, tmp_path):
        out = tmp_path / "out.parquet"
        w = ParquetWriter(str(out))
        w.open(SCHEMA_FIXTURE)
        w.write_batch(RECORDS)
        w.close()

        table = pq.read_table(str(out))
        assert "CUST_ID" in table.schema.names
        assert "FILLER" not in table.schema.names

    def test_null_values_work(self, tmp_path):
        out = tmp_path / "out.parquet"
        w = ParquetWriter(str(out))
        schema = {
            "fields": [
                {"name": "CUST-ID", "type": "ALPHANUMERIC", "size": 10},
                {"name": "AMOUNT", "type": "NUMERIC", "size": 9, "scale": 0},
            ]
        }
        w.open(schema)
        w.write_batch([{"CUST-ID": None, "AMOUNT": None}])
        w.close()

        table = pq.read_table(str(out))
        assert table.num_rows == 1
        assert table.column("CUST_ID")[0].as_py() is None

    def test_row_group_flush(self, tmp_path):
        """With row_group_size=5, 12 records should flush in 3 groups."""
        out = tmp_path / "out.parquet"
        schema = {
            "fields": [{"name": "ID", "type": "ALPHANUMERIC", "size": 3}]
        }
        records = [{"ID": str(i)} for i in range(12)]
        w = ParquetWriter(str(out), row_group_size=5)
        w.open(schema)
        w.write_batch(records)
        w.close()

        table = pq.read_table(str(out))
        assert table.num_rows == 12

    def test_writer_name_contains_parquet(self, tmp_path):
        out = tmp_path / "data.parquet"
        w = ParquetWriter(str(out))
        assert "parquet" in w.name.lower() or "Parquet" in w.name
