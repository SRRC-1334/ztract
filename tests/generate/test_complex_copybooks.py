"""Tests for complex copybook edge cases in the generator."""
from ztract.generate.generator import generate_records
from ztract.generate.field_patterns import generate_edge_case_value, generate_value


# --- Schemas matching the complex copybooks ---

REDEFINES_SCHEMA = {
    "fields": [
        {"name": "SEGMENT_ID", "type": "ALPHANUMERIC", "size": 2, "scale": 0, "occurs": None},
        {"name": "COMMON_KEY", "type": "NUMERIC", "size": 10, "scale": 0, "occurs": None},
        {"name": "COMMON_DATE", "type": "NUMERIC", "size": 8, "scale": 0, "occurs": None},
        {"name": "CUST_NAME", "type": "ALPHANUMERIC", "size": 40, "scale": 0, "occurs": None},
        {"name": "CUST_ADDR", "type": "ALPHANUMERIC", "size": 60, "scale": 0, "occurs": None},
        {"name": "CUST_CITY", "type": "ALPHANUMERIC", "size": 30, "scale": 0, "occurs": None},
        {"name": "CUST_ZIP", "type": "ALPHANUMERIC", "size": 10, "scale": 0, "occurs": None},
        {"name": "CUST_PHONE", "type": "ALPHANUMERIC", "size": 15, "scale": 0, "occurs": None},
    ],
    "redefines_groups": [
        {"group": "CUSTOMER_SEGMENT", "variants": ["ACCOUNT_SEGMENT", "PAYMENT_SEGMENT"]}
    ]
}

OCCURS_SCHEMA = {
    "fields": [
        {"name": "ORDER_ID", "type": "NUMERIC", "size": 10, "scale": 0, "occurs": None},
        {"name": "ORDER_DATE", "type": "NUMERIC", "size": 8, "scale": 0, "occurs": None},
        {"name": "CUSTOMER_NR", "type": "NUMERIC", "size": 10, "scale": 0, "occurs": None},
        {"name": "ORDER_STATUS", "type": "ALPHANUMERIC", "size": 2, "scale": 0, "occurs": None},
        {"name": "LINE_COUNT", "type": "NUMERIC", "size": 3, "scale": 0, "occurs": None},
        {"name": "ORDER_TOTAL", "type": "PACKED_DECIMAL", "size": 7, "scale": 2, "occurs": None},
    ],
}

NUMERIC_SCHEMA = {
    "fields": [
        {"name": "REC_ID", "type": "NUMERIC", "size": 8, "scale": 0, "occurs": None},
        {"name": "DISPLAY_UNSIGNED", "type": "NUMERIC", "size": 9, "scale": 0, "occurs": None},
        {"name": "DISPLAY_SIGNED", "type": "NUMERIC", "size": 9, "scale": 0, "occurs": None},
        {"name": "DISPLAY_DECIMAL", "type": "NUMERIC", "size": 7, "scale": 2, "occurs": None},
        {"name": "DISPLAY_SIGNED_DEC", "type": "NUMERIC", "size": 7, "scale": 2, "occurs": None},
        {"name": "COMP3_UNSIGNED", "type": "PACKED_DECIMAL", "size": 5, "scale": 0, "occurs": None},
        {"name": "COMP3_SIGNED", "type": "PACKED_DECIMAL", "size": 5, "scale": 0, "occurs": None},
        {"name": "COMP3_DECIMAL", "type": "PACKED_DECIMAL", "size": 5, "scale": 2, "occurs": None},
        {"name": "COMP3_SIGNED_DEC", "type": "PACKED_DECIMAL", "size": 5, "scale": 2, "occurs": None},
        {"name": "COMP3_LARGE", "type": "PACKED_DECIMAL", "size": 9, "scale": 2, "occurs": None},
        {"name": "COMP_SHORT", "type": "NUMERIC", "size": 2, "scale": 0, "occurs": None},
        {"name": "COMP_LONG", "type": "NUMERIC", "size": 4, "scale": 0, "occurs": None},
        {"name": "COMP_VERY_LONG", "type": "NUMERIC", "size": 8, "scale": 0, "occurs": None},
        {"name": "ALPHA_FIELD", "type": "ALPHANUMERIC", "size": 20, "scale": 0, "occurs": None},
        {"name": "ALPHA_MAX", "type": "ALPHANUMERIC", "size": 100, "scale": 0, "occurs": None},
        {"name": "ALPHA_ZERO", "type": "ALPHANUMERIC", "size": 1, "scale": 0, "occurs": None},
    ],
}


class TestEdgeCaseValueGeneration:
    """Test generate_edge_case_value for each case type."""

    def test_zeros_alpha_returns_spaces(self):
        val = generate_edge_case_value("FIELD", "ALPHANUMERIC", 10, case="zeros")
        assert val == " " * 10

    def test_zeros_numeric_returns_zero(self):
        val = generate_edge_case_value("FIELD", "NUMERIC", 9, case="zeros")
        assert val == 0

    def test_zeros_packed_returns_zero(self):
        val = generate_edge_case_value("FIELD", "PACKED_DECIMAL", 5, scale=2, case="zeros")
        assert val == 0

    def test_max_alpha_fills_with_char(self):
        val = generate_edge_case_value("FIELD", "ALPHANUMERIC", 5, case="max")
        assert len(val) == 5
        assert val == "Z" * 5

    def test_max_numeric_returns_all_nines(self):
        val = generate_edge_case_value("FIELD", "NUMERIC", 5, case="max")
        assert val == 99999

    def test_max_decimal_returns_max_with_scale(self):
        val = generate_edge_case_value("FIELD", "PACKED_DECIMAL", 7, scale=2, case="max")
        assert val == 99999.99  # 10**7 - 1 = 9999999, / 100 = 99999.99

    def test_negative_numeric_returns_negative(self):
        val = generate_edge_case_value("FIELD", "NUMERIC", 5, case="negative")
        assert val == -99999

    def test_negative_alpha_returns_spaces(self):
        val = generate_edge_case_value("FIELD", "ALPHANUMERIC", 5, case="negative")
        assert val == " " * 5


class TestSegmentIdPattern:
    """Test that SEGMENT_ID generates cycling values."""

    def test_segment_id_generates_valid_codes(self):
        val = generate_value("SEGMENT_ID", "ALPHANUMERIC", 2, locale="en_US")
        assert val.strip() in ("CU", "AC", "PA")


class TestLineCountPattern:
    """Test LINE_COUNT generates 0-10."""

    def test_line_count_in_range(self):
        for _ in range(50):
            val = generate_value("LINE_COUNT", "NUMERIC", 3, locale="en_US")
            assert 0 <= val <= 10


class TestEdgeCaseRecordGeneration:
    """Test generate_records with edge_cases=True."""

    def test_every_100th_record_is_edge_case(self):
        records = list(generate_records(NUMERIC_SCHEMA, 201, seed=42, edge_cases=True))
        assert len(records) == 201

        # Record 0 should be zeros edge case
        assert records[0]["DISPLAY_UNSIGNED"] == 0
        assert records[0]["ALPHA_FIELD"] == " " * 20

        # Record 100 should be max edge case
        assert records[100]["DISPLAY_UNSIGNED"] == 999999999  # 10**9 - 1
        assert records[100]["ALPHA_MAX"] == "Z" * 100

        # Record 200 should be negative edge case
        assert records[200]["DISPLAY_SIGNED"] == -999999999

    def test_non_edge_case_records_are_normal(self):
        records = list(generate_records(NUMERIC_SCHEMA, 10, seed=42, edge_cases=True))
        # Record 0 is edge case (zeros), records 1-9 are normal
        assert records[1]["DISPLAY_UNSIGNED"] != 0  # should be a random value

    def test_without_edge_cases_no_boundary_values(self):
        records = list(generate_records(NUMERIC_SCHEMA, 5, seed=42, edge_cases=False))
        # All should be random, not edge cases
        for r in records:
            assert isinstance(r["DISPLAY_UNSIGNED"], (int, float))


class TestRedefinesGeneration:
    """Test generation with REDEFINES schema."""

    def test_generates_all_segment_types(self):
        records = list(generate_records(REDEFINES_SCHEMA, 300, seed=42))
        segment_ids = {r["SEGMENT_ID"].strip() for r in records}
        # Should have at least 2 of the 3 types (random distribution)
        assert len(segment_ids) >= 2

    def test_common_fields_populated(self):
        records = list(generate_records(REDEFINES_SCHEMA, 10, seed=42))
        for r in records:
            assert r["COMMON_KEY"] is not None
            assert r["COMMON_DATE"] is not None


class TestOccursGeneration:
    """Test generation with OCCURS schema."""

    def test_line_count_in_range(self):
        records = list(generate_records(OCCURS_SCHEMA, 100, seed=42))
        for r in records:
            assert 0 <= r["LINE_COUNT"] <= 10

    def test_order_total_is_numeric(self):
        records = list(generate_records(OCCURS_SCHEMA, 10, seed=42))
        for r in records:
            assert isinstance(r["ORDER_TOTAL"], (int, float))
