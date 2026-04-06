"""Tests for writers/base.py - sanitize_column_name and flatten_occurs."""
import pytest
from ztract.writers.base import sanitize_column_name, flatten_occurs


class TestSanitizeColumnName:
    def test_hyphen_to_underscore(self):
        assert sanitize_column_name("CUST-ID") == "CUST_ID"

    def test_multiple_hyphens(self):
        assert sanitize_column_name("FIRST-LAST-NAME") == "FIRST_LAST_NAME"

    def test_no_hyphens_unchanged(self):
        assert sanitize_column_name("CUSTNAME") == "CUSTNAME"

    def test_empty_string(self):
        assert sanitize_column_name("") == ""

    def test_already_underscored(self):
        assert sanitize_column_name("CUST_ID") == "CUST_ID"

    def test_mixed(self):
        assert sanitize_column_name("A-B_C-D") == "A_B_C_D"


class TestFlattenOccurs:
    def test_flat_record_passes_through(self):
        fields = [
            {"name": "CUST-ID", "type": "ALPHANUMERIC"},
            {"name": "NAME", "type": "ALPHANUMERIC"},
        ]
        record = {"CUST-ID": "001", "NAME": "Alice"}
        result = flatten_occurs(record, fields)
        assert result == {"CUST_ID": "001", "NAME": "Alice"}

    def test_occurs_flattened_with_indexed_names(self):
        fields = [
            {"name": "ITEM", "type": "ALPHANUMERIC", "occurs": 3,
             "children": [{"name": "CODE", "type": "ALPHANUMERIC"}]},
        ]
        record = {
            "ITEM": [{"CODE": "A"}, {"CODE": "B"}, {"CODE": "C"}]
        }
        result = flatten_occurs(record, fields)
        assert result == {
            "ITEM_1_CODE": "A",
            "ITEM_2_CODE": "B",
            "ITEM_3_CODE": "C",
        }

    def test_empty_occurs_array(self):
        fields = [
            {"name": "ITEM", "type": "ALPHANUMERIC", "occurs": 3,
             "children": [{"name": "CODE", "type": "ALPHANUMERIC"}]},
        ]
        record = {"ITEM": []}
        result = flatten_occurs(record, fields)
        assert result == {}

    def test_mixed_flat_and_occurs(self):
        fields = [
            {"name": "CUST-ID", "type": "ALPHANUMERIC"},
            {"name": "ITEM", "type": "ALPHANUMERIC", "occurs": 2,
             "children": [{"name": "VAL", "type": "NUMERIC"}]},
        ]
        record = {"CUST-ID": "001", "ITEM": [{"VAL": 10}, {"VAL": 20}]}
        result = flatten_occurs(record, fields)
        assert result["CUST_ID"] == "001"
        assert result["ITEM_1_VAL"] == 10
        assert result["ITEM_2_VAL"] == 20

    def test_non_occurs_names_sanitized(self):
        fields = [{"name": "FIRST-NAME", "type": "ALPHANUMERIC"}]
        record = {"FIRST-NAME": "Bob"}
        result = flatten_occurs(record, fields)
        assert "FIRST_NAME" in result
        assert result["FIRST_NAME"] == "Bob"
