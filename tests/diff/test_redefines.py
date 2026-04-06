"""Tests for ztract.diff.redefines — REDEFINES hex diff."""
from __future__ import annotations

import pytest

from ztract.diff.redefines import RedefinesComparison, RedefinesHandler


# ---------------------------------------------------------------------------
# Schema helpers
# ---------------------------------------------------------------------------

def _make_schema(groups: list[dict] | None = None, fields: list[dict] | None = None) -> dict:
    """Build a minimal schema dict as returned by bridge.get_schema()."""
    return {
        "redefines_groups": groups or [],
        "fields": fields or [],
    }


def _schema_with_one_group() -> dict:
    """Schema with a single REDEFINES group STATIC-DETAILS spanning bytes 100-150."""
    return _make_schema(
        groups=[
            {
                "name": "STATIC-DETAILS",
                "offset": 100,
                "length": 50,
                "variants": ["CONTACTS"],
            }
        ]
    )


# ---------------------------------------------------------------------------
# TestRedefinesComparison dataclass
# ---------------------------------------------------------------------------

class TestRedefinesComparisonDataclass:
    def test_fields_present(self):
        cmp = RedefinesComparison(
            group_name="GRP",
            offset=0,
            length=4,
            before_hex="C1C2C3C4",
            after_hex="C1C2C3C4",
            differs=False,
            variants=["VAR1"],
        )
        assert cmp.group_name == "GRP"
        assert cmp.offset == 0
        assert cmp.length == 4
        assert cmp.before_hex == "C1C2C3C4"
        assert cmp.after_hex == "C1C2C3C4"
        assert cmp.differs is False
        assert cmp.variants == ["VAR1"]


# ---------------------------------------------------------------------------
# TestRedefinesHandlerInit
# ---------------------------------------------------------------------------

class TestRedefinesHandlerInit:
    def test_init_with_empty_schema(self):
        handler = RedefinesHandler(_make_schema())
        assert handler._groups == {}

    def test_init_builds_group_map(self):
        schema = _schema_with_one_group()
        handler = RedefinesHandler(schema)
        assert "STATIC-DETAILS" in handler._groups
        grp = handler._groups["STATIC-DETAILS"]
        assert grp["offset"] == 100
        assert grp["length"] == 50
        assert grp["variants"] == ["CONTACTS"]

    def test_init_with_multiple_groups(self):
        schema = _make_schema(
            groups=[
                {"name": "GRP-A", "offset": 0, "length": 10, "variants": ["V1", "V2"]},
                {"name": "GRP-B", "offset": 10, "length": 20, "variants": ["V3"]},
            ]
        )
        handler = RedefinesHandler(schema)
        assert "GRP-A" in handler._groups
        assert "GRP-B" in handler._groups


# ---------------------------------------------------------------------------
# TestCompare
# ---------------------------------------------------------------------------

class TestCompare:
    def _handler(self) -> RedefinesHandler:
        return RedefinesHandler(_schema_with_one_group())

    def test_identical_bytes_differs_false(self):
        handler = self._handler()
        data = bytes(range(200))
        result = handler.compare(data, data, "STATIC-DETAILS")
        assert result.differs is False
        assert result.group_name == "STATIC-DETAILS"

    def test_identical_bytes_correct_hex(self):
        handler = self._handler()
        before = bytes(range(200))
        result = handler.compare(before, before, "STATIC-DETAILS")
        # bytes 100-150 of range(200) are 100, 101, ..., 149
        expected_hex = bytes(range(100, 150)).hex().upper()
        assert result.before_hex == expected_hex
        assert result.after_hex == expected_hex

    def test_different_bytes_differs_true(self):
        handler = self._handler()
        before = bytes(range(200))
        after = bytearray(range(200))
        after[125] = 0xFF  # change a byte in the group range (100-150)
        result = handler.compare(before, bytes(after), "STATIC-DETAILS")
        assert result.differs is True

    def test_different_bytes_correct_hex_strings(self):
        handler = self._handler()
        before = bytes(range(200))
        after = bytearray(range(200))
        after[125] = 0xE4
        result = handler.compare(before, bytes(after), "STATIC-DETAILS")
        before_slice = bytes(range(100, 150)).hex().upper()
        after_slice_bytes = bytearray(range(100, 150))
        after_slice_bytes[25] = 0xE4  # index 125 - 100 = 25 within slice
        assert result.before_hex == before_slice
        assert result.after_hex == bytes(after_slice_bytes).hex().upper()

    def test_compare_returns_correct_offset_and_length(self):
        handler = self._handler()
        data = bytes(200)
        result = handler.compare(data, data, "STATIC-DETAILS")
        assert result.offset == 100
        assert result.length == 50

    def test_compare_includes_variants(self):
        handler = self._handler()
        data = bytes(200)
        result = handler.compare(data, data, "STATIC-DETAILS")
        assert result.variants == ["CONTACTS"]

    def test_compare_unknown_group_raises(self):
        handler = self._handler()
        data = bytes(200)
        with pytest.raises(KeyError):
            handler.compare(data, data, "NONEXISTENT-GROUP")

    def test_change_outside_group_range_not_detected(self):
        handler = self._handler()
        before = bytes(range(200))
        after = bytearray(range(200))
        after[50] = 0xFF  # byte 50 is outside the group (100-150)
        result = handler.compare(before, bytes(after), "STATIC-DETAILS")
        assert result.differs is False


# ---------------------------------------------------------------------------
# TestCompareAll
# ---------------------------------------------------------------------------

class TestCompareAll:
    def test_no_groups_returns_empty_list(self):
        handler = RedefinesHandler(_make_schema())
        data = bytes(10)
        result = handler.compare_all(data, data)
        assert result == []

    def test_one_group_no_diff_returns_non_differing(self):
        handler = RedefinesHandler(_schema_with_one_group())
        data = bytes(200)
        results = handler.compare_all(data, data)
        assert len(results) == 1
        assert results[0].differs is False

    def test_one_group_with_diff_is_in_results(self):
        handler = RedefinesHandler(_schema_with_one_group())
        before = bytes(200)
        after = bytearray(200)
        after[110] = 0xAA
        results = handler.compare_all(before, bytes(after))
        assert len(results) == 1
        assert results[0].differs is True
        assert results[0].group_name == "STATIC-DETAILS"

    def test_multiple_groups_all_returned(self):
        schema = _make_schema(
            groups=[
                {"name": "GRP-A", "offset": 0, "length": 10, "variants": ["V1"]},
                {"name": "GRP-B", "offset": 10, "length": 10, "variants": ["V2"]},
            ]
        )
        handler = RedefinesHandler(schema)
        data = bytes(30)
        results = handler.compare_all(data, data)
        assert len(results) == 2

    def test_multiple_groups_only_differing_detected(self):
        schema = _make_schema(
            groups=[
                {"name": "GRP-A", "offset": 0, "length": 10, "variants": ["V1"]},
                {"name": "GRP-B", "offset": 10, "length": 10, "variants": ["V2"]},
            ]
        )
        handler = RedefinesHandler(schema)
        before = bytes(30)
        after = bytearray(30)
        after[5] = 0xBB  # inside GRP-A
        results = handler.compare_all(before, bytes(after))
        differing = [r for r in results if r.differs]
        non_differing = [r for r in results if not r.differs]
        assert len(differing) == 1
        assert differing[0].group_name == "GRP-A"
        assert len(non_differing) == 1
        assert non_differing[0].group_name == "GRP-B"


# ---------------------------------------------------------------------------
# TestFormatHexDiff
# ---------------------------------------------------------------------------

class TestFormatHexDiff:
    def _handler(self) -> RedefinesHandler:
        return RedefinesHandler(_schema_with_one_group())

    def _make_comparison(
        self,
        before_hex: str = "C1C2C3D4D5",
        after_hex: str = "C1C2C3E4E5",
        differs: bool = True,
    ) -> RedefinesComparison:
        return RedefinesComparison(
            group_name="STATIC-DETAILS",
            offset=100,
            length=5,
            before_hex=before_hex,
            after_hex=after_hex,
            differs=differs,
            variants=["CONTACTS"],
        )

    def test_format_contains_group_name(self):
        handler = self._handler()
        cmp = self._make_comparison()
        output = handler.format_hex_diff(cmp)
        assert "STATIC-DETAILS" in output

    def test_format_contains_variant_names(self):
        handler = self._handler()
        cmp = self._make_comparison()
        output = handler.format_hex_diff(cmp)
        assert "CONTACTS" in output

    def test_format_contains_offset_range(self):
        handler = self._handler()
        cmp = self._make_comparison()
        output = handler.format_hex_diff(cmp)
        assert "100" in output
        assert "105" in output  # 100 + 5 = 105

    def test_format_contains_before_hex(self):
        handler = self._handler()
        cmp = self._make_comparison(before_hex="C1C2C3D4D5")
        output = handler.format_hex_diff(cmp)
        assert "C1C2C3D4D5" in output

    def test_format_contains_after_hex(self):
        handler = self._handler()
        cmp = self._make_comparison(after_hex="C1C2C3E4E5")
        output = handler.format_hex_diff(cmp)
        assert "C1C2C3E4E5" in output

    def test_format_contains_caret_markers_for_differing_bytes(self):
        handler = self._handler()
        # C1C2C3D4D5 vs C1C2C3E4E5 — bytes 4 and 5 (D4 vs E4, D5 vs E5) differ
        cmp = self._make_comparison(
            before_hex="C1C2C3D4D5",
            after_hex="C1C2C3E4E5",
        )
        output = handler.format_hex_diff(cmp)
        assert "^" in output

    def test_format_no_carets_for_identical(self):
        handler = self._handler()
        cmp = self._make_comparison(
            before_hex="C1C2C3C4C5",
            after_hex="C1C2C3C4C5",
            differs=False,
        )
        output = handler.format_hex_diff(cmp)
        assert "^" not in output

    def test_format_is_multiline(self):
        handler = self._handler()
        cmp = self._make_comparison()
        output = handler.format_hex_diff(cmp)
        assert "\n" in output
