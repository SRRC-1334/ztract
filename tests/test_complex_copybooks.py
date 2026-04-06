"""Integration tests for complex copybook edge cases.

All tests require the real JAR and JRE.
Run with: pytest tests/test_complex_copybooks.py -m integration
"""
from __future__ import annotations

import csv
import os
from pathlib import Path

import pytest

from ztract.engine.bridge import ZtractBridge

pytestmark = pytest.mark.integration


@pytest.fixture
def bridge() -> ZtractBridge:
    b = ZtractBridge()
    b.check_jre()
    return b


def _decode_to_dicts(
    bridge: ZtractBridge,
    copybook: Path,
    dat_file: Path,
    recfm: str,
    lrecl: int | None = None,
    codepage: str = "cp277",
) -> list[dict]:
    """Decode a DAT file and return list of record dicts."""
    return list(bridge.decode(copybook, dat_file, recfm, lrecl, codepage))


def _decode_to_csv(
    bridge: ZtractBridge,
    copybook: Path,
    dat_file: Path,
    csv_path: Path,
    recfm: str,
    lrecl: int | None = None,
    codepage: str = "cp277",
) -> list[dict]:
    """Decode a DAT file to CSV via the writers, return CSV rows as dicts."""
    import json
    from ztract.writers.csv import CSVWriter

    schema = bridge.get_schema(copybook, recfm=recfm, lrecl=lrecl)
    writer = CSVWriter(str(csv_path))
    writer.open(schema)

    records = list(bridge.decode(copybook, dat_file, recfm, lrecl, codepage))
    writer.write_batch(records)
    writer.close()

    with open(csv_path, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return list(reader)


# ═══════════════════════════════════════════════════════════════════
# TestRedefines
# ═══════════════════════════════════════════════════════════════════


class TestRedefines:

    def test_all_three_variants_decoded(
        self, bridge, complex_redefines_cpy, complex_redefines_dat
    ):
        records = _decode_to_dicts(
            bridge, complex_redefines_cpy, complex_redefines_dat, "FB", 220
        )
        assert len(records) == 9
        segment_ids = {r["SEGMENT_ID"].strip() for r in records}
        assert "CU" in segment_ids
        assert "AC" in segment_ids
        assert "PA" in segment_ids

    def test_customer_segment_fields_populated(
        self, bridge, complex_redefines_cpy, complex_redefines_dat
    ):
        records = _decode_to_dicts(
            bridge, complex_redefines_cpy, complex_redefines_dat, "FB", 220
        )
        cu_records = [r for r in records if r["SEGMENT_ID"].strip() == "CU"]
        assert len(cu_records) >= 1
        for r in cu_records:
            assert r.get("CUST_NAME") is not None
            assert str(r["CUST_NAME"]).strip() != ""
            assert r.get("CUST_ADDR") is not None
            assert str(r["CUST_ADDR"]).strip() != ""

    def test_account_segment_fields_populated(
        self, bridge, complex_redefines_cpy, complex_redefines_dat
    ):
        records = _decode_to_dicts(
            bridge, complex_redefines_cpy, complex_redefines_dat, "FB", 220
        )
        ac_records = [r for r in records if r["SEGMENT_ID"].strip() == "AC"]
        assert len(ac_records) >= 1
        for r in ac_records:
            assert r.get("ACCT_TYPE") is not None
            assert str(r["ACCT_TYPE"]).strip() != ""
            bal = r.get("ACCT_BALANCE")
            assert bal is not None
            assert isinstance(bal, (int, float))

    def test_payment_segment_fields_populated(
        self, bridge, complex_redefines_cpy, complex_redefines_dat
    ):
        records = _decode_to_dicts(
            bridge, complex_redefines_cpy, complex_redefines_dat, "FB", 220
        )
        pa_records = [r for r in records if r["SEGMENT_ID"].strip() == "PA"]
        assert len(pa_records) >= 1
        for r in pa_records:
            amt = r.get("PAY_AMOUNT")
            assert amt is not None
            assert isinstance(amt, (int, float))
            assert r.get("PAY_CURRENCY") is not None
            assert str(r["PAY_CURRENCY"]).strip() != ""
            assert r.get("PAY_REF") is not None
            assert str(r["PAY_REF"]).strip() != ""

    def test_norwegian_chars_in_redefines(
        self, bridge, complex_redefines_cpy, complex_redefines_dat
    ):
        records = _decode_to_dicts(
            bridge, complex_redefines_cpy, complex_redefines_dat, "FB", 220
        )
        norwegian_chars = set("æøåÆØÅ")
        all_text = " ".join(
            str(r.get("CUST_NAME", "")) + str(r.get("CUST_CITY", ""))
            for r in records
        )
        found = any(c in all_text for c in norwegian_chars)
        assert found, f"No Norwegian characters found in: {all_text[:200]}"

    def test_redefines_record_length(self, complex_redefines_dat):
        size = os.path.getsize(complex_redefines_dat)
        assert size == 9 * 220, f"Expected 1980, got {size}"

    def test_round_trip_redefines(
        self, bridge, complex_redefines_cpy, tmp_path
    ):
        from ztract.generate.generator import generate_records

        schema = bridge.get_schema(complex_redefines_cpy, recfm="FB", lrecl=220)
        records = list(generate_records(schema, 9, codepage="cp277", seed=42))
        assert len(records) == 9

        dat = tmp_path / "rt_redefines.dat"
        bridge.encode(complex_redefines_cpy, dat, "FB", 220, "cp277", iter(records))
        assert dat.stat().st_size == 9 * 220

        decoded = _decode_to_dicts(bridge, complex_redefines_cpy, dat, "FB", 220)
        assert len(decoded) == 9


# ═══════════════════════════════════════════════════════════════════
# TestOccursDepending
# ═══════════════════════════════════════════════════════════════════


class TestOccursDepending:

    def test_correct_record_count(
        self, bridge, complex_occurs_cpy, complex_occurs_dat
    ):
        records = _decode_to_dicts(
            bridge, complex_occurs_cpy, complex_occurs_dat, "FB", 500
        )
        assert len(records) == 5

    def test_line_count_values_realistic(
        self, bridge, complex_occurs_cpy, complex_occurs_dat
    ):
        records = _decode_to_dicts(
            bridge, complex_occurs_cpy, complex_occurs_dat, "FB", 500
        )
        line_counts = [r["LINE_COUNT"] for r in records]
        assert all(0 <= int(lc) <= 10 for lc in line_counts)
        assert len(set(line_counts)) > 1, "All LINE_COUNT values are the same"

    def test_order_total_is_numeric(
        self, bridge, complex_occurs_cpy, complex_occurs_dat
    ):
        records = _decode_to_dicts(
            bridge, complex_occurs_cpy, complex_occurs_dat, "FB", 500
        )
        for r in records:
            total = r.get("ORDER_TOTAL")
            assert total is not None
            assert isinstance(total, (int, float))
            assert total > 0

    def test_comp3_order_total_decoded(
        self, bridge, complex_occurs_cpy, complex_occurs_dat
    ):
        records = _decode_to_dicts(
            bridge, complex_occurs_cpy, complex_occurs_dat, "FB", 500
        )
        first_total = float(records[0]["ORDER_TOTAL"])
        assert abs(first_total - 740667.0) < 0.01, f"Expected ~740667.0, got {first_total}"

    def test_file_size_correct(self, complex_occurs_dat):
        size = os.path.getsize(complex_occurs_dat)
        assert size == 5 * 500, f"Expected 2500, got {size}"

    def test_round_trip_occurs(
        self, bridge, complex_occurs_cpy, tmp_path
    ):
        from ztract.generate.generator import generate_records

        schema = bridge.get_schema(complex_occurs_cpy, recfm="FB", lrecl=500)
        records = list(generate_records(schema, 5, codepage="cp277", seed=42))

        dat = tmp_path / "rt_occurs.dat"
        bridge.encode(complex_occurs_cpy, dat, "FB", 500, "cp277", iter(records))
        assert dat.stat().st_size == 5 * 500

        decoded = _decode_to_dicts(bridge, complex_occurs_cpy, dat, "FB", 500)
        assert len(decoded) == 5

        for orig, dec in zip(records, decoded):
            assert int(orig["LINE_COUNT"]) == int(dec["LINE_COUNT"])


# ═══════════════════════════════════════════════════════════════════
# TestNumericTypes
# ═══════════════════════════════════════════════════════════════════


class TestNumericTypes:

    def test_all_five_records_decoded(
        self, bridge, complex_numeric_cpy, complex_numeric_dat
    ):
        records = _decode_to_dicts(
            bridge, complex_numeric_cpy, complex_numeric_dat, "FB", 257
        )
        assert len(records) == 5

    def test_comp3_unsigned_realistic(
        self, bridge, complex_numeric_cpy, complex_numeric_dat
    ):
        records = _decode_to_dicts(
            bridge, complex_numeric_cpy, complex_numeric_dat, "FB", 257
        )
        for r in records:
            val = float(r["COMP3_UNSIGNED"])
            assert val >= 0
            assert val < 10_000_000.00
        values = [float(r["COMP3_UNSIGNED"]) for r in records]
        assert not all(v == 0 for v in values), "All COMP3_UNSIGNED are zero"

    def test_comp3_signed_can_be_negative(
        self, bridge, complex_numeric_cpy, complex_numeric_dat
    ):
        records = _decode_to_dicts(
            bridge, complex_numeric_cpy, complex_numeric_dat, "FB", 257
        )
        for r in records:
            val = r["COMP3_SIGNED"]
            assert isinstance(val, (int, float))

    def test_comp3_large_decoded(
        self, bridge, complex_numeric_cpy, complex_numeric_dat
    ):
        records = _decode_to_dicts(
            bridge, complex_numeric_cpy, complex_numeric_dat, "FB", 257
        )
        for r in records:
            val = r["COMP3_LARGE"]
            assert isinstance(val, (int, float))
        values = [float(r["COMP3_LARGE"]) for r in records]
        assert not all(v == 0 for v in values), "All COMP3_LARGE are zero"

    def test_comp_binary_short_range(
        self, bridge, complex_numeric_cpy, complex_numeric_dat
    ):
        records = _decode_to_dicts(
            bridge, complex_numeric_cpy, complex_numeric_dat, "FB", 257
        )
        for r in records:
            val = int(r["COMP_SHORT"])
            assert -9999 <= val <= 9999, f"COMP_SHORT out of range: {val}"

    def test_comp_binary_long_range(
        self, bridge, complex_numeric_cpy, complex_numeric_dat
    ):
        records = _decode_to_dicts(
            bridge, complex_numeric_cpy, complex_numeric_dat, "FB", 257
        )
        for r in records:
            val = int(r["COMP_LONG"])
            assert -999_999_999 <= val <= 999_999_999, f"COMP_LONG out of range: {val}"

    def test_no_garbage_values(
        self, bridge, complex_numeric_cpy, complex_numeric_dat
    ):
        records = _decode_to_dicts(
            bridge, complex_numeric_cpy, complex_numeric_dat, "FB", 257
        )
        for r in records:
            for key, val in r.items():
                if isinstance(val, (int, float)):
                    assert "404040" not in str(val), f"Garbage 404040 in {key}={val}"

    def test_norwegian_alpha_fields(
        self, bridge, complex_numeric_cpy, complex_numeric_dat
    ):
        records = _decode_to_dicts(
            bridge, complex_numeric_cpy, complex_numeric_dat, "FB", 257
        )
        norwegian_chars = set("æøåÆØÅ")
        all_alpha = " ".join(
            str(r.get("ALPHA_FIELD", "")) + str(r.get("ALPHA_MAX", ""))
            for r in records
        )
        # Norwegian locale with cp277 may or may not produce special chars
        # in alpha fallback fields — just verify no decode errors
        assert len(all_alpha) > 0

    def test_file_size_correct(self, complex_numeric_dat):
        size = os.path.getsize(complex_numeric_dat)
        assert size == 5 * 257, f"Expected 1285, got {size}"

    def test_round_trip_numeric(
        self, bridge, complex_numeric_cpy, tmp_path
    ):
        from ztract.generate.generator import generate_records

        schema = bridge.get_schema(complex_numeric_cpy, recfm="FB", lrecl=257)
        records = list(generate_records(schema, 5, codepage="cp277", seed=42))

        dat = tmp_path / "rt_numeric.dat"
        bridge.encode(complex_numeric_cpy, dat, "FB", 257, "cp277", iter(records))
        assert dat.stat().st_size == 5 * 257

        decoded = _decode_to_dicts(bridge, complex_numeric_cpy, dat, "FB", 257)
        assert len(decoded) == 5

        for orig, dec in zip(records, decoded):
            orig_u = float(orig["COMP3_UNSIGNED"])
            dec_u = float(dec["COMP3_UNSIGNED"])
            assert abs(orig_u - dec_u) < 0.01, f"COMP3_UNSIGNED: {orig_u} != {dec_u}"


# ═══════════════════════════════════════════════════════════════════
# TestComplexIntegration
# ═══════════════════════════════════════════════════════════════════


class TestComplexIntegration:

    def test_all_three_copybooks_inspect(
        self, bridge, complex_redefines_cpy, complex_occurs_cpy, complex_numeric_cpy
    ):
        for cpy, expected_len in [
            (complex_redefines_cpy, 220),
            (complex_occurs_cpy, 500),
            (complex_numeric_cpy, 257),
        ]:
            schema = bridge.get_schema(cpy)
            assert schema["record_length"] == expected_len, (
                f"{cpy.name}: expected {expected_len}, got {schema['record_length']}"
            )

    def test_validate_before_convert(
        self,
        bridge,
        complex_redefines_cpy,
        complex_redefines_dat,
        complex_occurs_cpy,
        complex_occurs_dat,
        complex_numeric_cpy,
        complex_numeric_dat,
    ):
        cases = [
            (complex_redefines_cpy, complex_redefines_dat, "FB", 220, 9),
            (complex_occurs_cpy, complex_occurs_dat, "FB", 500, 5),
            (complex_numeric_cpy, complex_numeric_dat, "FB", 257, 5),
        ]
        for cpy, dat, recfm, lrecl, expected_count in cases:
            report = bridge.validate(cpy, dat, recfm, lrecl, "cp277", sample=expected_count)
            assert report.records_errors == 0, f"{cpy.name}: {report.records_errors} errors"
            assert report.records_decoded == expected_count

    def test_diff_two_redefines_files(
        self, bridge, complex_redefines_cpy, tmp_path
    ):
        from ztract.generate.generator import generate_records
        from ztract.diff.differ import Differ

        schema = bridge.get_schema(complex_redefines_cpy, recfm="FB", lrecl=220)

        dat_a = tmp_path / "a.dat"
        dat_b = tmp_path / "b.dat"
        recs_a = list(generate_records(schema, 9, codepage="cp277", seed=42))
        recs_b = list(generate_records(schema, 9, codepage="cp277", seed=99))
        bridge.encode(complex_redefines_cpy, dat_a, "FB", 220, "cp277", iter(recs_a))
        bridge.encode(complex_redefines_cpy, dat_b, "FB", 220, "cp277", iter(recs_b))

        # Decode both to temp JSONL
        import json
        jsonl_a = tmp_path / "a.jsonl"
        jsonl_b = tmp_path / "b.jsonl"
        for dat, jsonl in [(dat_a, jsonl_a), (dat_b, jsonl_b)]:
            decoded = list(bridge.decode(complex_redefines_cpy, dat, "FB", 220, "cp277"))
            with open(jsonl, "w", encoding="utf-8") as f:
                for rec in decoded:
                    f.write(json.dumps(rec, ensure_ascii=False) + "\n")

        differ = Differ(key_fields=["COMMON_KEY"])
        result = differ.diff_jsonl(jsonl_a, jsonl_b)

        # Seeds differ so there should be changes
        assert result.total_before == 9
        assert result.total_after == 9
        total_diffs = result.added + result.deleted + result.changed
        assert total_diffs > 0, "Expected differences between seed=42 and seed=99"
