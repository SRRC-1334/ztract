"""Tests for generate/field_patterns.py and generate/generator.py.

Written in TDD style — all assertions target public behaviour only.
No network I/O, no subprocess calls.
"""
from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from ztract.generate.field_patterns import generate_value, get_generator
from ztract.generate.generator import generate_records


# ---------------------------------------------------------------------------
# Helpers / fixtures
# ---------------------------------------------------------------------------

def _alpha(name: str, size: int = 20) -> dict:
    return {"name": name, "type": "ALPHANUMERIC", "size": size}


def _numeric(name: str, size: int = 9, scale: int = 0) -> dict:
    return {"name": name, "type": "NUMERIC", "size": size, "scale": scale}


SIMPLE_SCHEMA = {
    "fields": [
        _alpha("CUST-NAME", 30),
        _alpha("ADDR-LINE", 40),
        _numeric("CUST-ID", 6),
        _numeric("TXN-DATE", 8),
        _numeric("TXN-AMT", 11, scale=2),
        _alpha("FILLER", 4),        # must be skipped
    ]
}


# ---------------------------------------------------------------------------
# TestGetGenerator
# ---------------------------------------------------------------------------

class TestGetGenerator:
    """get_generator() returns a callable for known patterns, None otherwise."""

    def test_name_field_returns_generator(self):
        gen = get_generator("CUST-NAME", "ALPHANUMERIC", 30)
        assert gen is not None

    def test_navn_field_returns_generator(self):
        gen = get_generator("NAVN", "ALPHANUMERIC", 20)
        assert gen is not None

    def test_amt_field_returns_numeric_generator(self):
        gen = get_generator("TXN-AMT", "NUMERIC", 11)
        assert gen is not None

    def test_amount_field_returns_numeric_generator(self):
        gen = get_generator("AMOUNT", "NUMERIC", 9)
        assert gen is not None

    def test_date_field_returns_numeric_generator(self):
        gen = get_generator("TXN-DATE", "NUMERIC", 8)
        assert gen is not None

    def test_dato_field_returns_numeric_generator(self):
        gen = get_generator("DATO", "NUMERIC", 8)
        assert gen is not None

    def test_id_field_returns_numeric_generator(self):
        gen = get_generator("CUST-ID", "NUMERIC", 6)
        assert gen is not None

    def test_unknown_alpha_field_returns_none(self):
        gen = get_generator("MYSTERY", "ALPHANUMERIC", 10)
        assert gen is None

    def test_unknown_numeric_field_returns_none(self):
        gen = get_generator("MYSTERY", "NUMERIC", 9)
        assert gen is None

    def test_addr_field_returns_generator(self):
        gen = get_generator("ADDR-LINE", "ALPHANUMERIC", 40)
        assert gen is not None

    def test_email_field_returns_generator(self):
        gen = get_generator("EMAIL-ADDR", "ALPHANUMERIC", 50)
        assert gen is not None

    def test_case_insensitive_match(self):
        # Lower-case field name should still match
        gen = get_generator("cust_name", "ALPHANUMERIC", 20)
        # 'name' substring — should find NAME pattern
        assert gen is not None

    def test_phone_pattern_matches_telefon(self):
        gen = get_generator("TELEFON", "ALPHANUMERIC", 15)
        assert gen is not None


# ---------------------------------------------------------------------------
# TestGenerateValue — size and type constraints
# ---------------------------------------------------------------------------

class TestGenerateValue:
    """generate_value() produces values that fit in the declared field size."""

    def test_alpha_name_fits_size(self):
        val = generate_value("CUST-NAME", "ALPHANUMERIC", 20, locale="en_US")
        assert isinstance(val, str)
        assert len(val) <= 20

    def test_alpha_result_exactly_size_chars(self):
        val = generate_value("CUST-NAME", "ALPHANUMERIC", 15, locale="en_US")
        assert len(val) == 15

    def test_alpha_unknown_field_fills_size(self):
        val = generate_value("XYZ", "ALPHANUMERIC", 10, locale="en_US")
        assert isinstance(val, str)
        assert len(val) == 10

    def test_numeric_amt_returns_number(self):
        val = generate_value("AMT", "NUMERIC", 9, scale=2, locale="en_US")
        assert isinstance(val, (int, float))

    def test_numeric_amt_within_bounds(self):
        val = generate_value("AMT", "NUMERIC", 9, scale=2, locale="en_US")
        max_val = 10 ** 9 - 1
        assert val <= max_val

    def test_numeric_date_returns_int(self):
        val = generate_value("TXN-DATE", "NUMERIC", 8, locale="en_US")
        assert isinstance(val, int)
        # Should look like YYYYMMDD
        assert 10_000_000 <= val <= 99_999_999

    def test_numeric_id_returns_int(self):
        val = generate_value("CUST-ID", "NUMERIC", 6, locale="en_US")
        assert isinstance(val, int)
        assert 1 <= val <= 999_999

    def test_no_no_locale_does_not_crash(self):
        val = generate_value("NAVN", "ALPHANUMERIC", 20, locale="no_NO")
        assert isinstance(val, str)

    def test_de_de_locale_does_not_crash(self):
        val = generate_value("NAME", "ALPHANUMERIC", 25, locale="de_DE")
        assert isinstance(val, str)

    def test_seed_produces_same_value_twice(self):
        val1 = generate_value("CUST-NAME", "ALPHANUMERIC", 20, locale="en_US", seed=42)
        val2 = generate_value("CUST-NAME", "ALPHANUMERIC", 20, locale="en_US", seed=42)
        assert val1 == val2

    def test_unknown_numeric_fallback_returns_int(self):
        val = generate_value("MYSTERY_NUM", "NUMERIC", 5, locale="en_US")
        assert isinstance(val, int)
        assert 0 <= val <= 99_999

    def test_unknown_numeric_with_scale_returns_float(self):
        # Use a field name that does not match any pattern
        val = generate_value("ZZZFIELD", "NUMERIC", 7, scale=2, locale="en_US")
        assert isinstance(val, float)


# ---------------------------------------------------------------------------
# TestGenerateRecords
# ---------------------------------------------------------------------------

class TestGenerateRecords:
    """generate_records() yields the correct number of valid record dicts."""

    def test_yields_correct_count(self):
        records = list(generate_records(SIMPLE_SCHEMA, count=5))
        assert len(records) == 5

    def test_yields_zero_records(self):
        records = list(generate_records(SIMPLE_SCHEMA, count=0))
        assert records == []

    def test_records_are_dicts(self):
        records = list(generate_records(SIMPLE_SCHEMA, count=3))
        for rec in records:
            assert isinstance(rec, dict)

    def test_filler_fields_excluded(self):
        records = list(generate_records(SIMPLE_SCHEMA, count=2))
        for rec in records:
            assert "FILLER" not in rec

    def test_all_non_filler_fields_present(self):
        expected_keys = {"CUST-NAME", "ADDR-LINE", "CUST-ID", "TXN-DATE", "TXN-AMT"}
        records = list(generate_records(SIMPLE_SCHEMA, count=2))
        for rec in records:
            assert expected_keys == set(rec.keys())

    def test_alpha_fields_are_strings(self):
        records = list(generate_records(SIMPLE_SCHEMA, count=3))
        for rec in records:
            assert isinstance(rec["CUST-NAME"], str)
            assert isinstance(rec["ADDR-LINE"], str)

    def test_numeric_fields_are_numbers(self):
        records = list(generate_records(SIMPLE_SCHEMA, count=3))
        for rec in records:
            assert isinstance(rec["CUST-ID"], (int, float))
            assert isinstance(rec["TXN-DATE"], (int, float))
            assert isinstance(rec["TXN-AMT"], (int, float))

    def test_seed_produces_deterministic_output(self):
        r1 = list(generate_records(SIMPLE_SCHEMA, count=3, seed=99))
        r2 = list(generate_records(SIMPLE_SCHEMA, count=3, seed=99))
        assert r1 == r2

    def test_codepage_cp277_uses_no_no_locale(self):
        """cp277 maps to no_NO — should not raise."""
        schema = {"fields": [_alpha("NAVN", 20)]}
        records = list(generate_records(schema, count=3, codepage="cp277"))
        assert len(records) == 3

    def test_codepage_cp273_uses_de_de_locale(self):
        schema = {"fields": [_alpha("NAME", 20)]}
        records = list(generate_records(schema, count=2, codepage="cp273"))
        assert len(records) == 2

    def test_unknown_codepage_falls_back_to_en_us(self):
        schema = {"fields": [_alpha("NAME", 20)]}
        records = list(generate_records(schema, count=2, codepage="cp999"))
        assert len(records) == 2

    def test_generator_is_lazy_iterator(self):
        """generate_records should return an iterator, not a list."""
        gen = generate_records(SIMPLE_SCHEMA, count=10)
        # It should be an iterator (has __next__)
        assert hasattr(gen, "__next__")

    def test_occurs_field_generates_array(self):
        schema = {
            "fields": [
                {
                    "name": "ITEMS",
                    "type": "ALPHANUMERIC",
                    "size": 0,
                    "occurs": 3,
                    "children": [
                        {"name": "ITEM-CODE", "type": "ALPHANUMERIC", "size": 4},
                        {"name": "ITEM-QTY", "type": "NUMERIC", "size": 3, "scale": 0},
                    ],
                }
            ]
        }
        records = list(generate_records(schema, count=2))
        for rec in records:
            assert "ITEMS" in rec
            assert isinstance(rec["ITEMS"], list)
            assert len(rec["ITEMS"]) == 3
            for item in rec["ITEMS"]:
                assert "ITEM-CODE" in item
                assert "ITEM-QTY" in item


# ---------------------------------------------------------------------------
# TestEBCDICWriter
# ---------------------------------------------------------------------------

class TestEBCDICWriter:
    """EBCDICWriter buffers records and delegates encoding to ZtractBridge."""

    def _make_bridge(self, encode_return: int = 5) -> MagicMock:
        bridge = MagicMock()
        bridge.encode.return_value = encode_return
        return bridge

    def test_name_contains_ebcdic(self, tmp_path):
        from ztract.writers.ebcdic import EBCDICWriter

        bridge = self._make_bridge()
        w = EBCDICWriter(
            output_path=tmp_path / "out.bin",
            bridge=bridge,
            copybook=Path("/fake/test.cpy"),
            recfm="FB",
            lrecl=80,
        )
        assert "EBCDIC" in w.name

    def test_name_contains_output_filename(self, tmp_path):
        from ztract.writers.ebcdic import EBCDICWriter

        bridge = self._make_bridge()
        w = EBCDICWriter(
            output_path=tmp_path / "myfile.bin",
            bridge=bridge,
            copybook=Path("/fake/test.cpy"),
            recfm="FB",
            lrecl=80,
        )
        assert "myfile.bin" in w.name

    def test_write_batch_buffers_records(self, tmp_path):
        from ztract.writers.ebcdic import EBCDICWriter

        bridge = self._make_bridge(encode_return=3)
        w = EBCDICWriter(
            output_path=tmp_path / "out.bin",
            bridge=bridge,
            copybook=Path("/fake/test.cpy"),
            recfm="FB",
            lrecl=80,
        )
        records = [{"ID": 1}, {"ID": 2}, {"ID": 3}]
        w.open({})
        w.write_batch(records)
        # Bridge not called yet — records are buffered
        bridge.encode.assert_not_called()

    def test_close_calls_bridge_encode(self, tmp_path):
        from ztract.writers.ebcdic import EBCDICWriter

        bridge = self._make_bridge(encode_return=2)
        w = EBCDICWriter(
            output_path=tmp_path / "out.bin",
            bridge=bridge,
            copybook=Path("/fake/test.cpy"),
            recfm="FB",
            lrecl=80,
            codepage="cp037",
        )
        w.open({})
        w.write_batch([{"ID": 1}, {"ID": 2}])
        stats = w.close()

        bridge.encode.assert_called_once()
        call_kwargs = bridge.encode.call_args
        assert call_kwargs.kwargs["recfm"] == "FB"
        assert call_kwargs.kwargs["lrecl"] == 80
        assert call_kwargs.kwargs["codepage"] == "cp037"

    def test_close_returns_writer_stats(self, tmp_path):
        from ztract.writers.ebcdic import EBCDICWriter
        from ztract.writers.base import WriterStats

        bridge = self._make_bridge(encode_return=4)
        w = EBCDICWriter(
            output_path=tmp_path / "out.bin",
            bridge=bridge,
            copybook=Path("/fake/test.cpy"),
            recfm="FB",
            lrecl=80,
        )
        w.open({})
        w.write_batch([{"ID": i} for i in range(4)])
        stats = w.close()

        assert isinstance(stats, WriterStats)
        assert stats.records_written == 4
        assert stats.elapsed_sec >= 0.0

    def test_multiple_write_batch_calls_all_encoded(self, tmp_path):
        from ztract.writers.ebcdic import EBCDICWriter

        bridge = self._make_bridge(encode_return=6)
        w = EBCDICWriter(
            output_path=tmp_path / "out.bin",
            bridge=bridge,
            copybook=Path("/fake/test.cpy"),
            recfm="FB",
            lrecl=80,
        )
        w.open({})
        w.write_batch([{"ID": 1}, {"ID": 2}, {"ID": 3}])
        w.write_batch([{"ID": 4}, {"ID": 5}, {"ID": 6}])
        w.close()

        # encode called once with all 6 records combined
        bridge.encode.assert_called_once()
        records_passed = list(bridge.encode.call_args.kwargs["records"])
        assert len(records_passed) == 6

    def test_lrecl_none_defaults_to_zero(self, tmp_path):
        from ztract.writers.ebcdic import EBCDICWriter

        bridge = self._make_bridge(encode_return=0)
        w = EBCDICWriter(
            output_path=tmp_path / "out.bin",
            bridge=bridge,
            copybook=Path("/fake/test.cpy"),
            recfm="VB",
            lrecl=None,
        )
        w.open({})
        w.close()
        assert bridge.encode.call_args.kwargs["lrecl"] == 0


# ---------------------------------------------------------------------------
# TestGenerateCLI
# ---------------------------------------------------------------------------

class TestGenerateCLI:
    """CLI integration tests using Click's test runner (no subprocess calls)."""

    def _make_bridge_mock(self, record_count: int = 3):
        bridge = MagicMock()
        bridge.check_jre.return_value = "17"
        bridge.get_schema.return_value = {
            "fields": [
                {"name": "CUST-ID", "type": "NUMERIC", "size": 6, "scale": 0},
                {"name": "NAME", "type": "ALPHANUMERIC", "size": 20},
            ]
        }
        bridge.encode.return_value = record_count
        return bridge

    def test_generate_command_registered(self):
        from click.testing import CliRunner
        from ztract.cli.root import cli

        runner = CliRunner()
        result = runner.invoke(cli, ["generate", "--help"])
        assert result.exit_code == 0
        assert "--copybook" in result.output
        assert "--records" in result.output
        assert "--output" in result.output

    def test_generate_runs_end_to_end(self, tmp_path):
        from click.testing import CliRunner
        from ztract.cli.generate import generate

        copybook = tmp_path / "test.cpy"
        copybook.write_text("FILLER.")
        output_file = tmp_path / "out.bin"

        bridge_mock = self._make_bridge_mock(record_count=5)

        with patch("ztract.cli.generate.ZtractBridge", return_value=bridge_mock):
            runner = CliRunner()
            result = runner.invoke(
                generate,
                [
                    "--copybook", str(copybook),
                    "--records", "5",
                    "--output", str(output_file),
                    "--recfm", "FB",
                    "--lrecl", "80",
                    "--seed", "42",
                ],
                obj={"debug": False, "quiet": True},
            )

        assert result.exit_code == 0, result.output
        assert "5" in result.output
        assert "Done" in result.output

    def test_invalid_codepage_exits_with_error(self, tmp_path):
        from click.testing import CliRunner
        from ztract.cli.generate import generate

        copybook = tmp_path / "test.cpy"
        copybook.write_text("FILLER.")

        runner = CliRunner()
        result = runner.invoke(
            generate,
            [
                "--copybook", str(copybook),
                "--records", "3",
                "--output", str(tmp_path / "out.bin"),
                "--recfm", "FB",
                "--codepage", "INVALID_CP",
            ],
            obj={"debug": False, "quiet": True},
        )

        assert result.exit_code != 0

    def test_jre_error_exits_with_error(self, tmp_path):
        from click.testing import CliRunner
        from ztract.cli.generate import generate
        from ztract.engine.bridge import JREError

        copybook = tmp_path / "test.cpy"
        copybook.write_text("FILLER.")

        bridge_mock = MagicMock()
        bridge_mock.check_jre.side_effect = JREError("Java not found")

        with patch("ztract.cli.generate.ZtractBridge", return_value=bridge_mock):
            runner = CliRunner()
            result = runner.invoke(
                generate,
                [
                    "--copybook", str(copybook),
                    "--records", "5",
                    "--output", str(tmp_path / "out.bin"),
                    "--recfm", "FB",
                ],
                obj={"debug": False, "quiet": True},
            )

        assert result.exit_code != 0

    def test_schema_error_exits_with_error(self, tmp_path):
        from click.testing import CliRunner
        from ztract.cli.generate import generate
        from ztract.engine.bridge import EngineError

        copybook = tmp_path / "test.cpy"
        copybook.write_text("FILLER.")

        bridge_mock = MagicMock()
        bridge_mock.check_jre.return_value = "17"
        bridge_mock.get_schema.side_effect = EngineError("bad copybook")

        with patch("ztract.cli.generate.ZtractBridge", return_value=bridge_mock):
            runner = CliRunner()
            result = runner.invoke(
                generate,
                [
                    "--copybook", str(copybook),
                    "--records", "5",
                    "--output", str(tmp_path / "out.bin"),
                    "--recfm", "FB",
                ],
                obj={"debug": False, "quiet": True},
            )

        assert result.exit_code != 0
        assert "Failed to read copybook schema" in result.output
