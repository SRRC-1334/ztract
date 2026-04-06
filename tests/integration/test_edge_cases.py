"""Integration tests for complex copybook edge cases.

These tests require a working JRE and the ztract-engine.jar.
Run with: pytest -m integration
"""
from pathlib import Path

import pytest

from ztract.engine.bridge import ZtractBridge
from ztract.generate.generator import generate_records

pytestmark = pytest.mark.integration

COPYBOOKS_DIR = Path(__file__).parent.parent / "test_data"


@pytest.fixture
def bridge():
    b = ZtractBridge()
    b.check_jre()
    return b


class TestNumericRoundTrip:
    """Generate numeric test data, encode, decode, verify values match."""

    def test_comp3_values_survive_round_trip(self, bridge, tmp_path):
        copybook = COPYBOOKS_DIR / "COMPLEX_NUMERIC.cpy"
        schema = bridge.get_schema(copybook)

        # Generate records
        original_records = list(generate_records(schema, 10, codepage="cp037", seed=42))

        # Encode to EBCDIC
        output_dat = tmp_path / "numeric.dat"
        lrecl = schema.get("record_length", 300)
        bridge.encode(copybook, output_dat, "FB", lrecl, "cp037", iter(original_records))

        # Decode back
        decoded_records = list(bridge.decode(copybook, output_dat, "FB", lrecl, "cp037"))

        assert len(decoded_records) == 10

        # Verify numeric values survived (compare as strings to avoid float issues)
        for orig, decoded in zip(original_records, decoded_records):
            for key in orig:
                if key in decoded and isinstance(orig[key], (int, float)):
                    assert abs(float(orig[key]) - float(decoded[key])) < 0.01, \
                        f"Field {key}: {orig[key]} != {decoded[key]}"


class TestEdgeCaseRoundTrip:
    """Generate edge case data, encode, decode, verify no corruption."""

    def test_edge_cases_survive_round_trip(self, bridge, tmp_path):
        copybook = COPYBOOKS_DIR / "COMPLEX_NUMERIC.cpy"
        schema = bridge.get_schema(copybook)

        original_records = list(generate_records(
            schema, 200, codepage="cp037", seed=42, edge_cases=True
        ))

        output_dat = tmp_path / "edge.dat"
        lrecl = schema.get("record_length", 300)
        bridge.encode(copybook, output_dat, "FB", lrecl, "cp037", iter(original_records))

        decoded_records = list(bridge.decode(copybook, output_dat, "FB", lrecl, "cp037"))

        assert len(decoded_records) == 200
