"""Tests for the dataset record format module."""
import pytest

from ztract.connectors.dataset_format import (
    ASA_DOUBLE_SPACE,
    ASA_NEW_PAGE,
    ASA_OVERPRINT,
    ASA_SINGLE_SPACE,
    DatasetFormatError,
    RecordFormat,
    has_asa_byte,
    requires_lrecl,
    validate_record_format,
)


class TestRecordFormatEnumValues:
    """All 6 enum values must exist with correct string values."""

    def test_f_value(self) -> None:
        assert RecordFormat.F.value == "F"

    def test_fb_value(self) -> None:
        assert RecordFormat.FB.value == "FB"

    def test_v_value(self) -> None:
        assert RecordFormat.V.value == "V"

    def test_vb_value(self) -> None:
        assert RecordFormat.VB.value == "VB"

    def test_fba_value(self) -> None:
        assert RecordFormat.FBA.value == "FBA"

    def test_vba_value(self) -> None:
        assert RecordFormat.VBA.value == "VBA"


class TestRecordFormatFromStr:
    """from_str must parse case-insensitively and raise DatasetFormatError for invalid values."""

    def test_lowercase_fb_parses_to_FB(self) -> None:
        assert RecordFormat.from_str("fb") == RecordFormat.FB

    def test_mixed_case_vb_parses_to_VB(self) -> None:
        assert RecordFormat.from_str("Vb") == RecordFormat.VB

    def test_uppercase_f_parses_to_F(self) -> None:
        assert RecordFormat.from_str("F") == RecordFormat.F

    def test_lowercase_v_parses_to_V(self) -> None:
        assert RecordFormat.from_str("v") == RecordFormat.V

    def test_lowercase_fba_parses_to_FBA(self) -> None:
        assert RecordFormat.from_str("fba") == RecordFormat.FBA

    def test_uppercase_vba_parses_to_VBA(self) -> None:
        assert RecordFormat.from_str("VBA") == RecordFormat.VBA

    def test_invalid_value_raises_dataset_format_error(self) -> None:
        with pytest.raises(DatasetFormatError):
            RecordFormat.from_str("INVALID")

    def test_error_message_lists_valid_formats(self) -> None:
        with pytest.raises(DatasetFormatError, match="FB"):
            RecordFormat.from_str("XYZ")

    def test_empty_string_raises_dataset_format_error(self) -> None:
        with pytest.raises(DatasetFormatError):
            RecordFormat.from_str("")

    def test_dataset_format_error_is_value_error(self) -> None:
        with pytest.raises(ValueError):
            RecordFormat.from_str("BAD")


class TestAsaByteConstants:
    """ASA byte constants must have the correct hexadecimal values."""

    def test_asa_single_space_is_0x40(self) -> None:
        assert ASA_SINGLE_SPACE == 0x40

    def test_asa_double_space_is_0xF0(self) -> None:
        assert ASA_DOUBLE_SPACE == 0xF0

    def test_asa_new_page_is_0xF1(self) -> None:
        assert ASA_NEW_PAGE == 0xF1

    def test_asa_overprint_is_0x4E(self) -> None:
        assert ASA_OVERPRINT == 0x4E


class TestRequiresLrecl:
    """requires_lrecl must return True for F/FB/FBA and False for V/VB/VBA."""

    def test_f_requires_lrecl(self) -> None:
        assert requires_lrecl(RecordFormat.F) is True

    def test_fb_requires_lrecl(self) -> None:
        assert requires_lrecl(RecordFormat.FB) is True

    def test_fba_requires_lrecl(self) -> None:
        assert requires_lrecl(RecordFormat.FBA) is True

    def test_v_does_not_require_lrecl(self) -> None:
        assert requires_lrecl(RecordFormat.V) is False

    def test_vb_does_not_require_lrecl(self) -> None:
        assert requires_lrecl(RecordFormat.VB) is False

    def test_vba_does_not_require_lrecl(self) -> None:
        assert requires_lrecl(RecordFormat.VBA) is False


class TestHasAsaByte:
    """has_asa_byte must return True for FBA/VBA and False for F/FB/V/VB."""

    def test_fba_has_asa_byte(self) -> None:
        assert has_asa_byte(RecordFormat.FBA) is True

    def test_vba_has_asa_byte(self) -> None:
        assert has_asa_byte(RecordFormat.VBA) is True

    def test_f_has_no_asa_byte(self) -> None:
        assert has_asa_byte(RecordFormat.F) is False

    def test_fb_has_no_asa_byte(self) -> None:
        assert has_asa_byte(RecordFormat.FB) is False

    def test_v_has_no_asa_byte(self) -> None:
        assert has_asa_byte(RecordFormat.V) is False

    def test_vb_has_no_asa_byte(self) -> None:
        assert has_asa_byte(RecordFormat.VB) is False


class TestValidateRecordFormat:
    """validate_record_format must raise DatasetFormatError when LRECL required but not given."""

    def test_fb_without_lrecl_raises(self) -> None:
        with pytest.raises(DatasetFormatError):
            validate_record_format(RecordFormat.FB, None)

    def test_fb_with_lrecl_ok(self) -> None:
        validate_record_format(RecordFormat.FB, 80)  # must not raise

    def test_f_without_lrecl_raises(self) -> None:
        with pytest.raises(DatasetFormatError):
            validate_record_format(RecordFormat.F, None)

    def test_vb_without_lrecl_ok(self) -> None:
        validate_record_format(RecordFormat.VB, None)  # must not raise

    def test_fba_without_lrecl_raises(self) -> None:
        with pytest.raises(DatasetFormatError):
            validate_record_format(RecordFormat.FBA, None)

    def test_fba_with_lrecl_ok(self) -> None:
        validate_record_format(RecordFormat.FBA, 133)  # must not raise

    def test_vba_without_lrecl_ok(self) -> None:
        validate_record_format(RecordFormat.VBA, None)  # must not raise

    def test_v_without_lrecl_ok(self) -> None:
        validate_record_format(RecordFormat.V, None)  # must not raise
