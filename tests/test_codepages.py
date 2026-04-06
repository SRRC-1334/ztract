"""Tests for the EBCDIC codepage registry."""
import pytest

from ztract.codepages import CodepageError, list_codepages, resolve_codepage


class TestCanonicalNames:
    """All canonical names must resolve to themselves."""

    @pytest.mark.parametrize(
        "canonical",
        ["cp037", "cp277", "cp273", "cp875", "cp870", "cp1047", "cp838", "cp1025"],
    )
    def test_canonical_resolves_to_itself(self, canonical: str) -> None:
        assert resolve_codepage(canonical) == canonical


class TestNumericAliases:
    """Numeric aliases (without the 'cp' prefix) must resolve to canonical names."""

    @pytest.mark.parametrize(
        "alias, expected",
        [
            ("037", "cp037"),
            ("277", "cp277"),
            ("273", "cp273"),
            ("875", "cp875"),
            ("870", "cp870"),
            ("1047", "cp1047"),
            ("838", "cp838"),
            ("1025", "cp1025"),
        ],
    )
    def test_numeric_alias(self, alias: str, expected: str) -> None:
        assert resolve_codepage(alias) == expected


class TestFriendlyAliases:
    """Friendly human-readable aliases must resolve to the correct canonical name."""

    # cp037 aliases
    def test_us_resolves_to_cp037(self) -> None:
        assert resolve_codepage("us") == "cp037"

    def test_usa_resolves_to_cp037(self) -> None:
        assert resolve_codepage("usa") == "cp037"

    def test_canada_resolves_to_cp037(self) -> None:
        assert resolve_codepage("canada") == "cp037"

    def test_default_resolves_to_cp037(self) -> None:
        assert resolve_codepage("default") == "cp037"

    # cp277 aliases
    def test_norway_resolves_to_cp277(self) -> None:
        assert resolve_codepage("norway") == "cp277"

    def test_norwegian_resolves_to_cp277(self) -> None:
        assert resolve_codepage("norwegian") == "cp277"

    def test_danish_resolves_to_cp277(self) -> None:
        assert resolve_codepage("danish") == "cp277"

    def test_denmark_resolves_to_cp277(self) -> None:
        assert resolve_codepage("denmark") == "cp277"

    def test_nordic_resolves_to_cp277(self) -> None:
        assert resolve_codepage("nordic") == "cp277"

    # cp273 aliases
    def test_germany_resolves_to_cp273(self) -> None:
        assert resolve_codepage("germany") == "cp273"

    def test_german_resolves_to_cp273(self) -> None:
        assert resolve_codepage("german") == "cp273"

    def test_austria_resolves_to_cp273(self) -> None:
        assert resolve_codepage("austria") == "cp273"

    def test_switzerland_resolves_to_cp273(self) -> None:
        assert resolve_codepage("switzerland") == "cp273"

    # cp875 aliases
    def test_greek_resolves_to_cp875(self) -> None:
        assert resolve_codepage("greek") == "cp875"

    def test_greece_resolves_to_cp875(self) -> None:
        assert resolve_codepage("greece") == "cp875"

    # cp870 aliases
    def test_eastern_europe_resolves_to_cp870(self) -> None:
        assert resolve_codepage("eastern_europe") == "cp870"

    def test_poland_resolves_to_cp870(self) -> None:
        assert resolve_codepage("poland") == "cp870"

    def test_hungary_resolves_to_cp870(self) -> None:
        assert resolve_codepage("hungary") == "cp870"

    def test_czech_resolves_to_cp870(self) -> None:
        assert resolve_codepage("czech") == "cp870"

    # cp1047 aliases
    def test_open_systems_resolves_to_cp1047(self) -> None:
        assert resolve_codepage("open_systems") == "cp1047"

    def test_latin1_resolves_to_cp1047(self) -> None:
        assert resolve_codepage("latin1") == "cp1047"

    # cp838 aliases
    def test_thailand_resolves_to_cp838(self) -> None:
        assert resolve_codepage("thailand") == "cp838"

    def test_thai_resolves_to_cp838(self) -> None:
        assert resolve_codepage("thai") == "cp838"

    # cp1025 aliases
    def test_cyrillic_resolves_to_cp1025(self) -> None:
        assert resolve_codepage("cyrillic") == "cp1025"

    def test_russian_resolves_to_cp1025(self) -> None:
        assert resolve_codepage("russian") == "cp1025"


class TestCaseInsensitivity:
    """Resolution must be case-insensitive."""

    def test_uppercase_norway(self) -> None:
        assert resolve_codepage("NORWAY") == "cp277"

    def test_titlecase_norway(self) -> None:
        assert resolve_codepage("Norway") == "cp277"

    def test_uppercase_canonical(self) -> None:
        assert resolve_codepage("CP277") == "cp277"

    def test_mixed_case_canonical(self) -> None:
        assert resolve_codepage("Cp277") == "cp277"

    def test_uppercase_us(self) -> None:
        assert resolve_codepage("US") == "cp037"

    def test_uppercase_default(self) -> None:
        assert resolve_codepage("DEFAULT") == "cp037"

    def test_uppercase_german(self) -> None:
        assert resolve_codepage("GERMAN") == "cp273"


class TestUnknownCodepage:
    """Unknown codepages must raise CodepageError with supported codepages in the message."""

    def test_unknown_raises_codepage_error(self) -> None:
        with pytest.raises(CodepageError):
            resolve_codepage("unknown_codepage")

    def test_error_message_contains_cp277(self) -> None:
        with pytest.raises(CodepageError, match="cp277"):
            resolve_codepage("not_a_real_codepage")

    def test_error_message_contains_invalid_name(self) -> None:
        with pytest.raises(CodepageError, match="bogus"):
            resolve_codepage("bogus")

    def test_codepage_error_is_value_error(self) -> None:
        with pytest.raises(ValueError):
            resolve_codepage("totally_unknown")

    def test_empty_string_raises_codepage_error(self) -> None:
        with pytest.raises(CodepageError):
            resolve_codepage("")


class TestListCodepages:
    """list_codepages must return all registered codepages with their aliases."""

    def test_returns_dict(self) -> None:
        result = list_codepages()
        assert isinstance(result, dict)

    def test_contains_all_canonical_names(self) -> None:
        result = list_codepages()
        expected = {"cp037", "cp277", "cp273", "cp875", "cp870", "cp1047", "cp838", "cp1025"}
        assert expected == set(result.keys())

    def test_cp037_has_expected_aliases(self) -> None:
        result = list_codepages()
        assert "us" in result["cp037"]
        assert "default" in result["cp037"]

    def test_cp277_has_expected_aliases(self) -> None:
        result = list_codepages()
        assert "norway" in result["cp277"]
        assert "nordic" in result["cp277"]

    def test_cp1047_has_expected_aliases(self) -> None:
        result = list_codepages()
        assert "open_systems" in result["cp1047"]
        assert "latin1" in result["cp1047"]

    def test_returns_copy_not_original(self) -> None:
        result1 = list_codepages()
        result1["cp037"].append("mutated")
        result2 = list_codepages()
        assert "mutated" not in result2["cp037"]
