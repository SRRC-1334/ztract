"""Dataset record format handling for mainframe datasets."""
from __future__ import annotations

from enum import Enum


class DatasetFormatError(ValueError):
    """Raised when a dataset record format is invalid or misconfigured."""


class RecordFormat(Enum):
    """Mainframe dataset record format (RECFM)."""

    F = "F"
    FB = "FB"
    V = "V"
    VB = "VB"
    FBA = "FBA"
    VBA = "VBA"

    @classmethod
    def from_str(cls, value: str) -> "RecordFormat":
        """Parse a RECFM string case-insensitively.

        Raises DatasetFormatError for unrecognised values, listing all valid formats.
        """
        normalised = value.upper()
        try:
            return cls(normalised)
        except ValueError:
            valid = ", ".join(m.value for m in cls)
            raise DatasetFormatError(
                f"Invalid record format {value!r}. Valid formats are: {valid}"
            ) from None


# ASA (American National Standard Institute carriage-control) byte constants.
ASA_SINGLE_SPACE: int = 0x40
ASA_DOUBLE_SPACE: int = 0xF0
ASA_NEW_PAGE: int = 0xF1
ASA_OVERPRINT: int = 0x4E

# Formats that require a logical record length (LRECL).
_REQUIRES_LRECL = {RecordFormat.F, RecordFormat.FB, RecordFormat.FBA}

# Formats that carry an ASA carriage-control byte.
_HAS_ASA_BYTE = {RecordFormat.FBA, RecordFormat.VBA}


def requires_lrecl(recfm: RecordFormat) -> bool:
    """Return True if *recfm* requires an explicit LRECL (fixed-length formats)."""
    return recfm in _REQUIRES_LRECL


def has_asa_byte(recfm: RecordFormat) -> bool:
    """Return True if *recfm* includes an ASA carriage-control byte."""
    return recfm in _HAS_ASA_BYTE


def validate_record_format(recfm: RecordFormat, lrecl: int | None) -> None:
    """Validate that *lrecl* is provided when required by *recfm*.

    Raises DatasetFormatError if the format requires an LRECL but *lrecl* is None.
    """
    if requires_lrecl(recfm) and lrecl is None:
        raise DatasetFormatError(
            f"Record format {recfm.value} requires an LRECL value but none was provided."
        )
