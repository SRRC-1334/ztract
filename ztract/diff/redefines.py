"""ztract.diff.redefines — REDEFINES hex diff.

When a COBOL record has REDEFINES, the same byte range can be interpreted
multiple ways.  During diff, if fields in a REDEFINES group change, we show
the raw hex comparison so the caller can inspect what actually changed.
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass
class RedefinesComparison:
    """Result of comparing a REDEFINES byte range."""

    group_name: str
    offset: int
    length: int
    before_hex: str
    after_hex: str
    differs: bool
    variants: list[str]  # names of REDEFINES variants


class RedefinesHandler:
    """Compares REDEFINES byte ranges between two records.

    When daff detects changes in fields belonging to a REDEFINES group,
    this handler extracts the raw bytes at the REDEFINES offset range
    and performs a hex comparison.
    """

    def __init__(self, schema: dict) -> None:
        """Init with schema from bridge.get_schema().

        Builds a map of redefines groups from schema["redefines_groups"]
        and field offsets from schema["fields"].
        """
        self._groups: dict[str, dict] = {}
        for group in schema.get("redefines_groups", []):
            self._groups[group["name"]] = {
                "offset": group["offset"],
                "length": group["length"],
                "variants": group.get("variants", []),
            }

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def compare(
        self,
        before_bytes: bytes,
        after_bytes: bytes,
        group_name: str,
    ) -> RedefinesComparison:
        """Compare a specific REDEFINES group's byte range.

        Extracts bytes at the group's offset/length from both records,
        converts to hex strings, compares.

        Raises:
            KeyError: if group_name is not present in the schema.
        """
        group = self._groups[group_name]  # raises KeyError if absent
        offset: int = group["offset"]
        length: int = group["length"]
        variants: list[str] = group["variants"]

        before_slice = before_bytes[offset : offset + length]
        after_slice = after_bytes[offset : offset + length]

        before_hex = before_slice.hex().upper()
        after_hex = after_slice.hex().upper()

        return RedefinesComparison(
            group_name=group_name,
            offset=offset,
            length=length,
            before_hex=before_hex,
            after_hex=after_hex,
            differs=(before_hex != after_hex),
            variants=variants,
        )

    def compare_all(
        self,
        before_bytes: bytes,
        after_bytes: bytes,
    ) -> list[RedefinesComparison]:
        """Compare all REDEFINES groups between two records."""
        return [
            self.compare(before_bytes, after_bytes, name)
            for name in self._groups
        ]

    def format_hex_diff(self, comparison: RedefinesComparison) -> str:
        """Format a comparison as readable hex diff output.

        Shows offset, before hex, after hex, and which bytes differ.

        Example::

          REDEFINES group: STATIC-DETAILS (variants: CONTACTS)
          Offset: 100-150 (50 bytes)
          Before: C1C2C3D4D5...
          After:  C1C2C3E4E5...
          Diff:         ^^^^
        """
        end = comparison.offset + comparison.length
        variants_str = ", ".join(comparison.variants)

        header = (
            f"REDEFINES group: {comparison.group_name}"
            f" (variants: {variants_str})"
        )
        offset_line = (
            f"Offset: {comparison.offset}-{end}"
            f" ({comparison.length} bytes)"
        )

        before_line = f"Before: {comparison.before_hex}"
        after_line  = f"After:  {comparison.after_hex}"

        # Build caret line aligned under the hex digits.
        # Each byte is represented as 2 hex characters — compare pair-by-pair.
        before_hex = comparison.before_hex
        after_hex  = comparison.after_hex
        max_len = max(len(before_hex), len(after_hex))

        # Pad to same length (should be equal for same group, but be safe)
        before_padded = before_hex.ljust(max_len)
        after_padded  = after_hex.ljust(max_len)

        carets: list[str] = []
        for i in range(0, max_len, 2):
            b_byte = before_padded[i : i + 2]
            a_byte = after_padded[i : i + 2]
            if b_byte != a_byte:
                carets.append("^^")
            else:
                carets.append("  ")
        diff_line = "Diff:   " + "".join(carets)

        lines = [header, offset_line, before_line, after_line]
        if comparison.differs:
            lines.append(diff_line)

        return "\n".join(lines)
