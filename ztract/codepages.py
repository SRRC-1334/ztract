"""EBCDIC codepage registry with alias resolution.

Provides a central mapping of friendly aliases to canonical Cobrix codepage
names, used by CLI validation and YAML schema validation.
"""

from __future__ import annotations

__all__ = ["CodepageError", "list_codepages", "resolve_codepage"]


class CodepageError(ValueError):
    """Raised when an unknown or unsupported EBCDIC codepage is requested."""


# Registry: canonical name → list of aliases (not including the canonical name itself)
_CODEPAGE_REGISTRY: dict[str, list[str]] = {
    "cp037": ["037", "us", "usa", "canada", "default"],
    "cp277": ["277", "norway", "norwegian", "danish", "denmark", "nordic"],
    "cp273": ["273", "germany", "german", "austria", "switzerland"],
    "cp875": ["875", "greek", "greece"],
    "cp870": ["870", "eastern_europe", "poland", "hungary", "czech"],
    "cp1047": ["1047", "latin1", "open_systems"],
    "cp838": ["838", "thailand", "thai"],
    "cp1025": ["1025", "cyrillic", "russian"],
}

# Reverse lookup: lowercase alias → canonical name
# Canonical names also map to themselves.
_ALIAS_MAP: dict[str, str] = {}

for _canonical, _aliases in _CODEPAGE_REGISTRY.items():
    _ALIAS_MAP[_canonical.lower()] = _canonical
    for _alias in _aliases:
        _ALIAS_MAP[_alias.lower()] = _canonical


def resolve_codepage(name: str) -> str:
    """Resolve a codepage name or alias to its canonical Cobrix codepage name.

    Lookup is case-insensitive. Canonical names (e.g. ``cp277``) resolve to
    themselves; numeric aliases (e.g. ``"277"``) and friendly aliases
    (e.g. ``"norway"``) resolve to the corresponding canonical name.

    Parameters
    ----------
    name:
        Codepage name or alias to resolve.

    Returns
    -------
    str
        Canonical codepage name (e.g. ``"cp277"``).

    Raises
    ------
    CodepageError
        If *name* is not a known codepage or alias. The error message lists
        all supported canonical names.
    """
    canonical = _ALIAS_MAP.get(name.lower())
    if canonical is None:
        supported = ", ".join(sorted(_CODEPAGE_REGISTRY.keys()))
        raise CodepageError(
            f"Unknown codepage {name!r}. Supported codepages: {supported}"
        )
    return canonical


def list_codepages() -> dict[str, list[str]]:
    """Return a copy of the codepage registry.

    Returns
    -------
    dict[str, list[str]]
        Mapping of canonical codepage name to its list of aliases. Mutating
        the returned dict does not affect the internal registry.
    """
    return {canonical: list(aliases) for canonical, aliases in _CODEPAGE_REGISTRY.items()}
