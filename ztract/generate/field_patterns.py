"""Field-name-aware mock data generators using Faker.

Patterns are matched case-insensitively against the field name.
For alphanumeric fields the generator returns a str padded/truncated to
exactly *size* characters.  For numeric fields it returns an int or float
that fits within *size* digits.
"""
from __future__ import annotations

import random
import re
from datetime import datetime, timedelta
from typing import Callable


# ---------------------------------------------------------------------------
# Pattern tables
# ---------------------------------------------------------------------------

# Each entry is (pattern_str, lambda(faker, size) -> str)
_ALPHA_PATTERNS: list[tuple[str, Callable]] = [
    (r"(NAME|NAVN)", lambda faker, size: faker.name()[:size]),
    (r"(ADDR|ADRESSE)", lambda faker, size: faker.street_address()[:size]),
    (r"(CITY|BY)", lambda faker, size: faker.city()[:size]),
    (r"(PHONE|TELEFON|TLF)", lambda faker, size: faker.phone_number()[:size]),
    (r"EMAIL", lambda faker, size: faker.email()[:size]),
    (r"(ZIP|POST)", lambda faker, size: faker.postcode()[:size]),
    (r"(COUNTRY|LAND)", lambda faker, size: faker.country()[:size]),
    (r"(DESC|TEXT|BESKR)", lambda faker, size: faker.sentence()[:size]),
    (r"(CODE|KODE)", lambda faker, size: faker.bothify("?" * min(size, 8))[:size]),
]

# Each entry is (pattern_str, lambda(faker, size, scale) -> int|float)
_NUMERIC_PATTERNS: list[tuple[str, Callable]] = [
    (
        r"(AMT|AMOUNT|BELOP|BELOEP)",
        lambda f, sz, sc: round(random.uniform(100, 999999), sc),
    ),
    (
        r"(DATE|DATO)",
        lambda f, sz, sc: int(
            (datetime.now() - timedelta(days=random.randint(0, 3650))).strftime(
                "%Y%m%d"
            )
        ),
    ),
    (
        r"(ID|NR|NUM)",
        lambda f, sz, sc: random.randint(1, 10 ** min(sz, 9) - 1),
    ),
]


# ---------------------------------------------------------------------------
# Public helpers
# ---------------------------------------------------------------------------

def get_generator(
    field_name: str,
    field_type: str,
    size: int,
) -> Callable | None:
    """Return a bound generator callable for *field_name*, or ``None``.

    The lookup is case-insensitive.  For ALPHANUMERIC fields the generator
    accepts ``(faker, size)``.  For NUMERIC fields it accepts
    ``(faker, size, scale)``.  Returns ``None`` when no pattern matches.
    """
    name_upper = field_name.upper()
    ftype = field_type.upper()

    if "ALPHA" in ftype or ftype in ("ALPHANUMERIC", "ALPHA", "AN", "X"):
        for pattern, gen in _ALPHA_PATTERNS:
            if re.search(pattern, name_upper):
                return gen

    if "NUMERIC" in ftype or "DECIMAL" in ftype or "PACKED" in ftype or "INTEGRAL" in ftype or ftype in ("NUMERIC", "NUM", "N", "9", "BINARY"):
        for pattern, gen in _NUMERIC_PATTERNS:
            if re.search(pattern, name_upper):
                return gen

    return None


def generate_value(
    field_name: str,
    field_type: str,
    size: int,
    scale: int = 0,
    locale: str = "en_US",
    seed: int | None = None,
) -> str | int | float:
    """Generate a single mock value for a field.

    Parameters
    ----------
    field_name:
        COBOL field name (used for pattern matching).
    field_type:
        COBOL type string (e.g. ``"ALPHANUMERIC"``, ``"NUMERIC"``).
    size:
        Field width / maximum number of digits.
    scale:
        Number of decimal places for numeric fields.
    locale:
        Faker locale (e.g. ``"no_NO"``).
    seed:
        Optional random seed for reproducibility.

    Returns
    -------
    str | int | float
        Generated value.  Alphanumeric values are left-padded with spaces
        to exactly *size* characters; numeric values are capped to fit in
        *size* digits.
    """
    from faker import Faker  # local import keeps startup fast

    faker = Faker(locale)
    if seed is not None:
        Faker.seed(seed)
        random.seed(seed)

    ftype = field_type.upper()
    is_alpha = "ALPHA" in ftype or ftype in ("ALPHANUMERIC", "ALPHA", "AN", "X")
    is_numeric = (
        "NUMERIC" in ftype
        or "DECIMAL" in ftype
        or "PACKED" in ftype
        or "INTEGRAL" in ftype
        or ftype in ("NUMERIC", "NUM", "N", "9", "BINARY")
    )

    gen = get_generator(field_name, field_type, size)

    if gen is not None:
        if is_alpha:
            raw = gen(faker, size)
            # Pad or truncate to exactly size characters
            return raw.ljust(size)[:size]
        else:
            # Numeric generator
            value = gen(faker, size, scale)
            # Cap to fit in size digits
            max_val = 10 ** size - 1
            if isinstance(value, float):
                return min(value, float(max_val))
            return min(int(value), max_val)

    # --- Fallback ---
    if is_alpha:
        raw = faker.lexify("?" * size)
        return raw.ljust(size)[:size]

    if is_numeric:
        if scale > 0:
            max_val = 10 ** size - 1
            return round(random.uniform(0, min(max_val, 999999)), scale)
        max_val = 10 ** size - 1
        return random.randint(0, max_val)

    # Unknown type — return blank string of correct width
    return " " * size
