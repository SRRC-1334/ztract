"""Mock COBOL record generator.

Streams generated records one at a time so callers can pipe them directly
into an encoder without buffering everything in memory.
"""
from __future__ import annotations

import random
from typing import Iterator

from ztract.generate.field_patterns import generate_edge_case_value, generate_value


# ---------------------------------------------------------------------------
# Edge-case cycling
# ---------------------------------------------------------------------------

_EDGE_CASE_CYCLE = ["zeros", "max", "negative"]


# ---------------------------------------------------------------------------
# Codepage → Faker locale mapping
# ---------------------------------------------------------------------------

_CODEPAGE_LOCALES: dict[str, str] = {
    "cp277": "no_NO",
    "cp273": "de_DE",
    "cp037": "en_US",
    "cp875": "el_GR",
    "cp870": "pl_PL",
    "cp1047": "en_US",
    "cp838": "th_TH",
    "cp1025": "ru_RU",
}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def generate_records(
    schema: dict,
    count: int,
    codepage: str = "cp037",
    seed: int | None = None,
    edge_cases: bool = False,
) -> Iterator[dict]:
    """Yield *count* mock records that conform to *schema*.

    Parameters
    ----------
    schema:
        Schema dict as returned by ``ZtractBridge.get_schema()``.  Must
        contain a ``"fields"`` list of field descriptors with at least
        ``name``, ``type``, and ``size`` keys.
    count:
        Number of records to generate.
    codepage:
        EBCDIC codepage string used to select an appropriate Faker locale.
    seed:
        Optional integer seed for reproducibility.  When provided both
        ``random`` and ``Faker`` are seeded before generation starts.
    edge_cases:
        When ``True``, every 100th record (0, 100, 200, ...) cycles
        through boundary value types: zeros, max, negative.

    Yields
    ------
    dict
        One generated record per iteration.
    """
    from faker import Faker  # local import keeps startup fast

    locale = _CODEPAGE_LOCALES.get(codepage, "en_US")

    if seed is not None:
        Faker.seed(seed)
        random.seed(seed)

    fields: list[dict] = schema.get("fields", [])
    edge_case_index = 0

    for i in range(count):
        record: dict = {}

        # Every 100th record is an edge case when enabled
        use_edge = edge_cases and (i % 100 == 0)

        if use_edge:
            case_type = _EDGE_CASE_CYCLE[edge_case_index % len(_EDGE_CASE_CYCLE)]
            edge_case_index += 1
            for field_def in fields:
                name = field_def.get("name", "")
                if name.upper() == "FILLER":
                    continue
                if field_def.get("type", "").upper() == "GROUP":
                    continue
                record[name] = generate_edge_case_value(
                    name,
                    field_def.get("type", "ALPHANUMERIC"),
                    int(field_def.get("size", 1)),
                    int(field_def.get("scale", 0)),
                    case=case_type,
                )
            yield record
            continue

        for field_def in fields:
            name: str = field_def.get("name", "")

            # Skip FILLER and GROUP fields — they carry no data
            if name.upper() == "FILLER":
                continue
            if field_def.get("type", "").upper() == "GROUP":
                continue

            ftype: str = field_def.get("type", "ALPHANUMERIC")
            size: int = int(field_def.get("size", 1))
            scale: int = int(field_def.get("scale", 0))
            occurs: int | None = field_def.get("occurs")

            if occurs:
                # Repeating group — generate an array of sub-records
                children: list[dict] = field_def.get("children", [])
                occurrences: list[dict] = []
                for _ in range(occurs):
                    sub: dict = {}
                    for child in children:
                        child_name = child.get("name", "")
                        if child_name.upper() == "FILLER":
                            continue
                        child_type = child.get("type", "ALPHANUMERIC")
                        child_size = int(child.get("size", 1))
                        child_scale = int(child.get("scale", 0))
                        sub[child_name] = generate_value(
                            field_name=child_name,
                            field_type=child_type,
                            size=child_size,
                            scale=child_scale,
                            locale=locale,
                            seed=None,  # seed was already applied globally
                        )
                    occurrences.append(sub)
                record[name] = occurrences
            else:
                record[name] = generate_value(
                    field_name=name,
                    field_type=ftype,
                    size=size,
                    scale=scale,
                    locale=locale,
                    seed=None,  # seed already applied globally
                )

        yield record
