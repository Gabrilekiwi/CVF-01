"""Validation helpers shared by config, models, and connectors."""

from __future__ import annotations

import re

_CANONICAL_PERPETUAL_PATTERN = re.compile(r"^[A-Z0-9]+-[A-Z0-9]+-PERP$")


def validate_canonical_symbol(value: str, *, allow_wildcard: bool = False) -> str:
    """Normalize and validate a canonical perpetual symbol."""

    normalized = value.strip().upper()
    if allow_wildcard and normalized == "*":
        return normalized
    if not _CANONICAL_PERPETUAL_PATTERN.fullmatch(normalized):
        raise ValueError(
            f"invalid canonical perpetual symbol {value!r}; expected BASE-QUOTE-PERP"
        )
    return normalized

