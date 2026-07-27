"""Canonical hashes for immutable runtime configuration and persisted records."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from decimal import Decimal
from enum import Enum
from typing import Any
from uuid import UUID

from pydantic import BaseModel


def canonical_json(value: object) -> str:
    """Encode JSON-compatible values with stable ordering and no insignificant space."""

    return json.dumps(
        value,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )


def sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def model_payload(model: BaseModel) -> dict[str, Any]:
    """Return a validation-compatible JSON payload without computed fields."""

    return model.model_dump(mode="json", exclude_computed_fields=True)


def model_payload_json(model: BaseModel) -> str:
    return canonical_json(model_payload(model))


def model_payload_sha256(model: BaseModel) -> str:
    return sha256_text(model_payload_json(model))


def settings_fingerprint(settings: BaseModel) -> str:
    return sha256_text(canonical_json(settings.model_dump(mode="json")))


def canonicalize_for_hash(value: object) -> object:
    """Normalize nested audit values into canonical JSON-compatible values."""

    if isinstance(value, BaseModel):
        return canonicalize_for_hash(model_payload(value))
    if isinstance(value, Mapping):
        return {
            str(key): canonicalize_for_hash(item)
            for key, item in value.items()
        }
    if isinstance(value, Sequence) and not isinstance(
        value,
        (str, bytes, bytearray),
    ):
        return [canonicalize_for_hash(item) for item in value]
    if isinstance(value, bytes):
        return {
            "bytes_sha256": hashlib.sha256(value).hexdigest(),
            "length": len(value),
        }
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat()
    if isinstance(value, (Decimal, UUID)):
        return str(value)
    if isinstance(value, Enum):
        return value.value
    return value
