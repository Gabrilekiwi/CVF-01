"""Incremental, content-bound summaries for feature source lineage."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

from pydantic import BaseModel

from cvf.utils.fingerprint import (
    canonical_json,
    canonicalize_for_hash,
    model_payload,
    sha256_text,
)

_DIGEST_MODULUS = 1 << 256
_EMPTY_SUM = 0
_EMPTY_XOR = 0


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("source-lineage timestamps must be timezone-aware")
    return value.astimezone(UTC)


def semantic_source_digest(timestamp: datetime, value: object) -> str:
    """Hash one semantic input while excluding nondeterministic processing time."""

    payload: object
    if isinstance(value, BaseModel):
        model_value = model_payload(value)
        model_value.pop("normalization_timestamp", None)
        payload = model_value
    else:
        payload = value
    return sha256_text(
        canonical_json(
            canonicalize_for_hash(
                {
                    "domain": "cvf-feature-source-leaf-v1",
                    "timestamp": _utc(timestamp),
                    "value": payload,
                }
            )
        )
    )


@dataclass(frozen=True, slots=True)
class SourceLineage:
    """Mergeable multiset commitment over exact semantic source payloads.

    The count, modular sum, and xor of independently computed SHA-256 item
    digests let bounded windows be combined or subtracted without rescanning
    every payload. The final SHA-256 commitment also binds the time extent.
    """

    count: int = 0
    digest_sum: int = _EMPTY_SUM
    digest_xor: int = _EMPTY_XOR
    oldest_timestamp: datetime | None = None
    newest_timestamp: datetime | None = None

    def __post_init__(self) -> None:
        if self.count < 0:
            raise ValueError("source-lineage count cannot be negative")
        if not 0 <= self.digest_sum < _DIGEST_MODULUS:
            raise ValueError("source-lineage digest sum is outside uint256")
        if not 0 <= self.digest_xor < _DIGEST_MODULUS:
            raise ValueError("source-lineage digest xor is outside uint256")
        if self.count == 0:
            if (
                self.digest_sum != _EMPTY_SUM
                or self.digest_xor != _EMPTY_XOR
                or self.oldest_timestamp is not None
                or self.newest_timestamp is not None
            ):
                raise ValueError("empty source lineage cannot contain metadata")
            return
        if self.oldest_timestamp is None or self.newest_timestamp is None:
            raise ValueError("nonempty source lineage requires timestamp bounds")
        oldest = _utc(self.oldest_timestamp)
        newest = _utc(self.newest_timestamp)
        if newest < oldest:
            raise ValueError("source-lineage timestamps are reversed")
        object.__setattr__(self, "oldest_timestamp", oldest)
        object.__setattr__(self, "newest_timestamp", newest)

    @classmethod
    def from_digest(cls, timestamp: datetime, digest: str) -> SourceLineage:
        if len(digest) != 64:
            raise ValueError("source-lineage item digest must be SHA-256")
        try:
            value = int(digest, 16)
        except ValueError as exc:
            raise ValueError(
                "source-lineage item digest must be hexadecimal"
            ) from exc
        at = _utc(timestamp)
        return cls(
            count=1,
            digest_sum=value,
            digest_xor=value,
            oldest_timestamp=at,
            newest_timestamp=at,
        )

    @classmethod
    def from_source(cls, timestamp: datetime, value: object) -> SourceLineage:
        return cls.from_digest(
            timestamp,
            semantic_source_digest(timestamp, value),
        )

    def combine(self, *others: SourceLineage) -> SourceLineage:
        count = self.count
        digest_sum = self.digest_sum
        digest_xor = self.digest_xor
        timestamps = [
            value
            for value in (self.oldest_timestamp, self.newest_timestamp)
            if value is not None
        ]
        for other in others:
            count += other.count
            digest_sum = (digest_sum + other.digest_sum) % _DIGEST_MODULUS
            digest_xor ^= other.digest_xor
            timestamps.extend(
                value
                for value in (
                    other.oldest_timestamp,
                    other.newest_timestamp,
                )
                if value is not None
            )
        if count == 0:
            return SourceLineage()
        return SourceLineage(
            count=count,
            digest_sum=digest_sum,
            digest_xor=digest_xor,
            oldest_timestamp=min(timestamps),
            newest_timestamp=max(timestamps),
        )

    @property
    def fingerprint(self) -> str:
        return sha256_text(
            canonical_json(
                {
                    "algorithm": "sha256-sum-xor-v1",
                    "count": self.count,
                    "digest_sum": f"{self.digest_sum:064x}",
                    "digest_xor": f"{self.digest_xor:064x}",
                    "oldest_timestamp": (
                        None
                        if self.oldest_timestamp is None
                        else self.oldest_timestamp.isoformat()
                    ),
                    "newest_timestamp": (
                        None
                        if self.newest_timestamp is None
                        else self.newest_timestamp.isoformat()
                    ),
                }
            )
        )
