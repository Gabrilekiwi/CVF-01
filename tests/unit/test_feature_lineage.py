"""Content-bound incremental source-lineage commitments."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from cvf.features.lineage import SourceLineage
from cvf.models import Exchange, MarkPrice

NOW = datetime(2026, 7, 24, 12, 0, tzinfo=UTC)


def mark_price(
    *,
    at: datetime,
    normalization_at: datetime,
    sequence: int,
    price: str,
) -> MarkPrice:
    return MarkPrice(
        exchange=Exchange.BINANCE,
        symbol="BTC-USDT-PERP",
        exchange_timestamp=at,
        local_receive_timestamp=at + timedelta(milliseconds=1),
        normalization_timestamp=normalization_at,
        sequence_id=sequence,
        raw_payload_reference=f"raw://{sequence:032x}",
        mark_price=Decimal(price),
    )


def test_lineage_merge_is_order_independent_and_binds_multiplicity() -> None:
    first = SourceLineage.from_source(
        NOW,
        mark_price(
            at=NOW,
            normalization_at=NOW + timedelta(milliseconds=2),
            sequence=1,
            price="100",
        ),
    )
    second = SourceLineage.from_source(
        NOW + timedelta(seconds=1),
        mark_price(
            at=NOW + timedelta(seconds=1),
            normalization_at=NOW + timedelta(seconds=1, milliseconds=2),
            sequence=2,
            price="101",
        ),
    )

    forward = first.combine(second)
    reverse = second.combine(first)

    assert forward == reverse
    assert forward.fingerprint == reverse.fingerprint
    assert forward.count == 2
    assert forward.oldest_timestamp == NOW
    assert forward.newest_timestamp == NOW + timedelta(seconds=1)
    assert first.combine(first).fingerprint != first.fingerprint


def test_lineage_ignores_only_nondeterministic_normalization_timestamp() -> None:
    first = mark_price(
        at=NOW,
        normalization_at=NOW + timedelta(milliseconds=2),
        sequence=1,
        price="100",
    )
    renormalized = first.model_copy(
        update={
            "normalization_timestamp": NOW + timedelta(seconds=10),
        }
    )
    changed_content = first.model_copy(
        update={"mark_price": Decimal("101")}
    )

    baseline = SourceLineage.from_source(NOW, first)

    assert (
        SourceLineage.from_source(NOW, renormalized).fingerprint
        == baseline.fingerprint
    )
    assert (
        SourceLineage.from_source(NOW, changed_content).fingerprint
        != baseline.fingerprint
    )

