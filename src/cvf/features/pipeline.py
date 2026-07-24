"""Normalized-event consumer that owns Phase-3 bounded state."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass

from cvf.features.state import MarketStateStore, StateUpdateResult, StateUpdateStatus
from cvf.normalization.common import NormalizedMarketEvent


@dataclass(frozen=True, slots=True)
class FeatureStatePipelineStats:
    accepted_events: int
    rejected_events: int
    status_counts: dict[str, int]
    state_count: int
    retained_items: int


class FeatureStatePipeline:
    """Async event-bus adapter around the deterministic synchronous state store."""

    def __init__(self, store: MarketStateStore) -> None:
        self.store = store
        self._status_counts: Counter[str] = Counter()
        self._accepted = 0
        self._rejected = 0

    @property
    def stats(self) -> FeatureStatePipelineStats:
        return FeatureStatePipelineStats(
            accepted_events=self._accepted,
            rejected_events=self._rejected,
            status_counts=dict(self._status_counts),
            state_count=len(self.store.states),
            retained_items=sum(state.total_items() for state in self.store.states),
        )

    async def consume(self, event: NormalizedMarketEvent) -> None:
        result = self.store.ingest(event)
        self._record(result)

    def _record(self, result: StateUpdateResult) -> None:
        self._status_counts[result.status.value] += 1
        if result.status is StateUpdateStatus.ACCEPTED:
            self._accepted += 1
        else:
            self._rejected += 1
