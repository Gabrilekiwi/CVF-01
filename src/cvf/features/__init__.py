"""Typed, bounded Phase-3 feature state foundations."""

from cvf.features.availability import FeatureAvailability, evaluate_availability
from cvf.features.models import (
    FeatureSnapshot,
    FeatureUnavailableCode,
    FeatureUnavailableReason,
)
from cvf.features.pipeline import FeatureStatePipeline, FeatureStatePipelineStats
from cvf.features.rolling import (
    AppendStatus,
    BoundedTimeWindow,
    LateEventPolicy,
    TimedValue,
    WindowStats,
)
from cvf.features.state import (
    FeatureBookView,
    FeatureOrderBookState,
    MarketStateStore,
    StateUpdateResult,
    StateUpdateStatus,
    VenueSymbolState,
)

__all__ = [
    "AppendStatus",
    "BoundedTimeWindow",
    "FeatureAvailability",
    "FeatureBookView",
    "FeatureOrderBookState",
    "FeatureSnapshot",
    "FeatureStatePipeline",
    "FeatureStatePipelineStats",
    "FeatureUnavailableCode",
    "FeatureUnavailableReason",
    "LateEventPolicy",
    "MarketStateStore",
    "StateUpdateResult",
    "StateUpdateStatus",
    "TimedValue",
    "VenueSymbolState",
    "WindowStats",
    "evaluate_availability",
]
