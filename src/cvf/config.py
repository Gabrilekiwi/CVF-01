"""Typed YAML configuration with environment-variable overlays."""

from __future__ import annotations

import copy
import math
import os
import sys
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from cvf.utils.validation import validate_canonical_symbol


class ConfigError(RuntimeError):
    """Raised when a configuration file cannot be loaded."""


class FrozenConfigModel(BaseModel):
    """Base for immutable, strict configuration sections."""

    model_config = ConfigDict(extra="forbid", frozen=True)


class AppConfig(FrozenConfigModel):
    name: str = "CVF-01"
    environment: str
    strategy_version: str
    status_interval_seconds: float = Field(gt=0)
    shutdown_timeout_seconds: float = Field(gt=0)
    paper_trading_only: Literal[True] = True


class MarketsConfig(FrozenConfigModel):
    canonical_symbols: list[str] = Field(min_length=1)
    max_open_positions: int = Field(ge=1, le=1)

    @field_validator("canonical_symbols")
    @classmethod
    def symbols_are_canonical_and_unique(cls, value: list[str]) -> list[str]:
        normalized = [validate_canonical_symbol(symbol) for symbol in value]
        if len(set(normalized)) != len(normalized):
            raise ValueError("canonical_symbols must not contain duplicates")
        return normalized


class TimingConfig(FrozenConfigModel):
    feature_update_seconds: float = Field(gt=0)
    signal_check_seconds: float = Field(gt=0)
    feature_windows_seconds: list[int] = Field(min_length=1)
    statistics_windows_seconds: list[int] = Field(min_length=1)
    expected_holding_minutes: tuple[int, int]
    time_stop_check_minutes: int = Field(gt=0)
    maximum_holding_minutes: int = Field(gt=0)

    @model_validator(mode="after")
    def validate_holding_range(self) -> TimingConfig:
        lower, upper = self.expected_holding_minutes
        if lower <= 0 or upper < lower:
            raise ValueError("expected_holding_minutes must be an increasing positive range")
        if self.maximum_holding_minutes < upper:
            raise ValueError("maximum_holding_minutes must cover expected_holding_minutes")
        return self


class ExchangeConnectionConfig(FrozenConfigModel):
    enabled: bool
    rest_url: str
    public_websocket_url: str
    symbols: dict[str, str] = Field(min_length=1)
    channels: list[str] = Field(min_length=1)
    rest_pollers: list[str] = Field(default_factory=list)
    heartbeat_timeout_seconds: float = Field(gt=0)
    connect_timeout_seconds: float = Field(default=10, gt=0)
    close_timeout_seconds: float = Field(default=5, gt=0)
    receive_timeout_seconds: float = Field(default=20, gt=0)
    rest_timeout_seconds: float = Field(default=10, gt=0)
    reconnect_initial_seconds: float = Field(gt=0)
    reconnect_max_seconds: float = Field(gt=0)
    reconnect_jitter_seconds: float = Field(ge=0)
    reconnect_stable_reset_seconds: float = Field(default=60, gt=0)
    open_interest_poll_seconds: float = Field(gt=0)
    book_snapshot_depth: int = Field(default=1_000, ge=100, le=1_000)
    book_buffer_events: int = Field(default=10_000, gt=0)

    @field_validator("symbols")
    @classmethod
    def validate_symbol_map(cls, value: dict[str, str]) -> dict[str, str]:
        normalized = {validate_canonical_symbol(key): venue.upper() for key, venue in value.items()}
        if len(set(normalized.values())) != len(normalized):
            raise ValueError("venue symbols must be unique within an exchange")
        return normalized

    @model_validator(mode="after")
    def validate_reconnect_bounds(self) -> ExchangeConnectionConfig:
        if self.reconnect_max_seconds < self.reconnect_initial_seconds:
            raise ValueError("reconnect_max_seconds cannot be below reconnect_initial_seconds")
        return self


class ExchangesConfig(FrozenConfigModel):
    binance: ExchangeConnectionConfig
    okx: ExchangeConnectionConfig


class FeaturesConfig(FrozenConfigModel):
    order_book_depth: int = Field(ge=1, le=20)
    order_book_level_weights: list[float] = Field(min_length=1)
    trade_imbalance_windows_seconds: list[int]
    price_impulse_windows_seconds: list[int]
    open_interest_windows_seconds: list[int]
    liquidation_windows_seconds: list[int]
    breakout_lookback_seconds: int = Field(gt=0)
    atr_period_seconds: int = Field(gt=0)
    zscore_lookback_seconds: int = Field(gt=0)
    spread_percentile_lookback_seconds: int = Field(gt=0)

    @model_validator(mode="after")
    def validate_depth_weights(self) -> FeaturesConfig:
        if len(self.order_book_level_weights) != self.order_book_depth:
            raise ValueError("order_book_level_weights must match order_book_depth")
        if any(weight <= 0 for weight in self.order_book_level_weights):
            raise ValueError("order-book weights must be positive")
        return self


class ExchangeFactorWeights(FrozenConfigModel):
    taker_imbalance: float = Field(ge=0)
    ofi: float = Field(ge=0)
    price_impulse: float = Field(ge=0)
    open_interest_impulse: float = Field(ge=0)
    liquidation_impulse: float = Field(ge=0)

    @model_validator(mode="after")
    def weights_sum_to_one(self) -> ExchangeFactorWeights:
        if not math.isclose(sum(self.model_dump().values()), 1.0, abs_tol=1e-9):
            raise ValueError("exchange factor weights must sum to 1.0")
        return self


class VenueWeights(FrozenConfigModel):
    binance: float = Field(ge=0)
    okx: float = Field(ge=0)
    cross_exchange_confirmation: float = Field(ge=0)

    @model_validator(mode="after")
    def weights_sum_to_one(self) -> VenueWeights:
        if not math.isclose(sum(self.model_dump().values()), 1.0, abs_tol=1e-9):
            raise ValueError("venue weights must sum to 1.0")
        return self


class ScoringConfig(FrozenConfigModel):
    exchange_factor_weights: ExchangeFactorWeights
    venue_weights: VenueWeights
    long_entry_threshold: float
    short_entry_threshold: float
    liquidity_penalty_max: float = Field(ge=0)
    crowding_penalty_max: float = Field(ge=0)
    data_health_penalty_max: float = Field(ge=0)
    divergence_penalty_max: float = Field(ge=0)

    @model_validator(mode="after")
    def thresholds_are_ordered(self) -> ScoringConfig:
        if self.short_entry_threshold >= self.long_entry_threshold:
            raise ValueError("short_entry_threshold must be below long_entry_threshold")
        return self


class SignalRulesConfig(FrozenConfigModel):
    taker_imbalance_15s_min: float = Field(ge=0, le=1)
    taker_imbalance_5s_confirmation: float = Field(ge=0, le=1)
    ofi_zscore_confirmation: float = Field(gt=0)
    open_interest_zscore_confirmation: float = Field(gt=0)
    other_venue_open_interest_zscore_floor: float
    maximum_recent_move_atr: float = Field(gt=0)
    maximum_cross_venue_spread_zscore: float = Field(gt=0)
    maximum_crowding_zscore: float = Field(gt=0)
    signal_ttl_seconds: int = Field(gt=0)


class FeeConfig(FrozenConfigModel):
    maker_bps: float = Field(ge=0)
    taker_bps: float = Field(ge=0)


class FeesConfig(FrozenConfigModel):
    binance: FeeConfig
    okx: FeeConfig


class SlippageConfig(FrozenConfigModel):
    order_book_levels: int = Field(ge=1)
    maximum_profit_share: float = Field(gt=0, le=1)
    fallback_bps: float = Field(ge=0)


class ExecutionConfig(FrozenConfigModel):
    fees: FeesConfig
    slippage: SlippageConfig
    latency_penalty_bps_per_100ms: float = Field(ge=0)
    depth_penalty_bps: float = Field(ge=0)


class RiskConfig(FrozenConfigModel):
    initial_balance_usdt: float = Field(gt=0)
    risk_per_trade_fraction: float = Field(gt=0, lt=1)
    daily_loss_limit_fraction: float = Field(gt=0, lt=1)
    maximum_daily_trades: int = Field(gt=0)
    consecutive_loss_limit: int = Field(gt=0)
    maximum_open_positions: int = Field(ge=1, le=1)
    maximum_notional_leverage: float = Field(gt=0)
    margin_mode: Literal["isolated"]
    allow_loss_adding: Literal[False] = False
    allow_martingale: Literal[False] = False


class ExitsConfig(FrozenConfigModel):
    initial_stop_atr_multiple: float = Field(gt=0)
    take_profit_1_atr_multiple: float = Field(gt=0)
    take_profit_2_atr_multiple: float = Field(gt=0)
    take_profit_1_fraction: float = Field(gt=0, lt=1)
    breakeven_offset_bps: float = Field(ge=0)
    time_stop_minutes: int = Field(gt=0)
    time_stop_minimum_profit_atr: float = Field(ge=0)
    maximum_holding_minutes: int = Field(gt=0)
    reverse_score_exit_threshold: float = Field(gt=0)

    @model_validator(mode="after")
    def take_profits_are_ordered(self) -> ExitsConfig:
        if self.take_profit_2_atr_multiple <= self.take_profit_1_atr_multiple:
            raise ValueError("take_profit_2_atr_multiple must exceed take_profit_1_atr_multiple")
        return self


class HealthConfig(FrozenConfigModel):
    maximum_core_latency_ms: int = Field(gt=0)
    stale_after_ms: int = Field(gt=0)
    clock_skew_warning_ms: int = Field(gt=0)
    duplicate_cache_size: int = Field(gt=0)
    duplicate_ttl_seconds: float = Field(default=300, gt=0)
    open_interest_stale_after_ms: int = Field(default=15_000, gt=0)
    channel_stale_after_ms: dict[str, int | None] = Field(default_factory=dict)
    order_book_gap_forces_resync: bool
    block_entry_statuses: list[str]

    @field_validator("channel_stale_after_ms")
    @classmethod
    def channel_stale_thresholds_are_valid(
        cls,
        value: dict[str, int | None],
    ) -> dict[str, int | None]:
        for channel, threshold in value.items():
            if not channel or channel.isspace():
                raise ValueError("health channel names cannot be empty")
            if threshold is not None and threshold <= 0:
                raise ValueError("channel stale thresholds must be positive or null")
        return value


class StorageConfig(FrozenConfigModel):
    database_url: str
    raw_data_path: Path
    processed_data_path: Path
    parquet_batch_rows: int = Field(gt=0)
    parquet_flush_seconds: float = Field(gt=0)
    parquet_queue_capacity: int = Field(default=50_000, gt=0)
    database_batch_rows: int = Field(gt=0)
    database_flush_seconds: float = Field(gt=0)


class PipelineConfig(FrozenConfigModel):
    consumer_queue_capacity: int = Field(default=10_000, gt=0)
    backpressure_policy: Literal["block"] = "block"


class LoggingConfig(FrozenConfigModel):
    level: str
    json_output: bool

    @field_validator("level")
    @classmethod
    def normalize_level(cls, value: str) -> str:
        normalized = value.upper()
        if normalized not in {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}:
            raise ValueError(f"unsupported logging level: {value}")
        return normalized


class ReplayConfig(FrozenConfigModel):
    preserve_receive_timestamps: bool
    default_speed: float = Field(gt=0)
    deterministic_seed: int


class Settings(FrozenConfigModel):
    app: AppConfig
    markets: MarketsConfig
    timing: TimingConfig
    exchanges: ExchangesConfig
    features: FeaturesConfig
    scoring: ScoringConfig
    signal_rules: SignalRulesConfig
    execution: ExecutionConfig
    risk: RiskConfig
    exits: ExitsConfig
    health: HealthConfig
    storage: StorageConfig
    pipeline: PipelineConfig
    logging: LoggingConfig
    replay: ReplayConfig

    @model_validator(mode="after")
    def validate_cross_section_invariants(self) -> Settings:
        expected = set(self.markets.canonical_symbols)
        for venue, exchange_config in (
            ("binance", self.exchanges.binance),
            ("okx", self.exchanges.okx),
        ):
            if set(exchange_config.symbols) != expected:
                raise ValueError(f"{venue} symbol map must exactly match markets.canonical_symbols")
        if self.risk.maximum_open_positions != self.markets.max_open_positions:
            raise ValueError("market and risk maximum_open_positions must agree")
        if self.execution.slippage.order_book_levels > self.features.order_book_depth:
            raise ValueError("slippage depth cannot exceed the stored order-book depth")
        return self


def _project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def default_config_path() -> Path:
    """Return the source-tree or installed default configuration path."""

    source_path = _project_root() / "config" / "default.yaml"
    if source_path.is_file():
        return source_path
    installed_path = Path(sys.prefix) / "share" / "cvf-01" / "default.yaml"
    if installed_path.is_file():
        return installed_path
    raise ConfigError(
        "cannot locate config/default.yaml in the source tree or installed data files"
    )


def _read_yaml(path: Path) -> dict[str, Any]:
    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise ConfigError(f"cannot read configuration file {path}: {exc}") from exc
    except yaml.YAMLError as exc:
        raise ConfigError(f"invalid YAML in {path}: {exc}") from exc
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise ConfigError(f"configuration root must be a mapping: {path}")
    return raw


def _deep_merge(base: dict[str, Any], overlay: Mapping[str, Any]) -> dict[str, Any]:
    merged = copy.deepcopy(base)
    for key, value in overlay.items():
        if isinstance(value, Mapping) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = copy.deepcopy(value)
    return merged


def _apply_environment_overrides(
    data: dict[str, Any], environ: Mapping[str, str], prefix: str = "CVF__"
) -> dict[str, Any]:
    result = copy.deepcopy(data)
    for name, raw_value in environ.items():
        if not name.startswith(prefix):
            continue
        path = [segment.lower() for segment in name[len(prefix) :].split("__") if segment]
        if not path:
            continue
        cursor = result
        for segment in path[:-1]:
            current = cursor.setdefault(segment, {})
            if not isinstance(current, dict):
                raise ConfigError(f"environment override {name} crosses a non-mapping setting")
            cursor = current
        try:
            cursor[path[-1]] = yaml.safe_load(raw_value)
        except yaml.YAMLError as exc:
            raise ConfigError(f"invalid YAML scalar in environment override {name}") from exc
    return result


def load_settings(
    config_path: str | Path | None = None,
    *,
    environ: Mapping[str, str] | None = None,
) -> Settings:
    """Load defaults, an optional YAML overlay, and ``CVF__`` environment overrides."""

    environment = os.environ if environ is None else environ
    base_path = default_config_path().resolve()
    data = _read_yaml(base_path)

    selected = config_path or environment.get("CVF_CONFIG_FILE")
    if selected:
        overlay_path = Path(selected)
        if not overlay_path.is_absolute():
            overlay_path = (Path.cwd() / overlay_path).resolve()
        else:
            overlay_path = overlay_path.resolve()
        if overlay_path != base_path:
            data = _deep_merge(data, _read_yaml(overlay_path))

    data = _apply_environment_overrides(data, environment)
    return Settings.model_validate(data)
