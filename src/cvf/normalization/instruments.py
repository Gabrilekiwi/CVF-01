"""Derivative contract metadata used for unit-safe normalization."""

from __future__ import annotations

from decimal import Decimal
from typing import Literal

from pydantic import computed_field, model_validator

from cvf.models.common import FrozenModel, PositiveDecimal
from cvf.models.enums import Exchange
from cvf.utils.validation import validate_canonical_symbol


class ContractSpecification(FrozenModel):
    """Public contract metadata required to convert contracts into base units."""

    exchange: Exchange
    canonical_symbol: str
    venue_symbol: str
    contract_type: Literal["linear"]
    contract_value: PositiveDecimal
    contract_multiplier: PositiveDecimal
    contract_value_currency: str

    @model_validator(mode="after")
    def validate_linear_value_currency(self) -> ContractSpecification:
        canonical = validate_canonical_symbol(self.canonical_symbol)
        base_currency = canonical.split("-", maxsplit=1)[0]
        if self.contract_value_currency.upper() != base_currency:
            raise ValueError(
                "linear contract value currency must match the canonical base currency"
            )
        return self

    @computed_field  # type: ignore[prop-decorator]
    @property
    def base_units_per_contract(self) -> Decimal:
        """Base-asset quantity represented by one contract."""

        return self.contract_value * self.contract_multiplier

    def contracts_to_base(self, contracts: Decimal) -> Decimal:
        """Convert a contract count into normalized base-asset quantity."""

        return contracts * self.base_units_per_contract
