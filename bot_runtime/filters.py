from __future__ import annotations

from decimal import Decimal
from typing import Any

from .common import Event
from .market import MarketAssessment


class ScamFilter:
    """Conservative, auditable anti-spam rules; this is not a fraud oracle."""

    def __init__(self, chain_config: dict[str, Any]):
        self.config = chain_config

    def evaluate(
        self, event: Event, market: MarketAssessment | None = None
    ) -> tuple[bool, str]:
        if event.amount_raw <= 0:
            return True, "zero_value"
        # An explicit transaction-risk signal is independent of DEX
        # price availability. Fail-open applies only to missing market evidence.
        if event.source_risk:
            return True, event.source_risk
        if event.asset_id != "native" and not event.metadata_complete:
            # Without decimals there is no safe human amount or USD threshold.
            # Keep the event fail-open; a later metadata pass can
            # replace the metadata and re-evaluate the filter in place.
            return False, ""
        if event.asset_id == "native":
            threshold = Decimal(str(self.config.get("min_native", "0")))
            if event.amount < threshold:
                return True, "native_below_minimum"
            return False, ""
        if market is None or market.valuable is None:
            # An unavailable DEX lookup must never be treated as evidence that
            # an otherwise unknown token is worthless or fraudulent.
            return False, "market_check_unavailable"
        if not market.valuable:
            return True, market.reason or "no_market_value"
        minimum_usd = Decimal(str(self.config.get("market_min_transfer_usd", "1")))
        if market.price_usd is not None and event.amount * market.price_usd < minimum_usd:
            return True, "token_value_below_usd_minimum"
        return False, ""
