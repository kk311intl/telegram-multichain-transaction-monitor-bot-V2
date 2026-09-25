from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any

from monitor.rpc import RequestStats
import json
import urllib.request

def get_json(url, headers, timeout, stats):
    try:
        request = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(request, timeout=timeout) as response:
            body = response.read(4*1024*1024+1)
        if len(body) > 4*1024*1024:
            raise ValueError("market response too large")
        result = json.loads(body)
        stats.record(True,len(body))
        return result
    except Exception:
        stats.record(False)
        raise



@dataclass(frozen=True)
class MarketAssessment:
    valuable: bool | None
    price_usd: Decimal | None = None
    liquidity_usd: Decimal | None = None
    reason: str = ""


class DexScreenerOracle:
    """Live market-value signal. It is not a fraud or contract-safety oracle."""

    def __init__(self) -> None:
        self._stats: dict[str, RequestStats] = {}

    def stats_for(self, chain: str) -> RequestStats:
        return self._stats.setdefault(chain, RequestStats())

    @staticmethod
    def _decimal(value: Any) -> Decimal | None:
        try:
            parsed = Decimal(str(value))
            return parsed if parsed.is_finite() else None
        except (InvalidOperation, TypeError, ValueError):
            return None

    def assess(self, config: dict[str, Any], contract: str) -> MarketAssessment:
        chain = str(config.get("market_chain", "")).strip()
        if not chain:
            return MarketAssessment(None, reason="market_chain_unavailable")

        try:
            result = get_json(
                f"https://api.dexscreener.com/token-pairs/v1/{chain}/{contract}",
                {"User-Agent":"Mozilla/5.0 (compatible; CryptoAddressMonitor/2)", "Accept":"application/json"}, timeout=12,
                stats=self.stats_for(chain),
            )
            if not isinstance(result, list):
                raise RuntimeError("DEX Screener returned an invalid response")
            pairs = result
            def same_address(value):
                return value.lower() == contract.lower() if config.get('type', 'evm') == 'evm' else value == contract
            candidates: list[tuple[Decimal, Decimal]] = []
            for pair in pairs:
                if pair.get("chainId") != chain:
                    continue
                base = str(pair.get("baseToken", {}).get("address", ""))
                quote = str(pair.get("quoteToken", {}).get("address", ""))
                price = None
                if same_address(base):
                    price = self._decimal(pair.get("priceUsd"))
                elif same_address(quote):
                    base_usd = self._decimal(pair.get("priceUsd"))
                    base_in_quote = self._decimal(pair.get("priceNative"))
                    if base_usd is not None and base_in_quote is not None and base_in_quote > 0:
                        price = base_usd / base_in_quote
                else:
                    continue
                liquidity = self._decimal((pair.get("liquidity") or {}).get("usd"))
                if price is not None and liquidity is not None and price > 0 and liquidity > 0:
                    candidates.append((liquidity, price))
            minimum = Decimal(str(config.get("market_min_liquidity_usd", "10000")))
            if not candidates:
                assessment = MarketAssessment(False, reason="no_market_value")
            else:
                liquidity, price = max(candidates)
                assessment = MarketAssessment(
                    liquidity >= minimum, price, liquidity,
                    "" if liquidity >= minimum else "market_liquidity_below_minimum",
                )
        except Exception:
            # Fail open: an unavailable price service is not proof that a token is worthless.
            assessment = MarketAssessment(None, reason="market_check_unavailable")
        return assessment
