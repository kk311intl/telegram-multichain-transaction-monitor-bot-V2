"""Bounded token lookups; transaction consumers only read persistent results."""
import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal
from threading import Lock, local

from scanner_adapters import build_adapter
from .decoder import metadata
from .market import DexScreenerOracle, MarketAssessment
from .security import SecurityDeferred
from .store import Store
from . import lookup_retry
from .settings import BotSettings


class TokenEnrichment:
    def __init__(self, path, owner, security, inflight, settings=None):
        self.settings = settings if settings is not None else BotSettings.from_env()
        self.path, self.owner, self.security, self.inflight = path, owner, security, inflight
        self.pool = ThreadPoolExecutor(max_workers=self.settings.token_workers, thread_name_prefix='token')
        self.lock = Lock()
        self.jobs = {}
        self.local = local()

    @staticmethod
    def key(chain, asset):
        return f'token_enriched:{chain}:{asset}'

    def request(self, adapter, store, asset):
        key = self.key(adapter.name, asset)
        cached = json.loads(store.meta(key, '{}'))
        if time.time() >= cached.get('next', 0):
            with self.lock:
                for done_key, future in list(self.jobs.items()):
                    if future.done():
                        del self.jobs[done_key]
                        try:
                            future.result()
                        except Exception as exc:
                            logging.getLogger(__name__).warning('token lookup deferred error=%s', type(exc).__name__)
                if key not in self.jobs and len(self.jobs) < self.settings.token_pending:
                    self.jobs[key] = self.pool.submit(self.lookup, adapter.name, adapter.config, asset)
        return cached if cached.get('complete') else None

    def metadata(self, adapter, store, asset):
        cached = self.request(adapter, store, asset)
        return (cached['symbol'], cached['decimals'], True) if cached else ('UNKNOWN', 0, False)

    def assessment(self, store, chain, asset):
        cached = store.token_market(chain, asset)
        if not cached:
            return None
        return MarketAssessment(bool(cached['valuable']),
            Decimal(cached['price_usd']) if cached['price_usd'] else None,
            Decimal(cached['liquidity_usd']) if cached['liquidity_usd'] else None, str(cached['reason']))

    def warnings(self, store, config, name, asset):
        warnings=[]
        chain='tron' if config['type']=='tron' else str(config['expected_chain_id'])
        security=json.loads(store.meta(f'goplus_token:{chain}:{asset}','{}'))
        state=security.get('state')
        if not security:
            warnings.append('security_check_unavailable')
        elif state in ('unsupported','unknown'):
            warnings.append('security_'+state)
        elif security.get('expires',0)<time.time():
            warnings.append('security_cache_stale')
        market=store.token_market(name,asset)
        if market and market['checked_at']<time.time()-86400:
            warnings.append('market_cache_stale')
        return warnings

    def lookup(self, name, config, asset):
        key = self.key(name, asset)
        self.inflight[key] = time.monotonic()
        store = None
        try:
            store = Store(self.path, self.owner)
            cached = json.loads(store.meta(key, '{}'))
            cached['next'] = time.time()+60
            try:
                if not hasattr(self.local, 'adapters'):
                    self.local.adapters = {}
                if name not in self.local.adapters:
                    self.local.adapters[name] = build_adapter(name, config)
                adapter = self.local.adapters[name]
                symbol, decimals, complete = metadata(adapter, store, asset)
                if complete:
                    risk = self.security.assess(store, config, asset)
                    market = store.token_market(name, asset)
                    if not risk and (not market or market['checked_at'] < time.time()-86400) and lookup_retry.ready(store, 'market', name, asset):
                        result = DexScreenerOracle().assess(config, asset)
                        if result.valuable is not None:
                            store.save_token_market(name, asset, result.valuable, result.price_usd, result.liquidity_usd, result.reason)
                            lookup_retry.clear(store, 'market', name, asset)
                        else:
                            lookup_retry.failed(store, 'market', name, asset)
                    cached.update(symbol=symbol, decimals=decimals, risk=risk, complete=True)
            except SecurityDeferred:
                cached['next'] = max(time.time()+3, float(store.meta('goplus_next_request', '0')))
            store.set_meta(key, json.dumps(cached))
        finally:
            if store is not None:
                store.db.close()
            self.inflight.pop(key, None)

    def close(self):
        self.pool.shutdown(wait=False, cancel_futures=True)
