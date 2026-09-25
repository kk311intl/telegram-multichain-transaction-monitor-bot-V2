"""Free GoPlus signals with a shared budget and persistent contract cache."""
import json
import threading
import time
import urllib.error
from urllib.parse import urlencode
from monitor.rpc import RequestStats
from .market import get_json
from . import lookup_retry


def security_chain(config):
    return str(config['expected_chain_id']) if config['type']=='evm' else config['type']


class SecurityDeferred(Exception):
    """Keep the hit pending until free API budget is available."""


class TokenSecurity:
    def __init__(self):
        self.lock = threading.Lock()

    def request(self, store, path):
        with self.lock:
            now = time.time()
            if now < float(store.meta('goplus_next_request', '0')):
                raise SecurityDeferred()
            store.set_meta('goplus_next_request', now + 3)
        try:
            result = get_json('https://api.gopluslabs.io/api/v1/' + path, {}, 12, RequestStats())
            if not isinstance(result, dict) or result.get('code') != 1:
                raise ValueError('GoPlus rejected request')
            return result['result']
        except Exception as exc:
            with self.lock:
                delay = 60
                if isinstance(exc, urllib.error.HTTPError) and exc.code == 429:
                    try: delay = max(60, min(3600, int(exc.headers.get('Retry-After', '60'))))
                    except (ValueError, TypeError): pass
                store.set_meta('goplus_next_request', max(time.time()+delay, float(store.meta('goplus_next_request', '0'))))
            raise

    def assess(self, store, config, asset):
        chain = security_chain(config)
        name = f'goplus_token:{chain}:{asset}'
        cached = json.loads(store.meta(name, '{}'))
        if cached and time.time() < cached['expires']:
            return cached.get('risk', '')
        if not lookup_retry.ready(store, 'security', chain, asset):
            return cached.get('risk', '')
        try:
            supported = json.loads(store.meta('goplus_supported', '{}'))
            if not supported or time.time() >= supported['expires']:
                values = self.request(store, 'supported_chains')
                if not isinstance(values, list) or not values or any(not isinstance(v,dict) or 'id' not in v for v in values):
                    raise ValueError('invalid GoPlus chain list')
                supported = dict(ids=[str(v['id']) for v in values], expires=time.time()+86400)
                store.set_meta('goplus_supported', json.dumps(supported))
            if chain not in supported['ids'] and chain != 'solana':
                state, risk, ttl = 'unsupported', '', 86400
            else:
                route = 'solana/token_security' if chain == 'solana' else 'token_security/'+chain
                values = self.request(store, route + '?' + urlencode({'contract_addresses':asset}))
                if not isinstance(values, dict):
                    raise ValueError('invalid GoPlus token response')
                data = values.get(asset) if config['type'] != 'evm' else next((v for k,v in values.items() if k.lower()==asset.lower()), None)
                if data is None or data == {}:
                    state, risk, ttl = 'unknown', '', 3600
                elif not isinstance(data, dict):
                    raise ValueError('invalid GoPlus token data')
                else:
                    fake = data.get('fake_token')
                    risk = ('goplus_fake_token' if isinstance(fake,dict) and str(fake.get('value')) == '1'
                            else 'goplus_honeypot' if str(data.get('is_honeypot')) == '1' else '')
                    state, ttl = ('risk' if risk else 'no_flag'), 86400
                    if chain == 'solana' and not risk and fake is None and 'is_honeypot' not in data:
                        state = 'unknown'
            store.set_meta(name, json.dumps(dict(state=state,risk=risk,checked_at=time.time(),expires=time.time()+ttl)))
            lookup_retry.clear(store, 'security', chain, asset)
            return risk
        except SecurityDeferred:
            if cached:
                return cached.get('risk', '')
            raise
        except Exception:
            lookup_retry.failed(store, 'security', chain, asset)
            return cached.get('risk', '')
