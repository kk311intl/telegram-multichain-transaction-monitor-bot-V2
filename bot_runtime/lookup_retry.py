"""Persist per-token failure cooldowns without treating failure as a verdict."""
import json
import time


def key(kind, chain, asset):
    return f'lookup_retry:{kind}:{chain}:{asset}'


def ready(store, kind, chain, asset):
    state = json.loads(store.meta(key(kind, chain, asset), '{}'))
    return time.time() >= state.get('next', 0)


def failed(store, kind, chain, asset):
    name = key(kind, chain, asset)
    state = json.loads(store.meta(name, '{}'))
    attempts = min(7, state.get('attempts', 0) + 1)
    store.set_meta(name, json.dumps({'attempts': attempts,
        'next': time.time() + min(3600, 60 * 2 ** (attempts - 1))}))


def clear(store, kind, chain, asset):
    with store.db:
        store.db.execute('DELETE FROM meta WHERE key=?', (key(kind, chain, asset),))
