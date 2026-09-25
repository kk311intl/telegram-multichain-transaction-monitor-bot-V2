"""Bounded asynchronous RPC verification, never holds the heartbeat connection."""
import threading
from concurrent.futures import ThreadPoolExecutor
from block_validation import header


class CheckpointVerifier:
    def __init__(self, adapters):
        self.adapters = adapters
        self.pool = ThreadPoolExecutor(max_workers=4, thread_name_prefix='checkpoint')
        self.lock = threading.Lock()
        self.pending = {}

    def __call__(self, chain, height, digest, epoch):
        accepted = None
        with self.lock:
            pending = self.pending.get(chain)
            if pending and pending[3].done():
                h, d, e, future = self.pending.pop(chain)
                try:
                    if future.result()[0] == d and e == epoch:
                        accepted = (h, d)
                except Exception:
                    pass
            if chain not in self.pending:
                self.pending[chain] = (height, digest, epoch, self.pool.submit(header, self.adapters[chain], height))
        return accepted
