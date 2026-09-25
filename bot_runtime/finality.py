"""Settle persisted pending events by block, without downloading transaction bodies."""
import time
import logging
from block_validation import header


class Finalizer:
    def __init__(self, adapter, store):
        self.adapter,self.store=adapter,store
        self.next_tip=self.next_check=0
        self.safe=-1

    def safe_height(self):
        if time.monotonic()>=self.next_tip:
            self.safe=self.adapter.safe_tip()
            self.next_tip=time.monotonic()+5
        return self.safe

    def settle(self):
        if time.monotonic()<self.next_check:
            return
        self.next_check=time.monotonic()+5
        rows=self.store.db.execute("SELECT block_height,block_hash FROM events WHERE chain=? AND confirmation_state='pending' AND orphaned=0 AND finality_next_at<=? GROUP BY block_height,block_hash ORDER BY MIN(finality_next_at),block_height LIMIT 32",(self.adapter.name,time.time())).fetchall()
        if not rows:return
        safe=self.safe_height()
        hashes={}
        deadline=time.monotonic()+10
        for row in rows:
            if time.monotonic()>=deadline:
                break  # Yield to new hits after the current RPC completes.
            height=row['block_height']
            if height not in hashes:
                try:
                    hashes[height]=header(self.adapter,height)[0]  # Never trust cached headers for settlement.
                except Exception as exc:
                    hashes[height]=None
                    with self.store.db:
                        self.store.db.execute("UPDATE events SET finality_next_at=? WHERE chain=? AND block_height=? AND confirmation_state='pending'",(time.time()+30,self.adapter.name,height))
                    logging.getLogger(__name__).warning('confirmation deferred chain=%s height=%s error=%s',self.adapter.name,height,type(exc).__name__)
            if hashes[height] is None:
                continue
            canonical=hashes[height]==row['block_hash']
            if not canonical or height<=safe:
                with self.store.db:
                    self.store.db.execute("UPDATE events SET confirmation_state=?,orphaned=? WHERE chain=? AND block_height=? AND block_hash=? AND confirmation_state='pending'",
                        ('confirmed' if canonical else 'orphaned',int(not canonical),self.adapter.name,height,row['block_hash']))
            else:
                with self.store.db:
                    self.store.db.execute("UPDATE events SET finality_next_at=? WHERE chain=? AND block_height=? AND block_hash=? AND confirmation_state='pending'",(time.time()+5,self.adapter.name,height,row['block_hash']))
