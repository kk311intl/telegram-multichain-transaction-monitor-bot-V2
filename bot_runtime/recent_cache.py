"""Coordinator-only byte-bounded cache. Serialized values cannot be mutated by callers."""
import json
import threading
import time
from collections import OrderedDict


class RecentCache:
    def __init__(self, max_bytes=256*1024*1024, max_entries=50000, clock=time.monotonic):
        self.max_bytes,self.max_entries,self.clock=max_bytes,max_entries,clock
        self.items=OrderedDict()
        self.bytes=self.hits=self.misses=0
        self.lock=threading.Lock()

    def get(self, key, loader, ttl=300):
        now=self.clock()
        with self.lock:
            entry=self.items.pop(key,None)
            if entry:
                deadline,body=entry
                if deadline>now:
                    self.items[key]=entry
                    self.hits+=1
                    return json.loads(body)
                self.bytes-=len(body)
            self.misses+=1
        value=loader()  # Network calls never hold the shared cache lock.
        body=json.dumps(value,separators=(',',':')).encode()
        if len(body)<=self.max_bytes:
            with self.lock:
                previous=self.items.pop(key,None)
                if previous:self.bytes-=len(previous[1])
                self.items[key]=(self.clock()+ttl,body)
                self.bytes+=len(body)
                while self.bytes>self.max_bytes or len(self.items)>self.max_entries:
                    _,(_,removed)=self.items.popitem(last=False)
                    self.bytes-=len(removed)
        return value

    def stats(self):
        with self.lock:
            return dict(bytes=self.bytes,limit_bytes=self.max_bytes,entries=len(self.items),hits=self.hits,misses=self.misses)
