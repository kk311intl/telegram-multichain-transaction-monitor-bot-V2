"""Per-chat notification pacing and independent server flood deadlines."""
import math
import json
import sqlite3
import threading
import time
from contextlib import closing, contextmanager
from .settings import BotSettings


class TelegramThrottle(Exception):
    """Local cooldown, not a failed delivery attempt."""
    def __init__(self, message='', retry_after=1):
        super().__init__(message)
        self.retry_after = max(1, retry_after)


class TelegramRate:
    def __init__(self, path=None, settings=None):
        self.settings = settings if settings is not None else BotSettings.from_env()
        self.path = path
        self.lock = threading.Lock()
        self.wire_lock = threading.Lock()
        self.local = threading.local()
        self.wire_global = self.next_global = 0.0
        self.waiters = {}
        self.until = self.poll_until = 0.0
        self.chat_until = {}
        self.background_next = {}
        if path:
            with closing(sqlite3.connect(path)) as db:
                for key, attr in [('telegram_cooldown_until','until'), ('telegram_poll_cooldown_until','poll_until')]:
                    row = db.execute('SELECT value FROM meta WHERE key=?',(key,)).fetchone()
                    setattr(self, attr, float(row[0]) if row else 0.0)
                for key, attr in [('telegram_chat_cooldowns','chat_until'), ('telegram_background_next','background_next')]:
                    row = db.execute('SELECT value FROM meta WHERE key=?',(key,)).fetchone()
                    setattr(self, attr, json.loads(row[0]) if row else {})
                row = db.execute("SELECT value FROM meta WHERE key='telegram_global_next'").fetchone()
                if row:
                    self.next_global = self.wire_global = time.monotonic()+max(0,float(row[0])-time.time())
        # Persist wall-clock deadlines, but never let a live clock correction end a wait.
        wall, mono = time.time(), time.monotonic()
        self.wall_offset = wall-mono
        self.until = mono + self.until-wall if self.until>wall else 0
        self.poll_until = mono + self.poll_until-wall if self.poll_until>wall else 0
        self.chat_until = {k:mono+max(0,v-wall) for k,v in self.chat_until.items() if v>wall}
        self.background_next = {k:mono+max(0,v-wall) for k,v in self.background_next.items() if v>wall}

    @contextmanager
    def background(self, bucket="interaction"):
        old = getattr(self.local, 'background', False)
        old_bucket = getattr(self.local, 'bucket', 'interaction')
        self.local.background = True
        self.local.bucket = bucket
        try:
            yield
        finally:
            self.local.background = old
            self.local.bucket = old_bucket

    @contextmanager
    def interactive(self, chat):
        chat = str(chat)
        previous = getattr(self.local,'interactive_chat',None)
        self.local.interactive_chat = chat
        with self.lock:
            self.waiters[chat] = self.waiters.get(chat, 0)+1
        try:
            yield
        finally:
            self.local.interactive_chat = previous
            with self.lock:
                self.waiters[chat] -= 1
                if not self.waiters[chat]:
                    del self.waiters[chat]

    def remaining(self, chat=None, poll=False):
        with self.lock:
            self.sync_clock()
            deadline = self.poll_until if poll else max(self.until,self.chat_until.get(str(chat),0))
            return max(0, deadline-time.monotonic())

    def background_remaining(self, chat, bucket='notification'):
        with self.lock:
            self.sync_clock()
            return max(0, self.until-time.monotonic(),
                       self.chat_until.get(str(chat),0)-time.monotonic(),
                       self.background_next.get(bucket+':'+str(chat),0)-time.monotonic(),
                       0.05 if self.waiters.get(str(chat),0) else 0)

    def sync_clock(self):
        # Called under lock: keep restart/pressure-protection deadlines aligned after a clock step.
        offset = time.time()-time.monotonic()
        if abs(offset-self.wall_offset)>1:
            for key,deadline in [('telegram_cooldown_until',self.until),
                                 ('telegram_poll_cooldown_until',self.poll_until)]:
                if deadline:
                    self.save(key,offset+deadline)
            if self.chat_until:
                self.save_deadlines('telegram_chat_cooldowns',self.chat_until)
            if self.background_next:
                self.save_deadlines('telegram_background_next',self.background_next)
            self.wall_offset = offset

    def save_deadlines(self, key, deadlines):
        wall, mono = time.time(), time.monotonic()
        self.save(key,json.dumps({k:wall+v-mono for k,v in deadlines.items()}))

    def record_flood(self, record):
        # Bounded diagnostic history; never infer a bot-wide ban from one chat.
        with self.lock:
            history = []
            if self.path:
                with closing(sqlite3.connect(self.path)) as db:
                    row = db.execute("SELECT value FROM meta WHERE key='telegram_recent_floods'").fetchone()
                    history = json.loads(row[0]) if row else []
            history = [r for r in history if r['at'] >= record['at']-600]
            self.save('telegram_recent_floods',json.dumps((history+[record])[-128:]))
            self.save('telegram_last_flood',json.dumps(record))

    def save(self, key, value):
        if self.path:
            with closing(sqlite3.connect(self.path, timeout=5)) as db, db:
                db.execute('INSERT OR REPLACE INTO meta(key,value) VALUES(?,?)', (key, str(value)))

    def cooldown(self, seconds, chat=None, poll=False):
        with self.lock:
            deadline = time.monotonic()+max(1,seconds)
            if chat is not None and not poll:
                key = str(chat)
                self.chat_until[key] = max(self.chat_until.get(key,0),deadline)
                self.save_deadlines('telegram_chat_cooldowns',self.chat_until)
            else:
                attr = 'poll_until' if poll else 'until'
                setattr(self,attr,max(getattr(self,attr),deadline))
                self.save('telegram_poll_cooldown_until' if poll else 'telegram_cooldown_until',
                          time.time()+max(0,getattr(self,attr)-time.monotonic()))

    def _background_key(self, chat):
        if chat is not None and getattr(self.local,'background',False):
            return getattr(self.local,'bucket','interaction')+':'+str(chat)
        return None

    def acquire(self, chat=None, poll=False):
        chat = str(chat) if chat is not None else None
        while True:
            remaining = self.remaining(chat,poll)
            if remaining:
                raise TelegramThrottle('server cooldown',math.ceil(remaining))
            with self.lock:
                delay = self.next_global-time.monotonic()
                key = self._background_key(chat)
                if key:
                    delay = max(delay,self.background_next.get(key,0)-time.monotonic())
                    if self.waiters.get(chat,0) or delay>0:
                        raise TelegramThrottle('waiting for background slot or interaction',math.ceil(max(1,delay)))
                if delay<=0:
                    self.save('telegram_global_next',time.time()+1/self.settings.global_api_rps)
                    self.next_global=time.monotonic()+1/self.settings.global_api_rps
                    return
            time.sleep(min(delay,0.05))

    def transmit(self, chat, write_request, poll=False, guard=None):
        # Only background work has a per-chat delay; interactive menus have none.
        chat = str(chat) if chat is not None else None
        key = self._background_key(chat)
        gap = self.settings.notification_seconds if key and key.startswith('notification:') else self.settings.housekeeping_seconds
        while True:
            with self.wire_lock:
                remaining = self.remaining(chat,poll)
                if remaining:
                    raise TelegramThrottle('server cooldown',math.ceil(remaining))
                delay = self.wire_global-time.monotonic()
                if key:
                    with self.lock:
                        delay = max(delay,self.background_next.get(key,0)-time.monotonic())
                        waiting = self.waiters.get(chat,0)
                    if waiting or delay>0:
                        raise TelegramThrottle('waiting for background slot or interaction',math.ceil(max(1,delay)))
                if delay<=0:
                    if guard is not None and not guard():
                        raise TelegramThrottle('recipient no longer authorized')
                    if key:
                        with self.lock:
                            self.background_next={k:v for k,v in self.background_next.items() if v>time.monotonic()}
                            self.background_next[key]=time.monotonic()+gap
                            self.save_deadlines('telegram_background_next',self.background_next)
                    try:
                        return write_request()
                    finally:
                        self.wire_global=time.monotonic()+1/self.settings.global_api_rps
                        self.save('telegram_global_next',time.time()+1/self.settings.global_api_rps)
                        if key:
                            with self.lock:
                                self.background_next[key]=time.monotonic()+gap
                                self.save_deadlines('telegram_background_next',self.background_next)
            time.sleep(min(delay,0.05))
