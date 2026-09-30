from __future__ import annotations

from contextlib import nullcontext
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable

from .store import Store
from .telegram import Telegram, TelegramError
from .telegram_rate import TelegramThrottle
from .address_pressure import remove_overloaded, send_alert
from .settings import BotSettings


LOG = logging.getLogger("crypto-address-monitor")


class NotificationDispatcher:
    """Deliver queued events without consulting interactive Telegram state."""

    def __init__(
        self,
        telegram: Telegram,
        send_to_user: Callable[[str, int, Store, str | None], Any],
        render: Callable[[Any, str | None], str],
    ):
        self.telegram = telegram
        self.send_to_user = send_to_user
        self.render = render

    def flush(self, store: Store, user_id=-1, single=False, prefer_edit=False) -> None:
        if user_id != -1 and send_alert(self, store, user_id):
            return
        pending = store.pending_notifications(user_id=user_id, limit=1 if single else 100)
        updates = store.notification_updates(user_id=user_id, limit=1 if single else 100)
        if single:
            if updates and (prefer_edit or not pending):
                pending = []
            else:
                updates = []
        for row in pending:
            recipient = int(row["user_id"])
            if not store.is_authorized(recipient):
                continue
            try:
                text = self.render(row, None)
                message_id = self.send_to_user(text, recipient, store, row["event_id"])
                store.mark_notified(
                    row["event_id"], message_id if isinstance(message_id, int) else 0,
                    sent_state=row["confirmation_state"], user_id=recipient,
                )
            except TelegramThrottle:
                return
            except TelegramError as exc:
                attempts = int(row["notify_attempts"]) + 1
                delay = exc.retry_after or min(3600, 15 * (2 ** min(attempts - 1, 8)))
                dead = exc.status != 429 and ((exc.status in {400, 403, 404} and attempts >= 3) or attempts >= 10)
                store.defer_notification(
                    row["event_id"], delay, f"HTTP {exc.status or 'network'}", dead,
                    user_id=recipient,
                )
                LOG.warning(
                    "notification deferred event=%s attempts=%s delay=%ss dead=%s",
                    row["event_id"][:12], attempts, delay, dead,
                )
                if exc.status == 429:
                    return
            except Exception as exc:
                attempts = int(row["notify_attempts"]) + 1
                delay = min(3600, 15 * (2 ** min(attempts - 1, 8)))
                store.defer_notification(
                    row["event_id"], delay, type(exc).__name__, attempts >= 10,
                    user_id=recipient,
                )
                LOG.exception("notification deferred after unexpected error")

        for row in updates:
            state = (
                "orphaned" if row["orphaned"] else
                ("filtered" if row["filtered"] else row["confirmation_state"])
            )
            recipient = int(row["user_id"])
            if not store.is_authorized(recipient):
                continue
            try:
                self.telegram.call("editMessageText", {
                    "chat_id": recipient,
                    "message_id": int(row["telegram_message_id"]),
                    "text": self.render(row, state),
                    "parse_mode": "HTML",
                    "disable_web_page_preview": True,
                }, guard=lambda:store.is_authorized(recipient))
                store.mark_message_state(row["event_id"], state, recipient)
            except TelegramThrottle:
                return
            except TelegramError as exc:
                if exc.status in {400, 403, 404}:
                    store.mark_message_state(row["event_id"], state, recipient)
                else:
                    attempts = int(row["edit_attempts"]) + 1
                    delay = exc.retry_after or min(
                        3600, 15 * (2 ** min(attempts - 1, 8))
                    )
                    store.defer_message_update(
                        row["event_id"], delay,
                        f"HTTP {exc.status or 'network'}", exc.status != 429 and attempts >= 10,
                        user_id=recipient,
                    )
                    LOG.warning(
                        "message edit deferred event=%s attempts=%s delay=%ss",
                        row["event_id"][:12], attempts, delay,
                    )
                    if exc.status == 429:
                        break
            except Exception as exc:
                attempts = int(row["edit_attempts"]) + 1
                delay = min(3600, 15 * (2 ** min(attempts - 1, 8)))
                store.defer_message_update(
                    row["event_id"], delay, type(exc).__name__, attempts >= 10,
                    user_id=recipient,
                )
                LOG.exception("confirmation edit deferred after unexpected error")


class NotificationPump:
    """At most one operation per recipient, 32 concurrent private chats."""
    def __init__(self, dispatcher, path, owner, inflight):
        self.dispatcher, self.path, self.owner, self.inflight = dispatcher, path, owner, inflight
        rate = getattr(dispatcher.telegram,'rate',None)
        self.max_workers = rate.settings.notification_workers if rate else 32
        self.pool = ThreadPoolExecutor(max_workers=self.max_workers, thread_name_prefix='delivery')
        self.pending = {}
        self.turns = {}
        self.cursor = 0
        self.next_pressure = 0
        self.next_check = {}
        self.users, self.next_users = [], 0

    def deliver(self, user, prefer_edit):
        key = f'notification:{user}'
        self.inflight[key] = time.monotonic()
        store = None
        try:
            # App initializes/migrates the database before starting background workers.
            store = Store(self.path, self.owner, initialize=False)
            rate = getattr(self.dispatcher.telegram, "rate", None)
            with rate.background("notification") if rate else nullcontext():
                self.dispatcher.flush(store, user_id=user, single=True, prefer_edit=prefer_edit)
        finally:
            if store is not None:
                store.db.close()
            self.inflight.pop(key, None)

    def tick(self, store, paused=False):
        rate = getattr(self.dispatcher.telegram,'rate',None)
        settings = rate.settings if rate else BotSettings()
        if not paused and time.monotonic() >= self.next_pressure:
            remove_overloaded(store,limit=settings.pending_limit,usage_notice=settings.usage_notice)
            self.next_pressure = time.monotonic()+5
        for user, future in list(self.pending.items()):
            if future.done():
                del self.pending[user]
                future.result()
                self.next_check[user] = time.monotonic()+1
        if paused:
            return 1
        now = time.monotonic()
        if now >= self.next_users:
            self.users = store.notification_users()
            self.next_users = now+1
            active = set(self.users) | self.pending.keys()
            self.turns = {u:n for u,n in self.turns.items() if u in active}
        users = self.users
        self.next_check = {u:deadline for u,deadline in self.next_check.items() if deadline>now}
        if not users:
            return 1
        wait = 1.0  # Discover newly queued events/authorized users within one second.
        start = self.cursor % len(users)
        for offset in range(len(users)):
            user = users[(start+offset) % len(users)]
            if len(self.pending) >= self.max_workers:
                break
            self.cursor = (start+offset+1) % len(users)
            if user in self.pending:
                continue
            delay = max(0,self.next_check.get(user,0)-time.monotonic(),
                        rate.background_remaining(user) if rate else 0)
            if delay:
                wait = min(wait,delay)
                continue
            turn = self.turns.get(user, 0)
            self.turns[user] = turn+1
            self.pending[user] = self.pool.submit(self.deliver, user, turn % 4 == 3)
        return 0.05 if self.pending else max(0.05,wait)

    def close(self):
        self.pool.shutdown(wait=False, cancel_futures=True)
