from __future__ import annotations

from .schema import initialize_schema

import sqlite3
import time
from pathlib import Path

from .common import Event


CHAIN_META_PREFIXES = {
    "adaptive_poll_seconds", "adaptive_window", "block_rate", "failure_count",
    "last_failure", "last_pending_failure", "last_pending_success", "last_success",
    "market_status", "native_balance", "native_cursor", "poll_error_score",
    "poll_error_updated", "realtime_started", "reorg_count", "reorg_last",
    "reorg_orphaned", "skipped", "skipped_blocks", "source", "tip",
    "worker_restart_count", "worker_restart_last",
}


def retire_unsupported_chains(database_path: Path, supported: set[str]) -> list[str]:
    """Remove every row owned by adapters absent from the supported manifest."""
    address_chains = set(supported) | {"evm"}
    db = sqlite3.connect(database_path)
    db.execute("PRAGMA foreign_keys=ON")
    try:
        existing_tables = {
            str(row[0])
            for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
        chain_tables = [
            table for table in (
                "addresses", "events", "cursors", "traffic_buckets", "token_market_cache",
            )
            if table in existing_tables
        ]
        observed = {
            str(row[0])
            for table in chain_tables
            for row in db.execute(f"SELECT DISTINCT chain FROM {table}")
        }
        for (key,) in db.execute("SELECT key FROM meta"):
            parts = str(key).split(":", 2)
            if len(parts) >= 2 and parts[0] in CHAIN_META_PREFIXES:
                observed.add(parts[1])
        removed = sorted(observed - address_chains)
        if removed:
            placeholders = ",".join("?" for _ in removed)
            with db:
                db.execute(f"DELETE FROM addresses WHERE chain IN ({placeholders})", removed)
                db.execute(f"DELETE FROM events WHERE chain IN ({placeholders})", removed)
                db.execute(f"DELETE FROM cursors WHERE chain IN ({placeholders})", removed)
                db.execute(f"DELETE FROM traffic_buckets WHERE chain IN ({placeholders})", removed)
                if "token_market_cache" in existing_tables:
                    db.execute(
                        f"DELETE FROM token_market_cache WHERE chain IN ({placeholders})", removed,
                    )
                keys = []
                for (key,) in db.execute("SELECT key FROM meta"):
                    parts = str(key).split(":", 2)
                    if (
                        len(parts) >= 2 and parts[0] in CHAIN_META_PREFIXES
                        and parts[1] in removed
                    ):
                        keys.append(str(key))
                db.executemany("DELETE FROM meta WHERE key=?", ((key,) for key in keys))
            db.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        integrity = db.execute("PRAGMA integrity_check").fetchone()[0]
        if integrity != "ok":
            raise RuntimeError(f"post-retirement integrity_check={integrity}")
        return removed
    finally:
        db.close()


class Store:
    def __init__(self, path: Path, owner_user_id: int | None = None):
        self.owner_user_id = owner_user_id
        path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(path)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA foreign_keys=ON")
        self.db.execute("PRAGMA busy_timeout=5000")
        initialize_schema(self.db, owner_user_id)

    def _user(self, user_id: int | None) -> int | None:
        return self.owner_user_id if user_id is None else int(user_id)

    def configure_owner(self, owner_user_id: int) -> None:
        """Enable multi-user semantics for injected/test stores and migrate user 0."""
        was_configured = self.owner_user_id is not None
        self.owner_user_id = int(owner_user_id)
        with self.db:
            self.db.execute(
                "INSERT OR IGNORE INTO authorized_users(user_id,authorized_at) VALUES(?,?)",
                (self.owner_user_id, int(time.time())),
            )
            if not was_configured:
                self.db.execute(
                    "UPDATE addresses SET user_id=? WHERE user_id=0", (self.owner_user_id,)
                )
                if self.db.execute(
                    "SELECT 1 FROM meta WHERE key='multi_user_migrated_owner'"
                ).fetchone() is None:
                    self.db.execute(
                        "INSERT OR IGNORE INTO event_deliveries("
                        "event_id,user_id,notified,notify_attempts,notify_next_at,notify_last_error,"
                        "notify_dead,telegram_message_id,message_state,edit_attempts,edit_next_at,"
                        "edit_last_error,edit_dead) "
                        "SELECT event_id,?,notified,notify_attempts,notify_next_at,notify_last_error,"
                        "notify_dead,telegram_message_id,message_state,edit_attempts,edit_next_at,"
                        "edit_last_error,edit_dead FROM events",
                        (self.owner_user_id,),
                    )
                    self.db.execute(
                        "INSERT INTO meta(key,value) VALUES('multi_user_migrated_owner',?)",
                        (str(self.owner_user_id),),
                    )

    def is_authorized(self, user_id: int) -> bool:
        if self.owner_user_id is not None and int(user_id) == self.owner_user_id:
            return True
        return self.db.execute(
            "SELECT 1 FROM authorized_users WHERE user_id=?", (int(user_id),)
        ).fetchone() is not None

    def authorize_user(self, user_id: int) -> bool:
        with self.db:
            cur = self.db.execute(
                "INSERT OR IGNORE INTO authorized_users(user_id,authorized_at) VALUES(?,?)",
                (int(user_id), int(time.time())),
            )
        return cur.rowcount > 0

    def revoke_user(self, user_id: int) -> bool:
        if self.owner_user_id is not None and int(user_id) == self.owner_user_id:
            return False
        with self.db:
            # Revocation is a notification boundary. Discard this user's old
            # delivery history so re-authorizing later cannot release messages
            # that were queued before access was removed. Address settings stay
            # intact and resume from the monitor's current realtime position.
            self.db.execute("DELETE FROM event_deliveries WHERE user_id=?", (int(user_id),))
            cur = self.db.execute("DELETE FROM authorized_users WHERE user_id=?", (int(user_id),))
        return cur.rowcount > 0

    def authorized_users(self) -> list[sqlite3.Row]:
        return list(self.db.execute(
            "SELECT user_id,authorized_at FROM authorized_users ORDER BY user_id"
        ))

    def meta(self, key: str, default: str = "") -> str:
        row = self.db.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        return row["value"] if row else default

    def meta_with_prefix(self, prefix: str) -> dict[str, str]:
        return {
            str(row["key"]): str(row["value"])
            for row in self.db.execute(
                "SELECT key,value FROM meta WHERE key LIKE ?", (prefix + "%",)
            )
        }

    def user_event_summary(self, user_id: int) -> sqlite3.Row:
        return self.db.execute(
            "SELECT COUNT(*) AS total,0 AS filtered,"
            "SUM(d.notified=0 AND e.orphaned=0 AND d.notify_dead=0) AS pending "
            "FROM event_deliveries d JOIN events e ON e.event_id=d.event_id "
            "WHERE d.user_id=?",
            (int(user_id),),
        ).fetchone()

    def event_summary(self) -> sqlite3.Row:
        return self.db.execute(
            "SELECT COUNT(*) total,SUM(filtered=1 AND orphaned=0) filtered,"
            "SUM(notified=1 AND orphaned=0) notified,SUM(orphaned=1) orphaned "
            "FROM events"
        ).fetchone()

    def set_meta(self, key: str, value: object) -> None:
        with self.db:
            self.db.execute(
                "INSERT INTO meta VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, str(value)),
            )

    def schedule_message_deletion(
        self, chat_id: int, message_id: int, delete_at: int,
    ) -> None:
        if int(chat_id) == 0 or int(message_id) <= 0:
            return
        with self.db:
            self.db.execute(
                "INSERT INTO telegram_deletions(chat_id,message_id,delete_at) VALUES(?,?,?) "
                "ON CONFLICT(chat_id,message_id) DO UPDATE SET "
                "delete_at=excluded.delete_at,attempts=0,last_error=''",
                (int(chat_id), int(message_id), int(delete_at)),
            )

    def due_message_deletions(self, now: int | None = None, limit: int = 100) -> list[sqlite3.Row]:
        current = int(time.time()) if now is None else int(now)
        return list(self.db.execute(
            "SELECT chat_id,message_id,delete_at,attempts,last_error "
            "FROM telegram_deletions WHERE delete_at<=? ORDER BY delete_at LIMIT ?",
            (current, int(limit)),
        ))

    def finish_message_deletion(self, chat_id: int, message_id: int) -> None:
        with self.db:
            self.db.execute(
                "DELETE FROM telegram_deletions WHERE chat_id=? AND message_id=?",
                (int(chat_id), int(message_id)),
            )

    def defer_message_deletion(
        self, chat_id: int, message_id: int, delay: int, error: str,
    ) -> None:
        with self.db:
            self.db.execute(
                "UPDATE telegram_deletions SET delete_at=?,attempts=attempts+1,last_error=? "
                "WHERE chat_id=? AND message_id=?",
                (
                    int(time.time()) + max(1, int(delay)), str(error)[:160],
                    int(chat_id), int(message_id),
                ),
            )

    def add_traffic(
        self, chain: str, source: str, requests: int, failures: int,
        response_bytes: int, now: int | None = None,
    ) -> None:
        if requests <= 0 and failures <= 0 and response_bytes <= 0:
            return
        bucket = ((int(time.time()) if now is None else int(now)) // 300) * 300
        with self.db:
            self.db.execute(
                "INSERT INTO traffic_buckets VALUES(?,?,?,?,?,?) "
                "ON CONFLICT(chain,source,bucket) DO UPDATE SET "
                "requests=requests+excluded.requests,"
                "failures=failures+excluded.failures,"
                "response_bytes=response_bytes+excluded.response_bytes",
                (chain, source, bucket, max(0, int(requests)),
                 max(0, int(failures)), max(0, int(response_bytes))),
            )

    def traffic_24h(self, chain: str, now: int | None = None) -> sqlite3.Row:
        current = int(time.time()) if now is None else int(now)
        return self.db.execute(
            "SELECT COALESCE(SUM(requests),0) requests,"
            "COALESCE(SUM(failures),0) failures,"
            "COALESCE(SUM(response_bytes),0) response_bytes "
            "FROM traffic_buckets WHERE chain=? AND bucket>=?",
            (chain, current - 86400),
        ).fetchone()

    def addresses(
        self, chain: str | None = None, include_evm: bool = False,
        user_id: int | None = None,
    ) -> list[sqlite3.Row]:
        all_authorized = user_id == -1
        selected_user = None if all_authorized else self._user(user_id)
        user_sql = (
            "user_id IN (SELECT user_id FROM authorized_users) AND "
            if all_authorized else ("" if selected_user is None else "user_id=? AND ")
        )
        user_args: tuple[object, ...] = () if selected_user is None else (selected_user,)
        if chain:
            if include_evm:
                return list(self.db.execute(
                    f"SELECT * FROM addresses WHERE {user_sql}chain IN (?, 'evm') AND enabled=1 ORDER BY id",
                    (*user_args, chain),
                ))
            return list(self.db.execute(
                f"SELECT * FROM addresses WHERE {user_sql}chain=? AND enabled=1 ORDER BY id",
                (*user_args, chain),
            ))
        if selected_user is not None:
            list_scope = "WHERE user_id=? "
        elif all_authorized:
            list_scope = "WHERE user_id IN (SELECT user_id FROM authorized_users) "
        else:
            list_scope = ""
        return list(self.db.execute(
            f"SELECT * FROM addresses {list_scope}"
            "ORDER BY CASE WHEN TRIM(label)='' THEN 1 ELSE 0 END, "
            "label COLLATE NOCASE, address COLLATE NOCASE, id", user_args
        ))

    def address(self, row_id: int, user_id: int | None = None) -> sqlite3.Row | None:
        selected_user = None if user_id == -1 else self._user(user_id)
        if selected_user is None:
            return self.db.execute("SELECT * FROM addresses WHERE id=?", (row_id,)).fetchone()
        return self.db.execute(
            "SELECT * FROM addresses WHERE id=? AND user_id=?", (row_id, selected_user)
        ).fetchone()

    def add(self, chain: str, address: str, label: str, user_id: int | None = None) -> None:
        selected_user = int(self._user(user_id) or 0)
        with self.db:
            self.db.execute(
                "INSERT INTO addresses(user_id,chain,address,label,created_at) VALUES(?,?,?,?,?)",
                (selected_user, chain, address, label, int(time.time())),
            )

    def add_many(self, items: list[tuple[str, str, str]], user_id: int | None = None) -> int:
        """Add a group of addresses and quietly skip existing chain/address pairs."""
        added = 0
        created_at = int(time.time())
        selected_user = int(self._user(user_id) or 0)
        with self.db:
            for chain, address, label in items:
                cur = self.db.execute(
                    "INSERT OR IGNORE INTO addresses(user_id,chain,address,label,created_at) VALUES(?,?,?,?,?)",
                    (selected_user, chain, address, label, created_at),
                )
                added += cur.rowcount
        return added

    def edit(self, row_id: int, label: str, user_id: int | None = None) -> bool:
        selected_user = None if user_id == -1 else self._user(user_id)
        with self.db:
            sql = "UPDATE addresses SET label=? WHERE id=?" + ("" if selected_user is None else " AND user_id=?")
            cur = self.db.execute(sql, (label, row_id) if selected_user is None else (label, row_id, selected_user))
        return cur.rowcount > 0

    def set_watch_direction(self, row_id: int, direction: str, user_id: int | None = None) -> bool:
        if direction not in {"both", "in", "out"}:
            raise ValueError("invalid watch direction")
        selected_user = self._user(user_id)
        with self.db:
            cur = self.db.execute(
                "UPDATE addresses SET watch_direction=? WHERE id=?" + ("" if selected_user is None else " AND user_id=?"),
                (direction, row_id) if selected_user is None else (direction, row_id, selected_user),
            )
        return cur.rowcount > 0

    def set_enabled(self, row_id: int, enabled: bool, user_id: int | None = None) -> bool:
        selected_user = self._user(user_id)
        with self.db:
            cur = self.db.execute(
                "UPDATE addresses SET enabled=? WHERE id=?" + ("" if selected_user is None else " AND user_id=?"),
                (int(enabled), row_id) if selected_user is None else (int(enabled), row_id, selected_user),
            )
        return cur.rowcount > 0

    def remove(self, row_id: int, user_id: int | None = None) -> bool:
        selected_user = self._user(user_id)
        with self.db:
            cur = self.db.execute(
                "DELETE FROM addresses WHERE id=?" + ("" if selected_user is None else " AND user_id=?"),
                (row_id,) if selected_user is None else (row_id, selected_user),
            )
        return cur.rowcount > 0

    def replace_addresses(self, user_id: int, items: list[dict[str, object]]) -> int:
        with self.db:
            self.db.execute("DELETE FROM addresses WHERE user_id=?", (int(user_id),))
            for item in items:
                self.db.execute(
                    "INSERT INTO addresses(user_id,chain,address,label,enabled,created_at,"
                    "watch_direction,token_scope) VALUES(?,?,?,?,?,?,?,?)",
                    (int(user_id), item["chain"], item["address"], item["label"],
                     int(bool(item["enabled"])), int(item["created_at"]),
                     item["watch_direction"], "all"),
                )
        return len(items)

    def cursor(self, chain: str) -> tuple[int, str] | None:
        row = self.db.execute("SELECT height,block_hash FROM cursors WHERE chain=?", (chain,)).fetchone()
        return (row["height"], row["block_hash"]) if row else None

    def set_cursor(self, chain: str, height: int, block_hash: str) -> None:
        with self.db:
            self.db.execute(
                "INSERT INTO cursors VALUES(?,?,?) ON CONFLICT(chain) DO UPDATE SET "
                "height=excluded.height,block_hash=excluded.block_hash",
                (chain, height, block_hash),
            )

    def token_market(self, chain: str, asset_id: str) -> sqlite3.Row | None:
        return self.db.execute(
            "SELECT * FROM token_market_cache WHERE chain=? AND asset_id=?",
            (chain, asset_id),
        ).fetchone()

    def save_token_market(
        self, chain: str, asset_id: str, valuable: bool,
        price_usd: object | None, liquidity_usd: object | None, reason: str,
    ) -> None:
        with self.db:
            self.db.execute(
                "INSERT INTO token_market_cache VALUES(?,?,?,?,?,?,?) "
                "ON CONFLICT(chain,asset_id) DO UPDATE SET "
                "valuable=excluded.valuable,price_usd=excluded.price_usd,"
                "liquidity_usd=excluded.liquidity_usd,reason=excluded.reason,"
                "checked_at=excluded.checked_at",
                (
                    chain, asset_id, int(valuable),
                    "" if price_usd is None else str(price_usd),
                    "" if liquidity_usd is None else str(liquidity_usd),
                    reason, int(time.time()),
                ),
            )

    def save_event(
        self, event: Event, filtered: bool, reason: str,
        source: str = "", usd_value: str = "", confirmed: bool = True,
        user_ids: set[int] | None = None, address_ids: set[int] | None = None,
    ) -> bool:
        with self.db:
            cur = self.db.execute(
                "INSERT INTO events("
                "event_id,chain,txid,block_height,block_hash,address,direction,asset_id,"
                "symbol,amount_raw,decimals,counterparty,filtered,filter_reason,notified,"
                "orphaned,created_at,source,usd_value,confirmation_state,block_timestamp,"
                "metadata_complete"
                ") VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) "
                "ON CONFLICT(event_id) DO UPDATE SET block_height=excluded.block_height,"
                "block_hash=excluded.block_hash,symbol=excluded.symbol,"
                "amount_raw=excluded.amount_raw,decimals=excluded.decimals,"
                "counterparty=excluded.counterparty,filtered=excluded.filtered,"
                "filter_reason=excluded.filter_reason,source=excluded.source,"
                "usd_value=excluded.usd_value,block_timestamp=CASE "
                "WHEN excluded.block_timestamp>0 THEN excluded.block_timestamp "
                "ELSE events.block_timestamp END,metadata_complete=excluded.metadata_complete,"
                "message_state=CASE WHEN events.filtered<>excluded.filtered "
                "OR events.filter_reason<>excluded.filter_reason "
                "OR events.confirmation_state<>(CASE WHEN excluded.confirmation_state='confirmed' "
                "THEN 'confirmed' ELSE events.confirmation_state END) "
                "THEN '' ELSE events.message_state END,orphaned=0,confirmation_state="
                "CASE WHEN events.orphaned=1 OR events.block_hash<>excluded.block_hash THEN excluded.confirmation_state "
                "WHEN excluded.confirmation_state='confirmed' THEN 'confirmed' ELSE events.confirmation_state END",
                (
                    event.event_id, event.chain, event.txid, event.block_height,
                    event.block_hash, event.address, event.direction, event.asset_id,
                    event.symbol, str(event.amount_raw), event.decimals,
                    event.counterparty, int(filtered), reason, 0, 0, int(time.time()),
                    source, usd_value, "confirmed" if confirmed else "pending",
                    int(event.block_timestamp), int(event.metadata_complete),
                ),
            )
            if self.owner_user_id is not None:
                recipients = user_ids if user_ids is not None else {self.owner_user_id}
                if address_ids is not None:
                    current = self.addresses(event.chain, include_evm=event.address.startswith('0x'), user_id=-1)
                    recipients = recipients & {r['user_id'] for r in current if r['id'] in address_ids
                        and r['address']==event.address and r['watch_direction'] in ('both',event.direction)}
                self.db.executemany(
                    "INSERT OR IGNORE INTO event_deliveries(event_id,user_id) "
                    "SELECT ?,? FROM authorized_users WHERE user_id=?",
                    [
                        (event.event_id, int(user_id), int(user_id))
                        for user_id in recipients
                    ],
                )
        return cur.rowcount > 0

    def event_exists(self, event_id: str) -> bool:
        return self.db.execute(
            "SELECT 1 FROM events WHERE event_id=?", (event_id,)
        ).fetchone() is not None

    def delivery_allowed(self, event_id, user_id):
        return self.is_authorized(user_id) and (event_id is None or self.db.execute(
            'SELECT 1 FROM event_deliveries d JOIN events e USING(event_id) WHERE event_id=? AND user_id=? AND d.notified=0 AND d.notify_dead=0 AND e.filtered=0 AND e.orphaned=0',
            (event_id,user_id)).fetchone() is not None)

    def commit_scan(self, chain: str, height: int, block_hash: str, metadata: dict[str, object]) -> None:
        """Commit source checkpoints only after all discovered events are saved."""
        with self.db:
            self.db.executemany(
                "INSERT INTO meta VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                [(key, str(value)) for key, value in metadata.items()],
            )
            self.db.execute(
                "INSERT INTO cursors VALUES(?,?,?) ON CONFLICT(chain) DO UPDATE SET "
                "height=excluded.height,block_hash=excluded.block_hash",
                (chain, height, block_hash),
            )

    def refresh_pending_event(self, event: Event) -> None:
        """Refresh inclusion without repeating pricing or changing notification IDs."""
        with self.db:
            self.db.execute(
                "UPDATE events SET block_height=?,block_hash=?,block_timestamp=?,"
                "orphaned=0,confirmation_state='pending' WHERE event_id=? "
                "AND (orphaned=1 OR confirmation_state='pending')",
                (event.block_height, event.block_hash, event.block_timestamp, event.event_id),
            )

    def mark_notified(
        self, event_id: str, message_id: int = 0, sent_state: str | None = None,
        user_id: int | None = None,
    ) -> None:
        selected_user = self._user(user_id)
        with self.db:
            if self.owner_user_id is not None and selected_user is not None:
                self.db.execute(
                    "UPDATE event_deliveries SET notified=1,notify_last_error='',"
                    "telegram_message_id=?,message_state=COALESCE(?,(SELECT CASE "
                    "WHEN orphaned=1 THEN 'orphaned' WHEN filtered=1 THEN 'filtered' "
                    "ELSE confirmation_state END FROM events WHERE event_id=?)) "
                    "WHERE event_id=? AND user_id=?",
                    (message_id, sent_state, event_id, event_id, selected_user),
                )
                if selected_user != self.owner_user_id:
                    return
            self.db.execute(
                "UPDATE events SET notified=1,notify_last_error='',telegram_message_id=?,"
                "message_state=COALESCE(?,CASE WHEN orphaned=1 THEN 'orphaned' "
                "WHEN filtered=1 THEN 'filtered' ELSE confirmation_state END) "
                "WHERE event_id=?", (message_id, sent_state, event_id)
            )

    def notification_updates(
        self, limit: int = 100, user_id: int | None = None,
    ) -> list[sqlite3.Row]:
        selected_user = None if user_id == -1 else self._user(user_id)
        if self.owner_user_id is not None:
            user_clause = "" if selected_user is None else "AND d.user_id=? "
            args: tuple[object, ...] = (
                (int(time.time()), limit) if selected_user is None
                else (int(time.time()), selected_user, limit)
            )
            return list(self.db.execute(
                "SELECT d.user_id,d.notified,d.notify_attempts,d.notify_next_at,"
                "d.notify_last_error,d.notify_dead,d.telegram_message_id,d.message_state,"
                "d.edit_attempts,d.edit_next_at,d.edit_last_error,d.edit_dead,e.*,"
                "COALESCE(exact.label,grouped.label) AS label FROM event_deliveries d "
                "JOIN events e ON e.event_id=d.event_id "
                "JOIN authorized_users u ON u.user_id=d.user_id "
                "LEFT JOIN addresses exact ON exact.user_id=d.user_id AND exact.chain=e.chain "
                "AND exact.address=e.address LEFT JOIN addresses grouped ON grouped.user_id=d.user_id "
                "AND grouped.chain='evm' AND grouped.address=e.address "
                "WHERE d.notified=1 AND d.telegram_message_id>0 AND d.edit_dead=0 "
                "AND d.edit_next_at<=? " + user_clause +
                "AND (CASE WHEN e.orphaned=1 THEN 'orphaned' WHEN e.filtered=1 THEN 'filtered' "
                "ELSE e.confirmation_state END)<>d.message_state ORDER BY e.created_at LIMIT ?",
                args,
            ))
        return list(self.db.execute(
            "SELECT e.*,COALESCE(exact.label,grouped.label) AS label FROM events e "
            "LEFT JOIN addresses exact ON exact.chain=e.chain AND exact.address=e.address "
            "LEFT JOIN addresses grouped ON grouped.chain='evm' AND grouped.address=e.address "
            "WHERE e.notified=1 AND e.telegram_message_id>0 "
            "AND e.edit_dead=0 AND e.edit_next_at<=? "
            "AND (CASE WHEN e.orphaned=1 THEN 'orphaned' WHEN e.filtered=1 THEN 'filtered' "
            "ELSE e.confirmation_state END)<>e.message_state "
            "ORDER BY e.created_at LIMIT ?", (int(time.time()), limit)
        ))

    def mark_message_state(
        self, event_id: str, state: str, user_id: int | None = None,
    ) -> None:
        selected_user = self._user(user_id)
        with self.db:
            if self.owner_user_id is not None and selected_user is not None:
                self.db.execute(
                    "UPDATE event_deliveries SET message_state=?,edit_attempts=0,edit_next_at=0,"
                    "edit_last_error='' WHERE event_id=? AND user_id=?",
                    (state, event_id, selected_user),
                )
                if selected_user != self.owner_user_id:
                    return
            self.db.execute(
                "UPDATE events SET message_state=?,edit_attempts=0,edit_next_at=0,"
                "edit_last_error='' WHERE event_id=?", (state, event_id)
            )

    def defer_message_update(
        self, event_id: str, delay: int, error: str, dead: bool = False,
        user_id: int | None = None,
    ) -> None:
        selected_user = self._user(user_id)
        with self.db:
            if self.owner_user_id is not None and selected_user is not None:
                self.db.execute(
                    "UPDATE event_deliveries SET edit_attempts=edit_attempts+1,edit_next_at=?,"
                    "edit_last_error=?,edit_dead=? WHERE event_id=? AND user_id=?",
                    (int(time.time()) + max(1, delay), error[:160], int(dead), event_id, selected_user),
                )
                if selected_user != self.owner_user_id:
                    return
            self.db.execute(
                "UPDATE events SET edit_attempts=edit_attempts+1,edit_next_at=?,"
                "edit_last_error=?,edit_dead=? WHERE event_id=?",
                (int(time.time()) + max(1, delay), error[:160], int(dead), event_id),
            )

    def unsettled_events_through(self, chain: str, height: int) -> list[sqlite3.Row]:
        return list(self.db.execute(
            "SELECT * FROM events WHERE chain=? AND confirmation_state='pending' "
            "AND orphaned=0 AND block_height>0 AND block_height<=? ORDER BY block_height",
            (chain, height)
        ))

    def reconcile_pending_mempool(
        self, chain: str, seen_event_ids: set[str],
    ) -> int:
        rows = list(self.db.execute(
            "SELECT event_id FROM events WHERE chain=? AND confirmation_state='pending' "
            "AND orphaned=0 AND block_height=0", (chain,)
        ))
        missing = [row["event_id"] for row in rows if row["event_id"] not in seen_event_ids]
        if not missing:
            return 0
        with self.db:
            self.db.executemany(
                "UPDATE events SET orphaned=1,confirmation_state='orphaned' WHERE event_id=?",
                [(event_id,) for event_id in missing],
            )
        return len(missing)

    def settle_event(self, event_id: str, canonical: bool) -> None:
        with self.db:
            if canonical:
                self.db.execute(
                    "UPDATE events SET confirmation_state='confirmed' WHERE event_id=?", (event_id,)
                )
            else:
                self.db.execute(
                    "UPDATE events SET orphaned=1,confirmation_state='orphaned' WHERE event_id=?",
                    (event_id,),
                )

    def reconcile_pending_window(
        self, chain: str, start: int, end: int, seen_event_ids: set[str]
    ) -> int:
        rows = list(self.db.execute(
            "SELECT event_id FROM events WHERE chain=? AND confirmation_state='pending' "
            "AND orphaned=0 AND block_height BETWEEN ? AND ?", (chain, start, end)
        ))
        missing = [row["event_id"] for row in rows if row["event_id"] not in seen_event_ids]
        if not missing:
            return 0
        with self.db:
            self.db.executemany(
                "UPDATE events SET orphaned=1,confirmation_state='orphaned' WHERE event_id=?",
                [(event_id,) for event_id in missing],
            )
        return len(missing)

    def defer_notification(
        self, event_id: str, delay: int, error: str, dead: bool = False,
        user_id: int | None = None,
    ) -> None:
        selected_user = self._user(user_id)
        with self.db:
            if self.owner_user_id is not None and selected_user is not None:
                self.db.execute(
                    "UPDATE event_deliveries SET notify_attempts=notify_attempts+1,notify_next_at=?,"
                    "notify_last_error=?,notify_dead=? WHERE event_id=? AND user_id=?",
                    (int(time.time()) + max(1, delay), error[:160], int(dead), event_id, selected_user),
                )
                if selected_user != self.owner_user_id:
                    return
            self.db.execute(
                "UPDATE events SET notify_attempts=notify_attempts+1,notify_next_at=?,"
                "notify_last_error=?,notify_dead=? WHERE event_id=?",
                (int(time.time()) + max(1, delay), error[:160], int(dead), event_id),
            )

    def recent_filtered(
        self, limit: int = 10, offset: int = 0, user_id: int | None = None,
    ) -> list[sqlite3.Row]:
        selected_user = self._user(user_id)
        if self.owner_user_id is not None and selected_user is not None:
            return list(self.db.execute(
                "SELECT e.*,COALESCE(exact.label,grouped.label) AS label FROM event_deliveries d "
                "JOIN events e ON e.event_id=d.event_id "
                "LEFT JOIN addresses exact ON exact.user_id=d.user_id AND exact.chain=e.chain AND exact.address=e.address "
                "LEFT JOIN addresses grouped ON grouped.user_id=d.user_id AND grouped.chain='evm' AND grouped.address=e.address "
                "WHERE d.user_id=? AND e.filtered=1 AND e.orphaned=0 "
                "ORDER BY e.created_at DESC,e.rowid DESC LIMIT ? OFFSET ?",
                (selected_user, limit, offset),
            ))
        return list(self.db.execute(
            "SELECT e.*,COALESCE(exact.label,grouped.label) AS label FROM events e "
            "LEFT JOIN addresses exact ON exact.chain=e.chain AND exact.address=e.address "
            "LEFT JOIN addresses grouped ON grouped.chain='evm' AND grouped.address=e.address "
            "WHERE e.filtered=1 AND e.orphaned=0 "
            "ORDER BY e.created_at DESC,e.rowid DESC LIMIT ? OFFSET ?",
            (limit, offset),
        ))

    def filtered_count(self, user_id: int | None = None) -> int:
        selected_user = self._user(user_id)
        if self.owner_user_id is not None and selected_user is not None:
            return int(self.db.execute(
                "SELECT COUNT(*) FROM event_deliveries d JOIN events e ON e.event_id=d.event_id "
                "WHERE d.user_id=? AND e.filtered=1 AND e.orphaned=0",
                (selected_user,),
            ).fetchone()[0])
        return int(self.db.execute(
            "SELECT COUNT(*) FROM events WHERE filtered=1 AND orphaned=0"
        ).fetchone()[0])

    def pending_notifications(
        self, limit: int = 100, user_id: int | None = None,
    ) -> list[sqlite3.Row]:
        selected_user = None if user_id == -1 else self._user(user_id)
        if self.owner_user_id is not None:
            user_clause = "" if selected_user is None else "AND d.user_id=? "
            args: tuple[object, ...] = (
                (int(time.time()), limit) if selected_user is None
                else (int(time.time()), selected_user, limit)
            )
            return list(self.db.execute(
                "SELECT d.user_id,d.notified,d.notify_attempts,d.notify_next_at,"
                "d.notify_last_error,d.notify_dead,d.telegram_message_id,d.message_state,"
                "d.edit_attempts,d.edit_next_at,d.edit_last_error,d.edit_dead,e.*,"
                "COALESCE(exact.label,grouped.label) AS label FROM event_deliveries d "
                "JOIN events e ON e.event_id=d.event_id "
                "JOIN authorized_users u ON u.user_id=d.user_id "
                "LEFT JOIN addresses exact ON exact.user_id=d.user_id AND exact.chain=e.chain "
                "AND exact.address=e.address LEFT JOIN addresses grouped ON grouped.user_id=d.user_id "
                "AND grouped.chain='evm' AND grouped.address=e.address "
                "WHERE e.filtered=0 AND e.orphaned=0 AND d.notified=0 AND d.notify_dead=0 "
                "AND d.notify_next_at<=? " + user_clause +
                "ORDER BY e.created_at,e.rowid LIMIT ?",
                args,
            ))
        return list(self.db.execute(
            "SELECT e.*,COALESCE(exact.label,grouped.label) AS label FROM events e "
            "LEFT JOIN addresses exact ON exact.chain=e.chain AND exact.address=e.address "
            "LEFT JOIN addresses grouped ON grouped.chain='evm' AND grouped.address=e.address "
            "WHERE e.filtered=0 AND e.notified=0 AND e.orphaned=0 "
            "AND e.notify_dead=0 AND e.notify_next_at<=? "
            "ORDER BY e.created_at,e.rowid LIMIT ?",
            (int(time.time()), limit),
        ))

    def notification_stats(self, user_id: int | None = None) -> sqlite3.Row:
        selected_user = self._user(user_id)
        if self.owner_user_id is not None:
            where = "" if selected_user is None else "WHERE d.user_id=?"
            args: tuple[object, ...] = () if selected_user is None else (selected_user,)
            return self.db.execute(
                "SELECT SUM(e.filtered=0 AND d.notified=0 AND e.orphaned=0 AND d.notify_dead=0) pending,"
                "SUM(((d.notify_dead=1 AND d.notify_last_error<>'address_overload') OR d.edit_dead=1) AND e.orphaned=0) dead,"
                "MIN(CASE WHEN e.filtered=0 AND d.notified=0 AND e.orphaned=0 "
                "AND d.notify_dead=0 THEN e.created_at END) oldest FROM event_deliveries d "
                "JOIN events e ON e.event_id=d.event_id " + where,
                args,
            ).fetchone()
        return self.db.execute(
            "SELECT SUM(filtered=0 AND notified=0 AND orphaned=0 AND notify_dead=0) pending,"
            "SUM((notify_dead=1 OR edit_dead=1) AND orphaned=0) dead,"
            "MIN(CASE WHEN filtered=0 AND notified=0 AND orphaned=0 AND notify_dead=0 "
            "THEN created_at END) oldest FROM events"
        ).fetchone()

    def prune_inactive_chains(self, active: set[str]) -> list[str]:
        known = {row["chain"] for row in self.db.execute("SELECT chain FROM cursors")}
        stale = sorted(known - active)
        if not stale:
            return []
        placeholders = ",".join("?" for _ in stale)
        with self.db:
            self.db.execute(f"DELETE FROM cursors WHERE chain IN ({placeholders})", stale)
            self.db.execute(
                f"DELETE FROM token_market_cache WHERE chain IN ({placeholders})", stale,
            )
            for chain in stale:
                self.db.execute("DELETE FROM meta WHERE key LIKE ?", (f"%:{chain}",))
        return stale

    def rollback_from(self, chain: str, height: int) -> int:
        with self.db:
            cur = self.db.execute(
                "UPDATE events SET orphaned=1,confirmation_state='orphaned' "
                "WHERE chain=? AND block_height>=? AND orphaned=0",
                (chain, height),
            )
        return cur.rowcount

    def cleanup(self, now: int | None = None, keep_disposable: int = 25_000,
                obsolete_days: int = 7, confirmed_days: int = 30) -> int:
        current = int(time.time()) if now is None else now
        with self.db:
            before = self.db.total_changes
            if self.owner_user_id is not None:
                self.db.execute(
                    "DELETE FROM events WHERE filtered=1 AND created_at<?", (current - obsolete_days * 86400,)
                )
                self.db.execute(
                    "DELETE FROM events WHERE confirmation_state='confirmed' AND filtered=0 "
                    "AND created_at<? AND NOT EXISTS (SELECT 1 FROM event_deliveries d "
                    "WHERE d.event_id=events.event_id AND d.notified=0 AND d.notify_dead=0)",
                    (current - confirmed_days * 86400,),
                )
                self.db.execute(
                    "DELETE FROM events WHERE orphaned=1 AND created_at<?",
                    (current - obsolete_days * 86400,),
                )
                self.db.execute(
                    "DELETE FROM events WHERE event_id IN (SELECT e.event_id FROM events e "
                    "WHERE e.filtered=1 OR e.orphaned=1 OR (e.confirmation_state='confirmed' "
                    "AND NOT EXISTS (SELECT 1 FROM event_deliveries d WHERE d.event_id=e.event_id "
                    "AND d.notified=0 AND d.notify_dead=0)) ORDER BY e.created_at DESC "
                    "LIMIT -1 OFFSET ?)", (keep_disposable,),
                )
            else:
                self.db.execute(
                    "DELETE FROM events WHERE filtered=1 AND created_at<?", (current - obsolete_days * 86400,)
                )
                self.db.execute(
                    "DELETE FROM events WHERE notified=1 AND confirmation_state='confirmed' "
                    "AND created_at<?", (current - confirmed_days * 86400,)
                )
                self.db.execute(
                    "DELETE FROM events WHERE (orphaned=1 OR notify_dead=1 OR edit_dead=1) "
                    "AND created_at<?", (current - obsolete_days * 86400,)
                )
                self.db.execute(
                    "DELETE FROM events WHERE event_id IN ("
                    "SELECT event_id FROM events WHERE filtered=1 OR orphaned=1 "
                    "OR notify_dead=1 OR edit_dead=1 "
                    "OR (notified=1 AND confirmation_state='confirmed') "
                    "ORDER BY created_at DESC LIMIT -1 OFFSET ?)",
                    (keep_disposable,),
                )
            self.db.execute(
                "DELETE FROM traffic_buckets WHERE bucket<?", (current - 25 * 3600,)
            )
            deleted = self.db.total_changes - before
        # These are opportunistic maintenance operations. Other per-chain
        # writers may briefly own the SQLite lock; retention itself has already
        # committed and must not be reported as failed for a busy checkpoint.
        for statement in ("PRAGMA wal_checkpoint(PASSIVE)", "PRAGMA optimize"):
            try:
                self.db.execute(statement)
            except sqlite3.OperationalError as exc:
                if "locked" not in str(exc).lower() and "busy" not in str(exc).lower():
                    raise
        return deleted
