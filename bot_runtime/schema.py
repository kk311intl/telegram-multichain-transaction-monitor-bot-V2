"""SQLite schema setup and legacy upgrades, in their original execution order."""
import time


def initialize_schema(db, owner_user_id):
    _create_tables(db)
    _migrate_addresses(db, owner_user_id)
    _migrate_event_columns(db)
    _create_indexes(db)
    _migrate_owner_deliveries(db, owner_user_id)
    db.commit()


def _create_tables(db):
    db.executescript(
        """
            CREATE TABLE IF NOT EXISTS addresses(
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              user_id INTEGER NOT NULL DEFAULT 0,
              chain TEXT NOT NULL, address TEXT NOT NULL, label TEXT NOT NULL,
              enabled INTEGER NOT NULL DEFAULT 1, created_at INTEGER NOT NULL,
              watch_direction TEXT NOT NULL DEFAULT 'both',
              token_scope TEXT NOT NULL DEFAULT 'all',
              UNIQUE(user_id,chain,address)
            );
            CREATE TABLE IF NOT EXISTS authorized_users(
              user_id INTEGER PRIMARY KEY, authorized_at INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS events(
              event_id TEXT PRIMARY KEY, chain TEXT NOT NULL, txid TEXT NOT NULL,
              block_height INTEGER NOT NULL, block_hash TEXT NOT NULL,
              address TEXT NOT NULL, direction TEXT NOT NULL, asset_id TEXT NOT NULL,
              symbol TEXT NOT NULL, amount_raw TEXT NOT NULL, decimals INTEGER NOT NULL,
              counterparty TEXT NOT NULL, filtered INTEGER NOT NULL,
              filter_reason TEXT NOT NULL, notified INTEGER NOT NULL DEFAULT 0,
              orphaned INTEGER NOT NULL DEFAULT 0, created_at INTEGER NOT NULL,
              source TEXT NOT NULL DEFAULT '', usd_value TEXT NOT NULL DEFAULT '',
              notify_attempts INTEGER NOT NULL DEFAULT 0,
              notify_next_at INTEGER NOT NULL DEFAULT 0,
              notify_last_error TEXT NOT NULL DEFAULT '',
              notify_dead INTEGER NOT NULL DEFAULT 0,
              confirmation_state TEXT NOT NULL DEFAULT 'confirmed',
              telegram_message_id INTEGER NOT NULL DEFAULT 0,
              message_state TEXT NOT NULL DEFAULT '',
              block_timestamp INTEGER NOT NULL DEFAULT 0,
              metadata_complete INTEGER NOT NULL DEFAULT 1,
              edit_attempts INTEGER NOT NULL DEFAULT 0,
              edit_next_at INTEGER NOT NULL DEFAULT 0,
              edit_last_error TEXT NOT NULL DEFAULT '',
              edit_dead INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS cursors(
              chain TEXT PRIMARY KEY, height INTEGER NOT NULL, block_hash TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS traffic_buckets(
              chain TEXT NOT NULL, source TEXT NOT NULL, bucket INTEGER NOT NULL,
              requests INTEGER NOT NULL, failures INTEGER NOT NULL,
              response_bytes INTEGER NOT NULL,
              PRIMARY KEY(chain,source,bucket)
            );
            CREATE TABLE IF NOT EXISTS event_deliveries(
              event_id TEXT NOT NULL REFERENCES events(event_id) ON DELETE CASCADE,
              user_id INTEGER NOT NULL,
              notified INTEGER NOT NULL DEFAULT 0,
              notify_attempts INTEGER NOT NULL DEFAULT 0,
              notify_next_at INTEGER NOT NULL DEFAULT 0,
              notify_last_error TEXT NOT NULL DEFAULT '',
              notify_dead INTEGER NOT NULL DEFAULT 0,
              telegram_message_id INTEGER NOT NULL DEFAULT 0,
              message_state TEXT NOT NULL DEFAULT '',
              edit_attempts INTEGER NOT NULL DEFAULT 0,
              edit_next_at INTEGER NOT NULL DEFAULT 0,
              edit_last_error TEXT NOT NULL DEFAULT '',
              edit_dead INTEGER NOT NULL DEFAULT 0,
              PRIMARY KEY(event_id,user_id)
            );
            CREATE TABLE IF NOT EXISTS telegram_deletions(
              chat_id INTEGER NOT NULL,
              message_id INTEGER NOT NULL,
              delete_at INTEGER NOT NULL,
              attempts INTEGER NOT NULL DEFAULT 0,
              last_error TEXT NOT NULL DEFAULT '',
              PRIMARY KEY(chat_id,message_id)
            );
            CREATE TABLE IF NOT EXISTS pressure_alerts(
              address_id INTEGER PRIMARY KEY, user_id INTEGER NOT NULL,
              text TEXT NOT NULL, created_at INTEGER NOT NULL,
              sent INTEGER NOT NULL DEFAULT 0, attempts INTEGER NOT NULL DEFAULT 0,
              next_try INTEGER NOT NULL DEFAULT 0
            );
            CREATE INDEX IF NOT EXISTS events_address_created ON events(address,created_at);
            CREATE TABLE IF NOT EXISTS token_market_cache(
              chain TEXT NOT NULL,
              asset_id TEXT NOT NULL,
              valuable INTEGER NOT NULL,
              price_usd TEXT NOT NULL,
              liquidity_usd TEXT NOT NULL,
              reason TEXT NOT NULL,
              checked_at INTEGER NOT NULL,
              PRIMARY KEY(chain,asset_id)
            );
            """
    )


def _migrate_addresses(db, owner_user_id):
    address_columns = {
        row["name"] for row in db.execute("PRAGMA table_info(addresses)")
    }
    if "watch_direction" not in address_columns:
        db.execute(
            "ALTER TABLE addresses ADD COLUMN watch_direction TEXT NOT NULL DEFAULT 'both'"
        )
    if "token_scope" not in address_columns:
        db.execute(
            "ALTER TABLE addresses ADD COLUMN token_scope TEXT NOT NULL DEFAULT 'all'"
        )
    if "user_id" not in address_columns:
        assigned_owner = int(owner_user_id or 0)
        db.executescript(
            """
                ALTER TABLE addresses RENAME TO addresses_legacy;
                CREATE TABLE addresses(
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  user_id INTEGER NOT NULL,
                  chain TEXT NOT NULL, address TEXT NOT NULL, label TEXT NOT NULL,
                  enabled INTEGER NOT NULL DEFAULT 1, created_at INTEGER NOT NULL,
                  watch_direction TEXT NOT NULL DEFAULT 'both',
                  token_scope TEXT NOT NULL DEFAULT 'all',
                  UNIQUE(user_id,chain,address)
                );
                """
        )
        db.execute(
            "INSERT INTO addresses(id,user_id,chain,address,label,enabled,created_at,"
                "watch_direction,token_scope) SELECT id,?,chain,address,label,enabled,created_at,"
                "watch_direction,token_scope FROM addresses_legacy",
            (assigned_owner,),
        )
        db.execute("DROP TABLE addresses_legacy")
    # Token scope used to allow per-address stablecoin-only monitoring.
    # The product now always monitors every supported asset. Keep the
    # column only for backward-compatible databases and normalize old rows.
    db.execute("UPDATE addresses SET token_scope='all' WHERE token_scope<>'all'")
    if owner_user_id is not None:
        db.execute(
            "INSERT OR IGNORE INTO authorized_users(user_id,authorized_at) VALUES(?,?)",
            (int(owner_user_id), int(time.time())),
        )


def _migrate_event_columns(db):
    event_columns = {
        row["name"] for row in db.execute("PRAGMA table_info(events)")
    }
    migrations = {
        "finality_next_at": "INTEGER NOT NULL DEFAULT 0",
        "source": "TEXT NOT NULL DEFAULT ''",
        "usd_value": "TEXT NOT NULL DEFAULT ''",
        "notify_attempts": "INTEGER NOT NULL DEFAULT 0",
        "notify_next_at": "INTEGER NOT NULL DEFAULT 0",
        "notify_last_error": "TEXT NOT NULL DEFAULT ''",
        "notify_dead": "INTEGER NOT NULL DEFAULT 0",
        "confirmation_state": "TEXT NOT NULL DEFAULT 'confirmed'",
        "telegram_message_id": "INTEGER NOT NULL DEFAULT 0",
        "message_state": "TEXT NOT NULL DEFAULT ''",
        "block_timestamp": "INTEGER NOT NULL DEFAULT 0",
        "metadata_complete": "INTEGER NOT NULL DEFAULT 1",
        "edit_attempts": "INTEGER NOT NULL DEFAULT 0",
        "edit_next_at": "INTEGER NOT NULL DEFAULT 0",
        "edit_last_error": "TEXT NOT NULL DEFAULT ''",
        "edit_dead": "INTEGER NOT NULL DEFAULT 0",
    }
    for name, definition in migrations.items():
        if name not in event_columns:
            db.execute(f"ALTER TABLE events ADD COLUMN {name} {definition}")


def _create_indexes(db):
    db.executescript(
        """
            CREATE INDEX IF NOT EXISTS addresses_enabled_lookup
              ON addresses(chain,address,user_id) WHERE enabled=1;
            CREATE INDEX IF NOT EXISTS pressure_alerts_due
              ON pressure_alerts(user_id,next_try) WHERE sent=0;
            CREATE INDEX IF NOT EXISTS events_finality_due
              ON events(chain,finality_next_at,block_height)
              WHERE confirmation_state='pending' AND orphaned=0;
            CREATE INDEX IF NOT EXISTS events_notify_queue
              ON events(notify_next_at,block_height,created_at)
              WHERE filtered=0 AND notified=0 AND orphaned=0 AND notify_dead=0;
            CREATE INDEX IF NOT EXISTS events_message_updates
              ON events(edit_next_at,created_at)
              WHERE notified=1 AND telegram_message_id>0 AND edit_dead=0;
            CREATE INDEX IF NOT EXISTS events_chain_pending
              ON events(chain,block_height)
              WHERE confirmation_state='pending' AND orphaned=0;
            CREATE INDEX IF NOT EXISTS events_filtered_recent
              ON events(created_at DESC) WHERE filtered=1 AND orphaned=0;
            CREATE INDEX IF NOT EXISTS events_retention ON events(created_at);
            CREATE INDEX IF NOT EXISTS traffic_buckets_retention
              ON traffic_buckets(bucket);
            CREATE INDEX IF NOT EXISTS deliveries_notify_queue
              ON event_deliveries(user_id,notify_next_at)
              WHERE notified=0 AND notify_dead=0;
            CREATE INDEX IF NOT EXISTS deliveries_message_updates
              ON event_deliveries(user_id,edit_next_at)
              WHERE notified=1 AND telegram_message_id>0 AND edit_dead=0;
            CREATE INDEX IF NOT EXISTS telegram_deletions_due
              ON telegram_deletions(delete_at);
            """
    )


def _migrate_owner_deliveries(db, owner_user_id):
    if owner_user_id is not None and db.execute(
        "SELECT 1 FROM meta WHERE key='multi_user_migrated_owner'"
    ).fetchone() is None:
        db.execute(
            "INSERT OR IGNORE INTO event_deliveries("
                "event_id,user_id,notified,notify_attempts,notify_next_at,notify_last_error,"
                "notify_dead,telegram_message_id,message_state,edit_attempts,edit_next_at,"
                "edit_last_error,edit_dead) "
                "SELECT event_id,?,notified,notify_attempts,notify_next_at,notify_last_error,"
                "notify_dead,telegram_message_id,message_state,edit_attempts,edit_next_at,"
                "edit_last_error,edit_dead FROM events",
            (int(owner_user_id),),
        )
        db.execute(
            "INSERT INTO meta(key,value) VALUES('multi_user_migrated_owner',?)",
            (str(int(owner_user_id)),),
        )
