"""Private, bounded and durable handoff from scanners to the Bot."""
import re
import sqlite3
import time
from contextlib import closing


class FeedStore:
    def __init__(self, path):
        self.path = str(path)
        with closing(self.connect()) as db, db:
            db.executescript('''
                CREATE TABLE IF NOT EXISTS scan_hits (
                  chain TEXT NOT NULL, txid TEXT NOT NULL, height INTEGER NOT NULL,
                  hash TEXT NOT NULL, seen REAL NOT NULL, next_try REAL NOT NULL DEFAULT 0,
                  attempts INTEGER NOT NULL DEFAULT 0, done INTEGER NOT NULL DEFAULT 0,
                  PRIMARY KEY(chain,txid,hash));
                CREATE INDEX IF NOT EXISTS hits_due ON scan_hits(done,next_try);
                CREATE TABLE IF NOT EXISTS token_metadata (
                  chain TEXT NOT NULL, asset TEXT NOT NULL, symbol TEXT NOT NULL,
                  decimals INTEGER NOT NULL, PRIMARY KEY(chain,asset));
            ''')

    def connect(self):
        db = sqlite3.connect(self.path, timeout=5)
        db.row_factory = sqlite3.Row
        return db

    def addresses(self, chain, evm):
        with closing(self.connect()) as db:
            return [r[0] for r in db.execute('''SELECT DISTINCT address FROM addresses
                WHERE enabled=1 AND chain IN (?,?) AND user_id IN (SELECT user_id FROM authorized_users)''',
                (chain, 'evm' if evm else chain))]

    def accept(self, chain, hits):
        if not isinstance(hits, list) or not 1 <= len(hits) <= 100:
            raise ValueError('invalid hit batch')
        for hit in hits:
            if not isinstance(hit, dict) or set(hit) != {'txid','height','hash'}:
                raise ValueError('invalid hit fields')
            if type(hit['height']) is not int or not 0 <= hit['height'] < 10**12:
                raise ValueError('invalid height')
            prefix = '' if chain == 'tron' else '0x'
            if any(not isinstance(hit[k], str) or not re.fullmatch(prefix + '[0-9a-f]{64}', hit[k]) for k in ('txid','hash')):
                raise ValueError('invalid transaction identity')
        with closing(self.connect()) as db, db:
            db.execute('BEGIN IMMEDIATE')
            db.executemany('INSERT OR IGNORE INTO scan_hits(chain,txid,height,hash,seen) VALUES(?,?,?,?,?)',
                [(chain,h['txid'],h['height'],h['hash'],time.time()) for h in hits])
            if db.execute('SELECT COUNT(*) FROM scan_hits WHERE done=0').fetchone()[0] > 10000:
                raise ValueError('notification ingestion at capacity')  # Roll back the whole batch.

    def due(self, chain):
        with closing(self.connect()) as db:
            return db.execute('SELECT * FROM scan_hits WHERE chain=? AND done=0 AND next_try<=? ORDER BY seen LIMIT 1', (chain,time.time())).fetchone()

    def claim(self, hit):
        with closing(self.connect()) as db, db:
            db.execute('UPDATE scan_hits SET attempts=attempts+1,next_try=? WHERE chain=? AND txid=? AND hash=?',
                (time.time()+min(3600,15*2**min(hit['attempts'],8)),hit['chain'],hit['txid'],hit['hash']))

    def finish(self, hit, success, delay=None):
        with closing(self.connect()) as db, db:
            db.execute('UPDATE scan_hits SET done=?,next_try=? WHERE chain=? AND txid=? AND hash=?',
                (int(success),time.time()+(delay if delay is not None else min(3600,15*2**min(hit['attempts'],8))),hit['chain'],hit['txid'],hit['hash']))

    def cleanup(self):
        with closing(self.connect()) as db, db:
            db.execute('DELETE FROM scan_hits WHERE done<>0 AND seen<?', (time.time()-7*86400,))
