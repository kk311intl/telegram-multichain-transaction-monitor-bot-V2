"""Persistent leases. Expiry fences work; epochs fence checkpoint updates."""
from __future__ import annotations
import ipaddress
import json
import re
import sqlite3
import time
from chain_identity import chain_kind, valid_identity
from contextlib import closing
from pathlib import Path

LEASE_SECONDS = 20
DRAIN_SECONDS = 30
NODE_TIMEOUT = 45
MAX_ROLLBACK = 128


def validate_config(config):
    nodes, chains = config['nodes'], config['chains']
    if not nodes or not chains or config['primary_node'] not in nodes:
        raise ValueError('invalid topology')
    for name, item in nodes.items():
        if not re.fullmatch(r'[a-z0-9-]{1,40}', name):
            raise ValueError('invalid node name')
        address = item.get('ip_address', item.get('tailscale_ip'))
        if address is not None:
            ipaddress.ip_address(address)
    for name, item in chains.items():
        if not re.fullmatch(r'[a-z0-9-]{1,40}', name) or item['preferred_node'] not in nodes:
            raise ValueError('invalid chain placement')
    for node, item in nodes.items():
        cap = item['max_chains']
        if type(cap) is not int or cap < sum(c['preferred_node'] == node for c in chains.values()):
            raise ValueError('insufficient preferred node capacity')
    if nodes[config['primary_node']]['max_chains'] < len(chains):
        raise ValueError('primary must have capacity for all chains')


class LeaseStore:
    def __init__(self, path, config, verifier=None, now=None, chain_types=None):
        self.chain_types = chain_types
        validate_config(config)
        self.path, self.config, self.verifier = str(path), config, verifier
        self.clock_wall, self.clock_mono = time.time(), time.monotonic()
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        now = self.clock_wall + time.monotonic() - self.clock_mono if now is None else now
        self.boot_at = now
        with closing(self.connect()) as db, db:
            db.executescript('''PRAGMA journal_mode=WAL;
                CREATE TABLE IF NOT EXISTS nodes(node_id TEXT PRIMARY KEY,last_seen REAL NOT NULL,metrics_json TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS leases(chain_name TEXT PRIMARY KEY,preferred_node TEXT NOT NULL,
                owner_node TEXT NOT NULL,epoch INTEGER NOT NULL,cursor INTEGER,updated_at REAL NOT NULL);''')
            existing = {r[1] for r in db.execute('PRAGMA table_info(leases)')}
            for field, kind in [('expires','REAL NOT NULL DEFAULT 0'),('target','TEXT'),
                                ('block_hash','TEXT'),('unhealthy_since','REAL'),('target_reason','TEXT'),('run_id','TEXT'),('reset_at','REAL'),('skipped_blocks','INTEGER NOT NULL DEFAULT 0'),('disabled','INTEGER NOT NULL DEFAULT 0')]:
                if field not in existing:
                    db.execute(f'ALTER TABLE leases ADD COLUMN {field} {kind}')
            for chain, item in config['chains'].items():
                preferred = item['preferred_node']
                row = db.execute('SELECT * FROM leases WHERE chain_name=?', (chain,)).fetchone()
                if row is None:
                    db.execute('INSERT INTO leases(chain_name,preferred_node,owner_node,epoch,updated_at,expires) VALUES(?,?,?,1,?,?)',
                               (chain, preferred, preferred, now, now + NODE_TIMEOUT))
                else:
                    db.execute('UPDATE leases SET expires=MAX(expires,?) WHERE chain_name=?', (now + DRAIN_SECONDS, chain))
                    if row['preferred_node'] != preferred or row['disabled']:
                        db.execute("UPDATE leases SET target=?,target_reason='drain',expires=MAX(expires,?),disabled=0 WHERE chain_name=?",
                                   (preferred, now + DRAIN_SECONDS, chain))
                    db.execute('UPDATE leases SET preferred_node=? WHERE chain_name=?', (preferred, chain))
            for row in db.execute('SELECT chain_name FROM leases').fetchall():
                if row[0] not in config['chains']:
                    db.execute('UPDATE leases SET disabled=1,target=NULL WHERE chain_name=?', (row[0],))

    def connect(self):
        db = sqlite3.connect(self.path, timeout=3)
        db.row_factory = sqlite3.Row
        return db

    @staticmethod
    def read_status(path):
        with closing(sqlite3.connect(Path(path).resolve().as_uri() + '?mode=ro', uri=True)) as db:
            db.row_factory = sqlite3.Row
            nodes = []
            for row in db.execute('SELECT * FROM nodes ORDER BY node_id'):
                node = dict(row)
                node['metrics'] = json.loads(node.pop('metrics_json'))
                nodes.append(node)
            return {'nodes': nodes, 'leases': [dict(r) for r in db.execute('SELECT * FROM leases ORDER BY chain_name')]}

    def status(self):
        return self.read_status(self.path)

    def heartbeat(self, node_id, metrics, now=None):
        if node_id not in self.config['nodes'] or not isinstance(metrics, dict) or len(metrics) > len(self.config['chains']):
            raise ValueError('invalid node or metrics')
        now = self.clock_wall + time.monotonic() - self.clock_mono if now is None else now
        # Validate before any mutation. Verification failures retain the last durable cursor.
        rows = {r['chain_name']: r for r in self.status()['leases']}
        verified = {}
        for chain, item in metrics.items():
            if chain not in rows or not isinstance(item, dict):
                raise ValueError('invalid chain metrics')
            row = rows[chain]
            epoch = item.get('epoch')
            if type(epoch) is not int or epoch != row['epoch'] or row['owner_node'] != node_id:
                raise ValueError('stale or foreign lease')
            cursor = item.get('cursor')
            if cursor is None:
                continue
            digest = item.get('block_hash', '')
            if type(cursor) is not int or not 0 <= cursor < 10**12 or not valid_identity(digest, chain_kind(chain, self.chain_types)):
                raise ValueError('invalid checkpoint')
            run_id = item.get('run_id')
            if not isinstance(run_id, str) or not re.fullmatch(r'[0-9a-f]{32}', run_id):
                raise ValueError('invalid scanner run identity')
            prior = row['cursor'] if row['run_id'] == run_id else None
            if prior is not None:
                if cursor > prior + 4096:
                    raise ValueError('checkpoint jump exceeds 4096 blocks')
                if cursor < prior and (prior - cursor > MAX_ROLLBACK or item.get('rollback') != 'reorg'):
                    raise ValueError('unapproved rollback')
            if cursor == prior and digest == row['block_hash']:
                continue
            if self.verifier is not None:
                try:
                    proof = self.verifier(chain, cursor, digest, (epoch, run_id))
                    if proof is True:
                        verified[chain] = (cursor, digest)
                    elif isinstance(proof, tuple) and (prior is None or prior <= proof[0] <= prior + 4096 or (item.get('rollback') == 'reorg' and 0 <= prior - proof[0] <= MAX_ROLLBACK)):
                        verified[chain] = proof
                except (RuntimeError, ValueError, OSError):
                    pass
        with closing(self.connect()) as db, db:
            db.execute('BEGIN IMMEDIATE')
            db.execute('INSERT INTO nodes VALUES(?,?,?) ON CONFLICT(node_id) DO UPDATE SET last_seen=excluded.last_seen,metrics_json=excluded.metrics_json',
                       (node_id, now, json.dumps(metrics, separators=(',', ':'))))
            seen = {r[0]: r[1] for r in db.execute('SELECT node_id,last_seen FROM nodes')}
            primary = self.config['primary_node']
            primary_alive = now - seen.get(primary, -1e20) <= NODE_TIMEOUT
            for row in db.execute('SELECT * FROM leases').fetchall():
                chain, owner = row['chain_name'], row['owner_node']
                if row['disabled']:
                    continue
                item = metrics.get(chain, {}) if owner == node_id else {}
                healthy = item.get('status') not in ('stalled', 'exited', 'held', 'cooldown', 'retrying')
                if owner == node_id:
                    unhealthy = None if healthy else (row['unhealthy_since'] if row['unhealthy_since'] is not None else now)
                    db.execute('UPDATE leases SET unhealthy_since=? WHERE chain_name=?', (unhealthy, chain))
                else:
                    unhealthy = row['unhealthy_since']
                failed = now - max(self.boot_at, seen.get(owner, row['updated_at'])) > NODE_TIMEOUT or (unhealthy is not None and now - unhealthy >= 120)
                target = row['target']
                if target is not None and row['target_reason'] == 'failure' and owner == node_id and healthy:
                    target = None
                    db.execute('UPDATE leases SET target=NULL,target_reason=NULL WHERE chain_name=?', (chain,))
                if failed and owner != primary and primary_alive and target is None:
                    target = primary
                    db.execute("UPDATE leases SET target=?,target_reason='failure' WHERE chain_name=?", (target, chain))
                if target is not None and now >= row['expires'] + 5:
                    # Capacity checks apply to failback and new-node drains as well.
                    count = db.execute('SELECT COUNT(*) FROM leases WHERE owner_node=? AND disabled=0', (target,)).fetchone()[0]
                    if now - seen.get(target, -1e20) <= NODE_TIMEOUT and (target == owner or count < self.config['nodes'][target]['max_chains']):
                        db.execute('UPDATE leases SET owner_node=?,epoch=epoch+1,target=NULL,target_reason=NULL,expires=?,updated_at=?,unhealthy_since=NULL WHERE chain_name=?',
                                   (target, now + LEASE_SECONDS, now, chain))
                    continue
                if owner == node_id and target is None:
                    if now > row['expires'] + 5:
                        db.execute('UPDATE leases SET epoch=epoch+1,expires=? WHERE chain_name=?', (now + LEASE_SECONDS, chain))
                        continue
                    if chain in verified and row['epoch'] == metrics[chain]['epoch']:
                        run_id = metrics[chain]['run_id']
                        if row['run_id'] != run_id:
                            baseline = metrics[chain].get('initial_head')
                            baseline = baseline if type(baseline) is int else verified[chain][0]
                            skipped = max(0, baseline - row['cursor']) if row['cursor'] is not None else 0
                            db.execute('UPDATE leases SET run_id=?,reset_at=?,skipped_blocks=? WHERE chain_name=?', (run_id, now, skipped, chain))
                        db.execute('UPDATE leases SET cursor=?,block_hash=?,updated_at=? WHERE chain_name=?', (*verified[chain], now, chain))
                    db.execute('UPDATE leases SET expires=? WHERE chain_name=?', (now + LEASE_SECONDS, chain))
            return [dict(chain=r['chain_name'], epoch=r['epoch'], start_cursor=-1 if r['cursor'] is None else r['cursor'],
                         block_hash=r['block_hash'], lease_seconds=LEASE_SECONDS)
                    for r in db.execute('SELECT * FROM leases WHERE owner_node=? AND target IS NULL AND disabled=0 ORDER BY chain_name', (node_id,))]

    def rebalance(self, node_id, now=None):
        if node_id not in self.config['nodes']:
            raise ValueError('unknown node')
        now = self.clock_wall + time.monotonic() - self.clock_mono if now is None else now
        with closing(self.connect()) as db, db:
            return db.execute("UPDATE leases SET target=?,target_reason='drain',expires=MAX(expires,?) WHERE preferred_node=? AND owner_node<>preferred_node AND disabled=0",
                              (node_id, now + DRAIN_SECONDS, node_id)).rowcount
