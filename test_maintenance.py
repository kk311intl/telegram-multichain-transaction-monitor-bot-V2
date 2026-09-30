"""Regression checks for delivery, retention and transient storage failures."""
import io
import json
import sqlite3
import tempfile
import time
import unittest
from concurrent.futures import Future
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch
from bot_runtime.common import Event
from bot_runtime.feed_store import FeedStore
from bot_runtime.notifications import NotificationPump
from bot_runtime.address_pressure import remove_overloaded
from bot_runtime.store import Store
from bot_runtime.backups import parse_user_backup
from lease_store import retry_read
from cluster import CoordinatorHandler


class MaintenanceTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.path = Path(self.temp.name)/'state.sqlite3'
        self.store = Store(self.path,100)
        self.store.authorize_user(200)
        self.feed = FeedStore(self.path)
        self.address = '0x'+'1'*40
        self.event = Event('ethereum','0x'+'3'*64,20,'0x'+'4'*64,
                           self.address,'in','native','ETH',10**18,18,'0x'+'2'*40)


    def tearDown(self):
        self.store.db.close()
        self.temp.cleanup()


    def test_idle_users_create_no_work_and_finished_scheduler_state_is_reclaimed(self):
        with self.store.db:
            self.store.db.executemany('INSERT INTO authorized_users VALUES(?,1)',((u,) for u in range(1000,101000)))
        self.assertEqual(self.store.notification_users(),[])
        with patch.object(self.store,'addresses',side_effect=AssertionError('full address scan')):
            self.assertEqual(remove_overloaded(self.store),[])
        pump=NotificationPump(SimpleNamespace(telegram=SimpleNamespace()),self.path,100,{})
        pump.pool.shutdown(wait=True)
        future=Future();future.set_result(None)
        pump.pool=Mock(submit=Mock(return_value=future))
        pump.tick(self.store)
        pump.pool.submit.assert_not_called()
        self.store.save_event(self.event,False,'',user_ids={200})
        pump.next_users=0
        pump.tick(self.store)
        self.assertEqual(pump.pool.submit.call_args.args[1],200)
        self.store.mark_notified(self.event.event_id,1,user_id=200)
        pump.next_users=0
        pump.tick(self.store)
        self.assertEqual(pump.users,[])
        self.assertEqual(pump.turns,{})
        self.assertFalse(pump.pending)


    def test_bounded_cleanup_preserves_pending_and_reuses_database_pages(self):
        self.store.save_event(self.event,False,'',user_ids={100})
        old=replace(self.event,txid='old')
        self.store.save_event(old,False,'',user_ids={100})
        self.store.mark_notified(old.event_id,1,user_id=100)
        with self.store.db:
            self.store.db.execute('UPDATE events SET created_at=1')
            self.store.db.executemany('INSERT INTO scan_hits VALUES(?,?,?,?,?,?,?,?)',
                (('ethereum',str(i),1,'hash',1,0,0,1) for i in range(2100)))
            self.store.db.execute("INSERT INTO scan_hits VALUES('ethereum','pending',1,'hash',1,0,0,0)")
        self.store.cleanup(now=40*86400)
        self.assertTrue(self.store.event_exists(self.event.event_id))
        self.assertFalse(self.store.event_exists(old.event_id))
        self.feed.cleanup()
        self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM scan_hits').fetchone()[0],101)
        self.feed.cleanup()
        self.assertEqual(self.store.db.execute('SELECT txid FROM scan_hits').fetchone()[0],'pending')
        self.assertGreater(self.store.db.execute('PRAGMA freelist_count').fetchone()[0],0)


    def test_initialization_failure_closes_database_handle(self):
        path=Path(self.temp.name)/'failed.sqlite3'
        with patch('bot_runtime.store.initialize_schema',side_effect=RuntimeError('failed migration')):
            with self.assertRaises(RuntimeError):Store(path,100)
        path.unlink()  # Windows refuses this while a failed constructor leaks the handle.


    def test_filtered_retention_is_independent_of_confirmed_retention(self):
        self.store.save_event(self.event,True,'low_liquidity',user_ids={100})
        with self.store.db:self.store.db.execute('UPDATE events SET created_at=1')
        self.store.cleanup(now=40*86400,obsolete_days=60,confirmed_days=30)
        self.assertTrue(self.store.event_exists(self.event.event_id))
        self.store.cleanup(now=70*86400,obsolete_days=60,confirmed_days=30)
        self.assertFalse(self.store.event_exists(self.event.event_id))


    def test_import_enabled_is_boolean_not_string_truthiness(self):
        row=dict(chain='ethereum',address=self.address,label='example')
        adapters={'ethereum':SimpleNamespace(normalize=lambda a:a)}
        for value in (False,True,0,1,'false','0',2,[],None):
            payload=dict(format='crypto-address-monitor-user-backup-v3',addresses=[dict(row,enabled=value)])
            content=json.dumps(payload).encode()
            if type(value) is bool or type(value) is int and value in (0,1):
                self.assertIs(parse_user_backup(content,adapters,[])[0]['enabled'],bool(value))
            else:
                with self.assertRaises(ValueError):parse_user_backup(content,adapters,[])

    def handler(self,path):
        handler=CoordinatorHandler.__new__(CoordinatorHandler)
        handler.path=path;handler._known_ip=lambda:True;handler._reply=Mock()
        handler.client_address=('127.0.0.1',1234)
        store=SimpleNamespace(clock_wall=100,clock_mono=0,status=Mock(),heartbeat=Mock())
        handler.server=SimpleNamespace(store=store,node_ips={'worker':'127.0.0.1'},
            chain_config={'ethereum':{'type':'evm'}},feed=Mock())
        content=json.dumps(dict(node='worker',chain='ethereum',epoch=1,hits=[])).encode()
        handler.headers={'Content-Length':str(len(content))};handler.rfile=io.BytesIO(content)
        return handler

    def test_database_errors_are_retryable_and_do_not_leak_paths(self):
        for path in ('/status','/heartbeat','/watch','/hits'):
            with self.subTest(path=path):
                h=self.handler(path)
                h.server.store.status.side_effect=sqlite3.OperationalError('private-path')
                h.server.store.heartbeat.side_effect=sqlite3.OperationalError('private-path')
                with patch('builtins.print') as log:
                    h.do_GET() if path=='/status' else h.do_POST()
                h._reply.assert_called_once_with(503,{'error':'database temporarily unavailable'})
                self.assertNotIn('private-path',str(log.call_args))
                h.server.feed.accept.assert_not_called()

    def test_lease_expired_during_read_is_rejected_before_accept(self):
        h=self.handler('/hits')
        h.server.store.status.return_value={'leases':[dict(chain_name='ethereum',owner_node='worker',
            epoch=1,expires=101,disabled=0,target=None)]}
        with patch('cluster.time.monotonic',return_value=2):h.do_POST()
        self.assertEqual(h._reply.call_args.args[0],403)
        h.server.feed.accept.assert_not_called()

    def test_503_contains_retry_after(self):
        h=self.handler('/status');h.send_response=Mock();h.send_header=Mock();h.end_headers=Mock();h.wfile=io.BytesIO()
        CoordinatorHandler._reply(h,503,{'error':'database temporarily unavailable'})
        self.assertIn(('Retry-After','1'),[c.args for c in h.send_header.call_args_list])
    def test_transient_read_retries_are_bounded_and_other_errors_propagate(self):
        error=sqlite3.OperationalError('private-path')
        error.sqlite_errorcode=14
        operation=Mock(side_effect=[error,error,42])
        with patch('lease_store.time.sleep'):
            self.assertEqual(retry_read(operation),42)
            operation=Mock(side_effect=error)
            with self.assertRaises(sqlite3.OperationalError):retry_read(operation)
            self.assertEqual(operation.call_count,3)
            operation=Mock(side_effect=sqlite3.OperationalError('syntax error'))
            with self.assertRaises(sqlite3.OperationalError):retry_read(operation)
            self.assertEqual(operation.call_count,1)


