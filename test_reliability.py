import json
import tempfile
import time
import unittest
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from bot_runtime.app import App
from bot_runtime.common import Event
from bot_runtime.decoder import metadata
from bot_runtime.finality import Finalizer
from bot_runtime.market import MarketAssessment
from bot_runtime import lookup_retry


class ReliabilityTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.config = json.loads((Path(__file__).parent / "examples/chains.example.json").read_text(encoding='utf-8'))
        self.app = App('test', 100, self.config, Path(self.temp.name)/'db', None)
        self.store = self.app.store
        self.address = '0x'+'1'*40
        self.asset = '0x'+'2'*40
        self.digest = '0x'+'3'*64
        self.hit = dict(txid='0x'+'4'*64, height=10, hash=self.digest)
        self.event = Event('ethereum', self.hit['txid'], 10, self.digest,
            self.address, 'in', self.asset, 'TOKEN', 10**18, 18, '0x'+'5'*40)
        self.store.add('ethereum', self.address, 'mine', user_id=100)
        self.adapter = SimpleNamespace(name='ethereum', config=self.config['chains']['ethereum'], safe_tip=lambda:100)

    def tearDown(self):
        self.store.db.close()
        self.temp.cleanup()

    def consume_one(self, events, assessment=None):
        self.app.stop.clear()
        original_due = self.app.feed.due
        first = True
        def due(name):
            nonlocal first
            if first:
                first = False
                return original_due(name)
            self.app.stop.set()
            return None
        with patch.object(self.app.feed, 'due', side_effect=due), \
             patch('bot_runtime.app.build_adapter', return_value=self.adapter), \
             patch('bot_runtime.app.Finalizer', return_value=SimpleNamespace(settle=lambda:None, safe_height=lambda:100)), \
             patch('bot_runtime.app.decode', return_value=events), \
             patch.object(self.app.token_security, 'assess', return_value=''), \
             patch('bot_runtime.app.header', return_value=(self.digest,'')), \
             patch('bot_runtime.enrichment.build_adapter', return_value=self.adapter), \
             patch('bot_runtime.enrichment.metadata', return_value=('TOKEN',18,True)), \
             patch('bot_runtime.enrichment.DexScreenerOracle.assess', return_value=assessment) as market, \
             patch.object(self.app.enrichment, 'request', return_value={'risk':''}):
            if any(e.asset_id != 'native' and e.metadata_complete for e in events):
                self.app.enrichment.lookup('ethereum', self.adapter.config, self.asset)
            self.app.consume('ethereum')
            return market.call_count

    def test_confirmation_failure_does_not_starve_later_blocks_after_restart(self):
        for height in range(10,44):
            self.store.save_event(replace(self.event, block_height=height, txid=str(height)), False, '', confirmed=False, user_ids={100})
        def header(adapter, height):
            if height < 42:
                raise OSError('old block unavailable')
            return self.digest,''
        with patch('bot_runtime.finality.header', side_effect=header) as rpc:
            Finalizer(self.adapter,self.store).settle()
            Finalizer(self.adapter,self.store).settle()  # New object still respects persisted cooldown.
        self.assertEqual(rpc.call_count,34)
        self.assertEqual(self.store.db.execute("SELECT COUNT(*) FROM events WHERE confirmation_state='confirmed'").fetchone()[0],2)

    def test_full_queue_accepts_duplicates_and_rolls_back_new_batch(self):
        self.app.feed.accept('ethereum',[self.hit])
        with self.store.db:
            self.store.db.executemany('INSERT INTO scan_hits(chain,txid,height,hash,seen) VALUES(?,?,?,?,?)',
                [('ethereum','0x'+format(i,'064x'),10,self.digest,time.time()) for i in range(9999)])
        self.app.feed.accept('ethereum',[self.hit,self.hit])
        with self.assertRaises(ValueError):
            self.app.feed.accept('ethereum',[self.hit,dict(self.hit,txid='0x'+'f'*64)])
        self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM scan_hits').fetchone()[0],10000)

    def test_metadata_failure_cools_then_success_is_permanent(self):
        calls=[]
        def rpc(method,args):
            calls.append(method)
            raise OSError('offline')
        self.adapter.rpc=SimpleNamespace(rpc=rpc)
        self.assertFalse(metadata(self.adapter,self.store,self.asset)[2])
        self.assertFalse(metadata(self.adapter,self.store,self.asset)[2])
        self.assertEqual(len(calls),1)
        with patch('bot_runtime.lookup_retry.time.time',return_value=time.time()+61):
            self.adapter.rpc.rpc=lambda method,args:'0x12' if args[0]['data']=='0x313ce567' else '0x'+b'TOKEN'.ljust(32,b'\0').hex()
            self.assertEqual(metadata(self.adapter,self.store,self.asset),('TOKEN',18,True))
        self.adapter.rpc.rpc=rpc
        self.assertEqual(metadata(self.adapter,self.store,self.asset),('TOKEN',18,True))
        self.assertEqual(len(calls),1)

    def test_missing_metadata_retries_without_blocking_native_and_without_duplicate_delivery(self):
        native=replace(self.event,asset_id='native',symbol='ETH')
        self.app.feed.accept('ethereum',[self.hit])
        self.consume_one([native,replace(self.event,metadata_complete=False)])
        self.assertEqual(self.store.db.execute('SELECT done FROM scan_hits').fetchone()[0],0)
        self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM events').fetchone()[0],1)
        with self.store.db:self.store.db.execute('UPDATE scan_hits SET next_try=0')
        self.consume_one([native,self.event],MarketAssessment(True))
        self.assertEqual(self.store.db.execute('SELECT done FROM scan_hits').fetchone()[0],1)
        self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM event_deliveries').fetchone()[0],2)

    def test_market_failure_cooldown_preserves_old_assessment(self):
        self.store.save_token_market('ethereum',self.asset,False,None,None,'no_market_value')
        with self.store.db:self.store.db.execute('UPDATE token_market_cache SET checked_at=0')
        self.app.feed.accept('ethereum',[self.hit])
        self.assertEqual(self.consume_one([self.event],MarketAssessment(None)),1)
        hit=dict(self.hit,txid='0x'+'6'*64)
        self.app.feed.accept('ethereum',[hit])
        self.assertEqual(self.consume_one([replace(self.event,txid=hit['txid'])],MarketAssessment(None)),0)
        self.assertFalse(lookup_retry.ready(self.store,'market','ethereum',self.asset))
        self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM events WHERE filtered=1').fetchone()[0],2)


if __name__ == '__main__':
    unittest.main()
