import json
import tempfile
import time
import unittest
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from bot_runtime.common import Event
from bot_runtime.feed_store import FeedStore
from bot_runtime.finality import Finalizer
from bot_runtime.notifications import NotificationDispatcher
from bot_runtime.recent_cache import RecentCache
from bot_runtime.store import Store
from scanner_adapters import build_adapter

HASH='0x'+'4'*64


class PendingTest(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory()
        self.path=Path(self.temp.name)/'db'
        self.store=Store(self.path,100)
        self.store.configure_owner(100)
        self.store.authorize_user(200)
        self.event=Event('ethereum','0x'+'3'*64,20,HASH,'0x'+'1'*40,'in','native','ETH',10**18,18,'0x'+'2'*40)
        self.adapter=SimpleNamespace(name='ethereum',safe_tip=lambda:19)
        self.sent,self.edits=[],[]
        def send(text,user,store,event_id=None):
            self.sent.append((text,user))
            return 1000+len(self.sent)
        telegram=SimpleNamespace(call=lambda method,payload,**kwargs:self.edits.append((method,payload)))
        self.dispatch=NotificationDispatcher(telegram,send,lambda row,state:state or row['confirmation_state'])

    def tearDown(self):
        self.store.db.close()
        self.temp.cleanup()

    def test_pending_then_confirmed_edits_same_messages_and_survives_restart(self):
        self.store.save_event(self.event,False,'',confirmed=False,user_ids={100,200})
        self.dispatch.flush(self.store)
        self.assertEqual(self.sent,[('pending',100),('pending',200)])
        self.store.db.close()
        self.store=Store(self.path,100)  # No in-memory queue is needed to resume confirmation.
        finalizer=Finalizer(self.adapter,self.store)
        with patch('bot_runtime.finality.header',return_value=(HASH,'parent')) as lookup:
            finalizer.settle()
            self.assertEqual(lookup.call_count,1)
            self.assertEqual(self.store.notification_updates(user_id=-1),[])
            self.adapter.safe_tip=lambda:20
            finalizer.next_check=finalizer.next_tip=0
            with patch('bot_runtime.finality.time.time',return_value=time.time()+6):
                finalizer.settle()
        self.dispatch.flush(self.store)
        self.assertEqual(len(self.sent),2)
        self.assertEqual([(e[1]['chat_id'],e[1]['message_id'],e[1]['text']) for e in self.edits],[(100,1001,'confirmed'),(200,1002,'confirmed')])

    def test_reorg_orphans_and_reappearance_restores_pending_without_new_send(self):
        self.store.save_event(self.event,False,'',confirmed=False,user_ids={100,200})
        self.dispatch.flush(self.store)
        finalizer=Finalizer(self.adapter,self.store)
        newer='0x'+'5'*64
        with patch('bot_runtime.finality.header',return_value=(newer,'parent')):
            finalizer.settle()
        self.dispatch.flush(self.store)
        self.assertEqual([e[1]['text'] for e in self.edits],['orphaned','orphaned'])
        self.store.revoke_user(200)
        self.store.save_event(replace(self.event,block_hash=newer),False,'',confirmed=False,user_ids={100})
        self.dispatch.flush(self.store)
        self.assertEqual(self.edits[-1][1]['text'],'pending')
        self.assertEqual(self.edits[-1][1]['chat_id'],100)
        self.assertEqual(len(self.sent),2)

    def test_confirmation_outage_does_not_invalidate_or_confirm(self):
        self.store.save_event(self.event,False,'',confirmed=False,user_ids={100})
        finalizer=Finalizer(self.adapter,self.store)
        with patch('bot_runtime.finality.header',side_effect=OSError('unavailable')):
            finalizer.settle()
        self.assertEqual(len(self.store.unsettled_events_through('ethereum',20)),1)

    def test_confirmation_queries_once_for_many_events_in_same_block(self):
        for index in range(20):
            event=replace(self.event,log_index=index)
            self.store.save_event(event,False,'',confirmed=False,user_ids={100})
        self.adapter.safe_tip=lambda:20
        with patch('bot_runtime.finality.header',return_value=(HASH,'parent')) as lookup:
            Finalizer(self.adapter,self.store).settle()
        self.assertEqual(lookup.call_count,1)
        self.assertEqual(self.store.unsettled_events_through('ethereum',20),[])

    def test_cache_bounds_ttl_mutation_and_failed_load(self):
        now=[0]
        cache=RecentCache(max_bytes=100,max_entries=2,clock=lambda:now[0])
        calls=[]
        def load():calls.append(1);return {'a':[1]}
        cache.get(('x',),load,ttl=2)['a'].append(2)
        self.assertEqual(cache.get(('x',),load,ttl=2),{'a':[1]})
        self.assertEqual(len(calls),1)
        now[0]=3
        cache.get(('x',),load,ttl=2)
        self.assertEqual(len(calls),2)
        for n in range(20):cache.get((n,),lambda:{'payload':'a'*60})
        self.assertLessEqual(cache.stats()['bytes'],100)
        self.assertLessEqual(cache.stats()['entries'],2)
        for _ in range(2):
            with self.assertRaises(ValueError):cache.get(('bad',),lambda:(_ for _ in ()).throw(ValueError()))

    def test_latest_scanning_keeps_separate_safe_tip(self):
        cfg=json.loads((Path(__file__).parent / "examples/chains.example.json").read_text(encoding='utf-8'))['chains']
        evm=build_adapter('ethereum',cfg['ethereum'])
        evm.rpc=SimpleNamespace(rpc=lambda *args,**kwargs:{'number':'0x64'})
        self.assertEqual(evm.tip(),100)
        self.assertEqual(evm.safe_tip(),88)
        tron=build_adapter('tron',cfg['tron'])
        paths=[]
        def post(body,path,validator):
            paths.append((body,path))
            height=95 if path.startswith('/walletsolidity/') else 100
            return {'blockID':format(height,'016x')+'1'*48,'block_header':{'raw_data':{'number':height,'parentHash':'2'*64}}}
        tron.rpc=SimpleNamespace(post_validated=post)
        self.assertEqual(tron.tip(),100)
        self.assertEqual(tron.safe_tip(),93)
        self.assertEqual(paths,[({'detail':False},'/wallet/getblock'),({'detail':False},'/walletsolidity/getblock')])

    def test_cached_decode_cannot_bypass_fresh_confirmation_header(self):
        from bot_runtime.app import App
        cfg=json.loads((Path(__file__).parent / "examples/chains.example.json").read_text(encoding='utf-8'))
        app=App('fake',100,cfg,self.path,SimpleNamespace())
        self.store.add('ethereum',self.event.address,'mine',user_id=100)
        hit=dict(chain='ethereum',height=20,hash=HASH,txid=self.event.txid,seen=time.time()+1,attempts=0)
        outcomes=[]
        def finish(hit,success):
            outcomes.append(success)
            app.stop.set()
        app.feed=SimpleNamespace(due=lambda name:hit,finish=finish,claim=lambda hit:None)
        try:
            with patch('bot_runtime.app.build_adapter',return_value=self.adapter), patch('bot_runtime.app.Finalizer',return_value=SimpleNamespace(settle=lambda:None,safe_height=lambda:20)), patch('bot_runtime.app.decode',return_value=[self.event]), patch('bot_runtime.app.header',return_value=('0x'+'5'*64,'parent')):
                app.consume('ethereum')
            self.assertEqual(outcomes,[False])
            self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM events').fetchone()[0],0)
        finally:app.store.db.close()


if __name__=='__main__':unittest.main()
