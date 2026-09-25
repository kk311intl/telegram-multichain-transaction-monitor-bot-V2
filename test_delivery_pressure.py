import io
import json
import tempfile
import threading
import time
import unittest
import urllib.error
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from bot_runtime.common import Event
from bot_runtime.enrichment import TokenEnrichment
from bot_runtime.feed_store import FeedStore
from bot_runtime.notifications import NotificationDispatcher, NotificationPump
from bot_runtime.store import Store
from bot_runtime.telegram import Telegram, TelegramError
from bot_runtime.telegram_rate import TelegramRate, TelegramThrottle
from bot_runtime.address_pressure import remove_overloaded


class DeliveryPressureTest(unittest.TestCase):
    def test_custom_rate_limits_and_pressure_threshold(self):
        from bot_runtime.settings import BotSettings
        settings=BotSettings(notification_seconds=12,global_api_rps=10,housekeeping_seconds=3,pending_limit=100)
        clock=[100.0]
        with patch('bot_runtime.telegram_rate.time.time',side_effect=lambda:clock[0]), \
             patch('bot_runtime.telegram_rate.time.monotonic',side_effect=lambda:clock[0]):
            rate=TelegramRate(settings=settings)
            with rate.background('notification'):rate.transmit(100,lambda:None)
            self.assertAlmostEqual(rate.background_remaining(100),12)
            self.assertAlmostEqual(rate.wire_global,100.1)
            clock[0]+=0.1
            with rate.background():rate.transmit(100,lambda:None)
            self.assertAlmostEqual(rate.background_remaining(100,'interaction'),3)
        self.pressure_events()
        self.assertEqual(remove_overloaded(self.store,now=1000,limit=100),[])
        self.assertTrue(remove_overloaded(self.store,now=1000,limit=10,usage_notice='<custom>'))
        text=self.store.db.execute('SELECT text FROM pressure_alerts').fetchone()[0]
        self.assertIn('超過 10 筆',text)
        self.assertIn('&lt;custom&gt;',text)

    def test_custom_retention_keeps_unsent_notifications(self):
        self.store.save_event(self.event,False,'',confirmed=True,user_ids={100})
        sent=replace(self.event,txid='sent')
        self.store.save_event(sent,False,'',confirmed=True,user_ids={100})
        self.store.mark_notified(sent.event_id,7,sent_state='confirmed',user_id=100)
        with self.store.db:self.store.db.execute('UPDATE events SET created_at=1')
        self.store.cleanup(now=3*86400,confirmed_days=1,obsolete_days=1)
        ids={r[0] for r in self.store.db.execute('SELECT event_id FROM events')}
        self.assertIn(self.event.event_id,ids)
        self.assertNotIn(sent.event_id,ids)

    def test_live_cooldown_and_notification_gap_ignore_wall_clock_jumps(self):
        wall, mono = [1000.0], [100.0]
        with patch('bot_runtime.telegram_rate.time.time',side_effect=lambda:wall[0]), \
             patch('bot_runtime.telegram_rate.time.monotonic',side_effect=lambda:mono[0]):
            rate=TelegramRate(self.path)
            rate.cooldown(60,chat=200)
            rate.cooldown(70,poll=True)
            with rate.background('notification'):
                rate.transmit(100,lambda:None)
            wall[0]+=3600
            mono[0]+=2
            self.assertAlmostEqual(rate.remaining(200),58)
            self.assertAlmostEqual(json.loads(self.store.meta('telegram_chat_cooldowns'))['200'],wall[0]+58)
            self.assertAlmostEqual(TelegramRate(self.path).remaining(200),58)
            self.assertAlmostEqual(rate.remaining(poll=True),68)
            self.assertAlmostEqual(rate.background_remaining(100),3)
            with self.assertRaises(TelegramThrottle):rate.acquire(200)
            wall[0]-=7200
            mono[0]+=58
            self.assertEqual(rate.remaining(200),0)
            self.assertEqual(rate.background_remaining(100),0)
            self.assertAlmostEqual(rate.remaining(poll=True),10)

    def test_pump_skips_cooling_slots_and_polls_empty_accounts_once_per_second(self):
        from concurrent.futures import Future
        from unittest.mock import Mock
        rate=TelegramRate()
        rate.cooldown(60,chat=200)
        pump=NotificationPump(SimpleNamespace(telegram=SimpleNamespace(rate=rate)),self.path,100,{})
        pump.pool.shutdown(wait=True)
        future=Future()
        future.set_result(None)
        pump.pool=Mock(submit=Mock(return_value=future))
        clock=[time.monotonic()]
        with patch('bot_runtime.notifications.time.monotonic',side_effect=lambda:clock[0]):
            pump.tick(self.store)
            self.assertEqual(pump.pool.submit.call_count,1)
            self.assertEqual(pump.pool.submit.call_args.args[1],100)
            for _ in range(20):pump.tick(self.store)
            self.assertEqual(pump.pool.submit.call_count,1)
            clock[0]+=1.01
            pump.tick(self.store)
            self.assertEqual(pump.pool.submit.call_count,2)
            with rate.background('notification'):rate.transmit(100,lambda:None)
            clock[0]+=1.01
            pump.tick(self.store)
            self.assertEqual(pump.pool.submit.call_count,2)
            clock[0]+=4.01
            pump.tick(self.store)
            self.assertEqual(pump.pool.submit.call_count,3)

    def test_flood_history_is_bounded_and_does_not_trigger_global_cooldown(self):
        rate=TelegramRate(self.path)
        for index in range(140):
            rate.record_flood(dict(at=1000+index,chat=index%2,method='sendMessage',retry_after=60))
        records=json.loads(self.store.meta('telegram_recent_floods'))
        self.assertEqual(len(records),128)
        self.assertEqual({r['chat'] for r in records},{0,1})
        self.assertEqual(rate.remaining(),0)
        rate.record_flood(dict(at=2000,chat=3,method='sendMessage',retry_after=60))
        self.assertEqual(len(json.loads(self.store.meta('telegram_recent_floods'))),1)

    def test_confirmed_while_waiting_sends_latest_state_once(self):
        self.store.save_event(self.event,False,'',confirmed=False,user_ids={100,200})
        self.store.save_event(self.event,False,'',confirmed=True,user_ids={100,200})
        sends,edits=[],[]
        def send(text,user,*args):
            sends.append((user,text))
            return user
        dispatcher=NotificationDispatcher(SimpleNamespace(call=lambda *a,**kw:edits.append(a)),
            send,lambda row,state:state or row['confirmation_state'])
        dispatcher.flush(self.store)
        dispatcher.flush(self.store)
        self.assertEqual(sends,[(100,'confirmed'),(200,'confirmed')])
        self.assertEqual(edits,[])

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.path = Path(self.temp.name)/'db'
        self.store = Store(self.path, 100)
        self.store.configure_owner(100)
        self.store.authorize_user(200)
        self.feed = FeedStore(self.path)
        self.event = Event('ethereum', '0x'+'3'*64, 20, '0x'+'4'*64,
            '0x'+'1'*40, 'in', 'native', 'ETH', 10**18, 18, '0x'+'2'*40)

    def tearDown(self):
        self.store.db.close()
        self.temp.cleanup()

    def test_slow_recipient_does_not_block_other_and_never_runs_twice(self):
        self.store.save_event(self.event, False, '', user_ids={100,200})
        release, entered, other = threading.Event(), threading.Event(), threading.Event()
        calls = []
        def send(text, user, store, event_id=None):
            calls.append(user)
            if user == 100:
                entered.set()
                self.assertTrue(release.wait(3))
            else:
                other.set()
            return user
        dispatcher = NotificationDispatcher(SimpleNamespace(), send, lambda r,s:'event')
        pump = NotificationPump(dispatcher, self.path, 100, {})
        try:
            pump.tick(self.store)
            self.assertTrue(entered.wait(2))
            self.assertTrue(other.wait(2))
            for _ in range(4):
                pump.tick(self.store)
            self.assertEqual(calls.count(100),1)
        finally:
            release.set()
            pump.pool.shutdown(wait=True)
        rows = self.store.db.execute('SELECT user_id,notified FROM event_deliveries ORDER BY user_id').fetchall()
        self.assertEqual([tuple(r) for r in rows],[(100,1),(200,1)])

    def test_429_never_dead_and_no_edit_bypass(self):
        self.store.save_event(self.event, False, '', confirmed=False, user_ids={100})
        old = replace(self.event, txid='old')
        self.store.save_event(old, False, '', confirmed=True, user_ids={100})
        self.store.mark_notified(old.event_id,1,sent_state='pending',user_id=100)
        with self.store.db:
            self.store.db.execute('UPDATE event_deliveries SET notify_attempts=20')
        edits = []
        def send(*args):
            raise TelegramError('flood',429,17)
        dispatcher = NotificationDispatcher(SimpleNamespace(call=lambda *a,**kw:edits.append(a)),send,lambda r,s:'event')
        dispatcher.flush(self.store)
        row=self.store.db.execute('SELECT notify_dead,notify_next_at FROM event_deliveries WHERE event_id=?',(self.event.event_id,)).fetchone()
        self.assertEqual(row['notify_dead'],0)
        self.assertGreaterEqual(row['notify_next_at'],int(time.time())+16)
        self.assertEqual(edits,[])

    def test_local_wait_is_not_a_delivery_failure(self):
        self.store.save_event(self.event,False,'',user_ids={100})
        def send(*args): raise TelegramThrottle()
        NotificationDispatcher(SimpleNamespace(),send,lambda r,s:'event').flush(self.store)
        self.assertEqual(self.store.db.execute('SELECT notify_attempts FROM event_deliveries').fetchone()[0],0)

    def test_notification_five_seconds_persists_and_menus_have_no_chat_delay(self):
        clock=[100.0]
        with patch('bot_runtime.telegram_rate.time.time',side_effect=lambda:clock[0]), \
             patch('bot_runtime.telegram_rate.time.monotonic',side_effect=lambda:clock[0]), \
             patch('bot_runtime.telegram_rate.time.sleep',side_effect=lambda x:clock.__setitem__(0,clock[0]+x)):
            rate=TelegramRate(self.path)
            with rate.background('notification'):rate.transmit(200,lambda:None)
            clock[0]=100.1
            resumed=TelegramRate(self.path)
            with resumed.background('notification'),self.assertRaises(TelegramThrottle):resumed.acquire(200)
            resumed.acquire(200)
            resumed.transmit(200,lambda:None)
            self.assertLess(clock[0],101)
            clock[0]=104.9
            with resumed.background('notification'),self.assertRaises(TelegramThrottle):resumed.transmit(200,lambda:None)
            clock[0]=105
            with resumed.background('notification'):resumed.transmit(200,lambda:None)

    def test_chat_429_blocks_only_that_chat_and_poll_is_independent(self):
        rate=TelegramRate(self.path)
        rate.cooldown(43078,chat=200)
        restored=TelegramRate(self.path)
        self.assertGreater(restored.remaining(200),43070)
        self.assertEqual(restored.remaining(100),0)
        restored.acquire(100)
        restored.acquire(poll=True)
        with self.assertRaises(TelegramThrottle):restored.acquire(200)
        with self.assertRaises(TelegramThrottle):restored.transmit(200,lambda:self.fail('must not send'))
        restored.cooldown(30,poll=True)
        with self.assertRaises(TelegramThrottle):restored.acquire(poll=True)

    def test_cooldown_backlog_does_not_delete_addresses_after_recovery(self):
        self.pressure_events()
        end=time.time()+1000
        self.store.set_meta('telegram_cooldown_until',end)
        self.assertEqual(remove_overloaded(self.store),[])
        self.assertEqual(remove_overloaded(self.store,now=int(end)+121),[])
        with self.store.db:self.store.db.execute('UPDATE events SET created_at=?',(int(end)+1,))
        self.assertTrue(remove_overloaded(self.store,now=int(end)+121))

    def test_callback_flood_is_scoped_to_interacting_account(self):
        telegram=Telegram('fake',self.path)
        error=urllib.error.HTTPError('redacted',429,'flood',{},io.BytesIO(b'{"parameters":{"retry_after":60}}'))
        with telegram.rate.interactive(200), patch.object(telegram,'_request',side_effect=error):
            with self.assertRaises(TelegramError):telegram.call('answerCallbackQuery',{'callback_query_id':'test'})
        self.assertGreater(telegram.rate.remaining(200),59)
        self.assertEqual(telegram.rate.remaining(100),0)
        self.assertIsNone(telegram.rate.local.interactive_chat)

    def test_chat_cooldown_only_suppresses_its_own_address_pressure(self):
        self.pressure_events()
        self.store.set_meta('telegram_chat_cooldowns',json.dumps({'100':time.time()+1000}))
        self.assertEqual(remove_overloaded(self.store),[])
        self.store.set_meta('telegram_chat_cooldowns',json.dumps({'200':time.time()+1000}))
        self.assertTrue(remove_overloaded(self.store))

    def test_flood_details_are_saved_without_bot_token(self):
        telegram=Telegram('secret-token',self.path)
        telegram.flood('editMessageText',43078,200,'retry secret-token')
        record=json.loads(self.store.meta('telegram_last_flood'))
        self.assertEqual(record['retry_after'],43078)
        self.assertEqual(record['method'],'editMessageText')
        self.assertNotIn('secret-token',record['description'])

    def test_interactive_priority_and_other_chat_isolation(self):
        rate=TelegramRate()
        with rate.interactive(100):
            with rate.background():
                with self.assertRaises(TelegramThrottle):rate.acquire(100)
                rate.acquire(200)
            rate.acquire(100)
        self.assertEqual(rate.waiters,{})

    def test_send_edit_delete_share_conservative_chat_budget(self):
        telegram=Telegram('fake')
        clock=[100.0]
        with patch('bot_runtime.telegram_rate.time.time',side_effect=lambda:clock[0]), \
             patch('bot_runtime.telegram_rate.time.monotonic',side_effect=lambda:clock[0]), \
             patch('bot_runtime.telegram_rate.time.sleep',side_effect=lambda seconds:clock.__setitem__(0,clock[0]+seconds)), \
             patch.object(telegram,'_request',side_effect=lambda r:io.BytesIO(b'{"ok":true,"result":true}')):
            telegram.call('sendMessage',{'chat_id':100})
            telegram.call('editMessageText',{'chat_id':100})
            telegram.call('deleteMessage',{'chat_id':100})
            self.assertAlmostEqual(clock[0],100.1,places=6)

    def test_transport_429_blocks_same_user_and_checks_auth_after_pacing(self):
        telegram=Telegram('fake',self.path)
        error=urllib.error.HTTPError('redacted',429,'flood',{},io.BytesIO(b'{"parameters":{"retry_after":19}}'))
        with patch.object(telegram,'_request',side_effect=error) as network:
            with self.assertRaises(TelegramError):telegram.send(100,'event')
            with self.assertRaises(TelegramThrottle):telegram.send(100,'event')
            self.assertEqual(network.call_count,1)
        telegram.rate.until=0
        with patch.object(telegram.rate,'acquire'),patch.object(telegram,'_request') as network:
            with self.assertRaises(TelegramThrottle):telegram.send(100,'event',guard=lambda:False)
            network.assert_not_called()

    def test_all_calls_without_chat_and_concurrent_wire_writes_obey_global_gap(self):
        from concurrent.futures import ThreadPoolExecutor
        rate=TelegramRate()
        times=[]
        with ThreadPoolExecutor(max_workers=24) as pool:
            list(pool.map(lambda n:rate.transmit(n,lambda:times.append(time.monotonic())),range(24)))
        self.assertEqual(len(times),24)
        self.assertTrue(all(b-a>=0.05 for a,b in zip(times,times[1:])))
        clock=[100.0]
        with patch('bot_runtime.telegram_rate.time.monotonic',side_effect=lambda:clock[0]), \
             patch('bot_runtime.telegram_rate.time.time',side_effect=lambda:clock[0]), \
             patch('bot_runtime.telegram_rate.time.sleep',side_effect=lambda seconds:clock.__setitem__(0,clock[0]+seconds)):
            rate=TelegramRate(self.path)
            rate.acquire()
            TelegramRate(self.path).acquire(poll=True)
            self.assertGreaterEqual(clock[0],100.05)
            gate=TelegramRate()
            gate.transmit(100,lambda:None)
            gate.transmit(100,lambda:None)
            self.assertAlmostEqual(clock[0],100.1)

    def test_wire_gate_rechecks_revocation_and_flood_after_reservation(self):
        rate=TelegramRate()
        sent=[]
        with self.assertRaises(TelegramThrottle):
            rate.transmit(100,lambda:sent.append(1),guard=lambda:False)
        rate.cooldown(10)
        with self.assertRaises(TelegramThrottle):rate.transmit(200,lambda:sent.append(2))
        self.assertEqual(sent,[])

    def test_retry_count_is_once_and_waiting_is_not_exponential(self):
        self.feed.accept('ethereum',[dict(txid=self.event.txid,height=20,hash=self.event.block_hash)])
        hit=self.feed.due('ethereum')
        self.feed.claim(hit)
        self.feed.finish(hit,False,delay=5)
        row=self.store.db.execute('SELECT * FROM scan_hits').fetchone()
        self.assertEqual(row['attempts'],1)
        self.assertLessEqual(row['next_try'],time.time()+5)

    def test_cross_chain_queue_uses_arrival_not_height_and_edits_get_a_turn(self):
        later=replace(self.event,chain='tron',txid='later',block_height=1)
        self.store.save_event(self.event,False,'',confirmed=False,user_ids={100})
        self.store.save_event(later,False,'',user_ids={100})
        with self.store.db:
            self.store.db.execute('UPDATE events SET created_at=1 WHERE event_id=?',(self.event.event_id,))
        self.assertEqual(self.store.pending_notifications(user_id=100)[0]['event_id'],self.event.event_id)
        self.store.mark_notified(self.event.event_id,9,sent_state='pending',user_id=100)
        self.store.settle_event(self.event.event_id,True)
        edits=[]
        dispatch=NotificationDispatcher(SimpleNamespace(call=lambda *a,**kw:edits.append(a)),lambda *a:self.fail('edit turn must not send'),lambda r,s:'event')
        dispatch.flush(self.store,user_id=100,single=True,prefer_edit=True)
        self.assertEqual(len(edits),1)

    def test_slow_token_job_is_deduplicated_and_consumer_does_not_wait(self):
        release,entered=threading.Event(),threading.Event()
        enrichment=TokenEnrichment(self.path,100,SimpleNamespace(),{})
        adapter=SimpleNamespace(name='ethereum',config={})
        def lookup(*args):
            entered.set()
            release.wait(3)
        try:
            with patch.object(enrichment,'lookup',side_effect=lookup) as job:
                started=time.monotonic()
                for _ in range(100):self.assertIsNone(enrichment.request(adapter,self.store,'token'))
                self.assertLess(time.monotonic()-started,1)
                self.assertTrue(entered.wait(1))
                self.assertEqual(job.call_count,1)
        finally:
            release.set()
            enrichment.pool.shutdown(wait=True)

    def pressure_events(self, unique=61):
        self.store.add('ethereum',self.event.address,'pressure',user_id=100)
        self.store.add('ethereum',self.event.address,'other user',user_id=200)
        with self.store.db:
            self.store.db.execute('UPDATE addresses SET created_at=900')
        for index in range(61):
            self.store.save_event(replace(self.event,txid=str(index % unique),log_index=index),False,'',user_ids={100,200})
        with self.store.db:
            self.store.db.execute('UPDATE events SET created_at=980')
            self.store.db.execute('UPDATE event_deliveries SET notified=1 WHERE user_id=200')
        old=replace(self.event,txid='old-backlog')
        self.store.save_event(old,False,'',user_ids={100})
        with self.store.db:
            self.store.db.execute('UPDATE events SET created_at=930 WHERE event_id=?',(old.event_id,))
        return old

    def test_pressure_removes_only_affected_user_and_priority_alert_survives_restart(self):
        old=self.pressure_events()
        removed=remove_overloaded(self.store,now=1000)
        self.assertEqual(len(removed),1)
        self.assertEqual(len(self.store.addresses(user_id=100)),0)
        self.assertEqual(len(self.store.addresses(user_id=200)),1)
        self.assertFalse(self.store.delivery_allowed(old.event_id,100))
        self.assertEqual(len(remove_overloaded(self.store,now=1000)),0)
        self.store.db.close()
        self.store=Store(self.path,100)
        sent=[]
        dispatcher=NotificationDispatcher(SimpleNamespace(),lambda text,user,store,event_id:sent.append((text,user,event_id)) or 77,lambda r,s:'normal')
        dispatcher.flush(self.store,user_id=100,single=True)
        dispatcher.flush(self.store,user_id=100,single=True)
        self.assertEqual(len(sent),1)
        self.assertIn('高頻地址已自動移除',sent[0][0])
        self.assertEqual(sent[0][1:],(100,None))
        self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM telegram_deletions WHERE chat_id=100 AND message_id=77').fetchone()[0],0)
        self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM event_deliveries WHERE user_id=200 AND notify_dead=1').fetchone()[0],0)

    def test_pressure_threshold_counts_messages_not_transactions_or_age(self):
        self.store.add('ethereum',self.event.address,'mine',user_id=100)
        for index in range(20):
            self.store.save_event(replace(self.event,log_index=index),False,'',user_ids={100})
        self.assertEqual(remove_overloaded(self.store),[])
        self.store.save_event(replace(self.event,log_index=20),False,'',user_ids={100})
        self.assertEqual(len(remove_overloaded(self.store)),1)
        self.assertIn('21 筆待發通知',self.store.db.execute('SELECT text FROM pressure_alerts').fetchone()[0])

    def test_pressure_excludes_filtered_orphaned_sent_and_dead(self):
        self.store.add('ethereum',self.event.address,'mine',user_id=100)
        for index in range(24):
            self.store.save_event(replace(self.event,log_index=index),False,'',user_ids={100})
        ids=[r[0] for r in self.store.db.execute('SELECT event_id FROM events ORDER BY event_id')]
        with self.store.db:
            self.store.db.execute('UPDATE events SET filtered=1 WHERE event_id=?',(ids[0],))
            self.store.db.execute('UPDATE events SET orphaned=1 WHERE event_id=?',(ids[1],))
            self.store.db.execute('UPDATE event_deliveries SET notified=1 WHERE event_id=?',(ids[2],))
            self.store.db.execute('UPDATE event_deliveries SET notify_dead=1 WHERE event_id=?',(ids[3],))
        self.assertEqual(remove_overloaded(self.store),[])

    def test_removed_subscription_cannot_receive_event_from_inflight_decoder(self):
        self.store.add('ethereum',self.event.address,'mine',user_id=100)
        original={self.store.addresses(user_id=100)[0]['id']}
        self.store.remove(next(iter(original)),user_id=100)
        self.store.add('ethereum',self.event.address,'readded',user_id=100)
        self.store.save_event(self.event,False,'',user_ids={100},address_ids=original)
        self.assertEqual(self.store.pending_notifications(user_id=100),[])

    def test_tls_connection_reused_and_uncertain_post_not_retried(self):
        telegram=Telegram('fake')
        response=SimpleNamespace(read=lambda n:b'{"ok":true,"result":true}',status=200)
        with patch('bot_runtime.telegram.http.client.HTTPSConnection') as connect:
            connect.return_value.getresponse.return_value=response
            telegram.call('getMe',{})
            telegram.call('getMe',{})
            self.assertEqual(connect.call_count,1)
            connect.return_value.request.side_effect=ConnectionError()
            with self.assertRaises(TelegramError):telegram.call('getMe',{})
            self.assertEqual(connect.return_value.request.call_count,3)
            connect.return_value.close.assert_called_once()


if __name__=='__main__':unittest.main()
