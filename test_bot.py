from contextlib import nullcontext
import json
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.request
from pathlib import Path
from types import SimpleNamespace

from bot_runtime.app import App
from bot_runtime.common import Event, TRANSFER_TOPIC, tron_from_hex
from bot_runtime.decoder import decode
from bot_runtime.feed_store import FeedStore
from bot_runtime.store import Store
from cluster import BoundedServer, CoordinatorHandler
from lease_store import LeaseStore
from match_feed import evm_hits, tron_hits

A, B = '0x'+'1'*40, '0x'+'2'*40
TX, HASH = '0x'+'3'*64, '0x'+'4'*64


class FakeTelegram:
    def __init__(self):
        self.sent=[]
    def call(self, method, payload):
        self.sent.append((method,payload))
        return True
    def send(self, user, text, markup=None):
        self.sent.append(('send',dict(user=user,text=text,markup=markup)))
        return {'message_id':len(self.sent)}


class BotTest(unittest.TestCase):
    def test_legacy_schema_upgrade_preserves_owner_deliveries_and_is_repeatable(self):
        path=Path(self.temp.name)/'legacy.sqlite3'
        legacy=Store(path)
        event=Event('ethereum',TX,1,HASH,A,'in','native','ETH',10**18,18,B)
        legacy.save_event(event,False,'')
        legacy.mark_notified(event.event_id,123)
        legacy.db.executescript("""
            DROP TABLE addresses;
            CREATE TABLE addresses(id INTEGER PRIMARY KEY,chain TEXT,address TEXT,label TEXT,
                                   enabled INTEGER,created_at INTEGER,token_scope TEXT);
        """)
        legacy.db.execute('INSERT INTO addresses VALUES(7,?,?,?,?,?,?)',('ethereum',A,'legacy',1,99,'stable'))
        legacy.db.commit();legacy.db.close()
        upgraded=Store(path,100)
        row=upgraded.address(7,100)
        self.assertEqual((row['address'],row['label'],row['watch_direction'],row['token_scope']),(A,'legacy','both','all'))
        self.assertIsNone(upgraded.address(7,200))
        delivery=upgraded.db.execute('SELECT * FROM event_deliveries').fetchone()
        self.assertEqual((delivery['user_id'],delivery['notified'],delivery['telegram_message_id']),(100,1,123))
        self.assertEqual(upgraded.db.execute('PRAGMA integrity_check').fetchone()[0],'ok')
        snapshot=list(upgraded.db.iterdump());upgraded.db.close()
        reopened=Store(path,100)
        self.assertEqual(list(reopened.db.iterdump()),snapshot)
        reopened.db.close()

    def test_address_callbacks_preserve_user_boundary_after_extraction(self):
        from unittest.mock import Mock, patch
        app=self.app
        app.store.authorize_user(200)
        app.store.add('ethereum',A,'private owner label',100)
        row=dict(app.store.addresses(user_id=100)[0])
        app.edit_menu_message=Mock()
        for action in ['addr','dir','enabled','scope','edit','delask','delete']:
            suffix=':out' if action=='dir' else ':0' if action in {'enabled','scope'} else ''
            query={'from':{'id':200},'message':self.message(200,''),'data':f'{action}:{row["id"]}{suffix}'}
            with self.subTest(action=action),patch('bot_runtime.telegram_controller.LOG.exception'):
                app.callback(query)
                self.assertEqual(dict(app.store.address(row['id'],100)),row)
                self.assertIn('找不到該地址',app.edit_menu_message.call_args.args[1])
                self.assertNotIn('private owner label',app.edit_menu_message.call_args.args[1])
        app.callback({'from':{'id':100},'message':self.message(100,''),'data':f'enabled:{row["id"]}:0'})
        self.assertFalse(app.store.address(row['id'],100)['enabled'])

    def test_custom_ui_uses_settings_and_removes_obsolete_help(self):
        from bot_runtime.settings import BotSettings
        app=self.app
        app.settings=BotSettings(timezone_minutes=330,menu_seconds=120,pending_limit=9,
                                 title='<Custom>',usage_notice='<notice>',filter_page_size=2,address_page_size=1)
        self.assertIn('&lt;Custom&gt;',app.menu())
        self.assertIn('UTC+5:30',app._display_time(0))
        self.assertIn('&lt;notice&gt;',app.help_text())
        self.assertIn('超過 9 筆',app.help_text())
        self.assertIn('120 秒',app.help_text())
        self.assertNotIn('無聲置頂',app.help_text())
        self.assertNotIn('60 筆交易',app.help_text())
        app._start_status_refresh({'chat':{'id':100},'message_id':30},now=100)
        self.assertEqual(app.status_refreshes[100]['expires_at'],220)
        app.config['chains']['ethereum']['market_min_liquidity_usd']='50000'
        self.assertIn('流動性 US$50000',app.info_text())
        self.assertNotIn('US$10,000',app._filter_reason('market_liquidity_below_minimum'))

    def test_status_uses_configured_chronology_without_notification_panel(self):
        from unittest.mock import Mock
        app=self.app
        app.adapters={name:SimpleNamespace(config={'display_name':name,'launch_date':date})
                      for name,date in [('newer','2025-01-01'),('older','2015-01-01'),('undated','9999-12-31')]}
        app.leases.status=Mock(return_value={'nodes':[], 'leases':[
            {'chain_name':name,'owner_node':'worker'} for name in ['newer','undated','older']]})
        app.telegram_status_lines=Mock(side_effect=AssertionError('obsolete panel called'))
        for user in [100,200]:
            text=app.status_text(user)
            self.assertLess(text.index('older'),text.index('newer'))
            self.assertLess(text.index('newer'),text.index('undated'))
            for label in ['通知狀態','429','冷卻','最老']:
                self.assertNotIn(label,text)
        app.telegram_status_lines.assert_not_called()

    def test_cooled_account_update_does_not_block_owner_or_mutate(self):
        from bot_runtime.telegram_rate import TelegramRate
        from unittest.mock import Mock
        app=self.app
        app.telegram.rate=TelegramRate()
        app.telegram.rate.cooldown(1000,chat=200)
        app.telegram.call=Mock(return_value=[
            {'update_id':10,'message':self.message(200,'/start')},
            {'update_id':11,'message':self.message(100,'/start')}])
        app.command=Mock()
        app.poll_telegram()
        self.assertEqual(app.command.call_count,1)
        self.assertEqual(app.command.call_args.args[0]['from']['id'],100)
        self.assertEqual(app.store.meta('telegram_offset'),'12')

    def setUp(self):
        self.temp=tempfile.TemporaryDirectory()
        self.path=Path(self.temp.name)/'bot.db'
        self.config=json.loads((Path(__file__).parent / "examples/chains.example.json").read_text(encoding='utf-8'))
        self.app=App('not-a-real-token',100,self.config,self.path,SimpleNamespace(status=lambda:{'nodes':[],'leases':[]}))
        self.app.telegram=FakeTelegram()
    def tearDown(self):
        self.app.store.db.close()
        self.temp.cleanup()
    def message(self,user,text):
        return dict(chat={'type':'private','id':user},**{'from':{'id':user}},message_id=10,text=text)

    def test_ui_authorization_backup_and_notifications_isolated(self):
        app=self.app
        app.command(self.message(200,'/start'))
        self.assertIn('200',app.telegram.sent[-1][1]['text'])
        app.store.authorize_user(200)
        app.store.add('ethereum',A,'owner',user_id=100)
        app.store.add('ethereum',B,'other',user_id=200)
        app.command(self.message(200,'/user'))
        self.assertIn('僅限所有者',app.telegram.sent[-1][1]['text'])
        self.assertNotIn(A,app.export_backup(200).decode())
        self.assertFalse(app.store.remove(app.store.addresses(user_id=100)[0]['id'],user_id=200))
        event=Event('ethereum',TX,1,HASH,A,'in','native','ETH',10**18,18,B)
        app.store.save_event(event,False,'',user_ids={100})
        # Current menu user is 200, delivery MUST still be 100.
        from bot_runtime.notifications import NotificationDispatcher
        sent=[]
        dispatcher=NotificationDispatcher(app.telegram,lambda text,user_id,store,event_id=None:sent.append(user_id) or 7,app._notification_text)
        dispatcher.flush(app.store)
        self.assertEqual(sent,[100])
        app.store.revoke_user(200)
        self.assertEqual(app.feed.addresses('ethereum',True),[A])

    def test_menu_commands_and_expiry(self):
        app=self.app
        app.register_commands()
        self.assertEqual([c['command'] for c in app.telegram.sent[0][1]['commands']],['start'])
        self.assertEqual([c['command'] for c in app.telegram.sent[1][1]['commands']],['start','info','user'])
        app.command(self.message(100,'/start'))
        self.assertEqual(len(app.store.due_message_deletions(now=int(time.time())+601)),2)
        app._start_status_refresh({'chat':{'id':100},'message_id':30},now=100)
        self.assertEqual(app.status_refreshes[100]['expires_at'],700)
        for text in (app.help_text(),app.info_text(),app.status_text()):
            self.assertIsInstance(text,str)

    def test_menu_no_longer_pins_but_still_expires(self):
        app=self.app
        app.command(self.message(100,'/start'))
        self.assertEqual(app.store.meta('pending_menu_pin:100','0'),'0')
        self.assertFalse(any(m=='pinChatMessage' for m,p in app.telegram.sent))
        self.assertTrue(app.store.due_message_deletions(now=int(time.time())+601))

    def test_bot_pin_notice_is_recalled_without_touching_menu_or_input(self):
        app=self.app
        app.command(self.message(100,'/start'))
        menu=app.latest_menu_message_id
        app.pending_input={'action':'add','chain':'ethereum'}
        app.command({'chat':{'id':100,'type':'private'},'from':{'id':999,'is_bot':True},
                     'message_id':1234,'pinned_message':{'message_id':menu}})
        self.assertEqual(app.pending_input,{'action':'add','chain':'ethereum'})
        due=app.store.due_message_deletions()
        self.assertEqual([r['message_id'] for r in due],[1234])
        app._flush_message_deletions()
        deletes=[p for m,p in app.telegram.sent if m=='deleteMessage']
        self.assertEqual(deletes,[{'chat_id':100,'message_id':1234}])
        self.assertTrue(app.store.db.execute('SELECT 1 FROM telegram_deletions WHERE chat_id=100 AND message_id=?',(menu,)).fetchone())
        app.command({'chat':{'id':200,'type':'private'},'from':{'id':999,'is_bot':True},
                     'message_id':1235,'pinned_message':{'message_id':menu}})
        app.command({'chat':{'id':100,'type':'private'},'from':{'id':999,'is_bot':True},
                     'message_id':1236,'pinned_message':{'message_id':9999}})
        self.assertEqual(app.store.due_message_deletions(),[])

    def test_cooldown_keeps_updates_unacknowledged_without_replaying_commands(self):
        calls=[]
        self.app.telegram.rate=SimpleNamespace(remaining=lambda user=None:600,interactive=lambda user:nullcontext())
        self.app.telegram.call=lambda method,payload:[{'update_id':50,'message':self.message(100,'/start')}]
        self.app.command=lambda message:calls.append(message)
        self.app.poll_telegram()
        self.assertEqual(calls,[])
        self.assertEqual(self.app.store.meta('telegram_offset','0'),'0')
        self.app.telegram.rate.remaining=lambda user=None:0
        self.app.poll_telegram()
        self.assertEqual(len(calls),1)
        self.assertEqual(self.app.store.meta('telegram_offset','0'),'51')

    def test_scanner_rpc_counts_reach_status(self):
        from cluster import Scanner
        scanner=Scanner.__new__(Scanner)
        scanner.output=Path(self.temp.name)
        scanner.started_at=time.time()-2
        scanner.chain='bnb'
        scanner.epoch=1
        scanner.lease_id='test'
        scanner.process=SimpleNamespace(pid=1,poll=lambda:None)
        state={'status':'running','last_success_at':time.time(),'lag':0,'qualified':9,'candidates':47}
        (scanner.output/'state.json').write_text(json.dumps({'states':{'bnb':state}}))
        metrics=scanner.metrics()
        self.app.leases=SimpleNamespace(status=lambda:{'nodes':[{'node_id':'primary','last_seen':time.time(),'metrics':{'bnb':metrics}}],
                                                     'leases':[{'chain_name':'bnb','owner_node':'primary'}]})
        self.assertIn('9/47',self.app.status_text())
        self.assertIn('<pre>',self.app.status_text())
        metrics['lag']=200
        metrics['chain_blocks_per_second']=2
        self.assertIn('⚠️',self.app.status_text())
        metrics['lag']=0
        self.assertIn('✅',self.app.status_text())
        metrics.pop('qualified')
        self.assertIn('檢測中',self.app.status_text())
        self.app.command(self.message(100,'/debug'))
        self.assertIn('文字指令已停用',self.app.telegram.sent[-1][1]['text'])

    def test_status_table_short_names_order_and_escaped_customization(self):
        from bot_runtime.app import status_cell
        import html
        app=self.app
        app.config=json.loads((Path(__file__).parent/'examples/chains.example.json').read_text())
        app.adapters={name:SimpleNamespace(config=cfg) for name,cfg in app.config['chains'].items()}
        metrics={name:{'status':'running','lag':0,'qualified':2,'candidates':6} for name in app.adapters}
        app.leases=SimpleNamespace(status=lambda:{'nodes':[{'node_id':'primary','last_seen':time.time(),'metrics':metrics}],
            'leases':[{'chain_name':name,'owner_node':'primary'} for name in reversed(list(app.adapters))]})
        text=app.status_text()
        table=html.unescape(text.split('<pre>')[1].split('</pre>')[0]).splitlines()
        self.assertEqual([row.split()[1] for row in table[1:]],
                         ['Bitcoin','Ethereum','TRON','Solana','Polygon','BNB','Avalanche','OP','Arbitrum','Base','HyperEVM'])
        self.assertEqual(len({len(row) for row in table[1:]}),1)
        app.config['chains']['ethereum']['status_name']='<ETH>'
        self.assertIn('&lt;ETH&gt;',app.status_text())
        self.assertEqual(status_cell('測試',6),'測試  ')
        metrics['ethereum']['lag']=123456
        self.assertIn('999+',app.status_text())

        state=app.leases.status()
        state['leases'].append({'chain_name':'retired-chain','owner_node':'primary','disabled':1})
        app.leases=SimpleNamespace(status=lambda:state)
        self.assertNotIn('retired',app.status_text())
        next(row for row in state['leases'] if row['chain_name']=='hyperliquid')['disabled']=1
        self.assertNotIn('HyperEVM',app.status_text())

    def test_ingestion_deduplicated_bounded_validated(self):
        hit=dict(txid=TX,height=1,hash=HASH)
        self.app.feed.accept('ethereum',[hit,hit])
        self.app.feed.accept('ethereum',[hit])
        self.assertEqual(self.app.store.db.execute('SELECT COUNT(*) FROM scan_hits').fetchone()[0],1)
        with self.assertRaises(ValueError):
            self.app.feed.accept('ethereum',[dict(hit,user_id=200)])
        restored=FeedStore(self.path)
        self.assertIsNotNone(restored.due('ethereum'))
        restored.finish(restored.due('ethereum'),True)
        self.assertIsNone(restored.due('ethereum'))

    def test_full_chain_matching_native_token_and_tron(self):
        blocks={1:dict(hash=HASH,transactions=[dict(hash=TX,**{'from':A,'to':None})])}
        self.assertEqual(len(evm_hits(blocks,[],{A})),1)
        self.assertEqual(evm_hits(blocks,[],{B}),[])
        log=dict(topics=[TRANSFER_TOPIC,'0x'+'0'*24+A[2:],'0x'+'0'*24+B[2:]],transactionHash=TX,blockNumber='0x1',blockHash=HASH)
        self.assertEqual(len(evm_hits(blocks,[log],{B})),1)
        ta=tron_from_hex(A[2:])
        block={'blockID':HASH[2:],'block_header':{'raw_data':{'number':1}},'transactions':[]}
        info={'id':TX[2:],'log':[{'topics':[t.removeprefix('0x') for t in log['topics']]}]}
        self.assertEqual(len(tron_hits(block,[info],{ta})),1)

    def test_independent_receipt_success_and_identity_checks(self):
        responses={
            'eth_getBlockByNumber':dict(number='0x1',hash=HASH,parentHash='0x'+'0'*64,timestamp='0x64',transactions=[TX]),
            'eth_getTransactionByHash':dict(hash=TX,blockHash=HASH,**{'from':A,'to':B},value='0xde0b6b3a7640000'),
            'eth_getTransactionReceipt':dict(transactionHash=TX,blockHash=HASH,blockNumber='0x1',status='0x1',logs=[])}
        adapter=SimpleNamespace(name='ethereum',config=self.config['chains']['ethereum'],rpc=SimpleNamespace(rpc=lambda m,p:responses[m]))
        hit=dict(txid=TX,height=1,hash=HASH)
        found=decode(adapter,self.app.store,hit,{A,B})
        self.assertEqual({(e.address,e.direction) for e in found},{(A,'out'),(B,'in')})
        responses['eth_getTransactionReceipt']['status']='0x0'
        self.assertEqual(decode(adapter,self.app.store,hit,{A}),[])
        responses['eth_getTransactionReceipt']['blockHash']='0x'+'5'*64
        with self.assertRaises(ValueError):
            decode(adapter,self.app.store,hit,{A})

    def test_feed_api_rejects_stale_and_foreign_leases(self):
        config=json.loads((Path(__file__).parent / "examples/cluster.example.json").read_text())
        leases=LeaseStore(Path(self.temp.name)/'cluster.db',config)
        server=BoundedServer(('127.0.0.1',0),CoordinatorHandler)
        server.store,server.feed,server.chain_config=leases,self.app.feed,self.config['chains']
        server.node_ips={'primary':'127.0.0.1','jp':'127.0.0.2','kr':'127.0.0.3'}
        thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
        try:
            def call(chain,epoch,node='primary'):
                body=json.dumps(dict(node=node,chain=chain,epoch=epoch)).encode()
                with urllib.request.urlopen(urllib.request.Request(f'http://127.0.0.1:{server.server_port}/watch',body),timeout=3) as r:
                    return json.load(r)
            self.assertEqual(call('bnb',1),{'addresses':[]})
            for chain,epoch,node in [('ethereum',1,'primary'),('bnb',2,'primary'),('bnb',1,'jp')]:
                with self.assertRaises(urllib.error.HTTPError) as error:
                    call(chain,epoch,node)
                self.assertEqual(error.exception.code,403)
        finally:
            server.shutdown();server.server_close();thread.join()


if __name__=='__main__':
    unittest.main()
