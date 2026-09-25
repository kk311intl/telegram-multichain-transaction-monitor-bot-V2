import copy
import json
import tempfile
import unittest
from decimal import Decimal
from functools import partial
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch
from concurrent.futures import ThreadPoolExecutor

from benchmark import FullChainBenchmark
from bitcoin_chain import BitcoinAdapter, satoshis
from block_validation import ChainMismatch
from bot_runtime.backups import export_user_backup, parse_user_backup
from bot_runtime.decoder import decode, metadata
from bot_runtime.feed_store import FeedStore
from bot_runtime.finality import Finalizer
from bot_runtime.market import DexScreenerOracle
from bot_runtime.recent_cache import RecentCache
from bot_runtime.security import TokenSecurity
from bot_runtime.store import Store
from chain_identity import bitcoin_address, base58, normalize_address, valid_identity
from lease_store import LeaseStore
from solana_chain import SolanaAdapter, SYSTEM, TOKEN_PROGRAMS

BTC_A = '1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa'
BTC_B = '3J98t1WpEZ73CNmQviecrnyiWrnqRhWNLy'
A, B, MINT, TA, TB = ['1'*31+c for c in '23456']
HASH, PARENT, SIG = '1'*31+'7', '1'*31+'8', '1'*63+'2'


def btc_block():
    return dict(height=100, hash='a'*64, previousblockhash='b'*64, time=1700000000, nTx=1,
                tx=[dict(txid='c'*64, vin=[dict(prevout=dict(value='1',scriptPubKey=dict(address=BTC_A)))],
                         vout=[dict(value='0.6',scriptPubKey=dict(address=BTC_B)),
                               dict(value='0.3999',scriptPubKey=dict(address=BTC_A))])])


def sol_block():
    balances = [dict(accountIndex=i,mint=MINT,owner=owner) for i,owner in [(2,A),(3,B)]]
    token = dict(programId=sorted(TOKEN_PROGRAMS)[0], parsed=dict(type='transferChecked',
                 info=dict(source=TA,destination=TB,mint=MINT,tokenAmount=dict(amount='2500000',decimals=6))))
    tx = dict(transaction=dict(signatures=[SIG],message=dict(accountKeys=[dict(pubkey=k) for k in [A,B,TA,TB]],
                   instructions=[dict(programId=SYSTEM,parsed=dict(type='transfer',info=dict(source=A,destination=B,lamports=2000000000)))])),
              meta=dict(err=None,preTokenBalances=balances,postTokenBalances=copy.deepcopy(balances),
                        innerInstructions=[dict(index=0,instructions=[token])]))
    return dict(blockhash=HASH,previousBlockhash=PARENT,parentSlot=99,blockTime=1700000000,transactions=[tx])


class BitcoinSolanaTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name)/'bot.db'
        self.config = json.loads((Path(__file__).parent/'examples/chains.example.json').read_text())
        self.btc = BitcoinAdapter('bitcoin',self.config['chains']['bitcoin'])
        self.sol = SolanaAdapter('solana',self.config['chains']['solana'])

    def test_address_checksums_network_and_case(self):
        for address in (BTC_A, BTC_B, 'bc1qw508d6qejxtdg4y5r3zarvary0c5xw7kv8f3t4',
                        'BC1SW50QGDZ25J', 'bc1zw508d6qejxtdg4y5r3zarvaryvaxxpcs'):
            with self.subTest(address=address):
                self.assertEqual(bitcoin_address(address),address.lower() if address.lower().startswith('bc1') else address)
        for address in (BTC_A[:-1]+'b', 'mipcBbFg9gMiCh81Kj8tqqdgoZub1ZJRfn',
                        'bc1Qw508d6qejxtdg4y5r3zarvary0c5xw7kv8f3t4', 'bc1qw508d6qejxtdg4y5r3zarvary0c5xw7kygt080',
                        'bc1sw50qa3jx3s', 'tb1qrp33g0q5c5txsp9arysrx4k6zdkfs4nce4y7wf'):
            with self.subTest(address=address), self.assertRaises(ValueError):
                bitcoin_address(address)
        mint = 'So11111111111111111111111111111111111111112'
        self.assertEqual(normalize_address('solana',mint),mint)
        self.assertNotEqual(normalize_address('solana',mint.lower()),mint)
        for value in ('0'*32, '1'*31, '1'*33):
            with self.assertRaises(ValueError): base58(value,32)
        self.assertTrue(valid_identity(SIG,'solana',transaction=True))
        self.assertFalse(valid_identity(SIG,'solana'))

    def test_bitcoin_net_change_and_complete_prevouts(self):
        block = btc_block()
        self.assertTrue(self.btc.validate_block(block,100))
        tx = block['tx'][0]
        events = self.btc.events(block,100,tx['txid'],tx,{BTC_A,BTC_B},None)
        self.assertEqual({(e.address,e.direction,e.amount) for e in events},
                         {(BTC_A,'out',Decimal('0.6001')),(BTC_B,'in',Decimal('0.6'))})
        self.assertEqual(len(events),2)  # Change does not create a second incoming notice.
        self.assertEqual(satoshis('0.00000001'),1)
        for value in ('NaN','Infinity','-1','0.000000001','21000001'):
            with self.assertRaises(ChainMismatch): satoshis(value)
        tx['vin'][0].pop('prevout')
        with self.assertRaises(ChainMismatch): self.btc.validate_block(block,100)

    def test_bitcoin_coinbase_and_wrong_block_are_not_silently_accepted(self):
        block = btc_block()
        block['tx'][0]['vin'] = [dict(coinbase='00')]
        self.assertEqual(self.btc.addresses(block['tx'][0]),{BTC_A,BTC_B})
        self.assertTrue(self.btc.validate_block(block,100))
        with self.assertRaises(ChainMismatch): self.btc.validate_block(block,101)
        block['tx'].append(copy.deepcopy(block['tx'][0])); block['nTx']=2
        with self.assertRaises(ChainMismatch): self.btc.validate_block(block,100)

    def test_sol_native_inner_spl_and_failed_transaction(self):
        block = sol_block(); tx = block['transactions'][0]
        self.assertTrue(self.sol.validate_block(block,101))
        self.assertIn(A,self.sol.addresses(tx))
        lookup = Mock(return_value=('TEST',6,True))
        events = self.sol.events(block,101,SIG,tx,{A,B},lookup)
        self.assertEqual(len(events),4)
        self.assertEqual({(e.address,e.direction,e.asset_id,e.amount) for e in events},
                         {(A,'out','native',Decimal(2)),(B,'in','native',Decimal(2)),
                          (A,'out',MINT,Decimal('2.5')),(B,'in',MINT,Decimal('2.5'))})
        self.assertEqual(len({e.event_id for e in events}),4)
        tx['meta']['err']={'InstructionError':[0,'Custom']}
        self.assertEqual(self.sol.events(block,101,SIG,tx,{A,B},lookup),[])
        self.assertEqual(self.sol.addresses(tx),set())

    def test_sol_program_mint_and_owner_boundaries(self):
        block = sol_block(); tx = block['transactions'][0]
        token = tx['meta']['innerInstructions'][0]['instructions'][0]
        token['programId'] = A
        self.assertEqual(len(self.sol.events(block,101,SIG,tx,{A},lambda _:('TEST',6,True))),1)
        token['programId'] = next(iter(TOKEN_PROGRAMS))
        token['parsed']['info']['mint'] = B
        with self.assertRaises(ChainMismatch): self.sol.events(block,101,SIG,tx,{A},lambda _:('TEST',6,True))
        token['parsed']['info']['mint'] = MINT
        tx['meta']['postTokenBalances'][0]['owner'] = B
        with self.assertRaises(ChainMismatch): self.sol.events(block,101,SIG,tx,{A},lambda _:('TEST',6,True))

    def test_sol_temporary_token_account_created_and_closed_in_one_transaction(self):
        block=sol_block();tx=block['transactions'][0]
        for key in ('preTokenBalances','postTokenBalances'):
            tx['meta'][key]=[b for b in tx['meta'][key] if b['accountIndex']!=2]
        inner=tx['meta']['innerInstructions'][0]['instructions']
        init=dict(programId=next(iter(TOKEN_PROGRAMS)),parsed=dict(type='initializeAccount3',info=dict(account=TA,mint=MINT,owner=A)))
        inner.insert(0,init)
        inner.append(dict(programId=next(iter(TOKEN_PROGRAMS)),parsed=dict(type='closeAccount',info=dict(account=TA,destination=A,owner=A))))
        events=self.sol.events(block,101,SIG,tx,{A},lambda _:('TEST',6,True))
        self.assertEqual([(e.asset_id,e.amount_raw) for e in events],[('native',2000000000),(MINT,2500000)])
        self.assertIn(A,self.sol.addresses(tx))
        init['programId']=SYSTEM
        with self.assertRaises(ChainMismatch):self.sol.events(block,101,SIG,tx,{A},lambda _:('TEST',6,True))

    def test_confirmation_modes_and_extra_solana_distance(self):
        self.btc.rpc=Mock();self.btc.rpc.rpc.return_value=100
        self.assertEqual(self.btc.tip(),100);self.assertEqual(self.btc.safe_tip(),95)
        self.btc.config['scan_unconfirmed']=False
        self.assertEqual(self.btc.tip(),95)
        self.sol.finality_blocks=10
        self.sol.rpc=Mock();self.sol.rpc.rpc.side_effect=[100,[100],95,[85]]
        self.assertEqual(self.sol.tip(),100)
        self.assertEqual(self.sol.safe_tip(),85)
        self.assertEqual(self.sol.rpc.rpc.call_args.args[1][-1],{'commitment':'finalized'})

    def test_solana_skipped_slots_and_false_omission(self):
        block = sol_block()
        rpc = Mock(); self.sol.rpc = rpc
        rpc.rpc.side_effect = [[101], block]
        self.assertEqual(self.sol.header(100)[0],'')  # Successor parent=99 proves slot 100 absent.
        block['parentSlot']=100
        rpc.rpc.side_effect = [[101],block]
        with self.assertRaises(ChainMismatch): self.sol.header(100)
        rpc.rpc.side_effect = [[101]]
        self.assertEqual(self.sol.batch_end(100,100,105),101)
        rpc.rpc.side_effect = [[106]]
        with self.assertRaises(ChainMismatch): self.sol.batch_end(100,100,105)
        for slots in ([100,100],[101,100],[99],[True]):
            with self.assertRaises(ChainMismatch): self.sol.validate_heights(slots,100,105)

    def test_solana_duplicate_inner_groups_fail_before_cursor_commit(self):
        block=sol_block();groups=block['transactions'][0]['meta']['innerInstructions']
        groups.append(copy.deepcopy(groups[0]))
        with self.assertRaises(ChainMismatch):self.sol.validate_block(block,101)

    def test_rpc_qualification_rejects_wrong_network(self):
        for adapter in (self.btc,self.sol):
            adapter.rpc._post_url=Mock(return_value={'result':'wrong-genesis'})
            with self.assertRaisesRegex(RuntimeError,'genesis'):adapter._validate(adapter.rpc.urls[0])

    def test_solana_proven_skipped_slot_orphans_pending_event(self):
        store=Store(self.path,100);self.addCleanup(store.db.close);store.configure_owner(100)
        block=sol_block();tx=block['transactions'][0]
        event=self.sol.events(block,101,SIG,tx,{A},lambda _:('TEST',6,True))[0]
        store.save_event(event,False,'',confirmed=False,user_ids={100})
        self.sol.safe_tip=Mock(return_value=200);self.sol.header=Mock(return_value=('',PARENT))
        Finalizer(self.sol,store).settle()
        row=store.db.execute('SELECT confirmation_state,orphaned FROM events WHERE event_id=?',(event.event_id,)).fetchone()
        self.assertEqual(tuple(row),('orphaned',1))

    def test_full_block_batch_checks_parent_before_handoff(self):
        block = sol_block()
        scanner = object.__new__(FullChainBenchmark)
        scanner.block_pool = ThreadPoolExecutor(max_workers=4)
        self.addCleanup(scanner.block_pool.shutdown)
        scanner.checkpoints = {99:PARENT}
        scanner.feed = Mock(); scanner.feed.refresh.return_value={A}
        adapter = self.sol
        adapter.heights = Mock(return_value=[101]); adapter.block = Mock(return_value=block)
        adapter.header = Mock(return_value=(HASH,PARENT))
        self.assertEqual(scanner._full_block_batch(adapter,100,101),(1,0))
        self.assertEqual(scanner.batch_hashes,{101:HASH})
        scanner.feed.submit.assert_called_once_with([dict(txid=SIG,height=101,hash=HASH)])
        scanner.feed.submit.reset_mock()
        block['parentSlot']=100
        with self.assertRaises(ChainMismatch): scanner._full_block_batch(adapter,100,101)
        scanner.feed.submit.assert_not_called()

    def test_coordinator_rechecks_canonical_hash_membership_and_caches(self):
        for adapter,block,height,txid,digest,watched in (
            (self.btc,btc_block(),100,'c'*64,'a'*64,{BTC_A}),
            (self.sol,sol_block(),101,SIG,HASH,{A})):
            with self.subTest(chain=adapter.name):
                adapter.header=Mock(return_value=(digest,None));adapter.block=Mock(return_value=block)
                hit=dict(height=height,txid=txid,hash=digest)
                cache=RecentCache()
                for _ in range(2):
                    self.assertTrue(decode(adapter,None,hit,watched,cache,lambda *args:('TEST',6,True)))
                adapter.block.assert_called_once()
                with self.assertRaises(ValueError):
                    decode(adapter,None,{**hit,'txid':'wrong'},watched)
                adapter.header.return_value=('changed',None)
                self.assertEqual(decode(adapter,None,hit,watched),[])

    def test_custom_names_feed_and_lease_identity_do_not_relax_evm(self):
        kinds={'btc-main':'bitcoin','sol-main':'solana','evm-main':'evm'}
        feed=FeedStore(self.path,kinds)
        feed.accept('sol-main',[dict(txid=SIG,height=101,hash=HASH)])
        feed.accept('btc-main',[dict(txid='c'*64,height=100,hash='a'*64)])
        for chain,txid,digest in [('evm-main',SIG,HASH),('btc-main','0x'+'a'*64,'a'*64),('sol-main',HASH,HASH)]:
            with self.assertRaises(ValueError): feed.accept(chain,[dict(txid=txid,height=100,hash=digest)])
        cfg=dict(primary_node='primary',nodes={'primary':dict(max_chains=1)},chains={'sol-main':dict(preferred_node='primary')})
        leases=LeaseStore(Path(self.temp.name)/'leases.db',cfg,verifier=lambda *args:True,now=0,chain_types=kinds)
        assignment=leases.heartbeat('primary',{},0)[0]
        state=dict(epoch=assignment['epoch'],cursor=101,block_hash=HASH,run_id='a'*32)
        leases.heartbeat('primary',{'sol-main':state},1)
        self.assertEqual(leases.status()['leases'][0]['block_hash'],HASH)

    def test_new_chain_user_isolation_export_and_confirmation(self):
        store=Store(self.path,100); self.addCleanup(store.db.close)
        store.authorize_user(200)
        store.configure_owner(100)
        store.add('bitcoin',BTC_A,'mine',user_id=100)
        store.add('solana',A,'other',user_id=200)
        for chain,block,height,txid,digest,address,owner,adapter in (
            ('bitcoin',btc_block(),100,'c'*64,'a'*64,BTC_A,100,self.btc),
            ('solana',sol_block(),101,SIG,HASH,A,200,self.sol)):
            tx=adapter.transactions(block)[0][1]
            event=adapter.events(block,height,txid,tx,{address},lambda _:('TEST',6,True))[0]
            store.save_event(event,False,'',confirmed=False,user_ids={100,200},
                             address_ids={r['id'] for r in store.addresses(user_id=-1)})
            self.assertTrue(store.delivery_allowed(event.event_id,owner))
            self.assertFalse(store.delivery_allowed(event.event_id,300-owner))
            adapter.safe_tip=Mock(return_value=height+10);adapter.header=Mock(return_value=(digest,None))
            Finalizer(adapter,store).settle()
            row=store.db.execute('SELECT confirmation_state FROM events WHERE event_id=?',(event.event_id,)).fetchone()
            self.assertEqual(row[0],'confirmed')
        adapters={name:SimpleNamespace(normalize=partial(normalize_address,cfg['type'])) for name,cfg in self.config['chains'].items()}
        restored=parse_user_backup(export_user_backup(store,self.config,[],200),adapters,[])
        self.assertEqual([(r['chain'],r['address']) for r in restored],[('solana',A)])

    def test_sol_metadata_persistent_and_non_mint_rejected(self):
        store=Store(self.path,100);self.addCleanup(store.db.close);FeedStore(self.path)
        self.sol.rpc=Mock()
        self.sol.rpc.rpc.return_value=dict(value=dict(owner=next(iter(TOKEN_PROGRAMS)),data=dict(parsed=dict(type='mint',info=dict(decimals=6)))))
        first=metadata(self.sol,store,MINT)
        self.assertEqual(first,('SPL '+MINT[:6],6,True))
        self.assertEqual(metadata(self.sol,store,MINT),first)
        self.sol.rpc.rpc.assert_called_once()
        self.sol.rpc.rpc.return_value['value']['owner']=SYSTEM
        with self.assertRaises(ValueError): self.sol.token_metadata(B)

    def test_sol_market_and_security_preserve_mint_case(self):
        mint='So11111111111111111111111111111111111111112'
        pair=dict(chainId='solana',baseToken=dict(address=mint.lower()),quoteToken=dict(address=A),priceUsd='1',liquidity=dict(usd=100000))
        with patch('bot_runtime.market.get_json',return_value=[pair]):
            self.assertFalse(DexScreenerOracle().assess(self.sol.config,mint).valuable)
        store=Store(self.path,100);self.addCleanup(store.db.close)
        security=TokenSecurity();security.request=Mock(side_effect=[[dict(id='1')],{mint:dict(fake_token=dict(value='1'))}])
        self.assertEqual(security.assess(store,self.sol.config,mint),'goplus_fake_token')
        self.assertIn('solana/token_security?',security.request.call_args.args[1])
        self.assertEqual(security.assess(store,self.sol.config,mint),'goplus_fake_token')
        self.assertEqual(security.request.call_count,2)


if __name__ == '__main__':
    unittest.main()
