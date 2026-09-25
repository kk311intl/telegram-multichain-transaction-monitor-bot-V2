import json
import tempfile
import threading
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch
from monitor.rpc import JsonClient, RpcRequestError
from benchmark import next_batch_span, TARGET_BATCH_BYTES
from cluster import Scanner


class RuntimeRecoveryTest(unittest.TestCase):
    def test_retrying_is_not_stalled_but_missing_cycle_progress_is(self):
        with tempfile.TemporaryDirectory() as d:
            scanner=Scanner.__new__(Scanner)
            scanner.output=Path(d);scanner.started_at=time.time()-500
            scanner.chain='x';scanner.epoch=1;scanner.lease_id='test'
            scanner.process=SimpleNamespace(pid=1,poll=lambda:None)
            state={'status':'retrying','last_success_at':time.time()-300,'last_cycle_at':time.time()}
            path=scanner.output/'state.json'
            path.write_text(json.dumps({'states':{'x':state}}))
            self.assertEqual(scanner.metrics()['status'],'retrying')
            self.assertGreater(scanner.metrics()['state_age_seconds'],120)
            state['last_cycle_at']=time.time()-121
            path.write_text(json.dumps({'states':{'x':state}}))
            self.assertEqual(scanner.metrics()['status'],'stalled')

    def test_checkpoint_restore_preserves_anchor_and_reorg_rules(self):
        from benchmark import FullChainBenchmark
        with tempfile.TemporaryDirectory() as directory:
            b=FullChainBenchmark.__new__(FullChainBenchmark)
            b.output=Path(directory)/'epoch-1';b.output.mkdir()
            b.args=SimpleNamespace(start_cursor=-1,start_hash='')
            retained=Path(directory)/'checkpoints.json'
            retained.write_text(json.dumps({'1':'too-old','171':'retained','299':'parent','300':'anchor'}))
            with patch('benchmark.header',return_value=('anchor','parent')):
                self.assertEqual(b._restore_checkpoints(None,300),(300,'anchor'))
                self.assertEqual(b.checkpoints,{299:'parent',300:'anchor'})
                retained.write_text('{broken')
                self.assertEqual(b._restore_checkpoints(None,300),(300,'anchor'))
                self.assertEqual(b.checkpoints,{300:'anchor'})
                b.args.start_cursor=300;b.args.start_hash='old-anchor'
                def recover(adapter,cursor):
                    self.assertEqual(b.checkpoints[300],'old-anchor')
                    b.checkpoints={299:'parent'}
                    return 299
                b._recover_reorg=recover
                self.assertEqual(b._restore_checkpoints(None,300),(299,'parent'))

    def test_hyper_batch_retries_as_one_endpoint_and_shares_cooldown(self):
        from benchmark import FullChainBenchmark
        from block_validation import ChainMismatch
        c=JsonClient(['https://bad.invalid','https://good.invalid'])
        a=SimpleNamespace(name='hyperliquid',rpc=c)
        b=FullChainBenchmark.__new__(FullChainBenchmark)
        calls=[]
        def batch(scoped,start,end):
            indices=scoped.rpc._candidate_indices()
            calls.append(indices)
            self.assertIs(scoped.rpc.stats,c.stats)
            if indices==[0]:raise ChainMismatch('incomplete source block')
            return (2,3)
        b._evm_batch_from_endpoint=batch
        self.assertEqual(b._evm_batch(a,1,2),(2,3))
        self.assertEqual(calls,[[0],[1]])
        self.assertFalse(c.endpoint_health[c.urls[0]])
        self.assertEqual(c.for_endpoint(0)._candidate_indices(),[])

    def test_hyper_receipt_indices_are_scoped_to_transaction(self):
        from block_validation import check_evm_logs, ChainMismatch
        import copy
        h='0x'+'a'*64
        txs=['0x'+'1'*64,'0x'+'2'*64]
        block={'hash':h,'transactions':[{'hash':tx} for tx in txs]}
        logs=[{'blockHash':h,'blockNumber':'0xa','transactionHash':tx,'transactionIndex':hex(i),
               'logIndex':hex(i),'address':'0x123','data':'0x01','topics':['0xabc']} for i,tx in enumerate(txs)]
        receipts={tx:{'blockHash':h,'blockNumber':'0xa','transactionHash':tx,'logs':[dict(log,logIndex='0x0')]}
                  for tx,log in zip(txs,logs)}
        check_evm_logs({10:block},copy.deepcopy(logs),receipts.get,reconcile_receipts=True)
        duplicated=copy.deepcopy(logs)+[dict(logs[0],logIndex='0x2')]
        with self.assertRaises(ChainMismatch):check_evm_logs({10:block},duplicated,receipts.get,reconcile_receipts=True)
        receipts[txs[1]]['logs'][0]['data']='0x02'
        with self.assertRaises(ChainMismatch):check_evm_logs({10:block},copy.deepcopy(logs),receipts.get,reconcile_receipts=True)

    def test_same_chain_id_wrong_genesis_is_rejected_before_data(self):
        from scanner_adapters import EvmAdapter
        a=EvmAdapter('hyperliquid',{'expected_chain_id':999,'expected_genesis_hash':'0x'+'1'*64,'finality_blocks':0,'rpc_urls':['https://wrong.invalid']})
        calls=[]
        def post(url,body,path=''):
            calls.append(body['method'])
            return {'result':'0x3e7' if body['method']=='eth_chainId' else {'hash':'0x'+'2'*64}}
        a.rpc._post_url=post
        with self.assertRaisesRegex(RuntimeError,'genesis'):a._validate(a.rpc.url)
        self.assertEqual(calls,['eth_chainId','eth_getBlockByNumber'])

    def test_stale_chain_head_is_rejected_and_falls_back(self):
        from scanner_adapters import EvmAdapter
        adapter=EvmAdapter('x',{'expected_chain_id':1,'finality_blocks':0,'rpc_urls':['https://old.invalid','https://fresh.invalid']})
        client=adapter.rpc
        client.set_endpoint_validator(lambda url:None)
        def response(url,payload,path=''):
            old='old' in url
            block={'number':hex(45087045 if old else 46596723),'timestamp':hex(int(time.time())-(86400 if old else 1)),
                   'hash':'0x'+'1'*64,'parentHash':'0x'+'2'*64}
            return {'id':payload['id'],'result':block}
        client._post_url=response
        self.assertEqual(adapter.tip(),46596723)
        self.assertFalse(client.endpoint_health[client.urls[0]])
        self.assertTrue(client.endpoint_health[client.urls[1]])

    def test_genesis_exception_is_exact_and_retains_data_checks(self):
        from scanner_adapters import EvmAdapter
        official, other = 'https://official.invalid/evm', 'https://other.invalid'
        config = {'expected_chain_id':999, 'expected_genesis_hash':'0x'+'1'*64,
                  'finality_blocks':2, 'rpc_urls':[official, other],
                  'genesis_optional_rpc_urls':[official]}
        adapter = EvmAdapter('hyperliquid', config)
        calls = []
        chain, age, logs = '0x3e7', 1, []
        def response(url, payload, path=''):
            method, params = payload['method'], payload['params']
            calls.append((url, method, params))
            if method == 'eth_chainId': return {'result':chain}
            if method == 'eth_getLogs': return {'result':logs}
            if method == 'eth_getBlockByNumber':
                if params[0] == '0x0': raise RuntimeError('genesis unavailable')
                return {'result':{'number':hex(1000) if params[0]=='latest' else params[0],
                                  'hash':'0x'+'a'*64, 'parentHash':'0x'+'b'*64,
                                  'timestamp':hex(int(time.time())-age), 'transactions':[]}}
            raise AssertionError(method)
        adapter.rpc._post_url = response
        adapter._validate(official)
        self.assertFalse(any(params and params[0]=='0x0' for _, _, params in calls))
        self.assertTrue(any(method=='eth_getLogs' for _, method, _ in calls))
        with self.assertRaisesRegex(RuntimeError,'genesis'): adapter._validate(other)
        chain = '0x1'
        with self.assertRaisesRegex(RuntimeError,'identity'): adapter._validate(official)
        chain, age = '0x3e7', 181
        with self.assertRaisesRegex(RuntimeError,'stale'): adapter._validate(official)
        age = -121
        with self.assertRaisesRegex(RuntimeError,'stale'): adapter._validate(official)
        age = 1
        adapter._highest_tip = 1129
        with self.assertRaisesRegex(RuntimeError,'stale'): adapter._validate(official)
        adapter._highest_tip, logs = 1000, None
        with self.assertRaisesRegex(RuntimeError,'Transfer'): adapter._validate(official)

    def test_genesis_exception_config_and_cache_identity(self):
        from scanner_adapters import EvmAdapter
        url = 'https://official.invalid/evm'
        config = {'expected_chain_id':999, 'finality_blocks':2, 'rpc_urls':[url]}
        strict = EvmAdapter('hyperliquid',config).rpc._validation_identity
        for invalid in [url, [url+'/other'], [None]]:
            with self.assertRaises(ValueError):
                EvmAdapter('hyperliquid',dict(config,genesis_optional_rpc_urls=invalid))
        relaxed = EvmAdapter('hyperliquid',dict(config,genesis_optional_rpc_urls=[url]))
        self.assertNotEqual(strict,relaxed.rpc._validation_identity)

    def test_validation_and_lock_share_budget(self):
        c=JsonClient('https://one.invalid')
        c.set_endpoint_validator(Mock())
        lock=c._validation_locks[c.url]
        lock.acquire()
        c._budget.deadline=time.monotonic()+0.03
        started=time.monotonic()
        try:
            with self.assertRaises(RpcRequestError):c._ensure_endpoint_validated(c.url)
            self.assertLess(time.monotonic()-started,0.3)
            c._endpoint_validator.assert_not_called()
        finally:lock.release()
        c._budget.deadline=None
        c._ensure_endpoint_validated(c.url)
        c._endpoint_validator.assert_called_once()

    def test_validation_cannot_renew_budget_or_accept_late_result(self):
        c=JsonClient('https://one.invalid')
        def slow(url):time.sleep(.04)
        c.set_endpoint_validator(slow)
        c._budget.deadline=time.monotonic()+.02
        with self.assertRaises(RuntimeError):c._ensure_endpoint_validated(c.url)
        self.assertEqual(c.qualified_urls(),[])

    def test_socket_body_uses_remaining_budget(self):
        from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
        class Handler(BaseHTTPRequestHandler):
            def log_message(self,*a):pass
            def do_POST(self):
                self.send_response(200);self.send_header('Content-Length','2');self.end_headers()
                time.sleep(.3)
                try:self.wfile.write(b'{}')
                except OSError:pass
        server=ThreadingHTTPServer(('127.0.0.1',0),Handler)
        thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
        try:
            c=JsonClient('http://127.0.0.1:'+str(server.server_port))
            c._budget.deadline=time.monotonic()+.08
            started=time.monotonic()
            with self.assertRaises(RpcRequestError):c._post_url(c.url,{})
            self.assertLess(time.monotonic()-started,.25)
        finally:server.shutdown();server.server_close();thread.join()

    def test_batch_respects_memory_and_slow_response_limits(self):
        self.assertEqual(next_batch_span(4,1000,3,100,1),8)
        self.assertEqual(next_batch_span(4,TARGET_BATCH_BYTES*3,1,100,1),2)
        self.assertEqual(next_batch_span(4,1000,20,100,1),2)
        self.assertEqual(next_batch_span(64,1000,1,1000,1),64)
        self.assertEqual(next_batch_span(4,1000,3,2,1),4)

    def test_exit_record_is_persistent_and_idempotent(self):
        with tempfile.TemporaryDirectory() as d:
            s=Scanner.__new__(Scanner)
            s.output=Path(d)/'epoch-1';s.output.mkdir()
            s.chain='x';s.epoch=1;s.lease_id='a'*32
            s.log=(s.output/'scanner.log').open('a')
            s.process=SimpleNamespace(poll=lambda:76)
            with patch('builtins.print'):
                s.stop('exited');s.stop('replaced')
            record=json.loads((Path(d)/'scanner-lifecycle.json').read_text())
            self.assertEqual(record['exit_code'],76)
            self.assertEqual(record['unexpected_exits'],1)
            self.assertEqual(record['reason'],'exited')
