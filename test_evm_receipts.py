import copy
import json
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from block_validation import ChainMismatch, check_evm_logs
from evm_receipts import BlockReceipts
from monitor.rpc import JsonClient, RpcRequestError, RpcMethodUnavailable, _PROVIDER_GATES


class ReceiptBatchTest(unittest.TestCase):
    def setUp(self):
        self.hash = '0x'+'a'*64
        self.txs = ['0x'+'1'*64,'0x'+'2'*64]
        self.blocks = {10:dict(hash=self.hash,transactions=[dict(hash=tx) for tx in self.txs])}
        self.logs = [dict(blockHash=self.hash,blockNumber='0xa',transactionHash=tx,transactionIndex=hex(i),
                         logIndex=hex(i),address='0x123',data='0x01',topics=['0xabc']) for i,tx in enumerate(self.txs)]
        self.receipts = [dict(blockHash=self.hash,blockNumber='0xa',transactionHash=tx,
                              logs=[dict(log,logIndex='0x0')]) for tx,log in zip(self.txs,self.logs)]
        self.client = JsonClient('https://receipts.invalid')
        self.calls = []
        self.unsupported = False
        def post(url,payload,path=''):
            method=payload['method'];self.calls.append(method)
            if method=='eth_getBlockReceipts':
                if self.unsupported:raise RpcMethodUnavailable('method unsupported')
                result=copy.deepcopy(self.receipts)
            else:result=copy.deepcopy(self.receipts[self.txs.index(payload['params'][0])])
            return dict(id=payload['id'],result=result)
        self.client._post_url=post

    def test_one_block_read_verifies_multiple_transactions_and_releases_batch_cache(self):
        lookup=BlockReceipts(self.client.for_endpoint(0),self.blocks)
        check_evm_logs(self.blocks,copy.deepcopy(self.logs),lookup,reconcile_receipts=True)
        self.assertEqual(self.calls,['eth_getBlockReceipts'])
        self.assertEqual(lookup(self.txs[0])['transactionHash'],self.txs[0])
        self.assertEqual(len(self.calls),1)

    def test_bulk_cache_releases_previous_block_receipts(self):
        from unittest.mock import Mock
        tx='0x'+'3'*64
        blocks=dict(self.blocks)
        blocks[11]=dict(hash='0x'+'b'*64,transactions=[dict(hash=tx)])
        later=dict(blockHash=blocks[11]['hash'],blockNumber='0xb',transactionHash=tx,logs=[])
        rpc=Mock()
        def optional(method,params,result_validator):
            result=self.receipts if params==['0xa'] else [later]
            self.assertTrue(result_validator(result))
            return result
        rpc.optional_rpc=optional
        lookup=BlockReceipts(rpc,blocks)
        lookup(self.txs[0]);self.assertEqual(len(lookup.receipts),2)
        lookup(tx);self.assertEqual(set(lookup.receipts),{tx})

    def test_unsupported_falls_back_without_poisoning_endpoint_and_persists_capability(self):
        self.unsupported=True
        with tempfile.TemporaryDirectory() as tmp:
            self.client._health_path=Path(tmp)/'health.json'
            lookup=BlockReceipts(self.client.for_endpoint(0),self.blocks)
            check_evm_logs(self.blocks,copy.deepcopy(self.logs),lookup,reconcile_receipts=True)
            self.assertEqual(self.calls.count('eth_getBlockReceipts'),1)
            self.assertEqual(self.calls.count('eth_getTransactionReceipt'),2)
            self.assertTrue(self.client.endpoint_health[self.client.urls[0]])
            restored=JsonClient(self.client.urls);restored._health_path=self.client._health_path;restored.restore_health()
            self.assertGreater(restored.unsupported_methods[(restored.urls[0],'eth_getBlockReceipts')],time.time())
            self.assertIsNone(restored.for_endpoint(0).optional_rpc('eth_getBlockReceipts',['0xb']))

    def test_incomplete_wrong_block_duplicate_and_timeout_never_fall_back_as_success(self):
        original=copy.deepcopy(self.receipts)
        variants=[[],original[:1],[original[0],original[0]],
                  [dict(original[0],blockHash='0x'+'b'*64),original[1]]]
        for receipts in variants:
            with self.subTest(receipts=len(receipts)):
                self.receipts=receipts
                c=JsonClient(self.client.urls);c._post_url=self.client._post_url
                with self.assertRaises(ChainMismatch):BlockReceipts(c.for_endpoint(0),self.blocks)(self.txs[0])
        self.assertNotIn('eth_getTransactionReceipt',self.calls)
        c=JsonClient(self.client.urls)
        c._post_url=lambda *a,**k:(_ for _ in ()).throw(RpcRequestError('TimeoutError'))
        with self.assertRaises(RpcRequestError):BlockReceipts(c.for_endpoint(0),self.blocks)(self.txs[0])
        self.assertFalse(c.unsupported_methods)

    def test_validation_reason_survives_rpc_and_healthy_peer_can_serve(self):
        c=JsonClient(['https://bad.invalid','https://good.invalid'])
        c._post_url=lambda u,p,path='':dict(id=p['id'],result=[] if 'bad' in u else [1])
        def validate(value):
            if not value:raise ChainMismatch('log/block hash mismatch')
            return True
        self.assertEqual(c.rpc('eth_getLogs',[],result_validator=validate),[1])
        c=JsonClient('https://bad.invalid');c._post_url=lambda u,p,path='':dict(id=p['id'],result=[])
        with self.assertRaisesRegex(ChainMismatch,'log/block hash mismatch'):
            c.rpc('eth_getLogs',[],result_validator=validate)

    def test_configured_provider_gate_is_shared_and_validated(self):
        provider='receipt-test-provider';_PROVIDER_GATES.pop(provider,None)
        a=JsonClient('https://one.invalid');b=JsonClient('https://two.invalid')
        for c in (a,b):c.configure_endpoints({c.urls[0]:dict(min_interval_seconds=.75,max_log_blocks=50,provider=provider)})
        clock=[1000.]
        def sleep(seconds):clock[0]+=seconds
        with patch('monitor.rpc.time.monotonic',side_effect=lambda:clock[0]),patch('monitor.rpc.time.sleep',side_effect=sleep):
            a._wait_endpoint(a.urls[0]);b._wait_endpoint(b.urls[0])
            self.assertAlmostEqual(clock[0],1000.75)
            self.assertEqual(a.log_range_limit(),50)
            a._mark_failure(a.urls[0],RpcRequestError('HTTP 429',60))
            with self.assertRaisesRegex(RpcRequestError,'provider cooling'):b._wait_endpoint(b.urls[0])
        for rule in [dict(min_interval_seconds=float('nan')),dict(max_log_blocks=0),dict(provider='')]:
            with self.assertRaises(ValueError):a.configure_endpoints({a.urls[0]:rule})
        _PROVIDER_GATES.pop(provider,None)


if __name__=='__main__':unittest.main()
