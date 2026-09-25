import tempfile
import unittest
from argparse import Namespace
from pathlib import Path

from benchmark import FullChainBenchmark
from scanner_adapters import TRANSFER_TOPIC
from monitor.rpc import JsonClient


class FakeRpc(JsonClient):
    def __init__(self):
        super().__init__('https://invalid')

    def rpc(self, method, params, **kwargs):
        if method == "eth_getBlockByNumber":
            height = int(params[0],16)
            return {"number":params[0], "hash":"0x"+format(height,"064x"), "parentHash":"0x"+format(height-1,"064x"), "transactions":[{"hash":"0x"+format(height,"064x")}]}
        if method == "eth_getLogs":
            value = [{"topics": [TRANSFER_TOPIC, "a", "b"], "blockNumber":"0xa","blockHash":"0x"+format(10,"064x"),"transactionHash":"0x"+format(10,"064x"),"transactionIndex":"0x0","logIndex":"0x0"}]
            assert kwargs['result_validator'](value)
            return value
        raise AssertionError(method)


class ParallelBlockTest(unittest.TestCase):
    def test_unreturned_block_thread_is_retained_for_watchdog(self):
        import time, threading
        from concurrent.futures import ThreadPoolExecutor
        from types import SimpleNamespace
        release=threading.Event()
        rpc=JsonClient('https://blocked.invalid').with_deadline(time.monotonic()+.04)
        def blocked(*args,**kwargs):
            release.wait(3)
            raise RuntimeError('closed')
        rpc.rpc=blocked
        b=FullChainBenchmark.__new__(FullChainBenchmark)
        b.checkpoints={}
        with ThreadPoolExecutor(max_workers=1) as b.block_pool:
            try:
                with self.assertRaises(TimeoutError):
                    b._evm_batch_from_endpoint(SimpleNamespace(rpc=rpc),1,1)
                self.assertEqual(len(b.pending_block_reads),1)
            finally:release.set()

    def test_batch_counts_every_block_and_transfer_log(self):
        with tempfile.TemporaryDirectory() as directory:
            config = Path(directory) / "chains.json"
            config.write_text('{"chains":{"x":{"type":"evm","expected_chain_id":1,"finality_blocks":0,"rpc_urls":["https://invalid"]}}}', encoding="utf-8")
            benchmark = FullChainBenchmark(Namespace(
                output=directory, config=str(config), chains="x", seconds=1,
                pwsh="", start_cursor=-1, headless=True, log_interval=10,
            ))
            adapter = type("Adapter", (), {"rpc": FakeRpc()})()
            self.assertEqual(benchmark._evm_batch(adapter, 10, 13), (4, 1))
            benchmark.block_pool.shutdown()


if __name__ == "__main__":
    unittest.main()
