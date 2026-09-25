import copy
import json
import tempfile
import time
import unittest
from pathlib import Path
from lease_store import LeaseStore
from block_validation import ChainMismatch, evm_header, check_evm_logs, tron_header

HASH = '0x' + 'a' * 64


class SafetyTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / 'db'
        self.config = json.loads((Path(__file__).parent / "examples/cluster.example.json").read_text())
        self.store = LeaseStore(self.path, self.config, lambda *a: True, now=1000)

    def metric(self, assignment, cursor=100):
        return {assignment['chain']: {'epoch': assignment['epoch'], 'cursor': cursor, 'block_hash': HASH, 'run_id':'a'*32, 'status': 'running'}}

    def test_absent_node_fallback_and_never_primary_to_workers(self):
        self.store.heartbeat('primary', {}, 1000)
        result = self.store.heartbeat('primary', {}, 1051)
        self.assertEqual(len(result), 9)
        self.assertEqual(self.store.heartbeat('worker-1', {}, 1052), [])
        self.assertEqual(self.store.heartbeat('worker-1', {}, 2000), [])

    def test_rebalance_drains_old_lease_before_transfer(self):
        self.store.heartbeat('primary', {}, 1051)
        self.store.heartbeat('worker-1', {}, 1052)
        self.assertEqual(self.store.rebalance('worker-1', 1053), 3)
        self.assertEqual(self.store.heartbeat('worker-1', {}, 1087), [])
        self.assertEqual(len(self.store.heartbeat('worker-1', {}, 1089)), 3)

    def test_bad_payload_and_foreign_epoch_no_mutation(self):
        a = self.store.heartbeat('worker-1', {}, 1000)[0]
        before = self.store.status()
        for metrics in ([], self.metric(a, 10**15), {a['chain']: {'epoch': 999, 'cursor': 100}}):
            with self.assertRaises(ValueError):
                self.store.heartbeat('worker-1', metrics, 1001)
            self.assertEqual(self.store.status(), before)
        with self.assertRaises(ValueError):
            self.store.heartbeat('worker-2', self.metric(a), 1001)

    def test_proof_and_bounded_rollback(self):
        a = self.store.heartbeat('worker-1', {}, 1000)[0]
        self.store.heartbeat('worker-1', self.metric(a), 1001)
        with self.assertRaises(ValueError):
            self.store.heartbeat('worker-1', self.metric(a, 90), 1002)
        metric = self.metric(a, 90)
        metric[a['chain']]['rollback'] = 'reorg'
        self.store.heartbeat('worker-1', metric, 1002)
        row = next(r for r in self.store.status()['leases'] if r['chain_name'] == a['chain'])
        self.assertEqual(row['cursor'], 90)

    def test_config_reconcile_and_removal(self):
        config = copy.deepcopy(self.config)
        config['nodes']['worker-2']['max_chains'] = 3
        config['chains']['ethereum']['preferred_node'] = 'worker-2'
        del config['chains']['base']
        store = LeaseStore(self.path, config, now=1001)
        rows = {r['chain_name']: r for r in store.status()['leases']}
        self.assertEqual(rows['ethereum']['preferred_node'], 'worker-2')
        self.assertEqual(rows['ethereum']['target'], 'worker-2')
        self.assertEqual(rows['base']['disabled'], 1)

    def test_failed_proof_does_not_advance_cursor(self):
        self.store.verifier = lambda *a: False
        a = self.store.heartbeat('worker-1', {}, 1000)[0]
        self.store.heartbeat('worker-1', self.metric(a), 1001)
        self.assertIsNone(next(r for r in self.store.status()['leases'] if r['chain_name'] == a['chain'])['cursor'])

    def test_chain_unhealthy_moves_even_with_fresh_node_heartbeat(self):
        self.store.heartbeat('primary', {}, 1000)
        a = self.store.heartbeat('worker-1', {}, 1000)[0]
        m = {a['chain']: {'epoch':a['epoch'], 'status':'cooldown'}}
        self.store.heartbeat('worker-1', m, 1001)
        for t in range(1005,1126,5):
            self.store.heartbeat('primary', {}, t)
            self.store.heartbeat('worker-1', m, t)
        names = {x['chain'] for x in self.store.heartbeat('primary', {}, 1148)}
        self.assertIn(a['chain'], names)

    def test_headers_and_cross_response_validation(self):
        block = {'number':'0xa','hash':HASH,'parentHash':'0x'+'b'*64,'transactions':[]}
        self.assertEqual(evm_header(block,10)[0],HASH)
        with self.assertRaises(ChainMismatch):
            evm_header(block,11)
        with self.assertRaises(ChainMismatch):
            check_evm_logs({10:block}, [{'blockNumber':'0xa','blockHash':'0x'+'c'*64}])
        with self.assertRaises(ChainMismatch):
            tron_header({'blockID':'a'*64,'block_header':{'raw_data':{'number':10}}},10)

    def test_retrying_rpc_can_fail_over_without_restarting_scanner(self):
        self.store.heartbeat('primary', {}, 1000)
        a=self.store.heartbeat('worker-1', {}, 1000)[0]
        metric={a['chain']:{'epoch':a['epoch'],'status':'retrying'}}
        self.store.heartbeat('worker-1',metric,1001)
        for t in range(1005,1126,5):
            self.store.heartbeat('primary',{},t)
            self.store.heartbeat('worker-1',metric,t)
        self.assertIn(a['chain'],{x['chain'] for x in self.store.heartbeat('primary',{},1148)})

    def test_status_is_read_only(self):
        before = self.path.read_bytes()
        LeaseStore.read_status(self.path)
        self.assertEqual(before,self.path.read_bytes())

    def test_mismatched_log_index_requires_matching_receipt(self):
        txid = '0x' + 'b'*64
        block = {'hash':HASH, 'transactions':[{'hash':HASH},{'hash':txid}]}
        log = {'blockHash':HASH,'blockNumber':'0xa','transactionHash':txid,'transactionIndex':'0x0','logIndex':'0x0',
               'address':'0x123','data':'0x01','topics':['0xabc']}
        canonical = dict(log,logIndex='0x1',transactionIndex='0x1')
        receipt = {'blockHash':HASH,'blockNumber':'0xa','transactionHash':txid,'logs':[canonical]}
        check_evm_logs({10:block}, [log], lambda _:receipt)
        self.assertEqual(log['logIndex'],'0x1')
        receipt['logs'] = []
        log['transactionIndex'] = '0x0'
        with self.assertRaises(ChainMismatch):
            check_evm_logs({10:block}, [log], lambda _:receipt)

    def test_coordinator_restart_does_not_prematurely_take_over(self):
        self.store.heartbeat('worker-1', {}, 1000)
        recovered = LeaseStore(self.path, self.config, now=2000)
        self.assertEqual(len(recovered.heartbeat('primary', {}, 2000)),4)
        self.assertEqual(len(recovered.heartbeat('worker-1', {}, 2002)),3)
        self.assertEqual(len(recovered.heartbeat('primary', {}, 2036)),4)

    def test_new_run_starts_current_without_forced_historical_catchup(self):
        a = self.store.heartbeat('worker-1', {}, 1000)[0]
        self.store.heartbeat('worker-1', self.metric(a,100),1001)
        newer = self.metric(a,10000)
        newer[a['chain']].update(run_id='b'*32,initial_head=9998)
        self.store.heartbeat('worker-1',newer,1002)
        row=next(r for r in self.store.status()['leases'] if r['chain_name']==a['chain'])
        self.assertEqual(row['cursor'],10000)
        self.assertEqual(row['skipped_blocks'],9898)
        with self.assertRaises(ValueError):
            newer[a['chain']]['cursor']=20000
            self.store.heartbeat('worker-1',newer,1003)


if __name__ == '__main__':
    unittest.main()
