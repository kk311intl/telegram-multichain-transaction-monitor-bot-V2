import tempfile
import unittest
from pathlib import Path

from cluster import LeaseStore, load_json


class LeaseStoreTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.config = load_json((Path(__file__).parent / "examples/cluster.example.json"))
        self.store = LeaseStore(Path(self.temporary.name) / "cluster.db", self.config, verifier=lambda *a: True, now=0)

    def tearDown(self):
        self.temporary.cleanup()

    def test_worker_failure_moves_only_to_primary_and_does_not_fail_back(self):
        now = 0
        self.store.heartbeat("primary", {}, now)
        jp = self.store.heartbeat("worker-1", {}, now)
        self.store.heartbeat("worker-2", {}, now)
        self.assertEqual({x["chain"] for x in jp}, {"ethereum", "arbitrum", "optimism"})

        primary = self.store.heartbeat("primary", {}, now + 51)
        names = {x["chain"] for x in primary}
        self.assertTrue({"ethereum", "arbitrum", "optimism"}.issubset(names))

        jp = self.store.heartbeat("worker-1", {}, now + 52)
        self.assertEqual(jp, [])
        self.assertEqual(self.store.rebalance("worker-1", now + 53), 3)
        jp = self.store.heartbeat("worker-1", {}, now + 89)
        self.assertEqual({x["chain"] for x in jp}, {"ethereum", "arbitrum", "optimism"})

    def test_primary_failure_never_moves_heavy_chains_to_workers(self):
        now = 0
        self.store.heartbeat("primary", {}, now)
        self.store.heartbeat("worker-1", {}, now)
        self.store.heartbeat("worker-2", {}, now)
        jp = self.store.heartbeat("worker-1", {}, now + 51)
        kr = self.store.heartbeat("worker-2", {}, now + 51)
        self.assertNotIn("tron", {x["chain"] for x in jp})
        self.assertNotIn("bnb", {x["chain"] for x in kr})

    def test_cursor_never_moves_backwards(self):
        now = 0
        assignment = self.store.heartbeat("worker-1", {}, now)[0]
        chain = assignment["chain"]
        epoch = assignment["epoch"]
        self.store.heartbeat("worker-1", {chain: {"epoch": epoch, "cursor": 100, "block_hash": "0x" + "a"*64, "run_id": "a"*32}}, now + 1)
        with self.assertRaises(ValueError):
            self.store.heartbeat("worker-1", {chain: {"epoch": epoch, "cursor": 90, "block_hash": "0x" + "a"*64, "run_id": "a"*32}}, now + 2)
        lease = next(item for item in self.store.status()["leases"] if item["chain_name"] == chain)
        self.assertEqual(lease["cursor"], 100)


if __name__ == "__main__":
    unittest.main()
