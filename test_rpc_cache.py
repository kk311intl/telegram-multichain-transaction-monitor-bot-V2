import json
import tempfile
import unittest
from pathlib import Path

from benchmark import RPC_CACHE_SECONDS, cached_rpc_urls


class RpcCacheTest(unittest.TestCase):
    def test_cache_accepts_matching_fresh_candidates_only(self):
        candidates = ["https://one.invalid", "https://two.invalid"]
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "cache.json"
            import hashlib
            path.write_text(json.dumps({
                "saved_at": 100,
                "candidate_hash": hashlib.sha256("\n".join(candidates).encode()).hexdigest(),
                "qualified_urls": [candidates[1]],
            }), encoding="utf-8")
            self.assertEqual(cached_rpc_urls(path, candidates, 101), [candidates[1]])
            self.assertEqual(cached_rpc_urls(path, candidates, 100 + RPC_CACHE_SECONDS), [])
            self.assertEqual(cached_rpc_urls(path, list(reversed(candidates)), 101), [])
            self.assertEqual(cached_rpc_urls(path, candidates + ['https://new.invalid'],101,allow_changed=True),[candidates[1]])
            self.assertEqual(cached_rpc_urls(path, ['https://new.invalid'],101,allow_changed=True),[])


if __name__ == "__main__":
    unittest.main()
