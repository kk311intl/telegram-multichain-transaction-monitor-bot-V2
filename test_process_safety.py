import json
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.request
from pathlib import Path
from cluster import BoundedServer, CoordinatorHandler
from lease_store import LeaseStore


class ProcessSafetyTest(unittest.TestCase):
    def test_scanner_exits_when_parent_no_longer_renews(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'lease.json').write_text(json.dumps({'deadline':time.monotonic()+2, 'id':'test'}))
            (root / 'chains.json').write_text('{"chains":{"x":{}}}')
            script = '''import time
from argparse import Namespace
from benchmark import FullChainBenchmark
b=FullChainBenchmark(Namespace(output=ROOT,config=ROOT+'/chains.json',chains='x',seconds=300,pwsh='',headless=True,lease_file=ROOT+'/lease.json',lease_id='test'))
b._run_chain=lambda name: time.sleep(300)
b.run()
'''.replace('ROOT', repr(str(root)))
            process = subprocess.Popen([sys.executable, '-c', script], cwd=Path(__file__).parent,
                                       stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
            try:
                self.assertEqual(process.wait(timeout=8),75,process.stderr.read().decode())
            finally:
                if process.poll() is None:
                    process.kill()
                process.wait()
                process.stderr.close()

    def test_watchdog_persists_diagnostics_before_stalled_exit(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            (root/'chains.json').write_text('{"chains":{"x":{}}}')
            script="""import time
from argparse import Namespace
from benchmark import FullChainBenchmark
b=FullChainBenchmark(Namespace(output=ROOT,config=ROOT+'/chains.json',chains='x',seconds=300,pwsh='',headless=True))
b.last_cycle_monotonic=time.monotonic()-121
b._run_chain=lambda name:time.sleep(300)
b.run()
""".replace('ROOT',repr(str(root)))
            result=subprocess.run([sys.executable,'-c',script],cwd=Path(__file__).parent,
                                  capture_output=True,timeout=8)
            self.assertEqual(result.returncode,76,result.stderr.decode())
            saved=json.loads((root/'watchdog.json').read_text())
            self.assertEqual(saved['reason'],'cycle_stalled')
            self.assertIn('x',saved['states'])

    def test_malformed_http_body_is_400_and_no_mutation(self):
        with tempfile.TemporaryDirectory() as directory:
            config=json.loads((Path(__file__).parent / "examples/cluster.example.json").read_text())
            store=LeaseStore(Path(directory)/'db',config)
            server=BoundedServer(('127.0.0.1',0),CoordinatorHandler)
            server.node_ips={'primary':'127.0.0.1'}
            server.store=store
            worker=threading.Thread(target=server.serve_forever,daemon=True)
            worker.start()
            try:
                before=store.status()
                request=urllib.request.Request(f'http://127.0.0.1:{server.server_port}/heartbeat',data=b'{"node":"primary","metrics":[]}')
                with self.assertRaises(urllib.error.HTTPError) as caught:
                    urllib.request.urlopen(request,timeout=3)
                self.assertEqual(caught.exception.code,400)
                self.assertEqual(before,store.status())
            finally:
                server.shutdown()
                server.server_close()
                worker.join()


if __name__=='__main__':
    unittest.main()
