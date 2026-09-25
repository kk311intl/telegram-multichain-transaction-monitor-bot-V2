import json
import os
from pathlib import Path
import shutil
import ssl
import subprocess
import tempfile
import threading
import unittest
import urllib.error
import urllib.request
from unittest.mock import patch

from cluster import BoundedServer, CoordinatorHandler, request_assignments, node_addresses
from cluster_transport import open_cluster, tls_context
from lease_store import LeaseStore
from match_feed import MatchFeed
from types import SimpleNamespace


class ClusterTransportTest(unittest.TestCase):
    def test_public_plaintext_and_tls_downgrade_are_rejected(self):
        with patch.dict(os.environ,{},clear=True):
            for url in ('http://127.0.0.1:18765','https://localhost:18765'):
                with self.assertRaises(ValueError):open_cluster(urllib.request.Request(url))
            os.environ['CLUSTER_ALLOW_PLAINTEXT']='1'
            with self.assertRaises(ValueError):open_cluster(urllib.request.Request('http://8.8.8.8:18765'))
            os.environ['CLUSTER_TLS_CA']='configured'
            with self.assertRaises(ValueError):open_cluster(urllib.request.Request('http://127.0.0.1:18765'))

    def test_real_mutual_tls_node_identity_and_no_redirect(self):
        openssl=shutil.which('openssl')
        if not openssl:
            candidate=Path(os.environ.get('ProgramFiles',''))/'Git/usr/bin/openssl.exe'
            openssl=str(candidate) if candidate.is_file() else None
        if not openssl:
            self.skipTest('OpenSSL is required for the TLS integration test')
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            def run(*args):
                subprocess.run([openssl,*args],cwd=root,check=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
            run('req','-x509','-newkey','rsa:2048','-nodes','-keyout','ca.key','-out','ca.crt','-days','1','-subj','/CN=TestCA')
            for name,usage in [('server','serverAuth'),('worker-1','clientAuth'),('stranger','clientAuth')]:
                run('req','-newkey','rsa:2048','-nodes','-keyout',name+'.key','-out',name+'.csr','-subj','/CN='+name)
                extension='extendedKeyUsage='+usage+'\n'
                if name=='server':extension+='subjectAltName=DNS:localhost\n'
                (root/(name+'.ext')).write_text(extension)
                run('x509','-req','-in',name+'.csr','-CA','ca.crt','-CAkey','ca.key','-CAcreateserial','-out',name+'.crt','-days','1','-extfile',name+'.ext')
            class Handler(CoordinatorHandler):
                def do_GET(self):
                    if self.path=='/redirect':
                        self.send_response(302);self.send_header('Location','http://127.0.0.1/private');self.end_headers()
                    else:super().do_GET()
            server=BoundedServer(('127.0.0.1',0),Handler)
            config=json.loads((Path(__file__).parent/'examples/cluster.example.json').read_text())
            for node in config['nodes'].values():
                node.pop('ip_address')
            server.store=LeaseStore(root/'cluster.db',config)
            server.node_ips=node_addresses(config, True)
            server.chain_config={'ethereum':{'type':'evm'}}
            server.feed=SimpleNamespace(addresses=lambda *args:[])
            server.tls_context=tls_context(str(root/'ca.crt'),str(root/'server.crt'),str(root/'server.key'),True)
            thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
            url=f'https://localhost:{server.server_port}'
            try:
                settings={f'CLUSTER_TLS_{k}':str(root/v) for k,v in [('CA','ca.crt'),('CERT','worker-1.crt'),('KEY','worker-1.key')]}
                with patch.dict(os.environ,settings):
                    self.assertEqual(len(request_assignments(url,'worker-1',{})),3)
                    self.assertEqual(MatchFeed(url,'worker-1','ethereum',1).call('watch'),{'addresses':[]})
                    with self.assertRaises(urllib.error.HTTPError) as caught:request_assignments(url,'primary',{})
                    self.assertEqual(caught.exception.code,403)
                    with self.assertRaises(urllib.error.HTTPError) as caught:open_cluster(urllib.request.Request(url+'/redirect'))
                    self.assertEqual(caught.exception.code,302)
                    with self.assertRaises(urllib.error.URLError):request_assignments(url.replace('localhost','127.0.0.1'),'worker-1',{})
                    os.environ['CLUSTER_TLS_CERT']=str(root/'stranger.crt')
                    os.environ['CLUSTER_TLS_KEY']=str(root/'stranger.key')
                    with self.assertRaises(urllib.error.HTTPError) as caught:request_assignments(url,'worker-1',{})
                    self.assertEqual(caught.exception.code,403)
                context=ssl.create_default_context(cafile=str(root/'ca.crt'))
                with self.assertRaises((urllib.error.URLError,ssl.SSLError,ConnectionError)):
                    urllib.request.urlopen(url+'/health',context=context,timeout=3)
            finally:
                server.shutdown();server.server_close();thread.join()
                tls_context.cache_clear()
