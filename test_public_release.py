import json
import os
from pathlib import Path
import tarfile
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from build_release import build, source_files
from cluster import parse_args, run_coordinator, coordinator_configuration, node_addresses
from lease_store import LeaseStore


class PublicReleaseTest(unittest.TestCase):
    def test_single_node_selected_chain_reaches_bot_and_verifier(self):
        root = Path(__file__).parent
        config = json.loads((root/'examples/cluster.single.example.json').read_text())
        catalog = json.loads((root/'examples/chains.example.json').read_text())
        with tempfile.TemporaryDirectory() as directory:
            private = Path(directory)
            (private/'cluster.json').write_text(json.dumps(config))
            (private/'chains.json').write_text(json.dumps(catalog))
            args = SimpleNamespace(cluster_config=private/'cluster.json', chains_config=private/'chains.json',
                                   db=private/'cluster.db', bind='127.0.0.1', port=0)
            with patch.dict(os.environ, {'CLUSTER_TLS_CA':'test', 'BOT_TOKEN':'test', 'OWNER_USER_ID':'123'}, clear=True), \
                 patch('cluster.configured_tls'), patch('cluster.BoundedServer') as server, \
                 patch('cluster.build_adapter') as adapter, patch('cluster.threading.Thread') as thread, \
                 patch('bot_runtime.app.App') as app:
                run_coordinator(args)
                thread.call_args.kwargs['target']()
                self.assertEqual(set(app.call_args.args[2]['chains']), {'ethereum'})
                self.assertEqual(set(server.return_value.chain_config), {'ethereum'})
                self.assertEqual(adapter.call_count, 1)
                leases = server.return_value.store.heartbeat('primary', {})
                self.assertEqual([item['chain'] for item in leases], ['ethereum'])
                self.assertEqual(server.return_value.node_ips, {'primary':None})
        config['chains']['missing'] = {'preferred_node':'primary'}
        config['nodes']['primary']['max_chains'] = 2
        with self.assertRaisesRegex(ValueError, 'enabled chain missing'):
            coordinator_configuration(config, catalog['chains'])

    def test_tls_allows_shared_or_absent_ips_but_plaintext_does_not(self):
        config = json.loads((Path(__file__).parent/'examples/cluster.example.json').read_text())
        for node in config['nodes'].values():
            node['ip_address'] = '127.0.0.1'
        with tempfile.TemporaryDirectory() as directory:
            store = LeaseStore(Path(directory)/'cluster.db', config)
            self.assertEqual(len(store.status()['leases']), 9)
        self.assertEqual(set(node_addresses(config, True).values()), {'127.0.0.1'})
        with self.assertRaises(ValueError): node_addresses(config, False)
        for node in config['nodes'].values():
            node.pop('ip_address')
        self.assertEqual(set(node_addresses(config, True).values()), {None})
        with self.assertRaises(ValueError): node_addresses(config, False)

    def test_manifest_rejects_real_configs_and_secret_filename_variants(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            for name in ['cluster.json','chains.json','BOT.ENV','.env.local','bot.env.production',
                         'client.KEY','client.p12','bot.sqlite3-wal','bot.db-shm','Personal/note.txt']:
                with self.subTest(name=name):
                    path=root/name;path.parent.mkdir(exist_ok=True)
                    path.write_text('{}')
                    (root/'release-manifest.json').write_text(json.dumps([name]))
                    with self.assertRaises(ValueError):source_files(root)
            path=root/'examples/bot.env.example';path.parent.mkdir(exist_ok=True);path.write_text('BOT_TOKEN=')
            (root/'release-manifest.json').write_text(json.dumps(['examples/bot.env.example']))
            self.assertEqual(source_files(root),[path])

    def test_manifest_excludes_unlisted_personal_files_and_rejects_secrets(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            (root/'release-manifest.json').write_text('["app.py"]')
            (root/'app.py').write_text('print("public")')
            (root/'bot.env').write_text('BOT_TOKEN=private')
            (root/'cluster.json').write_text('{"nodes":"private"}')
            (root/'old-history.txt').write_text('private')
            archive=root/'out.tar.gz'
            build(root,archive)
            with tarfile.open(archive) as tar:
                self.assertEqual(set(tar.getnames()),{'app.py','RELEASE.txt'})
            (root/'app.py').write_text('123456789:'+('a'*35))
            with self.assertRaises(ValueError):source_files(root)
            (root/'release-manifest.json').write_text('["../private.txt"]')
            with self.assertRaises(ValueError):source_files(root)
            (root/'release-manifest.json').write_text('["bot.env"]')
            with self.assertRaises(ValueError):source_files(root)

    def test_coordinator_and_worker_accept_external_config(self):
        with patch.dict(os.environ,{'CLUSTER_CONFIG':'/private/cluster.json','CHAINS_CONFIG':'/private/chains.json'}):
            for role in ('coordinator','worker'):
                with patch('sys.argv',['cluster.py',role]):
                    args=parse_args()
                    self.assertEqual(args.cluster_config,'/private/cluster.json')
                    self.assertEqual(args.chains_config,'/private/chains.json')

    def test_public_manifest_covers_source_and_tests(self):
        root=Path(__file__).parent
        public=set(source_files(root))
        self.assertNotIn(root/"examples/backup.env.example",public)
        self.assertFalse(any(path.parent==root and "backup" in path.name for path in public))
        for path in list(root.glob('*.py'))+list((root/'bot_runtime').glob('*.py'))+list((root/'monitor').glob('*.py')):
            self.assertIn(path.resolve(),public)
