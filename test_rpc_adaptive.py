import tempfile
import unittest
from argparse import Namespace
from pathlib import Path
import json
from benchmark import FullChainBenchmark
from monitor.rpc import JsonClient


class AdaptiveRpcTest(unittest.TestCase):
    def test_embedded_batch_limit_cools_endpoint_and_retries_another(self):
        import time
        from types import SimpleNamespace
        from unittest.mock import Mock
        client=JsonClient(['https://limited.invalid','https://good.invalid'])
        class Response:
            headers={}
            def __init__(self,body):
                self.body=json.dumps(body).encode()
                self.fp=SimpleNamespace(raw=SimpleNamespace(_sock=Mock()))
            def __enter__(self):return self
            def __exit__(self,*a):pass
            def isclosed(self):return not self.body
            def read1(self,*a):
                value,self.body=self.body,b''
                return value
        good=[{'id':1,'result':'0x1'},{'id':2,'result':'0x2'}]
        limited=[good[0],{'id':2,'error':{'code':-32007,'message':'20/second request limit reached'}}]
        client._opener=Mock()
        client._opener.open.side_effect=[Response(limited),Response(good)]
        payload=[dict(jsonrpc='2.0',id=i,method='eth_call',params=[]) for i in (1,2)]
        self.assertEqual(client.post(payload),good)
        self.assertGreater(client.endpoint_retry_at[client.urls[0]],time.monotonic())
        self.assertEqual(client._opener.open.call_count,2)

    def test_slow_candidate_leaves_time_for_only_other_rpc(self):
        from unittest.mock import patch
        clock = [1000.0]
        c = JsonClient(['https://slow.invalid','https://good.invalid'])
        calls = []
        def post(url,payload,path=''):
            calls.append(url)
            if url == c.urls[0]:
                clock[0] += 16
                c._remaining()
            return {'id':payload['id'],'result':'0x10'}
        c._post_url = post
        with patch('monitor.rpc.time.monotonic',side_effect=lambda:clock[0]):
            self.assertEqual(c.rpc('eth_blockNumber'),'0x10')
        self.assertEqual(calls,c.urls)
        self.assertFalse(c.endpoint_health[c.urls[0]])
        self.assertTrue(c.endpoint_health[c.urls[1]])

    def test_batch_deadline_propagates_into_threads_without_leaking(self):
        import time
        from concurrent.futures import ThreadPoolExecutor
        from monitor.rpc import RpcRequestError
        c = JsonClient('https://one.invalid')
        view = c.for_endpoint(0).with_deadline(time.monotonic()+.03)
        def delayed():
            time.sleep(.04)
            return view._remaining()
        with ThreadPoolExecutor(max_workers=1) as pool:
            with self.assertRaises(RpcRequestError):pool.submit(delayed).result()
        self.assertGreater(c._remaining(),1)
        self.assertIsNone(getattr(c._budget,'deadline',None))

    def test_generic_batch_bounds_cumulative_work_and_switches_once(self):
        from types import SimpleNamespace
        from unittest.mock import patch
        clock = [1000.0]
        c = JsonClient(['https://slow.invalid','https://good.invalid'])
        b = FullChainBenchmark.__new__(FullChainBenchmark)
        calls = []
        def download(adapter,start,end):
            index=adapter.rpc._candidate_indices()[0];calls.append(index)
            if index == 0:
                for _ in range(3):
                    clock[0] += 10
                    adapter.rpc._remaining()
            return 2,3
        with patch('monitor.rpc.time.monotonic',side_effect=lambda:clock[0]):
            self.assertEqual(b._rpc_batch(SimpleNamespace(rpc=c),1,2,download),(2,3))
        self.assertEqual(calls,[0,1])
        self.assertFalse(c.endpoint_health[c.urls[0]])

    def test_repeated_limits_escalate_despite_success_and_recover_slowly(self):
        from unittest.mock import patch
        from monitor.rpc import RpcRequestError
        clock=[1000.0]
        c=JsonClient(['https://limited.invalid','https://reserve.invalid'])
        u=c.urls[0]
        with patch('monitor.rpc.time.monotonic',side_effect=lambda:clock[0]):
            for level,delay in enumerate([60,120,300,600,900],1):
                c._mark_failure(u,RpcRequestError('HTTP 429',60))
                self.assertEqual(c.endpoint_limits[u].level,level)
                self.assertEqual(c.endpoint_retry_at[u]-clock[0],delay)
                # Other in-flight replies from this burst must not jump levels.
                c._mark_failure(u,RpcRequestError('HTTP 429',60))
                self.assertEqual(c.endpoint_limits[u].level,level)
                self.assertEqual(c._candidate_indices(),[1])
                clock[0]+=delay+1
                for _ in range(20):c.mark_health(u,True,10)
                self.assertEqual(c.endpoint_limits[u].level,level)
            clock[0]+=601
            for _ in range(20):c.mark_health(u,True,10)
            self.assertEqual(c.endpoint_limits[u].level,4)
            clock[0]+=120;c.mark_health(u,True,10)
            self.assertEqual(c.endpoint_limits[u].level,3)

    def test_batch_views_share_pacing_and_recheck_concurrent_cooling(self):
        from unittest.mock import patch
        from monitor.rpc import RpcRequestError
        clock=[1000.0];c=JsonClient('https://limited.invalid');u=c.url
        view=c.for_endpoint(0)
        with patch('monitor.rpc.time.monotonic',side_effect=lambda:clock[0]), patch('monitor.rpc.time.sleep',side_effect=lambda s:clock.__setitem__(0,clock[0]+s)):
            c._mark_failure(u,RpcRequestError('HTTP 429',60));clock[0]+=61
            c._wait_endpoint(u)
            view._wait_endpoint(u)
            self.assertEqual(clock[0],1061.25)
            c._mark_failure(u,RpcRequestError('HTTP 429',60))
            failures=c.endpoint_failures[u]
            with self.assertRaises(RpcRequestError) as caught:view._wait_endpoint(u)
            c._mark_failure(u,caught.exception)
            self.assertEqual(c.endpoint_failures[u],failures)
            self.assertEqual(c.endpoint_limits[u].level,2)

    def test_limit_state_restores_without_losing_remaining_provider_deadline(self):
        import time
        from monitor.rpc import RpcRequestError
        from monitor.rpc_policy import retry_seconds
        from email.utils import formatdate
        self.assertEqual(retry_seconds(formatdate(2000,usegmt=True),1000),1000)
        for value in ['bad','nan','-3',None]:self.assertEqual(retry_seconds(value,1000),0)
        with tempfile.TemporaryDirectory() as d:
            c=JsonClient('https://limited.invalid');c._health_path=Path(d)/'health.json'
            c._mark_failure(c.url,RpcRequestError('HTTP 429',3600));c.save_health()
            restored=JsonClient(c.url);restored._health_path=c._health_path;restored.restore_health()
            self.assertEqual(restored.endpoint_limits[c.url].level,1)
            self.assertGreater(restored.endpoint_retry_at[c.url]-time.monotonic(),3595)
            self.assertEqual(restored._candidate_indices(),[])
            # Older cache files remain readable.
            data=json.loads(c._health_path.read_text());data['endpoints'][c.url].pop('limit')
            c._health_path.write_text(json.dumps(data));restored.restore_health()
            self.assertEqual(restored.endpoint_limits[c.url].level,0)

    def test_fast_download_promotes_after_one_verified_improvement(self):
        import time
        c=JsonClient(['https://old.invalid','https://improved.invalid'])
        for u in c.urls:c.mark_health(u,True,100);c._validated_endpoints[u]=time.monotonic()
        c._batch_trial_at=time.monotonic()
        c.record_batch(0,10,10);c.record_batch(1,10,20)
        self.assertEqual(c.batch_candidates()[0],0)
        c.record_batch(1,10,1)
        self.assertEqual(c.batch_candidates()[0],1)
        c.mark_health(c.urls[1],False,retry_after=60)
        self.assertEqual(c.batch_candidates(),[0])

    def test_periodic_trial_does_not_promote_more_limited_reserve(self):
        import time
        c=JsonClient(['https://healthy.invalid','https://recovering.invalid'])
        for u in c.urls:c.mark_health(u,True,100);c._validated_endpoints[u]=time.monotonic()
        c.record_batch(0,10,1);c.record_batch(1,10,10)
        c.endpoint_limits[c.urls[1]].level=3
        self.assertEqual(c.batch_candidates(),[0,1])
        c.endpoint_limits[c.urls[1]].level=0
        self.assertEqual(c.batch_candidates()[0],1)

    def test_http_retry_date_and_tron_json_limit_use_same_cooldown(self):
        import time,io,urllib.error
        from email.utils import formatdate
        from unittest.mock import Mock
        c=JsonClient('https://limited.invalid')
        c._opener.open=Mock(side_effect=urllib.error.HTTPError(c.url,429,'limited',
            {'Retry-After':formatdate(time.time()+3600,usegmt=True)},io.BytesIO()))
        with self.assertRaises(RuntimeError):c.post({})
        self.assertGreater(c.endpoint_retry_at[c.url]-time.monotonic(),3595)
        c=JsonClient('https://tron.invalid')
        c._post_url=lambda *a,**k:{'Error':'too many requests secret-value'}
        with self.assertRaises(RuntimeError) as caught:c.post({})
        self.assertNotIn('secret-value',str(caught.exception))
        self.assertEqual(c.endpoint_limits[c.url].level,1)

    def test_wire_429_falls_back_without_rehitting_cooling_endpoint(self):
        import threading
        from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
        seen=[]
        class Handler(BaseHTTPRequestHandler):
            def log_message(self,*args):pass
            def do_POST(self):
                payload=json.loads(self.rfile.read(int(self.headers['Content-Length'])))
                seen.append(self.path)
                limited=self.path=='/limited'
                body=json.dumps({'id':payload['id'],'result':'0x1'}).encode()
                self.send_response(429 if limited else 200)
                self.send_header('Retry-After','120')
                self.send_header('Content-Length',str(len(body)))
                self.end_headers();self.wfile.write(body)
        server=ThreadingHTTPServer(('127.0.0.1',0),Handler)
        thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
        try:
            base=f'http://127.0.0.1:{server.server_port}'
            c=JsonClient([base+'/limited',base+'/good'])
            self.assertEqual(c.rpc('eth_blockNumber'),'0x1')
            self.assertEqual(c.rpc('eth_blockNumber'),'0x1')
            self.assertEqual(seen,['/limited','/good','/good'])
            self.assertEqual(c.endpoint_limits[c.urls[0]].level,1)
            self.assertEqual(c.endpoint_limits[c.urls[1]].level,0)
        finally:server.shutdown();server.server_close();thread.join()

    def test_redundant_response_id_and_json_rate_limit(self):
        import time
        from monitor.rpc import RpcRequestError
        c=JsonClient(['https://wrong.invalid','https://good.invalid'])
        c._post_url=lambda url,payload,path='': {'id':True if 'wrong' in url else payload['id'],'result':'0x1'}
        self.assertEqual(c.rpc_redundant('eth_blockNumber'),['0x1'])
        self.assertFalse(c.endpoint_health[c.urls[0]])
        c=JsonClient('https://limited.invalid')
        c._post_url=lambda url,payload,path='': {'id':payload['id'],'error':{'code':-32005,'message':'Rate limit for secret-key'}}
        with self.assertRaises(RpcRequestError) as caught:c.rpc('eth_blockNumber')
        self.assertNotIn('secret-key',str(caught.exception))
        self.assertEqual(caught.exception.retry_after,60)
        self.assertEqual(c.endpoint_failures[c.url],1)
        self.assertGreater(c.endpoint_retry_at[c.url]-time.monotonic(),55)

    def test_wire_validation_and_redirect_do_not_leak_credentials(self):
        import threading
        from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
        from monitor.rpc import RpcRequestError
        response={};sink=[]
        class Handler(BaseHTTPRequestHandler):
            def log_message(self,*args):pass
            def do_POST(self):
                self.rfile.read(int(self.headers.get('Content-Length','0')))
                if self.path=='/redirect':
                    self.send_response(302)
                    self.send_header('Location',f'http://localhost:{self.server.server_port}/sink')
                    self.end_headers();return
                self.do_GET()
            def do_GET(self):
                if self.path=='/sink':sink.append(self.headers.get('X-Api-Key'))
                body=json.dumps(response).encode()
                self.send_response(200);self.send_header('Content-Length',str(len(body)));self.end_headers();self.wfile.write(body)
        server=ThreadingHTTPServer(('127.0.0.1',0),Handler)
        thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
        try:
            client=JsonClient(f'http://127.0.0.1:{server.server_port}',headers={'X-Api-Key':'private-fixture'})
            payload={'jsonrpc':'2.0','id':'identity','method':'eth_chainId','params':[]}
            for body in [{'id':'other','result':'0x1'}, {'id':'identity'},
                         {'id':'identity','error':{'code':429,'message':'quota exceeded private-fixture'}}]:
                response.clear();response.update(body)
                with self.assertRaises(RpcRequestError) as caught:client._post_url(client.url,payload)
                self.assertNotIn('private-fixture',str(caught.exception))
                if body.get('error'):self.assertEqual(caught.exception.retry_after,60)
            response.clear();response.update({'id':'identity','result':'0x1'})
            self.assertEqual(client._post_url(client.url,payload)['result'],'0x1')
            with self.assertRaises(RpcRequestError):client._post_url(client.url+'/redirect',payload)
            self.assertEqual(sink,[])
        finally:server.shutdown();server.server_close();thread.join()

    def test_rpc_https_keeps_certificate_and_hostname_verification(self):
        import ssl
        from monitor.rpc import RpcHTTPSConnection, rpc_connection
        connection=RpcHTTPSConnection('rpc.invalid',timeout=12)
        self.assertTrue(connection._context.check_hostname)
        self.assertEqual(connection._context.verify_mode,ssl.CERT_REQUIRED)
        self.assertIs(connection._create_connection,rpc_connection)

    def test_connection_prefers_ipv4_without_changing_global_resolution(self):
        import socket
        from unittest.mock import Mock, patch
        from monitor.rpc import rpc_connection
        addresses=[(socket.AF_INET6,socket.SOCK_STREAM,6,'',('::1',443,0,0)),
                   (socket.AF_INET,socket.SOCK_STREAM,6,'',('127.0.0.1',443))]
        sock=Mock()
        with patch('monitor.rpc.socket.getaddrinfo',return_value=addresses), patch('monitor.rpc.socket.socket',return_value=sock) as factory:
            self.assertIs(rpc_connection(('rpc.invalid',443),12),sock)
            factory.assert_called_once_with(socket.AF_INET,socket.SOCK_STREAM,6)
            sock.connect.assert_called_once_with(('127.0.0.1',443))

    def test_connection_retains_ipv6_fallback_and_closes_failed_socket(self):
        import socket
        from unittest.mock import Mock, patch
        from monitor.rpc import rpc_connection
        bad,good=Mock(),Mock()
        bad.connect.side_effect=OSError('unreachable')
        addresses=[(socket.AF_INET,socket.SOCK_STREAM,6,'',('127.0.0.1',443)),
                   (socket.AF_INET6,socket.SOCK_STREAM,6,'',('::1',443,0,0))]
        with patch('monitor.rpc.socket.getaddrinfo',return_value=addresses), patch('monitor.rpc.socket.socket',side_effect=[bad,good]):
            self.assertIs(rpc_connection(('rpc.invalid',443),12),good)
            bad.close.assert_called_once()
            good.connect.assert_called_once_with(('::1',443,0,0))

    def test_connection_addresses_share_deadline(self):
        import socket
        from unittest.mock import Mock, patch
        from monitor.rpc import rpc_connection
        sock=Mock();sock.connect.side_effect=TimeoutError()
        addresses=[(socket.AF_INET,socket.SOCK_STREAM,6,'',('127.0.0.1',443))]*2
        with patch('monitor.rpc.socket.getaddrinfo',return_value=addresses), patch('monitor.rpc.socket.socket',return_value=sock) as factory, patch('monitor.rpc.time.monotonic',side_effect=[0,0,13]):
            with self.assertRaises(TimeoutError):rpc_connection(('rpc.invalid',443),12)
            self.assertEqual(factory.call_count,1)
            sock.close.assert_called_once()

    def test_concurrent_success_does_not_cancel_rate_limit(self):
        import time, threading
        from concurrent.futures import ThreadPoolExecutor
        from monitor.rpc import RpcRequestError
        c=JsonClient('https://one.invalid')
        started,release=threading.Event(),threading.Event()
        def post(url,payload,path=''):
            if payload['method']=='eth_blockNumber':
                started.set()
                self.assertTrue(release.wait(3))
                return {'id':payload['id'],'result':'0x1'}
            raise RpcRequestError('HTTP 429',120)
        c._post_url=post
        with ThreadPoolExecutor(max_workers=1) as pool:
            pending=pool.submit(c.rpc,'eth_blockNumber')
            try:
                self.assertTrue(started.wait(3))
                with self.assertRaises(RpcRequestError):c.rpc('eth_getTransactionReceipt',['0x1'])
            finally:release.set()
            self.assertEqual(pending.result(),'0x1')
        self.assertFalse(c.endpoint_health[c.url])
        self.assertGreater(c.endpoint_retry_at[c.url]-time.monotonic(),115)
        self.assertEqual(c._candidate_indices(),[])
        c.mark_health(c.url,False)
        self.assertGreater(c.endpoint_retry_at[c.url]-time.monotonic(),115)

    def test_nested_receipt_rate_limit_preserves_type_delay_and_single_penalty(self):
        import time
        from monitor.rpc import RpcRequestError
        c=JsonClient('https://one.invalid')
        def post(url,payload,path=''):
            if payload['method']=='eth_getTransactionReceipt':raise RpcRequestError('HTTP 429',120)
            return {'id':payload['id'],'result':[]}
        c._post_url=post
        with self.assertRaises(RpcRequestError) as caught:
            c.rpc('eth_getLogs',[],result_validator=lambda logs:c.rpc('eth_getTransactionReceipt',['0x1']))
        self.assertEqual(caught.exception.retry_after,120)
        self.assertEqual(c.endpoint_failures[c.url],1)
        self.assertGreater(c.endpoint_retry_at[c.url]-time.monotonic(),115)

    def test_tron_invalid_height_cools_source_and_uses_valid_alternative(self):
        from types import SimpleNamespace
        c=JsonClient(['https://bad.invalid','https://good.invalid'])
        block={'blockID':format(1,'016x')+'a'*48,'block_header':{'raw_data':{'number':1,'parentHash':'0'*64}},'transactions':[]}
        def post(url,payload,path=''):
            if url==c.urls[0]:return {'block_header':{'raw_data':{'number':999}}}
            return [] if 'gettransactioninfobyblocknum' in path else block
        c._post_url=post
        b=FullChainBenchmark.__new__(FullChainBenchmark)
        b.checkpoints={}
        a=SimpleNamespace(rpc=c,block_prefix='/wallet',config={'type':'tron'})
        self.assertEqual(b._tron_batch(a,1,1),(0,0))
        self.assertFalse(c.endpoint_health[c.urls[0]])
        self.assertEqual(c._candidate_indices(),[1])

    def test_batch_speed_selection_cooling_trial_and_persistence(self):
        import time
        from unittest.mock import Mock
        c=JsonClient(['https://fast-ping.invalid','https://fast-download.invalid','https://reserve.invalid'])
        c.set_endpoint_validator(Mock(),'evm:1')
        for i,u in enumerate(c.urls):
            c.mark_health(u,True,10+i*100)
            c._validated_endpoints[u]=time.monotonic()
        c.record_batch(0,32,32)
        c.record_batch(1,32,4)
        c._batch_trial_at=time.monotonic()
        self.assertEqual(c.batch_candidates()[0],1)
        c._batch_trial_at=0
        self.assertEqual(c.batch_candidates(),[2,1])
        self.assertEqual(c.batch_candidates()[0],1)
        c.mark_health(c.urls[1],False,retry_after=120)
        self.assertNotIn(1,c.batch_candidates())
        with tempfile.TemporaryDirectory() as d:
            c._health_path=Path(d)/'health.json'
            c.save_health()
            restored=JsonClient(c.urls)
            restored.set_endpoint_validator(Mock(),'evm:1')
            restored._health_path=c._health_path
            restored.restore_health()
            self.assertEqual(restored.batch_performance,c.batch_performance)
            self.assertNotIn(1,restored.batch_candidates())
            restored.set_endpoint_validator(Mock(),'evm:2')
            restored.batch_performance.clear()
            restored.restore_health()
            self.assertEqual(restored.batch_performance,{})

    def test_validation_cache_keeps_original_expiry_and_binds_chain(self):
        import time
        from unittest.mock import Mock
        with tempfile.TemporaryDirectory() as d:
            c=JsonClient(['https://one.invalid'])
            c.set_endpoint_validator(Mock(), 'evm:1')
            c._health_path=Path(d)/'health.json'
            c._validated_endpoints[c.urls[0]]=time.monotonic()-3600
            c.mark_health(c.urls[0],True,200)
            c.save_health()
            for identity, expected in [('evm:1',1),('evm:2',0),(None,0)]:
                restored=JsonClient(c.urls)
                validator=Mock()
                restored.set_endpoint_validator(validator,identity)
                restored._health_path=c._health_path
                restored.restore_health()
                self.assertEqual(len(restored.qualified_urls()),expected)
                if expected:
                    self.assertAlmostEqual(time.monotonic()-restored._validated_endpoints[c.urls[0]],3600,delta=2)
                    restored._ensure_endpoint_validated(c.urls[0])
                    validator.assert_not_called()
                    restored._validated_endpoints[c.urls[0]]-=86400
                    self.assertEqual(restored.qualified_urls(),[])
                    restored._ensure_endpoint_validated(c.urls[0])
                    validator.assert_called_once()
            data=json.loads(c._health_path.read_text())
            for timestamp in [time.time()-86401,time.time()+3600,None]:
                data['endpoints'][c.urls[0]]['validated_at']=timestamp
                c._health_path.write_text(json.dumps(data))
                restored=JsonClient(c.urls)
                restored.set_endpoint_validator(Mock(),'evm:1')
                restored._health_path=c._health_path
                restored.restore_health()
                self.assertEqual(restored.qualified_urls(),[])

    def test_expired_failure_can_recover_ahead_of_extremely_slow_success(self):
        import time
        c=JsonClient(['https://fast.invalid','https://slow.invalid'],timeout=12)
        fast,slow=c.urls
        c.mark_health(fast,True,200)
        c.mark_health(fast,False)
        c.mark_health(slow,True,96000)
        self.assertEqual(c._candidate_indices(),[1])
        c.endpoint_retry_at[fast]=time.monotonic()-1
        self.assertEqual(c._candidate_indices()[0],0)
        self.assertNotIn(fast,c._validated_endpoints)

    def test_many_candidates_do_not_force_a_startup_network_sweep(self):
        with tempfile.TemporaryDirectory() as d:
            c={'rpc_urls':[f'https://node{i}.invalid' for i in range(300)]}
            p=Path(d)/'config.json'
            p.write_text(json.dumps({'chains':{'x':c}}))
            b=FullChainBenchmark(Namespace(output=d,config=str(p),chains='x',seconds=1))
            try:
                selected,results=b._qualify_rpcs('x',c)
                self.assertEqual(selected['rpc_urls'],c['rpc_urls'])
                self.assertEqual(results,[])
            finally:
                b.block_pool.shutdown()

    def test_inconsistent_result_cools_endpoint_and_tries_another(self):
        c=JsonClient(['https://one.invalid','https://two.invalid'])
        c._post_url=lambda url,payload,path='': {'id':payload['id'],'result':1 if 'one' in url else 2}
        self.assertEqual(c.rpc('test',result_validator=lambda x:x==2),2)
        self.assertEqual(c.endpoint_health[c.urls[0]],False)
        self.assertEqual(c.endpoint_health[c.urls[1]],True)
        self.assertGreater(c.endpoint_retry_at[c.urls[0]],0)

    def test_cooling_survives_restart_without_trusting_cached_identity(self):
        with tempfile.TemporaryDirectory() as d:
            c=JsonClient(['https://one.invalid'])
            c._health_path=Path(d)/'health.json'
            c.mark_health(c.urls[0],False)
            c.save_health()
            restored=JsonClient(c.urls)
            restored._health_path=c._health_path
            restored.restore_health()
            self.assertEqual(restored._candidate_indices(),[])
            self.assertEqual(restored._validated_endpoints,{})


if __name__=='__main__':
    unittest.main()
