from __future__ import annotations

import json
import copy
import http.client
import socket
import threading
import time
import urllib.error
import urllib.request
from functools import wraps
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Callable
from .rpc_policy import RateLimit, retry_seconds


RPC_RETRY_DELAYS = (15, 30, 60, 120, 300)
TRANSPORT_ERRORS = (
    urllib.error.URLError,
    TimeoutError,
    http.client.HTTPException,
    ConnectionError,
    OSError,
    json.JSONDecodeError,
    ValueError,
    RecursionError,
)


def rpc_connection(address, timeout=12, source_address=None):
    """Prefer IPv4; share one connect budget across resolved addresses."""
    deadline = time.monotonic() + timeout
    addresses = socket.getaddrinfo(*address, 0, socket.SOCK_STREAM)
    addresses.sort(key=lambda item: item[0] != socket.AF_INET)
    failure = OSError('no RPC addresses resolved')
    for family, kind, protocol, _, target in addresses:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError('RPC connection deadline')
        sock = socket.socket(family, kind, protocol)
        try:
            sock.settimeout(min(3, remaining))
            if source_address:
                sock.bind(source_address)
            sock.connect(target)
            sock.settimeout(max(0.001, deadline - time.monotonic()))
            return sock
        except OSError as exc:
            sock.close()
            failure = exc
    raise failure


class RpcHTTPSConnection(http.client.HTTPSConnection):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._create_connection = rpc_connection


class RpcHTTPSHandler(urllib.request.HTTPSHandler):
    def https_open(self, request):
        return self.do_open(RpcHTTPSConnection, request, context=self._context)


class RpcNoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, request, fp, code, msg, headers, newurl):
        raise urllib.error.HTTPError(request.full_url, code, 'RPC redirect refused', headers, fp)


def request_budget(method):
    @wraps(method)
    def bounded(self, *args, **kwargs):
        previous = getattr(self._budget, 'deadline', None)
        self._budget.deadline = previous if previous is not None else time.monotonic() + 30
        try:
            self._remaining()
            result = method(self, *args, **kwargs)
            self._remaining()
            return result
        finally:
            self._budget.deadline = previous
    return bounded


def trace_request(method):
    @wraps(method)
    def traced(self, url, payload, path=''):
        key = threading.get_ident()
        with self._health_lock:
            self._active_requests[key] = {
                'endpoint_index': self.urls.index(url) if url in self.urls else -1,
                'method': str(payload.get('method', 'TRON'))[:64] if isinstance(payload, dict) else 'RPC',
                'started': time.monotonic(),
            }
        try:
            return method(self, url, payload, path)
        finally:
            with self._health_lock:
                self._active_requests.pop(key, None)
    return traced


class RpcRequestError(RuntimeError):
    def __init__(self, kind: str, retry_after: int = 0):
        super().__init__(f"RPC request failed: {kind}")
        self.retry_after = max(0, min(86400, int(retry_after)))
        self.reported_urls = set()


def rpc_error(error):
    """Keep provider messages (which can echo credentials) out of errors/logs."""
    code = error.get('code') if isinstance(error, dict) else None
    message = str(error.get('message', '') if isinstance(error, dict) else error).lower()
    limited = code == 429 or any(text in message for text in ('rate limit', 'too many requests', 'quota exceeded'))
    label = str(code) if type(code) is int else 'provider error'
    return RpcRequestError('JSON-RPC ' + label, 60 if limited else 0)

class RequestStats:
    """Thread-safe, content-free network counters for diagnostics."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.requests = 0
        self.failures = 0
        self.response_bytes = 0

    def record(self, success: bool, response_bytes: int = 0) -> None:
        with self._lock:
            self.requests += 1
            self.failures += int(not success)
            self.response_bytes += max(0, int(response_bytes))

    def snapshot(self) -> tuple[int, int, int]:
        with self._lock:
            return self.requests, self.failures, self.response_bytes

    def record_validation_failure(self) -> None:
        """Count a rejected response without counting a second request."""
        with self._lock:
            self.failures += 1


class JsonClient:
    def __init__(
        self, url: str | list[str], timeout: int = 30,
        headers: dict[str, str] | None = None,
        fallback_urls: list[str] | None = None,
    ):
        primary = [url] if isinstance(url, str) else list(dict.fromkeys(url))
        fallback = [item for item in dict.fromkeys(fallback_urls or []) if item not in primary]
        self.fallback_urls = fallback
        self.urls = [*primary, *fallback]
        if not self.urls:
            raise ValueError("at least one RPC URL is required")
        self.url = self.urls[0]
        self._url_index = 0
        self.timeout = timeout
        self.headers = {
            "Content-Type": "application/json",
            "User-Agent": "CryptoMonitorClusterV2/1.0",
            **(headers or {}),
        }
        self.counter = 0
        self.stats = RequestStats()
        self._health_lock = threading.RLock()
        self._request_lock = threading.Lock()
        self._budget = threading.local()
        self._operation_deadline = None
        self._active_requests = {}
        self._probe_stop = threading.Event()
        self._probe_thread = None
        self._health_path = None
        self._probe_cursor = 0
        self._validation_locks = {url: threading.Lock() for url in self.urls}
        self._endpoint_validator: Callable[[str], None] | None = None
        self._validation_identity = None
        self._validated_endpoints: dict[str, float] = {}
        self.endpoint_health: dict[str, bool | None] = {candidate: None for candidate in self.urls}
        self.endpoint_latency_ms: dict[str, float | None] = {candidate: None for candidate in self.urls}
        self.endpoint_failures: dict[str, int] = {candidate: 0 for candidate in self.urls}
        self.endpoint_error_score: dict[str, float] = {candidate: 0.0 for candidate in self.urls}
        self.endpoint_retry_at: dict[str, float] = {candidate: 0.0 for candidate in self.urls}
        self.endpoint_limits = {candidate: RateLimit() for candidate in self.urls}
        self.health_checked_at = 0
        self.batch_performance = {}
        self._batch_trial_at = 0.0
        self._opener = urllib.request.build_opener(RpcHTTPSHandler(), RpcNoRedirect())

    def start_background_probes(self, health_path=None):
        if self._probe_thread is not None:
            return
        self._health_path = Path(health_path) if health_path else None
        self.restore_health()
        def probe():
            first = True
            while not self._probe_stop.wait(15 if first else (10 if len(self.qualified_urls()) < 3 else 30)):
                first = False
                with self._health_lock:
                    indices = [(self._probe_cursor+i) % len(self.urls) for i in range(len(self.urls))]
                    ready = [i for i in indices if self.endpoint_retry_at[self.urls[i]] <= time.monotonic()]
                    unqualified = [i for i in ready if time.monotonic()-self._validated_endpoints.get(self.urls[i], -1e20) >= 86400]
                    due = next(iter(unqualified or ready), None)
                    if due is None:
                        continue
                    self._probe_cursor = due + 1
                    url = self.urls[due]
                started = time.monotonic()
                try:
                    if self._endpoint_validator is not None:
                        self._validate_endpoint(url)
                    with self._health_lock:
                        self._validated_endpoints[url] = time.monotonic()
                    # A multi-request qualification is not a request latency sample.
                    latency = (time.monotonic() - started)*1000 if self.endpoint_latency_ms[url] is None else None
                    self.mark_health(url, True, latency)
                except Exception as exc:
                    self._mark_failure(url, exc)
                self.save_health()
        self._probe_thread = threading.Thread(target=probe, name="rpc-probe", daemon=True)
        self._probe_thread.start()

    def stop_background_probes(self):
        self._probe_stop.set()
        self.save_health()

    def restore_health(self):
        if self._health_path is None:
            return
        try:
            data = json.loads(self._health_path.read_text())
            age = time.time() - data['saved_at']
            if not 0 <= age < 86400:
                return
            self._probe_cursor = int(data.get('probe_cursor', 0))
            with self._health_lock:
                for url, item in data['endpoints'].items():
                    if url not in self.urls:
                        continue
                    self.endpoint_health[url] = item['healthy']
                    self.endpoint_latency_ms[url] = item['latency']
                    sample = item.get('batch')
                    if (data.get('validation_identity') == self._validation_identity
                            and isinstance(sample, dict) and 0 < sample.get('seconds_per_block', 0) < 600
                            and 0 <= time.time() - sample.get('at', 0) < 86400):
                        self.batch_performance[url] = sample
                    self.endpoint_failures[url] = min(20, max(0, int(item['failures'])))
                    self.endpoint_error_score[url] = min(12, max(0, float(item['score'])))
                    self.endpoint_retry_at[url] = time.monotonic() + max(0, min(86400,item['cooling']) - age)
                    self.endpoint_limits[url].restore(item.get('limit', {}), time.monotonic(), age)
                    validated_at = item.get('validated_at')
                    if (self._validation_identity is not None
                            and data.get('validation_identity') == self._validation_identity
                            and item['healthy'] is True and isinstance(validated_at, (int, float))
                            and 0 <= time.time() - validated_at < 86400):
                        self._validated_endpoints[url] = time.monotonic() - (time.time() - validated_at)
        except (OSError, ValueError, TypeError, KeyError):
            return

    def save_health(self):
        if self._health_path is None:
            return
        with self._health_lock:
            data = {'saved_at':time.time(), 'validation_identity':self._validation_identity, 'probe_cursor':self._probe_cursor, 'endpoints':{
                u:dict(healthy=self.endpoint_health[u], latency=self.endpoint_latency_ms[u], failures=self.endpoint_failures[u],
                       validated_at=(time.time() - (time.monotonic()-self._validated_endpoints[u])) if u in self._validated_endpoints else None,
                       batch=self.batch_performance.get(u),
                       limit=self.endpoint_limits[u].snapshot(time.monotonic()),
                       score=self.endpoint_error_score[u], cooling=max(0,self.endpoint_retry_at[u]-time.monotonic())) for u in self.urls}}
        temporary = self._health_path.with_suffix(f'.{threading.get_ident()}.tmp')
        try:
            temporary.write_text(json.dumps(data))
            temporary.replace(self._health_path)
        except OSError:
            pass

    def qualified_urls(self):
        with self._health_lock:
            return sorted((u for u in self.urls if self.endpoint_health[u] is True and
                           time.monotonic()-self._validated_endpoints.get(u,-1e20) < 86400),
                          key=lambda u:(self.endpoint_error_score[u],self.endpoint_latency_ms[u] or float("inf")))

    def mark_health(
        self, url: str, healthy: bool, latency_ms: float | None = None,
        retry_after: int = 0,
    ) -> None:
        with self._health_lock:
            cooling = self.endpoint_retry_at[url] > time.monotonic()
            if cooling and healthy:
                # A concurrent success cannot revoke an already active cooldown.
                self._validated_endpoints.pop(url, None)
                return
            if cooling and (not retry_after or self.endpoint_limits[url].level):
                # Already in flight responses extend a server deadline, not the ladder.
                self.endpoint_retry_at[url] = max(self.endpoint_retry_at[url],
                                                   time.monotonic() + min(86400, retry_after))
                if retry_after:
                    self.endpoint_limits[url].recover_at = max(self.endpoint_limits[url].recover_at,
                                                                 self.endpoint_retry_at[url] + 600)
                return
            self.endpoint_health[url] = healthy
            if latency_ms is not None:
                previous = self.endpoint_latency_ms[url]
                self.endpoint_latency_ms[url] = latency_ms if previous is None else previous * 0.4 + latency_ms * 0.6
            if healthy:
                self.endpoint_failures[url] = 0
                self.endpoint_error_score[url] *= 0.9
                self.endpoint_retry_at[url] = 0.0
                self.endpoint_limits[url].succeeded(time.monotonic())
            else:
                self._validated_endpoints.pop(url, None)
                failures = self.endpoint_failures[url] + 1
                self.endpoint_failures[url] = failures
                self.endpoint_error_score[url] = min(
                    12.0, self.endpoint_error_score[url] + 1.0,
                )
                delay = RPC_RETRY_DELAYS[min(failures - 1, len(RPC_RETRY_DELAYS) - 1)]
                if retry_after:
                    delay = self.endpoint_limits[url].limited(
                        time.monotonic(), min(86400, int(retry_after)),
                        self.endpoint_retry_at[url] > time.monotonic())
                self.endpoint_retry_at[url] = max(self.endpoint_retry_at[url], time.monotonic() + delay)
            self.health_checked_at = int(time.time())

    def _mark_failure(self, url, exc):
        if isinstance(exc, RpcRequestError):
            if url in exc.reported_urls:
                return
            exc.reported_urls.add(url)
        self.mark_health(url, False, retry_after=getattr(exc, 'retry_after', 0))
        if getattr(exc, 'retry_after', 0):
            self.save_health()

    def set_endpoint_validator(self, validator: Callable[[str], None], identity: str | None = None) -> None:
        """Require a chain-identity check before an endpoint can serve data."""
        self._endpoint_validator = validator
        self._validation_identity = identity
        with self._health_lock:
            self._validated_endpoints.clear()

    def _remaining(self):
        deadline = getattr(self._budget, 'deadline', None)
        remaining = deadline - time.monotonic() if deadline is not None else 30
        if self._operation_deadline is not None:
            remaining = min(remaining, self._operation_deadline - time.monotonic())
        if remaining <= 0:
            raise RpcRequestError('total deadline')
        return remaining

    @contextmanager
    def attempt_budget(self, seconds=15):
        previous = getattr(self._budget, 'deadline', None)
        self._budget.deadline = time.monotonic() + min(seconds, self._remaining())
        try:
            yield
            self._remaining()
        finally:
            self._budget.deadline = previous

    def with_deadline(self, deadline):
        view = copy.copy(self)
        view._operation_deadline = min(deadline, self._operation_deadline or deadline)
        return view

    def diagnostics(self):
        with self._health_lock:
            now = time.monotonic()
            return {
                'active': [{'endpoint_index':v['endpoint_index'], 'method':v['method'],
                            'age_seconds':round(now-v['started'],1)} for v in self._active_requests.values()],
                'endpoints': [{'index':i, 'healthy':self.endpoint_health[u],
                               'cooling_seconds':round(max(0,self.endpoint_retry_at[u]-now),1)}
                              for i,u in enumerate(self.urls)],
            }

    @request_budget
    def _validate_endpoint(self, url):
        with self.attempt_budget():
            self._endpoint_validator(url)

    @request_budget
    def _ensure_endpoint_validated(self, url: str) -> None:
        if self._endpoint_validator is None or time.monotonic() - self._validated_endpoints.get(url, -1e20) < 86400:
            return
        lock = self._validation_locks[url]
        if not lock.acquire(timeout=self._remaining()):
            raise RpcRequestError('validation lock deadline')
        try:
            if time.monotonic() - self._validated_endpoints.get(url, -1e20) < 86400:
                return
            try:
                self._validate_endpoint(url)
            except RpcRequestError:
                raise
            except Exception as exc:
                raise RuntimeError("endpoint validation failed: " + type(exc).__name__) from None
            with self._health_lock:
                self._validated_endpoints[url] = time.monotonic()
        finally:
            lock.release()

    def _candidate_indices(self) -> list[int]:
        with self._health_lock:
            return self._candidates_locked()

    def for_endpoint(self, index):
        """A batch view sharing health, counters and locks, with no inner failover."""
        view = copy.copy(self)
        view._candidate_indices = lambda: [index] if self.endpoint_retry_at[self.urls[index]] <= time.monotonic() else []
        return view

    def batch_candidates(self):
        """Prefer measured downloads; trial a qualified reserve every 30 seconds."""
        with self._health_lock:
            ready = self._candidates_locked()
            now = time.time()
            measured = [i for i in ready if self.endpoint_health[self.urls[i]] is True
                        and now - self.batch_performance.get(self.urls[i], {}).get('at', 0) < 86400]
            measured.sort(key=lambda i: self.batch_performance[self.urls[i]]['seconds_per_block']
                          * (1 + self.endpoint_error_score[self.urls[i]] + 2*self.endpoint_limits[self.urls[i]].level))
            ordered = measured + [i for i in ready if i not in measured]
            if measured and time.monotonic() - self._batch_trial_at >= 30:
                reserves = [i for i in ready if i != ordered[0]
                            and self.endpoint_health[self.urls[i]] is True
                            and self.endpoint_limits[self.urls[i]].level <= self.endpoint_limits[self.urls[ordered[0]]].level
                            and time.monotonic() - self._validated_endpoints.get(self.urls[i], -1e20) < 86400]
                if reserves:
                    # Among unmeasured candidates, ready already prefers low latency.
                    trial = min(reserves, key=lambda i:self.batch_performance.get(self.urls[i], {}).get('at', 0))
                    ordered.remove(trial)
                    ordered.insert(0, trial)
                    self._batch_trial_at = time.monotonic()
            return ordered[:2]

    def record_batch(self, index, blocks, elapsed):
        with self._health_lock:
            url = self.urls[index]
            rate = max(0.001, elapsed) / max(1, blocks)
            old = self.batch_performance.get(url)
            if old and time.time() - old['at'] < 86400:
                rate = old['seconds_per_block'] * 0.25 + rate * 0.75
            self.batch_performance[url] = {'seconds_per_block':rate, 'at':time.time()}
            self._url_index, self.url = index, url

    def _candidates_locked(self) -> list[int]:
        now = time.monotonic()
        indices = [(self._url_index + offset) % len(self.urls) for offset in range(len(self.urls))]
        ready = [index for index in indices if self.endpoint_retry_at[self.urls[index]] <= now]
        if ready:
            # A historical success must not put a 90-second endpoint ahead
            # of a recovered sub-second endpoint after its cooldown expires.
            health_rank = {True: 0, None: 1, False: 2}
            def cost(index):
                url = self.urls[index]
                latency = self.endpoint_latency_ms[url] or self.timeout * 1000
                penalty = 1 + min(10, self.endpoint_error_score[url]) + 2*self.endpoint_limits[url].level
                return max(latency, self.endpoint_limits[url].interval*1000) * penalty * (1 + health_rank[self.endpoint_health[url]])
            ordered = sorted(ready, key=lambda index: (
                1 if self.urls[index] in self.fallback_urls else 0,
                cost(index),
                health_rank[self.endpoint_health[self.urls[index]]],
                0 if index == self._url_index else 1,
            ))
            return ordered
        # Do not bypass cooling when every endpoint is unavailable. The chain
        # worker applies its own bounded backoff and retries once an endpoint is due.
        return []

    def _wait_endpoint(self, url):
        """Share paced admission across block threads, validation and batch views."""
        if url not in self.endpoint_limits:
            return
        while True:
            with self._health_lock:
                now = time.monotonic()
                if self.endpoint_retry_at[url] > now:
                    exc = RpcRequestError('endpoint cooling')
                    exc.reported_urls.add(url)  # Local admission is not another provider failure.
                    raise exc
                limit = self.endpoint_limits[url]
                wait = limit.next_at - now
                if wait <= 0:
                    limit.next_at = now + limit.interval
                    return
            time.sleep(min(wait, self._remaining(), 0.25))

    @request_budget
    @trace_request
    def _post_url(self, url: str, payload: Any, path: str = "") -> Any:
        self._wait_endpoint(url)
        request = urllib.request.Request(
            url.rstrip("/") + path,
            data=json.dumps(payload).encode(),
            headers=self.headers,
            method="POST",
        )
        raw = b""
        try:
            with self._opener.open(request, timeout=min(self.timeout,self._remaining())) as response:
                maximum = 16 * 1024 * 1024
                if int(response.headers.get("Content-Length", "0")) > maximum:
                    raise OSError("response exceeds 16 MiB")
                chunks = []
                size = 0
                deadline = time.monotonic() + min(25,self._remaining())
                while True:
                    if response.isclosed():
                        break
                    if time.monotonic() > deadline:
                        raise TimeoutError("response total deadline")
                    remaining = min(self.timeout,self._remaining(),deadline-time.monotonic())
                    if remaining <= 0:
                        raise TimeoutError('response total deadline')
                    response.fp.raw._sock.settimeout(remaining)
                    chunk = response.read1(min(65536, maximum + 1 - size))
                    if not chunk:
                        break
                    size += len(chunk)
                    if size > maximum:
                        raise OSError("response exceeds 16 MiB")
                    chunks.append(chunk)
                raw = b"".join(chunks)
            result = json.loads(raw)
            if isinstance(payload, dict) and payload.get('jsonrpc') == '2.0':
                if (not isinstance(result, dict) or type(result.get('id')) is not type(payload.get('id'))
                        or result.get('id') != payload.get('id')):
                    raise RpcRequestError('mismatched JSON-RPC response id')
                if result.get('error'):
                    raise rpc_error(result['error'])
                if 'result' not in result:
                    raise RpcRequestError('JSON-RPC result missing')
        except urllib.error.HTTPError as exc:
            self.stats.record(False, len(raw))
            retry_after = retry_seconds(exc.headers.get("Retry-After") if exc.headers else None, time.time())
            if exc.code == 429:
                retry_after = max(60, retry_after)
            exc.close()
            raise RpcRequestError(f"HTTP {exc.code}", retry_after) from exc
        except RpcRequestError:
            self.stats.record(False, len(raw))
            raise
        except TRANSPORT_ERRORS as exc:
            self.stats.record(False, len(raw))
            raise RpcRequestError(type(exc).__name__) from exc
        self.stats.record(True, len(raw))
        return result

    def post(self, payload: Any, path: str = "") -> Any:
        return self.post_validated(payload, path)

    @request_budget
    def post_validated(
        self, payload: Any, path: str = "",
        validator: Callable[[Any], bool] | None = None,
    ) -> Any:
        last_error: RuntimeError | None = None
        deadline = time.monotonic() + self._remaining()
        for index in self._candidate_indices():
            if time.monotonic() > deadline:
                break
            started = time.monotonic()
            try:
                with self.attempt_budget():
                    self._ensure_endpoint_validated(self.urls[index])
                    result = self._post_url(self.urls[index], payload, path)
                    if isinstance(result, dict) and (result.get("Error") or result.get("error")):
                        self.stats.record_validation_failure()
                        raise rpc_error(result.get('Error') or result['error'])
                    if validator is not None:
                        try:
                            valid = validator(result)
                        except RpcRequestError:
                            raise
                        except Exception:
                            valid = False
                        if not valid:
                            self.stats.record_validation_failure()
                            raise RuntimeError("RPC endpoint returned incomplete data")
                    with self._health_lock:
                        self._url_index = index
                        self.url = self.urls[index]
                    self.mark_health(self.urls[index], True, (time.monotonic() - started) * 1000)
                    return result
            except RuntimeError as exc:
                self._mark_failure(self.urls[index], exc)
                last_error = exc
        raise last_error or RuntimeError("RPC request failed")

    @request_budget
    def rpc(
        self, method: str, params: list[Any] | None = None, path: str = "",
        result_validator: Callable[[Any], bool] | None = None,
    ) -> Any:
        with self._request_lock:
            self.counter += 1
            request_id = self.counter
        payload = {"jsonrpc": "2.0", "id": request_id, "method": method, "params": params or []}
        last_error: RuntimeError | None = None
        deadline = time.monotonic() + self._remaining()
        for index in self._candidate_indices():
            if time.monotonic() > deadline:
                break
            started = time.monotonic()
            try:
                with self.attempt_budget():
                    self._ensure_endpoint_validated(self.urls[index])
                    body = self._post_url(self.urls[index], payload, path)
                    if not isinstance(body, dict) or type(body.get('id')) is not int or body.get("id") != request_id:
                        self.stats.record_validation_failure()
                        raise RuntimeError(f"{method}: malformed JSON-RPC response")
                    if body.get("error"):
                        self.stats.record_validation_failure()
                        raise rpc_error(body['error'])
                    if "result" not in body:
                        self.stats.record_validation_failure()
                        raise RuntimeError(f"{method}: JSON-RPC result missing")
                    result = body["result"]
                    if not self._valid_rpc_result(method, result):
                        self.stats.record_validation_failure()
                        raise RuntimeError(f"{method}: JSON-RPC result has invalid type")
                    if result_validator is not None:
                        try:
                            valid = result_validator(result)
                        except RpcRequestError:
                            raise
                        except Exception:
                            valid = False
                        if not valid:
                            self.stats.record_validation_failure()
                            raise RuntimeError(f"{method}: inconsistent chain data")
                    with self._health_lock:
                        self._url_index = index
                        self.url = self.urls[index]
                    self.mark_health(self.urls[index], True, (time.monotonic() - started) * 1000)
                    return result
            except RuntimeError as exc:
                self._mark_failure(self.urls[index], exc)
                last_error = exc
        raise last_error or RuntimeError(f"{method}: RPC request failed")

    @request_budget
    def rpc_redundant(
        self, method: str, params: list[Any] | None = None,
        max_endpoints: int = 2,
    ) -> list[Any]:
        """Return valid results from multiple endpoints for local reconciliation."""
        with self._request_lock:
            self.counter += 1
            request_id = self.counter
        payload = {
            "jsonrpc": "2.0", "id": request_id,
            "method": method, "params": params or [],
        }
        results: list[Any] = []
        last_error: RuntimeError | None = None
        deadline = time.monotonic() + self._remaining()
        for index in self._candidate_indices()[:max(1, int(max_endpoints))]:
            if time.monotonic() >= deadline:
                break
            started = time.monotonic()
            try:
                with self.attempt_budget():
                    self._ensure_endpoint_validated(self.urls[index])
                    body = self._post_url(self.urls[index], payload)
                    if isinstance(body, dict) and body.get('error'):
                        self.stats.record_validation_failure()
                        raise rpc_error(body['error'])
                    if (
                        not isinstance(body, dict) or type(body.get('id')) is not int or body.get('id') != request_id
                        or "result" not in body
                        or not self._valid_rpc_result(method, body["result"])
                    ):
                        self.stats.record_validation_failure()
                        raise RuntimeError(f"{method}: invalid redundant RPC response")
                    results.append(body["result"])
                    self.mark_health(
                        self.urls[index], True,
                        (time.monotonic() - started) * 1000,
                    )
            except RuntimeError as exc:
                self._mark_failure(self.urls[index], exc)
                last_error = exc
        if results:
            return results
        raise last_error or RuntimeError(f"{method}: redundant RPC request failed")

    @staticmethod
    def _valid_rpc_result(method: str, result: Any) -> bool:
        if method in {"eth_blockNumber", "eth_chainId", "eth_getBalance", "eth_call"}:
            if not isinstance(result, str) or not result.startswith("0x"):
                return False
            try:
                int("0" + result[2:], 16)
            except ValueError:
                return False
            return True
        if method == "eth_getLogs":
            return isinstance(result, list)
        if method in {"eth_getBlockByNumber", "eth_getTransactionReceipt"}:
            return isinstance(result, dict)
        return True
