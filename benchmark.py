from __future__ import annotations
import copy

import argparse
import csv
import ctypes
import hashlib
import json
import os
import shutil
import signal
import subprocess
import sys
import threading
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed, wait
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from block_validation import ChainMismatch, header, evm_header, tron_header, check_evm_logs
from match_feed import MatchFeed, evm_hits, tron_hits
from scanner_adapters import EvmAdapter, TronAdapter, TRANSFER_TOPIC, build_adapter


CHAIN_ORDER = (
    "ethereum", "tron", "polygon", "bnb", "avalanche",
    "optimism", "arbitrum", "base", "hyperliquid", "bitcoin", "solana",
)
MAX_BATCH_BLOCKS = 64
TARGET_BATCH_BYTES = 8 * 1024 * 1024
TARGET_BATCH_SECONDS = 2.0
RPC_CACHE_SECONDS = 86400
STATE_INTERVAL_SECONDS = 5


def next_batch_span(span, size, elapsed, lag, block_interval, target_bytes=TARGET_BATCH_BYTES):
    # Slow but small responses may still benefit from amortizing round trips.
    target = min(8.0, max(TARGET_BATCH_SECONDS, span * block_interval * 0.8))
    if size > target_bytes * 2:
        return max(1, span // 2)
    if size < target_bytes and lag > span * 4 and elapsed < 8:
        return min(MAX_BATCH_BLOCKS, span * 2)
    if elapsed > target * 2:
        return max(1, span // 2)
    if size < target_bytes and lag > span and elapsed < target:
        return min(MAX_BATCH_BLOCKS, span * 2)
    return span

SUMMARY_FIELDS = (
    "chain", "status", "duration_seconds",
    "initial_head", "final_head", "head_growth_blocks", "processed_blocks",
    "end_lag_blocks", "max_lag_blocks", "fell_behind", "catchup_ratio",
    "chain_blocks_per_second", "ingest_blocks_per_second", "capacity_margin",
    "transactions", "transfer_logs", "rpc_requests",
    "rpc_failures", "endpoint_switches", "response_bytes", "payload_mbps_avg",
    "physical_rx_bytes", "physical_rx_mbps_avg", "cpu_seconds", "cpu_percent_avg",
    "peak_working_set_bytes", "errors", "last_error",
)


def atomic_json(path: Path, value: Any) -> None:
    temporary = path.with_suffix(path.suffix + f".{threading.get_ident()}.tmp")
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    temporary.replace(path)


def cached_rpc_urls(path: Path, candidates: list[str], now: int | None = None, allow_changed: bool = False) -> list[str]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
        expected = hashlib.sha256("\n".join(candidates).encode()).hexdigest()
        age = (int(time.time()) if now is None else now) - int(value["saved_at"])
        urls = [url for url in value["qualified_urls"] if url in candidates]
        return urls if (allow_changed or value["candidate_hash"] == expected) and 0 <= age < RPC_CACHE_SECONDS else []
    except (OSError, KeyError, TypeError, ValueError, json.JSONDecodeError):
        return []


def working_set_bytes() -> int:
    if os.name != "nt":
        try:
            import resource
            value = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            return int(value * (1 if sys.platform == "darwin" else 1024))
        except Exception:
            return 0

    class Counters(ctypes.Structure):
        _fields_ = [
            ("cb", ctypes.c_ulong), ("PageFaultCount", ctypes.c_ulong),
            ("PeakWorkingSetSize", ctypes.c_size_t),
            ("WorkingSetSize", ctypes.c_size_t),
            ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
            ("QuotaPagedPoolUsage", ctypes.c_size_t),
            ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
            ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
            ("PagefileUsage", ctypes.c_size_t),
            ("PeakPagefileUsage", ctypes.c_size_t),
            ("PrivateUsage", ctypes.c_size_t),
        ]

    counters = Counters()
    counters.cb = ctypes.sizeof(counters)
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    psapi = ctypes.WinDLL("psapi", use_last_error=True)
    kernel32.GetCurrentProcess.restype = ctypes.c_void_p
    psapi.GetProcessMemoryInfo.argtypes = [
        ctypes.c_void_p, ctypes.POINTER(Counters), ctypes.c_ulong,
    ]
    psapi.GetProcessMemoryInfo.restype = ctypes.c_int
    handle = kernel32.GetCurrentProcess()
    if psapi.GetProcessMemoryInfo(handle, ctypes.byref(counters), counters.cb):
        return int(counters.WorkingSetSize)
    return 0


def physical_rx_bytes(pwsh: str = "") -> int | None:
    if os.name != "nt":
        try:
            total = 0
            for line in Path("/proc/net/dev").read_text(encoding="ascii").splitlines()[2:]:
                name, values = line.split(":", 1)
                name = name.strip()
                if name == "lo" or name.startswith(("tailscale", "docker", "veth", "br-")):
                    continue
                total += int(values.split()[0])
            return total
        except Exception:
            return None
    command = (
        "(Get-NetAdapter -Physical -ErrorAction SilentlyContinue | "
        "Where-Object Status -eq 'Up' | Get-NetAdapterStatistics | "
        "Measure-Object -Property ReceivedBytes -Sum).Sum"
    )
    try:
        result = subprocess.run(
            [pwsh, "-NoProfile", "-Command", command], capture_output=True,
            text=True, timeout=8, check=True,
        )
        text = result.stdout.strip()
        return int(text) if text else None
    except Exception:
        return None


def format_bytes(value: float) -> str:
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if abs(value) < 1024 or unit == "TiB":
            return f"{value:.1f}{unit}"
        value /= 1024
    return "0B"


def format_duration(seconds: float) -> str:
    seconds = max(0, int(seconds))
    hours, remainder = divmod(seconds, 3600)
    minutes, secs = divmod(remainder, 60)
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"


class FullChainBenchmark:
    def __init__(self, args: argparse.Namespace):
        self.args = args
        self.output = Path(args.output).resolve()
        self.output.mkdir(parents=True, exist_ok=True)
        self.config = json.loads(Path(args.config).read_text(encoding="utf-8"))
        requested = [item.strip() for item in args.chains.split(",") if item.strip()]
        self.chains = requested or [name for name in CHAIN_ORDER if name in self.config["chains"]]
        unknown = set(self.chains) - set(self.config["chains"])
        if unknown:
            raise ValueError("unknown chains: " + ",".join(sorted(unknown)))
        self.lock = threading.Lock()
        self.block_pool = ThreadPoolExecutor(max_workers=4, thread_name_prefix="full-block")
        self.stop = threading.Event()
        self.finished = threading.Event()
        self.started_at = time.time()
        self.checkpoints = {}
        self.batch_hashes = {}
        self.last_success_monotonic = time.monotonic()
        self.last_cycle_monotonic = self.last_success_monotonic
        self.current_chain = ""
        self.nic_total: int | None = None
        self.nic_sampled_at = 0.0
        self.nic_recent_mbps = 0.0
        self._previous_nic: tuple[float, int] | None = None
        self.states = {
            name: {
                "chain": name, "status": "排隊", "elapsed": 0.0,
                "blocks_per_second": 0.0, "transactions_per_second": 0.0,
                "logs_per_second": 0.0, "payload_mbps": 0.0,
                "lag": 0, "max_lag": 0, "requests": 0, "failures": 0,
                "errors": 0, "switches": 0, "response_bytes": 0,
                "processed_blocks": 0, "transactions": 0, "transfer_logs": 0,
                "span": 1,
                "last_error": "", "peak_memory": 0,
                "qualified": 0, "candidates": 0, "chain_blocks_per_second": 0.0,
                "capacity_margin": 0.0, "poll_sleep": 0.25,
                "cursor": None,
            }
            for name in self.chains
        }

    @staticmethod
    def _stats(adapter: Any) -> tuple[int, int, int]:
        return adapter.rpc.stats.snapshot()

    @staticmethod
    def _endpoint(adapter: Any) -> int:
        return int(getattr(adapter.rpc, "_url_index", 0))

    def _qualify_rpcs(self, name, config):
        candidates = list(dict.fromkeys(config["rpc_urls"]))
        cached = cached_rpc_urls(self.output.parent / "rpc-cache.json", candidates, allow_changed=True)
        selected = dict(config)
        selected.update(rpc_urls=cached + [u for u in candidates if u not in cached], rpc_timeout=config.get('rpc_timeout',12))
        if 'idle_poll_seconds' in selected and not 0.25 <= float(selected['idle_poll_seconds']) <= 60:
            raise ValueError('idle_poll_seconds must be between 0.25 and 60')
        if not 1 <= int(selected.get('target_batch_mib',8)) <= 64:
            raise ValueError('target_batch_mib must be between 1 and 64')
        self._update(name, status="RPC adaptive", qualified=0, candidates=len(candidates))
        return selected, []

    def _evm_batch(
        self, adapter: EvmAdapter, start: int, end: int,
    ) -> tuple[int, int]:
        return self._rpc_batch(adapter, start, end, self._evm_batch_from_endpoint)

    def _rpc_batch(self, adapter, start, end, download):
        failure = RuntimeError('RPC endpoints cooling')
        deadline = time.monotonic() + 50
        for index in adapter.rpc.batch_candidates():
            if time.monotonic() >= deadline:
                break
            scoped = copy.copy(adapter)
            scoped.rpc = adapter.rpc.for_endpoint(index).with_deadline(min(deadline, time.monotonic()+25))
            started = time.monotonic()
            try:
                result = download(scoped,start,end)
                scoped.rpc._remaining()
                adapter.rpc.record_batch(index,end-start+1,time.monotonic()-started)
                return result
            except (RuntimeError, TimeoutError) as exc:
                url = adapter.rpc.urls[index]
                if adapter.rpc.endpoint_health[url] is not False:
                    adapter.rpc.mark_health(url,False,retry_after=getattr(exc,'retry_after',0))
                failure = exc
        raise failure

    def _evm_batch_from_endpoint(self, adapter, start, end):
        blocks = {}
        feed = getattr(self, 'feed', None)
        watched = feed.refresh() if feed else set()
        def fetch_block(height):
            block = adapter.rpc.rpc("eth_getBlockByNumber", [hex(height), True], result_validator=lambda b: bool(evm_header(b, height)) and isinstance(b.get('transactions'), list))
            evm_header(block, height)
            if not isinstance(block.get("transactions"), list) or any(not isinstance(tx, dict) for tx in block["transactions"]):
                raise ChainMismatch("full transaction objects required")
            # Drop transaction bodies after validating the full download.
            block["transactions"] = [
                {k:tx[k] for k in ("hash","from","to") if k in tx}
                if tx.get("from", "").lower() in watched or (tx.get("to") or "").lower() in watched
                else {"hash":tx["hash"]} for tx in block["transactions"]]
            return {k:block[k] for k in ("number","hash","parentHash","transactions")}
        for chunk in self._block_chunks(adapter, range(start,end+1), fetch_block):
            blocks.update(chunk)
        previous = self.checkpoints.get(start-1)
        for height, block in blocks.items():
            current, parent = evm_header(block, height)
            if previous is not None and parent != previous:
                raise ChainMismatch("EVM parent continuity mismatch")
            previous = current
        def valid_logs(value):
            check_evm_logs(blocks, value, lambda txid: adapter.rpc.rpc('eth_getTransactionReceipt', [txid]),
                           reconcile_receipts=getattr(adapter, 'name', '') == 'hyperliquid')
            return all(log.get('topics') and str(log['topics'][0]).lower() == TRANSFER_TOPIC for log in value)
        logs = adapter.rpc.rpc("eth_getLogs", [{
            "fromBlock": hex(start), "toBlock": hex(end), "topics": [TRANSFER_TOPIC],
        }], result_validator=valid_logs)
        if feed:
            feed.submit(evm_hits(blocks,logs,watched))
        self.batch_hashes = {h: b["hash"].lower() for h,b in blocks.items()}
        return sum(len(b["transactions"]) for b in blocks.values()), len(logs)

    def _tron_batch(self, adapter, start, end):
        return self._rpc_batch(adapter, start, end, self._tron_batch_from_endpoint)

    def _block_chunks(self, adapter, heights, fetch_block):
        heights = list(heights)
        for offset in range(0,len(heights),4):
            futures = {h:self.block_pool.submit(fetch_block,h) for h in heights[offset:offset+4]}
            try:
                values = [(height,future.result(timeout=adapter.rpc._remaining())) for height,future in futures.items()]
            finally:
                for future in futures.values():
                    future.cancel()
                deadline = adapter.rpc._operation_deadline or time.monotonic()+1
                _,pending = wait(futures.values(),timeout=max(0,deadline-time.monotonic()))
                self.pending_block_reads = {f for f in getattr(self,'pending_block_reads',()) if not f.done()} | pending
            yield values
            del values, futures

    def _full_block_batch(self, adapter, start, end):
        feed = getattr(self, 'feed', None)
        watched = feed.refresh() if feed else set()
        previous_height, previous = start-1, self.checkpoints.get(start-1)
        hashes, hits, transactions = {}, [], 0
        for chunk in self._block_chunks(adapter, adapter.heights(start,end), adapter.block):
            for height, block in chunk:
                current, parent = adapter.block_header(block, height)
                if previous is not None and (parent != previous or adapter.parent_height(block, height) != previous_height):
                    raise ChainMismatch('block parent continuity mismatch')
                txs = adapter.transactions(block)
                if watched:
                    hits.extend(dict(txid=txid, height=height, hash=current) for txid, tx in txs
                                if adapter.addresses(tx) & watched)
                hashes[height] = current
                previous_height, previous = height, current
                transactions += len(txs)
            del chunk, block, txs
        if not hashes or header(adapter, previous_height)[0] != previous:
            raise ChainMismatch('block changed during download')
        if feed:
            feed.submit(hits)
        self.batch_hashes = hashes
        return transactions, 0

    def _tron_batch_from_endpoint(self, adapter, start, end):
        transactions = logs_count = 0
        feed = getattr(self, 'feed', None)
        watched = feed.refresh() if feed else set()
        previous = self.checkpoints.get(start-1)
        hashes = {}
        for height in range(start, end+1):
            block = adapter.rpc.post_validated({"num":height}, getattr(adapter,"block_prefix","/walletsolidity")+"/getblockbynum",
                                               lambda value: bool(tron_header(value, height)))
            current, parent = tron_header(block, height)
            if previous is not None and previous != parent:
                raise ChainMismatch("TRON parent continuity mismatch")
            txs = block.get("transactions", [])
            def valid_infos(value):
                return (isinstance(txs, list) and isinstance(value, list)
                        and all(isinstance(info, dict) and info.get('blockNumber') == height for info in value)
                        and {tx['txID'] for tx in txs} == {info.get('id') for info in value}
                        and len({info.get('id') for info in value}) == len(value))
            infos = adapter.rpc.post_validated({"num":height}, getattr(adapter,"block_prefix","/walletsolidity")+"/gettransactioninfobyblocknum", valid_infos)
            if not isinstance(txs, list) or not isinstance(infos, list):
                raise ChainMismatch("invalid TRON transaction list")
            expected = {tx["txID"] for tx in txs}
            actual = {info.get("id") for info in infos}
            if expected != actual or len(actual) != len(infos) or any(info.get("blockNumber") != height for info in infos):
                raise ChainMismatch("TRON transaction info mismatch")
            if header(adapter, height)[0] != current:
                raise ChainMismatch("TRON block changed during download")
            if feed:
                feed.submit(tron_hits(block,infos,watched))
            transactions += len(txs)
            for info in infos:
                logs_count += sum(bool(log.get("topics")) and str(log["topics"][0]).lower().removeprefix("0x") == TRANSFER_TOPIC[2:]
                                  for log in info.get("log", []))
            hashes[height], previous = current, current
        self.batch_hashes = hashes
        return transactions, logs_count

    def _recover_reorg(self, adapter, cursor):
        adapter = copy.copy(adapter)
        adapter.rpc = adapter.rpc.with_deadline(time.monotonic()+15)
        if header(adapter, cursor)[0] == self.checkpoints.get(cursor):
            return cursor
        for height in sorted(self.checkpoints, reverse=True):
            if cursor - height > 128:
                break
            if header(adapter, height)[0] == self.checkpoints[height]:
                self.checkpoints = {h:d for h,d in self.checkpoints.items() if h <= height}
                self._update(self.current_chain, rollback="reorg", block_hash=self.checkpoints[height], cursor=height)
                return height
        self._update(self.current_chain, status="held", last_error="reorg exceeds available checkpoint history")
        raise RuntimeError("no common ancestor within retained 128 blocks")

    def _update(self, name: str, **changes: Any) -> None:
        with self.lock:
            self.states[name].update(changes)

    def _snapshot(self) -> dict[str, Any]:
        with self.lock:
            return {
                "started_at": int(self.started_at), "updated_at": int(time.time()),
                "current_chain": self.current_chain,
                "chain_seconds": self.args.seconds,
                "physical_rx_mbps": self.nic_recent_mbps,
                "states": json.loads(json.dumps(self.states)),
            }

    def _restore_checkpoints(self, adapter, cursor):
        """Restore only checkpoints anchored to the requested starting cursor."""
        current_hash = header(adapter, cursor)[0]
        expected_hash = getattr(self.args, "start_hash", "")
        self.checkpoints = {cursor: expected_hash or current_hash}
        try:
            retained = json.loads((self.output.parent / "checkpoints.json").read_text())
            if retained.get(str(cursor)) == self.checkpoints[cursor]:
                self.checkpoints.update({int(h):d for h,d in retained.items() if cursor-128 <= int(h) <= cursor})
        except (OSError, ValueError, TypeError):
            pass
        if expected_hash and current_hash != expected_hash:
            cursor = self._recover_reorg(adapter, cursor)
            current_hash = self.checkpoints[cursor]
        return cursor, current_hash

    def _save_rpc_cache(self, name, adapter):
        healthy = adapter.rpc.qualified_urls()
        original = self.config["chains"][name]["rpc_urls"]
        if healthy:
            atomic_json(self.output.parent / "rpc-cache.json", {
                "saved_at": int(time.time()),
                "candidate_hash": hashlib.sha256(chr(10).join(original).encode()).hexdigest(),
                "qualified_urls": healthy,
            })
        adapter.rpc.save_health()
        self._update(name, qualified=len(healthy), candidates=len(original))

    def _append_progress_log(self, name, log_path):
        if log_path.exists() and log_path.stat().st_size > 4 * 1024 * 1024:
            log_path.replace(log_path.with_suffix(".jsonl.1"))
        record = {"timestamp": int(time.time()), **self.states[name]}
        with log_path.open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(record, ensure_ascii=False) + "\n")

    def _run_chain(self, name: str) -> None:
        self.feed = MatchFeed(self.args.feed_url,self.args.feed_node,name,self.args.feed_epoch) if getattr(self.args,'feed_url','') else None
        self.current_chain = name
        config = dict(self.config["chains"][name])
        try:
            config, _ = self._qualify_rpcs(name, config)
            adapter = build_adapter(name, config)
            self.active_rpc = adapter.rpc
            adapter.rpc.start_background_probes(self.output.parent / "rpc-health.json")
            self._update(name, qualified=len(adapter.rpc.qualified_urls()))
        except Exception as exc:
            summary = {
                field: "" for field in SUMMARY_FIELDS
            }
            summary.update({
                "chain": name, "status": "失敗", "duration_seconds": 0,
                "errors": 1, "last_error": f"{type(exc).__name__}: {str(exc)[:120]}",
                "fell_behind": True,
            })
            self._update(name, status="失敗", errors=1, last_error=summary["last_error"], summary=summary)
            self._write_summary()
            return
        started = time.monotonic()
        deadline = started + self.args.seconds
        cpu_started = time.process_time()
        stats_started = self._stats(adapter)
        endpoint = self._endpoint(adapter)
        initial_nic = self.nic_total
        cursor = initial_head = final_head = None
        span = 1
        consecutive_errors = 0
        samples: deque[tuple[float, int, int, int, int]] = deque()
        totals = {"blocks": 0, "transactions": 0, "logs": 0}
        max_lag = errors = switches = 0
        last_error = ""
        next_log = 0.0
        next_state = 0.0
        next_revalidate = 0.0
        next_rpc_cache = 0.0
        previous_head_sample: tuple[float, int] | None = None
        block_interval = 1.0
        log_path = self.output / f"{name}.jsonl"
        self._update(name, status="初始化")

        while time.monotonic() < deadline and not self.stop.is_set():
            try:
                self._update(name, phase='chain_head')
                safe_head = int(adapter.tip())
                sampled_at = time.monotonic()
                if previous_head_sample is not None and safe_head > previous_head_sample[1]:
                    measured = (sampled_at - previous_head_sample[0]) / (safe_head - previous_head_sample[1])
                    block_interval = block_interval * 0.8 + measured * 0.2
                if previous_head_sample is None or safe_head != previous_head_sample[1]:
                    previous_head_sample = (sampled_at, safe_head)
                if initial_head is None:
                    requested_cursor = int(self.args.start_cursor)
                    if requested_cursor > safe_head:
                        raise RuntimeError("RPC safe head behind durable cursor")
                    cursor = safe_head if requested_cursor < 0 else requested_cursor
                    cursor, current_hash = self._restore_checkpoints(adapter, cursor)
                    self._update(name, block_hash=current_hash, cursor=cursor)
                    initial_head = safe_head
                    final_head = safe_head
                    self._update(name, status="運行", initial_head=initial_head, cursor=cursor)
                else:
                    final_head = safe_head
                assert cursor is not None
                if time.monotonic() >= next_revalidate:
                    self._update(name, phase='reorg_check')
                    cursor = self._recover_reorg(adapter, cursor)
                    next_revalidate = time.monotonic() + 30
                lag = max(0, safe_head - cursor)
                max_lag = max(max_lag, lag)
                if lag == 0:
                    poll_sleep = float(config['idle_poll_seconds']) if 'idle_poll_seconds' in config else min(3.0, max(0.25, block_interval * 0.35))
                    self._update(name, poll_sleep=poll_sleep)
                    time.sleep(poll_sleep)
                else:
                    batch_end = min(safe_head, cursor + span)
                    if hasattr(adapter, 'batch_end'):
                        batch_end = adapter.batch_end(cursor+1, batch_end, safe_head)
                    self._update(name, phase='batch', batch_start=cursor+1, batch_end=batch_end)
                    before = self._stats(adapter)
                    batch_started = time.monotonic()
                    if isinstance(adapter, EvmAdapter):
                        txs, logs = self._evm_batch(adapter, cursor + 1, batch_end)
                    elif isinstance(adapter, TronAdapter):
                        txs, logs = self._tron_batch(adapter, cursor + 1, batch_end)
                    else:
                        txs, logs = self._rpc_batch(adapter, cursor+1, batch_end, self._full_block_batch)
                    after = self._stats(adapter)
                    elapsed = max(0.001, time.monotonic() - batch_started)
                    blocks = len(self.batch_hashes)
                    batch_bytes = max(0, after[2] - before[2])
                    cursor = max(self.batch_hashes)
                    self.checkpoints.update(self.batch_hashes)
                    self.checkpoints = {h:d for h,d in self.checkpoints.items() if h >= cursor - 128}
                    atomic_json(self.output.parent / "checkpoints.json", self.checkpoints)
                    self._update(name, block_hash=self.checkpoints[cursor], cursor=cursor)
                    totals["blocks"] += blocks
                    totals["transactions"] += txs
                    totals["logs"] += logs
                    consecutive_errors = 0
                    span = next_batch_span(span,batch_bytes,elapsed,lag,block_interval,
                                           int(config.get('target_batch_mib',8))*1024*1024)
                    samples.append((time.monotonic(), batch_bytes, blocks, txs, logs))
                self.last_success_monotonic = time.monotonic()
                self.last_cycle_monotonic = self.last_success_monotonic
                self._update(name, last_success_at=time.time(), last_cycle_at=time.time(), status="running")
            except Exception as exc:
                if isinstance(exc, ChainMismatch) and cursor is not None:
                    try:
                        cursor = self._recover_reorg(adapter, cursor)
                    except Exception:
                        pass
                errors += 1
                consecutive_errors += 1
                last_error = f"{type(exc).__name__}: {str(exc)[:120]}"
                span = max(1, span // 2)
                pending = sum(not f.done() for f in getattr(self, 'pending_block_reads', ()))
                cycle = {}
                if not pending:
                    self.last_cycle_monotonic = time.monotonic()
                    cycle['last_cycle_at'] = time.time()
                self._update(name, status='retrying', last_error=last_error, pending_rpc_workers=pending, **cycle)
                self.stop.wait(min(30, 2 ** min(consecutive_errors, 4)))

            current_endpoint = self._endpoint(adapter)
            if current_endpoint != endpoint:
                switches += 1
                endpoint = current_endpoint
            now = time.monotonic()
            while samples and samples[0][0] < now - 30:
                samples.popleft()
            window_seconds = max(1.0, min(30.0, now - started))
            recent_bytes = sum(item[1] for item in samples)
            recent_blocks = sum(item[2] for item in samples)
            recent_txs = sum(item[3] for item in samples)
            recent_logs = sum(item[4] for item in samples)
            requests, failures, response_bytes = self._stats(adapter)
            peak_memory = max(self.states[name].get("peak_memory", 0), working_set_bytes())
            elapsed_total = max(0.001, now - started)
            lag = max(0, (final_head or 0) - (cursor or final_head or 0))
            chain_rate = (
                max(0, (final_head or 0) - (initial_head or final_head or 0)) / elapsed_total
            )
            ingest_rate = totals["blocks"] / elapsed_total
            state_changes = dict(
                elapsed=elapsed_total, blocks_per_second=recent_blocks / window_seconds,
                transactions_per_second=recent_txs / window_seconds,
                logs_per_second=recent_logs / window_seconds,
                payload_mbps=recent_bytes * 8 / window_seconds / 1_000_000,
                lag=lag, max_lag=max_lag, requests=requests, failures=failures,
                errors=errors, switches=switches, response_bytes=response_bytes,
                processed_blocks=totals["blocks"], transactions=totals["transactions"],
                transfer_logs=totals["logs"], span=span,
                last_error=last_error, peak_memory=peak_memory,
                cpu_percent=(time.process_time() - cpu_started) / elapsed_total * 100,
                chain_blocks_per_second=chain_rate,
                capacity_margin=ingest_rate / chain_rate if chain_rate > 0 else 0.0,
                cursor=cursor,
            )
            self._update(name, **state_changes)
            if now >= next_rpc_cache:
                self._save_rpc_cache(name, adapter)
                next_rpc_cache = now + 60
            if now >= next_state:
                atomic_json(self.output / "state.json", self._snapshot())
                next_state = now + STATE_INTERVAL_SECONDS
            if now >= next_log:
                self._append_progress_log(name, log_path)
                next_log = now + self.args.log_interval

        adapter.rpc.stop_background_probes()
        ended = time.monotonic()
        try:
            final_head = int(adapter.tip())
        except Exception as exc:
            last_error = last_error or f"final head: {type(exc).__name__}"
        requests, failures, response_bytes = self._stats(adapter)
        duration = max(0.001, ended - started)
        cursor_value = cursor if cursor is not None else initial_head
        end_lag = max(0, (final_head or 0) - (cursor_value or final_head or 0))
        growth = max(0, (final_head or 0) - (initial_head or final_head or 0))
        ratio = totals["blocks"] / growth if growth else 1.0
        chain_rate = growth / duration
        ingest_rate = totals["blocks"] / duration
        final_nic = self.nic_total
        physical_bytes = (
            max(0, final_nic - initial_nic)
            if final_nic is not None and initial_nic is not None else 0
        )
        status = "停止" if self.stop.is_set() else "完成"
        summary = {
            "chain": name, "status": status, "duration_seconds": round(duration, 3),
            "initial_head": initial_head,
            "final_head": final_head, "head_growth_blocks": growth,
            "processed_blocks": totals["blocks"], "end_lag_blocks": end_lag,
            "max_lag_blocks": max_lag,
            "fell_behind": bool(end_lag > 2 or (growth and ratio < 0.98)),
            "catchup_ratio": round(ratio, 6), "transactions": totals["transactions"],
            "chain_blocks_per_second": round(chain_rate, 6),
            "ingest_blocks_per_second": round(ingest_rate, 6),
            "capacity_margin": round(ingest_rate / chain_rate, 6) if chain_rate > 0 else 0.0,
            "transfer_logs": totals["logs"],
            "rpc_requests": requests, "rpc_failures": failures,
            "endpoint_switches": switches, "response_bytes": response_bytes,
            "payload_mbps_avg": round(response_bytes * 8 / duration / 1_000_000, 6),
            "physical_rx_bytes": physical_bytes,
            "physical_rx_mbps_avg": round(physical_bytes * 8 / duration / 1_000_000, 6),
            "cpu_seconds": round(time.process_time() - cpu_started, 3),
            "cpu_percent_avg": round((time.process_time() - cpu_started) / duration * 100, 3),
            "peak_working_set_bytes": int(self.states[name].get("peak_memory", 0)),
            "errors": errors, "last_error": last_error,
        }
        atomic_json(self.output / f"{name}-summary.json", summary)
        self._update(name, status=status, elapsed=duration, lag=end_lag, summary=summary)
        self._write_summary()

    def _write_summary(self) -> None:
        completed = []
        with self.lock:
            for name in self.chains:
                summary = self.states[name].get("summary")
                if summary:
                    completed.append(summary)
        with (self.output / "summary.csv").open("w", newline="", encoding="utf-8-sig") as stream:
            writer = csv.DictWriter(stream, fieldnames=SUMMARY_FIELDS, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(completed)
        atomic_json(self.output / "summary.json", completed)
        atomic_json(self.output / "state.json", self._snapshot())

    def run_worker(self) -> None:
        try:
            for name in self.chains:
                if self.stop.is_set():
                    break
                self._run_chain(name)
        finally:
            self.current_chain = ""
            self._write_summary()
            self.finished.set()

    def update_nic(self) -> None:
        now = time.monotonic()
        if now - self.nic_sampled_at < 10:
            return
        current = physical_rx_bytes(self.args.pwsh)
        self.nic_sampled_at = now
        if current is not None and self._previous_nic is not None:
            elapsed = max(0.001, now - self._previous_nic[0])
            self.nic_recent_mbps = max(0, current - self._previous_nic[1]) * 8 / elapsed / 1_000_000
        if current is not None:
            self.nic_total = current
            self._previous_nic = (now, current)

    def render(self) -> None:
        snapshot = self._snapshot()
        elapsed = time.time() - self.started_at
        total = len(self.chains) * self.args.seconds
        completed_seconds = sum(
            min(float(item["elapsed"]), self.args.seconds)
            for item in snapshot["states"].values()
        )
        width = max(110, shutil.get_terminal_size((140, 40)).columns)
        print("\x1b[2J\x1b[H", end="")
        print("全鏈資料下載商用可行性測試")
        print(
            f"分支 test/full-chain-commercial-scan  | 每鏈 {format_duration(self.args.seconds)} "
            f"| 總進度 {completed_seconds / max(1, total) * 100:5.1f}% "
            f"| 已運行 {format_duration(elapsed)} | 實體網卡 RX {self.nic_recent_mbps:7.2f} Mb/s"
        )
        print("-" * min(width, 150))
        print(
            f"{'鏈':<13} {'狀態':<6} {'時間':>8} {'區塊/s':>8} {'交易/s':>9} "
            f"{'日誌/s':>9} {'Payload':>10} {'落後':>7} {'錯誤':>6} {'切換':>6} {'已下載':>10}"
        )
        print("-" * min(width, 150))
        for name in self.chains:
            item = snapshot["states"][name]
            print(
                f"{name:<13} {item['status']:<6} {format_duration(item['elapsed']):>8} "
                f"{item['blocks_per_second']:>8.2f} {item['transactions_per_second']:>9.1f} "
                f"{item['logs_per_second']:>9.1f} {item['payload_mbps']:>8.2f}Mb "
                f"{item['lag']:>7} {item['errors']:>6} {item['switches']:>6} "
                f"{format_bytes(item['response_bytes']):>10}"
            )
        current = snapshot["current_chain"]
        if current:
            item = snapshot["states"][current]
            print("-" * min(width, 150))
            print(
                f"目前 {current}: RPC評選 {item['qualified']}/{item['candidates']} | "
                f"批次 {item['span']} blocks | 等待 {item['poll_sleep']:.2f}s | "
                f"RPC {item['requests']} 次 / 失敗 {item['failures']} | "
                f"最大落後 {item['max_lag']} blocks"
            )
            print(
                f"CPU {item.get('cpu_percent', 0):.1f}% | 記憶體 {format_bytes(item['peak_memory'])} | "
                f"鏈速 {item['chain_blocks_per_second']:.2f} block/s | "
                f"容量倍率 {item['capacity_margin']:.2f}x | 最後錯誤 {item['last_error'] or '無'}"
            )
        elif self.finished.is_set():
            print("\n全部測試已完成。")
        print(f"\n結果目錄：{self.output}")
        print("按 Ctrl+C 可安全停止；已完成鏈與即時狀態不會遺失。", flush=True)

    def run(self) -> int:
        if os.name == "nt":
            ctypes.windll.kernel32.SetThreadExecutionState(0x80000001)
            os.system("")
        self.update_nic()
        if os.name != "nt":
            signal.signal(signal.SIGTERM, lambda *_: self.stop.set())
            signal.signal(signal.SIGINT, lambda *_: self.stop.set())
        worker = threading.Thread(target=self.run_worker, name="benchmark-worker", daemon=True)
        worker.start()
        next_publish = 0
        try:
            while not self.finished.wait(1):
                lease_file = getattr(self.args, "lease_file", "")
                if lease_file:
                    try:
                        lease = json.loads(Path(lease_file).read_text())
                        expired = time.monotonic() >= float(lease["deadline"]) or lease.get("id") != self.args.lease_id
                    except (OSError, ValueError, KeyError):
                        expired = True
                    if expired:
                        os._exit(75)
                if time.monotonic() - self.last_cycle_monotonic > 120:
                    diagnostic = {'reason':'cycle_stalled', **self._snapshot()}
                    if getattr(self, 'active_rpc', None) is not None:
                        diagnostic['rpc'] = self.active_rpc.diagnostics()
                    try:
                        atomic_json(self.output / 'watchdog.json', diagnostic)
                    except OSError:
                        print('watchdog diagnostic write failed', flush=True)
                    os._exit(76)
                if time.monotonic() >= next_publish:
                    atomic_json(self.output / "state.json", self._snapshot())
                    next_publish = time.monotonic() + STATE_INTERVAL_SECONDS
                self.update_nic()
                if not self.args.headless:
                    self.render()
            self.update_nic()
            if not self.args.headless:
                self.render()
            return 0
        except KeyboardInterrupt:
            self.stop.set()
            self.render()
            worker.join(timeout=30)
            return 130
        finally:
            self.block_pool.shutdown(wait=False, cancel_futures=True)
            if os.name == "nt":
                ctypes.windll.kernel32.SetThreadExecutionState(0x80000000)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--feed-url", default="")
    parser.add_argument("--feed-node", default="")
    parser.add_argument("--feed-epoch", type=int, default=0)
    parser.add_argument("--config", default="chains.json")
    parser.add_argument("--output", required=True)
    parser.add_argument("--seconds", type=int, default=1800)
    parser.add_argument("--chains", default="")
    parser.add_argument("--pwsh", default="")
    parser.add_argument("--headless", action="store_true")
    parser.add_argument("--start-cursor", type=int, default=-1)
    parser.add_argument("--start-hash", default="")
    parser.add_argument("--lease-file", default="")
    parser.add_argument("--lease-id", default="")
    parser.add_argument("--log-interval", type=int, default=10)
    args = parser.parse_args()
    if args.seconds <= 0:
        parser.error("--seconds must be positive")
    if args.log_interval <= 0:
        parser.error("--log-interval must be positive")
    return args


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    raise SystemExit(FullChainBenchmark(parse_args()).run())
