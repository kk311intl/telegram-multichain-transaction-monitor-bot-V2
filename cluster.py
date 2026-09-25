from __future__ import annotations

import argparse
import ipaddress
import json
import os
import signal
import ssl
import sqlite3
import subprocess
import sys
import threading
import time
import uuid
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from contextlib import closing


def load_json(path: str | Path) -> dict[str, Any]:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def atomic_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    temporary.replace(path)


from lease_store import LeaseStore, LEASE_SECONDS, validate_config
from scanner_adapters import build_adapter
from checkpoint_verifier import CheckpointVerifier
from cluster_transport import configured_tls, open_cluster, peer_node


class BoundedServer(ThreadingHTTPServer):
    daemon_threads = True
    request_queue_size = 16

    def __init__(self, *args):
        self.slots = threading.BoundedSemaphore(16)
        super().__init__(*args)

    def service_actions(self):
        if hasattr(self, "bot_thread") and not self.bot_thread.is_alive():
            raise RuntimeError("Bot controller stopped")

    def verify_request(self, request, address):
        request.settimeout(5)
        return bool(getattr(self,'tls_context',None)) or address[0] in self.node_ips.values()

    def process_request(self, request, address):
        if not self.slots.acquire(False):
            self.shutdown_request(request)
            return
        try:
            super().process_request(request, address)
        except Exception:
            self.slots.release()
            raise

    def process_request_thread(self, request, address):
        try:
            if getattr(self,'tls_context',None):
                request = self.tls_context.wrap_socket(request,server_side=True)
            super().process_request_thread(request, address)
        except (ssl.SSLError,TimeoutError,ConnectionError,OSError):
            request.close()
        finally:
            self.slots.release()


class CoordinatorHandler(BaseHTTPRequestHandler):
    server_version = "CryptoClusterV2/1"

    def _known_ip(self) -> bool:
        if getattr(self.server,'tls_context',None):
            return peer_node(self.connection) in self.server.node_ips
        return self.client_address[0] in self.server.node_ips.values()  # type: ignore[attr-defined]

    def _reply(self, status: int, value: Any) -> None:
        body = json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:
        if self.path == "/health":
            self._reply(200, {"ok": True})
        elif self.path == "/status" and self._known_ip():
            self._reply(200, self.server.store.status())  # type: ignore[attr-defined]
        else:
            self._reply(404, {"error": "not found"})

    def do_POST(self) -> None:
        if not self._known_ip():
            self._reply(403, {"error": "unknown source"})
            return
        if self.path not in ("/heartbeat", "/watch", "/hits"):
            self._reply(404, {"error": "not found"})
            return
        try:
            size = int(self.headers.get("Content-Length", "0"))
            if size <= 0 or size > 65_536:
                raise ValueError("invalid body size")
            payload = json.loads(self.rfile.read(size))
            if not isinstance(payload, dict) or self.headers.get("Transfer-Encoding"):
                raise ValueError("invalid request")
            node_id = str(payload["node"])
            expected_ip = self.server.node_ips.get(node_id)  # type: ignore[attr-defined]
            identity_ok = (peer_node(self.connection)==node_id and node_id in self.server.node_ips
                           if getattr(self.server,'tls_context',None) else expected_ip==self.client_address[0])
            if not identity_ok:
                self._reply(403, {"error": "node identity mismatch"})
                return
            if self.path != "/heartbeat":
                feed = getattr(self.server, "feed", None)
                if feed is None:
                    self._reply(503, {"error": "Bot feed unavailable"})
                    return
                chain = payload.get("chain")
                now = self.server.store.clock_wall + time.monotonic() - self.server.store.clock_mono
                lease = next((r for r in self.server.store.status()["leases"] if r["chain_name"] == chain), None)
                if lease is None or lease["disabled"] or lease["target"] is not None or lease["owner_node"] != node_id or type(payload.get("epoch")) is not int or payload["epoch"] != lease["epoch"] or now >= lease["expires"]:
                    self._reply(403, {"error": "inactive or foreign lease"})
                    return
                if self.path == "/watch":
                    addresses = feed.addresses(chain, self.server.chain_config[chain]["type"] == "evm")
                    self._reply(200, {"addresses": addresses})
                else:
                    feed.accept(chain, payload.get("hits"))
                    self._reply(200, {"accepted": True})
                return
            assignments = self.server.store.heartbeat(  # type: ignore[attr-defined]
                node_id, payload.get("metrics", {}),
            )
            self._reply(200, {"assignments": assignments, "server_time": int(time.time())})
        except (ValueError, KeyError, TypeError, RecursionError, json.JSONDecodeError) as exc:
            self._reply(400, {"error": str(exc)})

    def log_message(self, fmt: str, *args: Any) -> None:
        if len(args) > 1 and str(args[1]) != "200":
            print(f"coordinator {self.address_string()} {fmt % args}", flush=True)


def coordinator_configuration(config, catalog):
    validate_config(config)
    if any(name not in catalog for name in config['chains']):
        raise ValueError('enabled chain missing from chains configuration')
    return {name: catalog[name] for name in config['chains']}


def node_addresses(config, tls):
    addresses = {}
    for name, item in config['nodes'].items():
        address = item.get('ip_address', item.get('tailscale_ip'))
        addresses[name] = str(ipaddress.ip_address(address)) if address is not None else None
    if not tls and (None in addresses.values() or len(set(addresses.values())) != len(addresses)):
        raise ValueError('private HTTP requires a distinct IP for every node')
    return addresses


def run_coordinator(args: argparse.Namespace) -> int:
    config = load_json(args.cluster_config)
    chain_config = coordinator_configuration(config, load_json(args.chains_config)["chains"])
    tls = any(os.environ.get('CLUSTER_TLS_'+name) for name in ('CA','CERT','KEY'))
    context = configured_tls(server=True) if tls else None
    if not tls and os.environ.get('CLUSTER_ALLOW_PLAINTEXT') != '1':
        raise ValueError('coordinator requires mutual TLS or explicit private HTTP opt-in')
    addresses = node_addresses(config, tls)
    adapters = {name: build_adapter(name, settings) for name, settings in chain_config.items()}
    store = LeaseStore(args.db, config, verifier=CheckpointVerifier(adapters))
    server = BoundedServer((args.bind, args.port), CoordinatorHandler)
    server.store = store  # type: ignore[attr-defined]
    server.node_ips = addresses  # type: ignore[attr-defined]
    server.tls_context = context
    token = os.environ.get("BOT_TOKEN", "")
    if token:
        from bot_runtime.store import Store
        from bot_runtime.feed_store import FeedStore
        from bot_runtime.app import App
        owner = int(os.environ["OWNER_USER_ID"])
        if owner <= 0:
            raise ValueError("invalid Bot owner")
        state_path = Path(args.db).with_name("bot.sqlite3")
        initial = Store(state_path, owner)
        initial.configure_owner(owner)
        initial.db.close()
        server.feed = FeedStore(state_path)
        server.chain_config = chain_config
        def bot_main():
            App(token,owner,{"chains":chain_config},state_path,store).run()
        server.bot_thread = threading.Thread(target=bot_main, daemon=True, name="telegram")
        server.bot_thread.start()
    print(f"coordinator listening on {args.bind}:{args.port}", flush=True)
    server.serve_forever()
    return 0


class Scanner:
    def __init__(self, chain: str, epoch: int, cursor: int, args: argparse.Namespace):
        self.chain = chain
        self.epoch = epoch
        self.started_at = time.time()
        self.deadline = 0.0
        self.output = Path(args.state_dir) / chain / f"epoch-{epoch}"
        self.output.mkdir(parents=True, exist_ok=True)
        import shutil
        epochs = sorted((p for p in self.output.parent.glob("epoch-*") if p.is_dir() and not p.is_symlink()),
                        key=lambda p: p.stat().st_mtime, reverse=True)
        for old in epochs[8:]:
            if old != self.output and old.resolve().parent == self.output.parent.resolve():
                shutil.rmtree(old)
        self.lease_id = uuid.uuid4().hex
        self.lease_path = self.output / "lease.json"
        self.renew(args.lease_deadline)
        log_path = self.output / 'scanner.log'
        if log_path.exists() and log_path.stat().st_size > 1024 * 1024:
            log_path.replace(self.output / 'scanner.previous.log')
        self.log = log_path.open("a", encoding="utf-8")
        self.exit_recorded = False
        command = [
            sys.executable, str(Path(__file__).with_name("benchmark.py")),
            "--config", args.chains_config, "--output", str(self.output),
            "--chains", chain, "--seconds", str(args.scanner_seconds),
            "--start-cursor", "-1", "--log-interval", "60", "--headless",
            "--start-hash", "", "--lease-file", str(self.lease_path), "--lease-id", self.lease_id,
        ]
        command.extend(["--feed-url", os.environ.get("CLUSTER_COORDINATOR", ""), "--feed-node", os.environ.get("CLUSTER_NODE_ID", ""), "--feed-epoch", str(epoch)])
        self.process = subprocess.Popen(command, stdout=self.log, stderr=subprocess.STDOUT)
        print(json.dumps({'event':'scanner_start','chain':chain,'epoch':epoch,'run_id':self.lease_id,'pid':self.process.pid}),flush=True)

    def renew(self, deadline):
        if deadline <= time.monotonic():
            raise ValueError("expired lease response")
        self.deadline = deadline
        atomic_json(self.lease_path, {"deadline": deadline, "id": self.lease_id})

    def metrics(self) -> dict[str, Any]:
        path = self.output / "state.json"
        value: dict[str, Any] = {"epoch": self.epoch, "run_id": self.lease_id, "pid": self.process.pid}
        try:
            if path.stat().st_mtime < self.started_at:
                raise OSError("stale state")
            state = load_json(path)["states"][self.chain]
            for key in (
                "cursor", "block_hash", "rollback", "status", "lag", "errors", "failures", "payload_mbps",
                "initial_head", "response_bytes", "processed_blocks", "elapsed", "last_success_at", "last_cycle_at", "last_error",
                "cpu_percent", "peak_memory", "blocks_per_second",
                "chain_blocks_per_second", "capacity_margin", "span", "qualified", "candidates",
            ):
                value[key] = state.get(key)
            age = time.time() - float(state.get("last_success_at") or self.started_at)
            value["state_age_seconds"] = round(age, 1)
            cycle_age = time.time() - float(state.get('last_cycle_at') or state.get('last_success_at') or self.started_at)
            if cycle_age > 120:
                value["status"] = "stalled"
            elif age > 120:
                value["status"] = "retrying"
            if self.process.poll() is not None:
                value["status"] = "exited"
        except (OSError, KeyError, json.JSONDecodeError):
            value["status"] = ("stalled" if time.time() - self.started_at > 120 else "starting") if self.process.poll() is None else "exited"
        return value

    def stop(self, reason='shutdown') -> None:
        if getattr(self,'exit_recorded',False):
            return
        exit_code = self.process.poll()
        if self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=2)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=5)
        self.log.close()
        path = self.output.parent / 'scanner-lifecycle.json'
        try:
            previous = load_json(path)
        except (OSError, ValueError):
            previous = {}
        record = {'event':'scanner_stop','chain':self.chain,'epoch':self.epoch,'run_id':self.lease_id,
                  'reason':reason,'exit_code':exit_code,'final_exit_code':self.process.poll(),
                  'at':time.time(),'unexpected_exits':int(previous.get('unexpected_exits',0))+int(reason in ('stalled','exited'))}
        atomic_json(path,record)
        print(json.dumps(record),flush=True)
        self.exit_recorded = True


def request_assignments(url: str, node: str, metrics: dict[str, Any]) -> list[dict[str, Any]]:
    body = json.dumps({"node": node, "metrics": metrics}, separators=(",", ":")).encode()
    request = urllib.request.Request(
        url.rstrip("/") + "/heartbeat", data=body, method="POST",
        headers={"Content-Type": "application/json"},
    )
    with open_cluster(request, timeout=8) as response:
        return json.loads(response.read(65_537))["assignments"]


def run_worker(args: argparse.Namespace) -> int:
    coordinator = os.environ.get("CLUSTER_COORDINATOR", "")
    node_id = os.environ.get("CLUSTER_NODE_ID", "")
    if not coordinator or not node_id:
        raise SystemExit("CLUSTER_COORDINATOR and CLUSTER_NODE_ID are required")
    Path(args.state_dir).mkdir(parents=True, exist_ok=True)
    lock_file = (Path(args.state_dir) / "worker.lock").open("a")
    if os.name != "nt":
        import fcntl
        fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
    config = load_json(args.cluster_config)
    if node_id not in config["nodes"]:
        raise SystemExit(f"unknown node: {node_id}")
    args.scanner_seconds = int(config.get("scanner_seconds", 315360000))
    stop = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    scanners: dict[str, Scanner] = {}
    interval = int(config.get("heartbeat_seconds", 5))
    last_message: tuple[bool, tuple[str, ...] | str] | None = None
    last_log_at = 0.0
    restart_history = {}
    cooldown = {}
    try:
        while not stop.is_set():
            for chain in list(scanners):
                if scanners[chain].deadline <= time.monotonic():
                    scanners.pop(chain).stop('lease_expired')
            metrics = {chain: scanner.metrics() for chain, scanner in scanners.items()}
            for chain, item in list(metrics.items()):
                if item["status"] in ("stalled", "exited") and chain not in cooldown and not scanners[chain].exit_recorded:
                    history = [t for t in restart_history.get(chain, []) if time.monotonic() - t < 600]
                    history.append(time.monotonic())
                    restart_history[chain] = history
                    scanners[chain].stop(item['status'])
                    if len(history) >= 3:
                        cooldown[chain] = time.monotonic() + 180
                if chain in cooldown:
                    item["status"] = "cooldown"
            requested_at = time.monotonic()
            try:
                assignments = request_assignments(coordinator, node_id, metrics)
                wanted = {item["chain"]: item for item in assignments}
                for chain in set(scanners) - set(wanted):
                    scanners.pop(chain).stop('assignment_removed')
                for chain, item in wanted.items():
                    current = scanners.get(chain)
                    args.lease_deadline = requested_at + min(LEASE_SECONDS, int(item["lease_seconds"]))
                    args.start_hash = item.get("block_hash")
                    if time.monotonic() >= args.lease_deadline:
                        continue
                    if chain in cooldown:
                        if time.monotonic() < cooldown[chain]:
                            if current:
                                current.renew(args.lease_deadline)
                            continue
                        del cooldown[chain]
                    if current and current.epoch == int(item["epoch"]) and current.process.poll() is None:
                        current.renew(args.lease_deadline)
                        continue
                    if current:
                        current.stop('replaced')
                    scanners[chain] = Scanner(chain, int(item["epoch"]), int(item["start_cursor"]), args)
                atomic_json(Path(args.state_dir) / "worker.json", {
                    "node": node_id, "updated_at": int(time.time()), "assignments": assignments,
                })
                message = (True, tuple(sorted(scanners)))
                if message != last_message or time.monotonic() - last_log_at >= 300:
                    print(json.dumps({"node": node_id, "chains": sorted(scanners), "ok": True}), flush=True)
                    last_message, last_log_at = message, time.monotonic()
            except (OSError, urllib.error.URLError, ValueError, KeyError, json.JSONDecodeError) as exc:
                message = (False, str(exc)[:160])
                if message != last_message or time.monotonic() - last_log_at >= 60:
                    print(json.dumps({"node": node_id, "ok": False, "error": message[1]}), flush=True)
                    last_message, last_log_at = message, time.monotonic()
            stop.wait(interval)
    finally:
        for scanner in scanners.values():
            scanner.stop()
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--cluster-config", default=os.environ.get("CLUSTER_CONFIG", "/etc/crypto-monitor-v2/cluster.json"))
    sub = parser.add_subparsers(dest="command", required=True)
    coordinator = sub.add_parser("coordinator")
    coordinator.add_argument("--chains-config", default=os.environ.get("CHAINS_CONFIG", "/etc/crypto-monitor-v2/chains.json"))
    coordinator.add_argument("--bind", default="127.0.0.1")
    coordinator.add_argument("--port", type=int, default=18765)
    coordinator.add_argument("--db", default="run/cluster.sqlite3")
    worker = sub.add_parser("worker")
    worker.add_argument("--chains-config", default=os.environ.get("CHAINS_CONFIG", "/etc/crypto-monitor-v2/chains.json"))
    worker.add_argument("--state-dir", default="run/worker")
    status = sub.add_parser("status")
    status.add_argument("--db", default="run/cluster.sqlite3")
    rebalance = sub.add_parser("rebalance")
    rebalance.add_argument("--db", default="run/cluster.sqlite3")
    rebalance.add_argument("--node", required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.command == "coordinator":
        return run_coordinator(args)
    if args.command == "worker":
        return run_worker(args)
    if args.command == "status":
        print(json.dumps(LeaseStore.read_status(args.db), ensure_ascii=False, indent=2))
        return 0
    config = load_json(args.cluster_config)
    store = LeaseStore(args.db, config)
    if args.command == "rebalance":
        print(json.dumps({"moved": store.rebalance(args.node)}, ensure_ascii=False))
    else:
        print(json.dumps(store.status(), ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
