#!/usr/bin/env bash
set -euo pipefail

python3 - <<'PY'
import json, sqlite3, time

db = sqlite3.connect("file:/var/lib/crypto-monitor-v2-coordinator/cluster.sqlite3?mode=ro", uri=True)
db.row_factory = sqlite3.Row
for row in db.execute("SELECT * FROM nodes ORDER BY node_id"):
    metrics = json.loads(row["metrics_json"])
    print(f"NODE {row['node_id']} heartbeat_age={int(time.time()) - row['last_seen']}s chains={len(metrics)}")
    for chain, value in sorted(metrics.items()):
        print(
            f"  {chain}: status={value.get('status')} lag={value.get('lag')} "
            f"errors={value.get('errors')} rpc_failures={value.get('failures')} "
            f"cpu={value.get('cpu_percent', 0):.2f}% payload={value.get('payload_mbps', 0):.2f}Mbps"
        )
print("LEASES")
for row in db.execute("SELECT chain_name,owner_node,preferred_node,epoch,cursor FROM leases ORDER BY chain_name"):
    print(dict(row))
PY

echo "v2coord=$(systemctl is-active crypto-monitor-v2-coordinator.service)"
echo "v2worker=$(systemctl is-active crypto-monitor-v2-worker.service)"
systemctl show crypto-monitor-v2-coordinator.service crypto-monitor-v2-worker.service \
  -p Id -p NRestarts -p MemoryCurrent --no-pager
echo "release=$(readlink -f /opt/crypto-monitor-v2)"
ss -lntp | grep 18765
sha256sum /tmp/crypto-monitor-v2.tar.gz
