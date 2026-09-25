#!/usr/bin/env bash
set -euo pipefail

: "${V2_RELEASE:?}"
: "${EXPECTED_SHA256:?}"
: "${WORKER_MEMORY_MAX:?}"
: "${WORKER_CPU_QUOTA:?}"

archive=/tmp/crypto-monitor-v2.tar.gz
actual=$(sha256sum "$archive" | awk '{print $1}')
[ "$actual" = "$EXPECTED_SHA256" ] || { echo "archive hash mismatch" >&2; exit 2; }

installer=$(mktemp)
trap 'rm -f "$installer"' EXIT
tar -xOf "$archive" install_remote.sh > "$installer"
V2_ARCHIVE="$archive" V2_RELEASE="$V2_RELEASE" bash "$installer"
install -o root -g root -m 0600 /tmp/crypto-monitor-v2.env /etc/crypto-monitor-v2/cluster.env
install -d -m 0755 /etc/systemd/system/crypto-monitor-v2-worker.service.d
cat >/etc/systemd/system/crypto-monitor-v2-worker.service.d/resources.conf <<EOF
[Service]
MemoryHigh=$WORKER_MEMORY_MAX
CPUQuota=$WORKER_CPU_QUOTA
MemoryMax=$WORKER_MEMORY_MAX
OOMPolicy=stop
EOF
systemctl daemon-reload
echo "node=$(sed -n 's/^CLUSTER_NODE_ID=//p' /etc/crypto-monitor-v2/cluster.env)"
echo "memory_max=$WORKER_MEMORY_MAX"
