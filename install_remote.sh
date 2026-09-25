#!/usr/bin/env bash
set -euo pipefail

: "${V2_ARCHIVE:=/tmp/crypto-monitor-v2.tar.gz}"
: "${V2_RELEASE:?V2_RELEASE is required}"
[[ "$V2_RELEASE" =~ ^release-[0-9TZ-]+-[0-9a-f]{8}$ ]] || { echo "invalid V2_RELEASE" >&2; exit 2; }

release_dir="/opt/crypto-monitor-v2-releases/$V2_RELEASE"
id crypto-monitor-v2 >/dev/null 2>&1 || useradd --system --home /var/lib/crypto-monitor-v2 --shell /usr/sbin/nologin crypto-monitor-v2
id crypto-monitor-v2-coordinator >/dev/null 2>&1 || useradd --system --home /var/lib/crypto-monitor-v2-coordinator --shell /usr/sbin/nologin crypto-monitor-v2-coordinator
install -d -o crypto-monitor-v2-coordinator -g crypto-monitor-v2-coordinator -m 0700 /var/lib/crypto-monitor-v2-coordinator
install -d -m 0755 /opt/crypto-monitor-v2-releases
install -d -o crypto-monitor-v2 -g crypto-monitor-v2 -m 0700 /var/lib/crypto-monitor-v2
install -d -o root -g root -m 0755 /etc/crypto-monitor-v2
[ ! -e "$release_dir" ] || { echo "release already exists" >&2; exit 3; }
install -d -m 0755 "$release_dir"
tar -xzf "$V2_ARCHIVE" -C "$release_dir"
python3 -m py_compile "$release_dir"/*.py "$release_dir"/monitor/*.py
python3 -m unittest discover -s "$release_dir" -p 'test_*.py' -q
ln -sfn "$release_dir" /opt/crypto-monitor-v2
install -m 0644 "$release_dir/crypto-monitor-v2-coordinator.service" /etc/systemd/system/
install -m 0644 "$release_dir/crypto-monitor-v2-worker.service" /etc/systemd/system/
systemctl daemon-reload
echo "release=$V2_RELEASE"
