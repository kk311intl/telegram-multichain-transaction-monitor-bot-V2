"""Read-only snapshot. Compare counters across two samples with the same PID/epoch."""
import json
import subprocess
import time
from pathlib import Path


def snapshot():
    result = {'time': time.time(), 'chains': {}, 'nic_rx_bytes': 0, 'services': {}}
    from benchmark import physical_rx_bytes
    result['nic_rx_bytes'] = physical_rx_bytes()
    root = Path('/var/lib/crypto-monitor-v2/worker')
    try:
        active = {a['chain'] for a in json.loads((root / 'worker.json').read_text())['assignments']}
    except (OSError,ValueError,KeyError):
        active = set()
    for chain in sorted(root.iterdir()) if root.exists() else []:
        if not chain.is_dir() or chain.name not in active:
            continue
        states = sorted(chain.glob('epoch-*/state.json'), key=lambda p:p.stat().st_mtime, reverse=True)
        if states:
            data = json.loads(states[0].read_text())
            state = data['states'][chain.name]
            result['chains'][chain.name] = {'epoch':states[0].parent.name,'run_started_at':data['started_at'],'age':time.time()-data['updated_at'], **state}
    for service in ('crypto-monitor-v2-worker','crypto-monitor-v2-coordinator'):
        response = subprocess.run(['systemctl','show',service,'--property=ActiveState,SubState,NRestarts,MemoryCurrent,MemoryPeak,CPUUsageNSec,MainPID,UnitFileState'],capture_output=True,text=True)
        result['services'][service] = dict(line.split('=',1) for line in response.stdout.splitlines() if '=' in line)
    result['meminfo'] = {line.split(':')[0]:line.split(':')[1].strip() for line in Path('/proc/meminfo').read_text().splitlines() if line.startswith(('MemAvailable:','SwapFree:','SwapTotal:'))}
    return result


if __name__ == '__main__':
    print(json.dumps(snapshot(), ensure_ascii=False))
