"""Workers send only matching transaction identities, never Telegram user IDs."""
import json
import time
import urllib.request
from cluster_transport import open_cluster

from bot_runtime.common import TRANSFER_TOPIC, tron_to_hex


class MatchFeed:
    def __init__(self, url, node, chain, epoch):
        self.url, self.node, self.chain, self.epoch = url.rstrip('/'), node, chain, epoch
        self.addresses, self.next_refresh = set(), 0

    def call(self, action, **payload):
        body = json.dumps(dict(node=self.node, chain=self.chain, epoch=self.epoch, **payload)).encode()
        request = urllib.request.Request(self.url + '/' + action, body, {'Content-Type': 'application/json'})
        with open_cluster(request, timeout=8) as response:
            raw = response.read(4 * 1024 * 1024 + 1)
        if len(raw) > 4 * 1024 * 1024:
            raise ValueError('watch list too large')
        return json.loads(raw)

    def refresh(self):
        if time.monotonic() >= self.next_refresh:
            data = self.call('watch')
            self.addresses = set(data['addresses'])
            self.next_refresh = time.monotonic() + 5
        return self.addresses

    def submit(self, hits):
        # Ack before advancing the scanner cursor. Repeated chunks are idempotent.
        unique = list({(h['txid'], h['height'], h['hash']): h for h in hits}.values())
        for start in range(0, len(unique), 100):
            self.call('hits', hits=unique[start:start + 100])


def evm_hits(blocks, logs, addresses):
    hits = {}
    for height, block in blocks.items():
        for tx in block['transactions']:
            if tx.get('from', '').lower() in addresses or (tx.get('to') or '').lower() in addresses:
                hits[tx['hash'].lower()] = dict(txid=tx['hash'].lower(), height=height, hash=block['hash'].lower())
    for log in logs:
        topics = log.get('topics', [])
        if len(topics) == 3 and any('0x' + t[-40:].lower() in addresses for t in topics[1:3]):
            txid = log['transactionHash'].lower()
            hits[txid] = dict(txid=txid, height=int(log['blockNumber'], 16), hash=log['blockHash'].lower())
    return list(hits.values())


def tron_hits(block, infos, addresses):
    watched = {tron_to_hex(a) for a in addresses}
    selected = set()
    for tx in block.get('transactions', []):
        for contract in tx.get('raw_data', {}).get('contract', []):
            value = contract.get('parameter', {}).get('value', {})
            if any(str(value.get(k, '')).lower() in watched for k in ('owner_address', 'to_address')):
                selected.add(tx['txID'].lower())
    for info in infos:
        for log in info.get('log', []):
            topics = log.get('topics', [])
            if len(topics) == 3 and topics[0].lower().removeprefix('0x') == TRANSFER_TOPIC[2:] and any('41' + t[-40:].lower() in watched for t in topics[1:3]):
                selected.add(info['id'].lower())
    height = block['block_header']['raw_data']['number']
    return [dict(txid=txid, height=height, hash=block['blockID'].lower()) for txid in selected]
