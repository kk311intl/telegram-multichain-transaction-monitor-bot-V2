"""Bitcoin Core RPC: complete prevouts, per-address net BTC transfers."""
from collections import defaultdict
from decimal import Decimal, InvalidOperation

from block_validation import ChainMismatch, digest
from bot_runtime.common import Event
from chain_identity import bitcoin_address
from monitor.rpc import JsonClient, rpc_urls

GENESIS = '000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f'


def satoshis(value):
    try:
        amount = Decimal(str(value)) * 100_000_000
        if not amount.is_finite() or amount != amount.to_integral_value() or not 0 <= amount <= 21_000_000 * 100_000_000:
            raise ValueError('invalid BTC amount')
        return int(amount)
    except (InvalidOperation, ValueError) as exc:
        raise ChainMismatch('invalid BTC amount') from exc


def outputs(tx):
    incoming, outgoing = [], []
    if not isinstance(tx.get('vin'), list) or not tx['vin'] or not isinstance(tx.get('vout'), list) or not tx['vout']:
        raise ChainMismatch('incomplete BTC transaction')
    for item in tx['vin']:
        if 'coinbase' not in item:
            if not isinstance(item.get('prevout'), dict):
                raise ChainMismatch('BTC RPC omitted prevout; verbosity=3 with undo data required')
            incoming.append(item['prevout'])
    outgoing.extend(tx['vout'])
    def parse(items):
        result = []
        for item in items:
            value = satoshis(item['value'])
            script = item['scriptPubKey']
            address = script.get('address')
            # Scripts without a single standard address remain unassigned.
            if address:
                result.append((bitcoin_address(address), value))
        return result
    return parse(incoming), parse(outgoing)


class BitcoinAdapter:
    def __init__(self, name, config):
        self.name, self.config = name, config
        self.finality_blocks = max(0, int(config['finality_blocks']))
        self.rpc = JsonClient(rpc_urls(config), timeout=max(3, int(config.get('rpc_timeout', 20))),
                              max_response_bytes=int(config.get('max_response_mib', 32))*1024*1024)
        self.rpc.set_endpoint_validator(self._validate, f'bitcoin-prevouts-1:{GENESIS}')

    def _validate(self, url):
        def call(method, params):
            result = self.rpc._post_url(url, dict(jsonrpc='2.0', id=1, method=method, params=params))
            if result.get('error') or 'result' not in result:
                raise RuntimeError('Bitcoin RPC method unsupported')
            return result['result']
        if call('getblockhash', [0]) != GENESIS:
            raise RuntimeError('wrong Bitcoin genesis')
        height = call('getblockcount', [])
        block = call('getblock', [call('getblockhash', [height]), 3])
        self.validate_block(block, height)

    def tip(self):
        latest = self.rpc.rpc('getblockcount', [], result_validator=lambda v: type(v) is int and v >= 0)
        return latest if self.config.get('scan_unconfirmed') else max(0, latest-self.finality_blocks)

    def safe_tip(self):
        latest = self.rpc.rpc('getblockcount', [], result_validator=lambda v: type(v) is int and v >= 0)
        return max(0, latest-self.finality_blocks)

    def block_header(self, block, height):
        if not isinstance(block, dict) or block.get('height') != height:
            raise ChainMismatch('wrong BTC block height')
        return digest(block.get('hash'), False), digest(block.get('previousblockhash', '0'*64 if height == 0 else ''), False)

    def header(self, height):
        block_hash = self.rpc.rpc('getblockhash', [height])
        block = self.rpc.rpc('getblockheader', [block_hash, True])
        current, parent = self.block_header(block, height)
        if current != block_hash:
            raise ChainMismatch('BTC header identity mismatch')
        return current, parent

    def validate_block(self, block, height):
        self.block_header(block, height)
        txs = block.get('tx')
        if not isinstance(txs, list) or not txs or block.get('nTx') != len(txs):
            raise ChainMismatch('incomplete BTC block')
        ids = [digest(tx.get('txid'), False) for tx in txs]
        if len(set(ids)) != len(ids):
            raise ChainMismatch('duplicate BTC transaction')
        for tx in txs:
            outputs(tx)
        return True

    def block(self, height):
        block_hash = self.rpc.rpc('getblockhash', [height])
        block = self.rpc.rpc('getblock', [block_hash, 3], result_validator=lambda b: self.validate_block(b, height))
        if block['hash'] != block_hash:
            raise ChainMismatch('BTC block identity mismatch')
        return block

    def heights(self, start, end):
        return range(start, end+1)

    def parent_height(self, block, height):
        return height-1

    def transactions(self, block):
        return [(tx['txid'], tx) for tx in block['tx']]

    def addresses(self, tx):
        incoming, outgoing = outputs(tx)
        return {a for a, _ in incoming + outgoing}

    def events(self, block, height, txid, tx, watched, metadata):
        incoming, outgoing = outputs(tx)
        net = defaultdict(int)
        for address, value in incoming:
            net[address] -= value
        for address, value in outgoing:
            net[address] += value
        found = []
        for address in sorted(watched & net.keys()):
            value = net[address]
            if not value:
                continue
            peers = {a for a, _ in (incoming if value > 0 else outgoing) if a != address}
            found.append(Event(self.name, txid, height, block['hash'], address, 'in' if value > 0 else 'out',
                               'native', self.config.get('native_symbol', 'BTC'), abs(value), 8,
                               next(iter(peers)) if len(peers) == 1 else '', -1,
                               block_timestamp=int(block['time'])))
        return found
