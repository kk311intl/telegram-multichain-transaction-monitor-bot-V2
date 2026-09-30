"""Structural and cross-response checks; public RPC is still the trust root."""
import re
from monitor.rpc import RpcValidationError


class ChainMismatch(RpcValidationError):
    pass


def digest(value, evm=True):
    if not isinstance(value, str) or not re.fullmatch(r'0x[0-9a-fA-F]{64}' if evm else r'[0-9a-fA-F]{64}', value):
        raise ChainMismatch('invalid block hash')
    return value.lower()


def header(adapter, height):
    if adapter.config['type'] in ('bitcoin', 'solana'):
        return adapter.header(height)
    if adapter.config['type'] == 'evm':
        block = adapter.rpc.rpc('eth_getBlockByNumber', [hex(height), False], result_validator=lambda b: bool(evm_header(b, height)))
        return evm_header(block, height)
    block = adapter.rpc.post_validated({'id_or_num': str(height), 'detail':False}, getattr(adapter,'block_prefix','/walletsolidity')+'/getblock', lambda b: bool(tron_header(b,height)))
    return tron_header(block, height)


def evm_header(block, height):
    if not isinstance(block, dict) or int(block.get('number', '-1'), 16) != height:
        raise ChainMismatch('wrong EVM block height')
    return digest(block.get('hash')), digest(block.get('parentHash'))


def tron_header(block, height):
    raw = block.get('block_header', {}).get('raw_data', {}) if isinstance(block, dict) else {}
    if raw.get('number', 0 if height == 0 else -1) != height:
        raise ChainMismatch('wrong TRON block height')
    value = digest(block.get('blockID'), False)
    if int(value[:16], 16) != height:
        raise ChainMismatch('TRON blockID height mismatch')
    return value, digest(raw.get('parentHash'), False)


def check_evm_logs(blocks, logs, receipt_lookup=None, reconcile_receipts=False):
    if not isinstance(logs, list):
        raise ChainMismatch('invalid logs')
    seen = set()
    receipts = {}
    originals = set()
    for log in logs:
        if not isinstance(log, dict) or log.get('removed', False):
            raise ChainMismatch('removed or invalid log')
        height = int(log.get('blockNumber', '-1'), 16)
        block = blocks.get(height)
        if block is None or digest(log.get('blockHash')) != digest(block['hash']):
            raise ChainMismatch('log/block hash mismatch')
        txid = digest(log.get('transactionHash'))
        # HyperEVM providers may number receipt logs per transaction. The
        # receipt proves identity; equal indices in different transactions
        # must not consume one another's matching log.
        scope = (height, txid) if reconcile_receipts else (height,)
        tx_index = int(log.get('transactionIndex', '-1'), 16)
        txs = block['transactions']
        original = (height, txid, log.get('logIndex'))
        if original in originals:
            raise ChainMismatch('duplicate source log')
        originals.add(original)
        if reconcile_receipts or not 0 <= tx_index < len(txs) or txs[tx_index].get('hash', '').lower() != txid:
            if receipt_lookup is None or txid not in {tx.get('hash', '').lower() for tx in txs}:
                raise ChainMismatch(f'log transaction mismatch at {height}')
            if txid not in receipts:
                receipts[txid] = receipt_lookup(txid)
            receipt = receipts[txid]
            if (not isinstance(receipt, dict) or receipt.get('transactionHash', '').lower() != txid
                    or digest(receipt.get('blockHash')) != digest(block['hash'])
                    or int(receipt.get('blockNumber', '-1'), 16) != height):
                raise ChainMismatch('receipt identity mismatch')
            fields = ('transactionHash', 'blockHash', 'address', 'data', 'topics')
            candidates = [event for event in receipt.get('logs', []) if all(event.get(key) == log.get(key) for key in fields)
                          and not event.get('removed', False) and (*scope, int(event.get('logIndex','-1'),16)) not in seen]
            if not candidates:
                raise ChainMismatch('receipt does not confirm mismatched-index log')
            matched = min(candidates, key=lambda e:int(e['logIndex'],16))
            log['logIndex'] = matched['logIndex']
            log['transactionIndex'] = hex(next(i for i,tx in enumerate(txs) if tx.get('hash','').lower()==txid))
        key = (*scope, int(log.get('logIndex', '-1'), 16))
        if key[-1] < 0 or key in seen:
            raise ChainMismatch('duplicate or invalid log index')
        seen.add(key)
