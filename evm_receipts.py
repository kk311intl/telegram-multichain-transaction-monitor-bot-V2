"""Batch-local receipt cache with endpoint capability fallback and identity checks."""
from block_validation import ChainMismatch, digest


class BlockReceipts:
    def __init__(self, rpc, blocks):
        self.rpc, self.blocks = rpc, blocks
        self.heights = {digest(tx['hash']):height for height,block in blocks.items() for tx in block['transactions']}
        self.loaded = set()
        self.receipts = {}

    def _validate(self, height, receipts):
        if not isinstance(receipts,list):
            raise ChainMismatch('invalid block receipts')
        block = self.blocks[height]
        expected = {digest(tx['hash']) for tx in block['transactions']}
        found = set()
        for receipt in receipts:
            if not isinstance(receipt,dict):
                raise ChainMismatch('invalid receipt object')
            txid = digest(receipt.get('transactionHash'))
            if (txid in found or txid not in expected or digest(receipt.get('blockHash')) != digest(block['hash'])
                    or int(receipt.get('blockNumber','-1'),16) != height or not isinstance(receipt.get('logs'),list)):
                raise ChainMismatch('block receipt identity mismatch')
            found.add(txid)
        if found != expected:
            raise ChainMismatch('incomplete block receipts')
        return True

    def __call__(self, txid):
        txid = digest(txid)
        height = self.heights[txid]
        if height not in self.loaded:
            receipts = self.rpc.optional_rpc('eth_getBlockReceipts',[hex(height)],
                                             result_validator=lambda r:self._validate(height,r))
            if receipts is not None:
                # Retain only the current block; the validator owns receipts it
                # already used. Unrelated receipts from earlier blocks can go.
                self.receipts = {digest(r['transactionHash']):r for r in receipts}
            self.loaded.add(height)
        if txid not in self.receipts:
            self.receipts[txid] = self.rpc.rpc('eth_getTransactionReceipt',[txid])
        return self.receipts[txid]
