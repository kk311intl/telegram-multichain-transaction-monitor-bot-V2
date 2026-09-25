"""Solana confirmed/finalized blocks and standard parsed transfer instructions."""
from block_validation import ChainMismatch
from bot_runtime.common import Event
from chain_identity import base58
from monitor.rpc import JsonClient, rpc_urls

GENESIS = '5eykt4UsFv8P8NJdTREpY1vzqKqZKvdpKuc147dw2N9d'
SYSTEM = '11111111111111111111111111111111'
TOKEN_PROGRAMS = {'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA',
                  'TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb'}


def amount(value):
    if isinstance(value, bool) or not isinstance(value, (int, str)) or not str(value).isdigit():
        raise ChainMismatch('invalid Solana transfer amount')
    result = int(value)
    if not 0 <= result < 2**64:
        raise ChainMismatch('invalid Solana transfer amount')
    return result


def account_keys(tx):
    keys = tx['transaction']['message']['accountKeys']
    return [base58(k['pubkey'] if isinstance(k, dict) else k, 32) for k in keys]


def instructions(tx):
    outer = tx['transaction']['message']['instructions']
    inner = {}
    for group in tx['meta'].get('innerInstructions') or []:
        index = group['index']
        if type(index) is not int or not 0 <= index < len(outer) or index in inner or not isinstance(group['instructions'],list):
            raise ChainMismatch('invalid inner instruction index')
        inner[index] = group['instructions']
    result = []
    for index, instruction in enumerate(outer):
        result.extend([instruction, *inner.get(index,[])])
    if any(not isinstance(i,dict) for i in result):
        raise ChainMismatch('invalid instruction')
    return result


def initialized_accounts(tx):
    for instruction in instructions(tx):
        parsed = instruction.get('parsed')
        if (instruction.get('programId') in TOKEN_PROGRAMS and isinstance(parsed, dict)
                and parsed.get('type') in ('initializeAccount','initializeAccount2','initializeAccount3')):
            info = parsed['info']
            yield base58(info['account'],32), (base58(info['mint'],32),base58(info['owner'],32))


def token_accounts(tx):
    keys = account_keys(tx)
    result = {}
    for balance in (tx['meta'].get('preTokenBalances') or []) + (tx['meta'].get('postTokenBalances') or []):
        index = balance['accountIndex']
        if type(index) is not int or not 0 <= index < len(keys):
            raise ChainMismatch('invalid token account index')
        mint = base58(balance['mint'], 32)
        owner = base58(balance['owner'], 32) if balance.get('owner') else None
        value = (mint, owner)
        if keys[index] in result and result[keys[index]] != value:
            raise ChainMismatch('token owner or mint changed within transaction')
        result[keys[index]] = value
    # Accounts created and closed within one transaction have no pre/post
    # balance row. Their successful Token Program initialization is evidence.
    for account, value in initialized_accounts(tx):
        if account in result and result[account] != value:
            raise ChainMismatch('initialized token account conflicts with balances')
        result[account] = value
    return result


class SolanaAdapter:
    def __init__(self, name, config):
        self.name, self.config = name, config
        self.finality_blocks = max(0, int(config['finality_blocks']))
        self.commitment = 'confirmed' if config.get('scan_unconfirmed') else 'finalized'
        self.version = int(config.get('max_supported_transaction_version', 1))
        if not 0 <= self.version <= 127:
            raise ValueError('invalid Solana transaction version')
        self.rpc = JsonClient(rpc_urls(config), timeout=max(3, int(config.get('rpc_timeout', 20))),
                              max_response_bytes=int(config.get('max_response_mib', 32))*1024*1024)
        self.rpc.set_endpoint_validator(self._validate, f'solana-parsed-1:{GENESIS}:{self.version}:{self.commitment}')

    def options(self, details='full'):
        return dict(encoding='jsonParsed', transactionDetails=details, rewards=False,
                    commitment=self.commitment, maxSupportedTransactionVersion=self.version)

    def _validate(self, url):
        def call(method, params):
            result = self.rpc._post_url(url, dict(jsonrpc='2.0', id=1, method=method, params=params))
            if result.get('error') or 'result' not in result:
                raise RuntimeError('Solana RPC method unsupported')
            return result['result']
        if call('getGenesisHash', []) != GENESIS:
            raise RuntimeError('wrong Solana genesis')
        slot = call('getSlot', [dict(commitment=self.commitment)])
        slots = call('getBlocks', [max(0, slot-64), slot, dict(commitment=self.commitment)])
        if not slots:
            raise RuntimeError('no recent Solana blocks')
        self.validate_block(call('getBlock', [slots[-1], self.options()]), slots[-1])

    def _tip(self, commitment, offset=0):
        slot = self.rpc.rpc('getSlot', [dict(commitment=commitment)],
                            result_validator=lambda v: type(v) is int and v >= 0)
        end = max(0, slot-offset)
        slots = self.rpc.rpc('getBlocks', [max(0, end-128), end, dict(commitment=commitment)])
        self.validate_heights(slots, max(0, end-128), end)
        if not slots:
            raise RuntimeError('no produced Solana block near head')
        return slots[-1]

    def tip(self):
        return self._tip(self.commitment, 0 if self.config.get('scan_unconfirmed') else self.finality_blocks)

    def safe_tip(self):
        return self._tip('finalized', self.finality_blocks)

    def block_header(self, block, height):
        if not isinstance(block, dict) or type(block.get('parentSlot')) is not int or not 0 <= block['parentSlot'] < height:
            raise ChainMismatch('invalid Solana block header')
        try:
            return base58(block['blockhash'], 32), base58(block['previousBlockhash'], 32)
        except (ValueError, KeyError) as exc:
            raise ChainMismatch('invalid Solana block hash') from exc

    def header(self, height):
        slots = self.rpc.rpc('getBlocksWithLimit', [height, 1, dict(commitment=self.commitment)])
        self.validate_heights(slots, height, 10**12-1)
        if not slots:
            raise ChainMismatch('Solana canonical slot unavailable')
        actual = slots[0]
        block = self.rpc.rpc('getBlock', [actual, self.options('none')])
        result = self.block_header(block, actual)
        if actual == height:
            return result
        if block['parentSlot'] < height:
            # The successor proves this slot is absent on the current fork.
            return '', result[1]
        raise ChainMismatch('Solana RPC omitted a produced slot')

    def validate_block(self, block, height):
        self.block_header(block, height)
        if not isinstance(block.get('transactions'), list):
            raise ChainMismatch('full Solana transactions required')
        ids = []
        for tx in block['transactions']:
            ids.append(base58(tx['transaction']['signatures'][0], 64))
            if not isinstance(tx.get('meta'), dict) or 'err' not in tx['meta']:
                raise ChainMismatch('Solana transaction metadata required')
            account_keys(tx)
            if not isinstance(tx['transaction']['message'].get('instructions'), list):
                raise ChainMismatch('Solana instructions required')
            instructions(tx)
        if len(ids) != len(set(ids)):
            raise ChainMismatch('duplicate Solana signature')
        return True

    def block(self, height):
        return self.rpc.rpc('getBlock', [height, self.options()], result_validator=lambda b: self.validate_block(b, height))

    def validate_heights(self, slots, start, end):
        if (not isinstance(slots, list) or any(type(s) is not int or not start <= s <= end for s in slots)
                or slots != sorted(set(slots))):
            raise ChainMismatch('invalid produced slot list')

    def batch_end(self, start, end, head):
        # A span of one must also advance across skipped slots.
        slots = self.rpc.rpc('getBlocksWithLimit', [start, 1, dict(commitment=self.commitment)])
        self.validate_heights(slots, start, 10**12-1)
        if not slots or slots[0] > head:
            raise ChainMismatch('Solana head/slot list mismatch')
        return max(end, slots[0])

    def heights(self, start, end):
        slots = self.rpc.rpc('getBlocks', [start, end, dict(commitment=self.commitment)])
        self.validate_heights(slots, start, end)
        if not slots:
            raise ChainMismatch('no produced slots in batch')
        return slots

    def parent_height(self, block, height):
        return block['parentSlot']

    def transactions(self, block):
        return [(tx['transaction']['signatures'][0], tx) for tx in block['transactions']]

    def addresses(self, tx):
        if tx['meta']['err'] is not None:
            return set()
        balances = (tx['meta'].get('preTokenBalances') or []) + (tx['meta'].get('postTokenBalances') or [])
        return (set(account_keys(tx)) | {base58(b['owner'],32) for b in balances if b.get('owner')}
                | {owner for _,(_,owner) in initialized_accounts(tx)})

    def token_metadata(self, asset):
        data = self.rpc.rpc('getAccountInfo', [base58(asset, 32), dict(encoding='jsonParsed', commitment='confirmed')])['value']
        if not data or data.get('owner') not in TOKEN_PROGRAMS:
            raise ValueError('not a supported SPL mint')
        parsed = data['data']['parsed']
        decimals = parsed['info']['decimals']
        if parsed.get('type') != 'mint' or type(decimals) is not int or not 0 <= decimals <= 255:
            raise ValueError('invalid SPL mint metadata')
        # Mint accounts have no authoritative ticker; preserve their identity.
        return 'SPL '+asset[:6], decimals

    def events(self, block, height, txid, tx, watched, metadata):
        if tx['meta']['err'] is not None:
            return []
        accounts = token_accounts(tx)
        found = []
        for index, instruction in enumerate(instructions(tx)):
            parsed = instruction.get('parsed')
            if not isinstance(parsed, dict):
                continue  # Raw/program-specific instructions are outside this decoder's scope.
            kind, info, program = parsed.get('type'), parsed.get('info', {}), instruction.get('programId')
            if program == SYSTEM and kind in ('transfer', 'transferWithSeed'):
                sender, recipient = base58(info['source'], 32), base58(info['destination'], 32)
                asset, raw, decimals, symbol, complete = 'native', amount(info['lamports']), 9, self.config.get('native_symbol', 'SOL'), True
                sources, recipients = {sender}, {recipient}
            elif program in TOKEN_PROGRAMS and kind in ('transfer', 'transferChecked'):
                sender, recipient = base58(info['source'], 32), base58(info['destination'], 32)
                source, target = accounts.get(sender), accounts.get(recipient)
                if not source or not target or source[0] != target[0]:
                    raise ChainMismatch('SPL transfer missing consistent balance metadata')
                asset = source[0]
                if 'mint' in info and info['mint'] != asset:
                    raise ChainMismatch('SPL mint mismatch')
                sources, recipients = {sender, source[1]} - {None}, {recipient, target[1]} - {None}
                raw = amount(info['tokenAmount']['amount'] if kind == 'transferChecked' else info['amount'])
                if not ((sources | recipients) & watched):
                    continue
                symbol, decimals, complete = metadata(asset)
                if complete and kind == 'transferChecked' and info['tokenAmount']['decimals'] != decimals:
                    raise ChainMismatch('SPL decimals mismatch')
                sender, recipient = source[1] or sender, target[1] or recipient
            else:
                continue
            if not raw:
                continue
            for addresses, direction, peer in ((sources, 'out', recipient), (recipients, 'in', sender)):
                for address in sorted(addresses & watched):
                    found.append(Event(self.name, txid, height, block['blockhash'], address, direction, asset,
                                       symbol, raw, decimals, peer, index, block_timestamp=int(block.get('blockTime') or 0),
                                       metadata_complete=complete))
        return found
