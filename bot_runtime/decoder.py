"""Independently verify matching transactions and decode fungible transfers."""
from .common import Event, TRANSFER_TOPIC, normalize_evm, tron_from_hex, tron_to_hex
from block_validation import evm_header, tron_header, header
from . import lookup_retry


def abi_text(raw):
    data = bytes.fromhex(raw.removeprefix('0x'))
    if len(data) >= 64:
        offset = int.from_bytes(data[:32], 'big')
        if offset + 32 > len(data):
            raise ValueError('invalid ABI offset')
        length = int.from_bytes(data[offset:offset+32], 'big')
        if length > 256 or offset+32+length > len(data):
            raise ValueError('invalid ABI string')
        data = data[offset+32:offset+32+length]
    text = data.rstrip(b'\0').decode('utf-8')
    if not text or len(text) > 64 or any(ord(c) < 32 for c in text):
        raise ValueError('invalid token symbol')
    return text


def metadata(adapter, store, asset):
    row = store.db.execute('SELECT symbol,decimals FROM token_metadata WHERE chain=? AND asset=?', (adapter.name,asset)).fetchone()
    if row:
        return row['symbol'], row['decimals'], True
    if not lookup_retry.ready(store, 'metadata', adapter.name, asset):
        return 'UNKNOWN',0,False
    try:
        def call(selector, signature):
            if adapter.config['type'] == 'evm':
                return adapter.rpc.rpc('eth_call', [{'to':asset,'data':selector},'latest'])
            result = adapter.rpc.post({'owner_address':tron_to_hex(asset), 'contract_address':tron_to_hex(asset),
                                      'function_selector':signature, 'parameter':''}, '/wallet/triggerconstantcontract')
            return result['constant_result'][0]
        decimals = int(call('0x313ce567','decimals()').removeprefix('0x'),16)
        symbol = abi_text(call('0x95d89b41','symbol()'))
        if not 0 <= decimals <= 255:
            raise ValueError('invalid decimals')
        with store.db:
            store.db.execute('INSERT OR REPLACE INTO token_metadata VALUES(?,?,?,?)', (adapter.name,asset,symbol,decimals))
        lookup_retry.clear(store, 'metadata', adapter.name, asset)
        return symbol,decimals,True
    except Exception:
        lookup_retry.failed(store, 'metadata', adapter.name, asset)
        return 'UNKNOWN',0,False


def decode(adapter, store, hit, watched, cache=None, metadata_lookup=None):
    rpc, height, txid = adapter.rpc, hit['height'], hit['txid']
    evm = adapter.config['type'] == 'evm'
    found = []
    def remember(key, loader, validator, ttl=300):
        def checked():
            value=loader()
            if not validator(value):raise ValueError('cached RPC identity mismatch')
            return value
        return cache.get((adapter.name,*key),checked,ttl) if cache else checked()

    def transfer(sender, recipient, asset, raw, index, timestamp):
        sides = [(recipient,'in',sender),(sender,'out',recipient)]
        sides = [s for s in sides if s[0] in watched]
        if not sides:
            return
        symbol,decimals,complete = (adapter.config.get('native_symbol','ETH' if evm else 'TRX'),
                                   int(adapter.config.get('native_decimals',18 if evm else 6)),True) if asset == 'native' else (metadata_lookup or metadata)(adapter,store,asset)
        for address,direction,counterparty in sides:
            found.append(Event(adapter.name,txid,height,hit['hash'],address,direction,asset,symbol,raw,
                               decimals,counterparty,index,block_timestamp=timestamp,metadata_complete=complete))

    if evm:
        block = remember(('evm-block',height),lambda:rpc.rpc('eth_getBlockByNumber',[hex(height),False]),lambda b:bool(evm_header(b,height)),ttl=2)
        digest,_ = evm_header(block,height)
        if digest != hit['hash']:
            return []  # A replaced block must never produce a notification.
        if txid not in [str(t).lower() for t in block['transactions']]:
            raise ValueError('transaction absent from canonical block')
        tx = remember(('tx',digest,txid),lambda:rpc.rpc('eth_getTransactionByHash',[txid]),lambda t:isinstance(t,dict) and t.get('hash','').lower()==txid and t.get('blockHash','').lower()==digest)
        receipt = remember(('receipt',digest,txid),lambda:rpc.rpc('eth_getTransactionReceipt',[txid]),lambda r:isinstance(r,dict) and r.get('transactionHash','').lower()==txid and r.get('blockHash','').lower()==digest and int(r.get('blockNumber','-1'),16)==height)
        if tx['hash'].lower() != txid or tx['blockHash'].lower() != digest or receipt['transactionHash'].lower() != txid or receipt['blockHash'].lower() != digest or int(receipt['blockNumber'],16) != height:
            raise ValueError('transaction identity mismatch')
        if int(receipt['status'],16) != 1:
            return []
        timestamp = int(block['timestamp'],16)
        if tx.get('to') and int(tx.get('value','0x0'),16) > 0:
            transfer(normalize_evm(tx['from']),normalize_evm(tx['to']),'native',int(tx['value'],16),-1,timestamp)
        for log in receipt.get('logs',[]):
            topics = log.get('topics',[])
            if len(topics) != 3 or topics[0].lower() != TRANSFER_TOPIC:
                continue
            if log.get('removed') or log['blockHash'].lower() != digest or log['transactionHash'].lower() != txid:
                raise ValueError('receipt log identity mismatch')
            transfer(normalize_evm('0x'+topics[1][-40:]),normalize_evm('0x'+topics[2][-40:]),
                     normalize_evm(log['address']),int(log['data'],16),int(log['logIndex'],16),timestamp)
    else:
        prefix=getattr(adapter,'block_prefix','/walletsolidity')
        current=cache.get((adapter.name,'header',height),lambda:header(adapter,height),ttl=2)[0] if cache else hit['hash']
        if current!=hit['hash']:return []
        block = remember(('tron-block',hit['hash']),lambda:rpc.post({'num':height},prefix+'/getblockbynum'),lambda b:tron_header(b,height)[0]==hit['hash'])
        digest,_ = tron_header(block,height)
        if digest != hit['hash']:
            return []
        tx = next((t for t in block.get('transactions',[]) if t['txID'].lower() == txid),None)
        if tx is None:
            raise ValueError('transaction absent from canonical block')
        info = remember(('tron-info',digest,txid),lambda:rpc.post({'value':txid},prefix+'/gettransactioninfobyid'),lambda i:isinstance(i,dict) and i.get('id','').lower()==txid and i.get('blockNumber')==height)
        if info.get('id','').lower() != txid or info.get('blockNumber') != height:
            raise ValueError('TRON transaction identity mismatch')
        if any(r.get('contractRet') != 'SUCCESS' for r in tx.get('ret',[])) or info.get('receipt',{}).get('result','SUCCESS') != 'SUCCESS':
            return []
        timestamp = int(block['block_header']['raw_data']['timestamp'])//1000
        for index,contract in enumerate(tx.get('raw_data',{}).get('contract',[])):
            if contract.get('type') == 'TransferContract':
                value = contract['parameter']['value']
                transfer(tron_from_hex(value['owner_address']),tron_from_hex(value['to_address']),
                         'native',int(value['amount']),-index-1,timestamp)
        for index,log in enumerate(info.get('log',[])):
            topics = log.get('topics',[])
            if len(topics) == 3 and topics[0].lower().removeprefix('0x') == TRANSFER_TOPIC[2:]:
                transfer(tron_from_hex(topics[1][-40:]),tron_from_hex(topics[2][-40:]),tron_from_hex(log['address']),
                         int(log['data'] or '0',16),index,timestamp)
    return found
