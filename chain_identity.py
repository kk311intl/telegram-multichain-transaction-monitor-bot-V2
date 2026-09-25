"""Chain-specific address and wire identity validation."""
import re

from bot_runtime.common import b58decode_check, normalize_evm, tron_to_hex

BASE58 = '123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz'
BECH32 = 'qpzry9x8gf2tvdw0s3jn54khce6mua7l'


def base58(value, size):
    if not isinstance(value, str) or not 1 <= len(value) <= 88:
        raise ValueError('invalid Base58 length')
    number = 0
    for char in value:
        if char not in BASE58:
            raise ValueError('invalid Base58 character')
        number = number * 58 + BASE58.index(char)
    raw = b'\0' * (len(value) - len(value.lstrip('1'))) + number.to_bytes((number.bit_length()+7)//8, 'big')
    if len(raw) != size:
        raise ValueError('invalid Base58 decoded length')
    return value


def bitcoin_address(value):
    value = value.strip()
    if not 14 <= len(value) <= 90:
        raise ValueError('BTC 地址長度無效')
    if not value.lower().startswith('bc1'):
        payload = b58decode_check(value)
        if len(payload) != 21 or payload[0] not in (0, 5):
            raise ValueError('必須輸入 BTC 主網地址')
        return value
    if value != value.lower() and value != value.upper():
        raise ValueError('Bech32 地址不可混合大小寫')
    value = value.lower()
    try:
        data = [BECH32.index(c) for c in value[3:]]
    except ValueError as exc:
        raise ValueError('invalid Bech32 character') from exc
    if len(data) < 7 or data[0] > 16:
        raise ValueError('invalid witness version')
    checksum = 1
    for item in [3, 3, 0, 2, 3, *data]:  # HRP expansion of "bc".
        top = checksum >> 25
        checksum = ((checksum & 0x1ffffff) << 5) ^ item
        for bit, generator in enumerate((0x3b6a57b2, 0x26508e6d, 0x1ea119fa, 0x3d4233dd, 0x2a1462b3)):
            if (top >> bit) & 1:
                checksum ^= generator
    if checksum != (1 if data[0] == 0 else 0x2bc830a3):
        raise ValueError('BTC 地址校驗碼無效')
    accumulator = bits = 0
    program = []
    for item in data[1:-6]:
        accumulator = (accumulator << 5) | item
        bits += 5
        while bits >= 8:
            bits -= 8
            program.append((accumulator >> bits) & 255)
    if bits >= 5 or ((accumulator << (8-bits)) & 255):
        raise ValueError('invalid witness padding')
    if not 2 <= len(program) <= 40 or (data[0] == 0 and len(program) not in (20, 32)):
        raise ValueError('invalid witness program length')
    return value


def normalize_address(kind, address):
    if kind == 'evm':
        return normalize_evm(address)
    if kind == 'tron':
        tron_to_hex(address)
        return address.strip()
    if kind == 'bitcoin':
        return bitcoin_address(address)
    if kind == 'solana':
        return base58(address.strip(), 32)
    raise ValueError('unsupported chain type')


def chain_kind(name, kinds=None):
    return (kinds or {}).get(name, name if name in ('tron', 'bitcoin', 'solana') else 'evm')


def valid_identity(value, kind, transaction=False):
    if kind == 'solana':
        try:
            base58(value, 64 if transaction else 32)
            return True
        except ValueError:
            return False
    return isinstance(value, str) and bool(re.fullmatch(('0x' if kind == 'evm' else '') + '[0-9a-f]{64}', value))
