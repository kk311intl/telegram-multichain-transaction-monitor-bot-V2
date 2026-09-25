from __future__ import annotations

import hashlib
from dataclasses import dataclass
from decimal import Decimal


TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"


@dataclass(frozen=True)
class Event:
    chain: str
    txid: str
    block_height: int
    block_hash: str
    address: str
    direction: str
    asset_id: str
    symbol: str
    amount_raw: int
    decimals: int
    counterparty: str
    log_index: int = -1
    source_risk: str = ""
    block_timestamp: int = 0
    metadata_complete: bool = True

    @property
    def event_id(self) -> str:
        raw = f"{self.chain}|{self.txid}|{self.log_index}|{self.address}|{self.direction}|{self.asset_id}"
        return hashlib.sha256(raw.encode()).hexdigest()

    @property
    def amount(self) -> Decimal:
        return Decimal(self.amount_raw) / (Decimal(10) ** self.decimals)


def hex_int(value: str | None) -> int:
    return int("0x0" if value in {None, "", "0x"} else value, 16)


def normalize_evm(address: str) -> str:
    value = address.strip().lower()
    if not value.startswith("0x") or len(value) != 42:
        raise ValueError("EVM 地址必須是 0x 開頭的 20-byte 十六進位地址")
    if any(char not in "0123456789abcdef" for char in value[2:]):
        raise ValueError("EVM 地址含有非十六進位字元")
    return value


_B58 = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"


def b58decode_check(value: str) -> bytes:
    number = 0
    for char in value:
        if char not in _B58:
            raise ValueError("invalid Base58 character")
        number = number * 58 + _B58.index(char)
    raw = number.to_bytes((number.bit_length() + 7) // 8, "big")
    raw = b"\0" * (len(value) - len(value.lstrip("1"))) + raw
    if len(raw) < 5:
        raise ValueError("invalid Base58Check value")
    payload, checksum = raw[:-4], raw[-4:]
    expected = hashlib.sha256(hashlib.sha256(payload).digest()).digest()[:4]
    if checksum != expected:
        raise ValueError("invalid Base58Check checksum")
    return payload


def b58encode_check(payload: bytes) -> str:
    raw = payload + hashlib.sha256(hashlib.sha256(payload).digest()).digest()[:4]
    number = int.from_bytes(raw, "big")
    chars = ""
    while number:
        number, remainder = divmod(number, 58)
        chars = _B58[remainder] + chars
    return "1" * (len(raw) - len(raw.lstrip(b"\0"))) + chars


def tron_to_hex(address: str) -> str:
    try:
        payload = b58decode_check(address.strip())
    except ValueError as exc:
        raise ValueError("TRON 地址的 Base58Check 校驗碼無效") from exc
    if len(payload) != 21 or payload[0] != 0x41:
        raise ValueError("必須輸入合法的 TRON 主網地址")
    return payload.hex()


def tron_from_hex(value: str) -> str:
    raw = bytes.fromhex(value.removeprefix("0x"))
    if len(raw) == 20:
        raw = b"\x41" + raw
    if len(raw) != 21 or raw[0] != 0x41:
        raise ValueError("invalid TRON hex address")
    return b58encode_check(raw)
