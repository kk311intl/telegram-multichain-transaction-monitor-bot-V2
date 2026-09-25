from __future__ import annotations

from typing import Any
import json
import time
from urllib.parse import urlsplit

from monitor.rpc import JsonClient
from block_validation import evm_header, tron_header


TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"


def _rpc_urls(config: dict[str, Any]) -> list[str]:
    urls = list(dict.fromkeys(config.get("rpc_urls", [])))
    if not urls or any(urlsplit(url).scheme != "https" or not urlsplit(url).hostname for url in urls):
        raise ValueError("rpc_urls must contain valid HTTPS endpoints")
    return urls


class EvmAdapter:
    def __init__(self, name: str, config: dict[str, Any]):
        self.name = name
        self.config = config
        self.expected_chain_id = int(config["expected_chain_id"])
        self.finality_blocks = max(0, int(config["finality_blocks"]))
        self.rpc = JsonClient(_rpc_urls(config), timeout=max(3, int(config.get("rpc_timeout", 12))))
        self.expected_genesis_hash = config.get('expected_genesis_hash','').lower()
        optional = config.get('genesis_optional_rpc_urls', [])
        if not isinstance(optional, list) or any(not isinstance(url, str) or url not in self.rpc.urls for url in optional):
            raise ValueError('genesis_optional_rpc_urls must be a subset of rpc_urls')
        self.genesis_optional_rpc_urls = frozenset(optional)
        identity=f'evm-full-logs-1:{self.expected_chain_id}:{self.finality_blocks}'
        if optional:
            identity += ':genesis-optional:' + json.dumps(sorted(self.genesis_optional_rpc_urls))
        self.rpc.set_endpoint_validator(self._validate, identity+(':'+self.expected_genesis_hash if self.expected_genesis_hash else ''))

    def _validate(self, url: str) -> None:
        chain = self.rpc._post_url(url, {
            "jsonrpc": "2.0", "id": "identity", "method": "eth_chainId", "params": [],
        })
        if (
            not isinstance(chain, dict) or chain.get("error")
            or int(chain.get("result", "0x0"), 16) != self.expected_chain_id
        ):
            raise RuntimeError("wrong EVM chain identity")
        if self.expected_genesis_hash and url not in self.genesis_optional_rpc_urls:
            genesis=self.rpc._post_url(url,{'jsonrpc':'2.0','id':'genesis','method':'eth_getBlockByNumber','params':['0x0',False]})
            if str((genesis.get('result') or {}).get('hash','')).lower()!=self.expected_genesis_hash:
                raise RuntimeError('wrong EVM genesis')
        if url in self.genesis_optional_rpc_urls:
            tip = self.rpc._post_url(url, {"jsonrpc":"2.0","id":"tip","method":"eth_getBlockByNumber","params":["latest", False]})
            if not self._valid_tip(tip.get('result')):
                raise RuntimeError('stale EVM chain head')
            latest = int(tip['result']['number'], 16)
        else:
            tip = self.rpc._post_url(url, {"jsonrpc":"2.0","id":"tip","method":"eth_blockNumber","params":[]})
            latest = int(tip['result'], 16)
        height = max(0, latest - self.finality_blocks)
        full = self.rpc._post_url(url, {"jsonrpc":"2.0","id":"block","method":"eth_getBlockByNumber","params":[hex(height), True]})
        evm_header(full.get("result"), height)
        if not isinstance(full["result"].get("transactions"), list):
            raise RuntimeError("full block unsupported")
        logs = self.rpc._post_url(url, {
            "jsonrpc": "2.0", "id": "logs", "method": "eth_getLogs",
            "params": [{"fromBlock": hex(height), "toBlock": hex(height), "topics": [TRANSFER_TOPIC]}],
        })
        if not isinstance(logs, dict) or logs.get("error") or not isinstance(logs.get("result"), list):
            raise RuntimeError("unfiltered Transfer logs unsupported")

    def _valid_tip(self, block):
        height=int(block['number'],16)
        evm_header(block,height)
        age=time.time()-int(block['timestamp'],16)
        return -120 <= age <= 180 and height >= getattr(self,'_highest_tip',0)-128

    def _latest(self):
        block=self.rpc.rpc('eth_getBlockByNumber',['latest',False],result_validator=self._valid_tip)
        latest=int(block['number'],16)
        self._highest_tip=max(latest,getattr(self,'_highest_tip',0))
        return latest

    def tip(self) -> int:
        latest=self._latest()
        return latest if self.config.get('scan_unconfirmed') else max(0,latest-self.finality_blocks)

    def safe_tip(self) -> int:
        return max(0,self._latest()-self.finality_blocks)


class TronAdapter:
    def __init__(self, name: str, config: dict[str, Any]):
        self.name = name
        self.config = config
        self.block_prefix = '/wallet' if config.get('scan_unconfirmed') else '/walletsolidity'
        self.expected_genesis_hash = str(config["expected_genesis_hash"]).lower()
        self.finality_blocks = max(0, int(config["finality_blocks"]))
        self.rpc = JsonClient(_rpc_urls(config), timeout=max(3, int(config.get("rpc_timeout", 12))))
        self.rpc.set_endpoint_validator(self._validate, f'tron-infos-1:{self.expected_genesis_hash}:{self.block_prefix}')

    def _validate(self, url: str) -> None:
        genesis = self.rpc._post_url(url, {"num": 0}, "/walletsolidity/getblockbynum")
        if not isinstance(genesis, dict) or str(genesis.get("blockID", "")).lower() != self.expected_genesis_hash:
            raise RuntimeError("wrong TRON genesis")
        block = self.rpc._post_url(url, {"detail":False}, self.block_prefix+"/getblock")
        height = block["block_header"]["raw_data"]["number"]
        tron_header(block, height)
        infos = self.rpc._post_url(url, {"num": height}, self.block_prefix+"/gettransactioninfobyblocknum")
        if not isinstance(infos, list):
            raise RuntimeError("TRON transaction info unavailable")

    def _height(self, prefix):
        def valid(block):
            raw=block['block_header']['raw_data']
            height=raw['number']
            tron_header(block,height)
            return (type(height) is int and -120 <= time.time()-raw['timestamp']/1000 <= 180
                    and height >= getattr(self,'_highest_tip',0)-128)
        block=self.rpc.post_validated({'detail':False},prefix+'/getblock',
            valid)
        height=block['block_header']['raw_data']['number']
        tron_header(block,height)
        self._highest_tip=max(height,getattr(self,'_highest_tip',0))
        return height

    def tip(self) -> int:
        height=self._height(self.block_prefix)
        return height if self.config.get('scan_unconfirmed') else max(0,height-self.finality_blocks)

    def safe_tip(self) -> int:
        return max(0,self._height('/walletsolidity')-self.finality_blocks)


def build_adapter(name: str, config: dict[str, Any]) -> EvmAdapter | TronAdapter:
    kind = config.get("type")
    if kind == "evm":
        return EvmAdapter(name, config)
    if kind == "tron":
        return TronAdapter(name, config)
    raise ValueError(f"unsupported chain type: {kind}")
