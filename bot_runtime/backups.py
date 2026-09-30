from __future__ import annotations

import json
import time
from typing import Any


def export_user_backup(
    store: Any, config: dict[str, Any], evm_group: list[str], user_id: int,
) -> bytes:
    safe_chain_keys = (
        "display_name", "launch_date", "type", "chain_id", "confirmations", "min_native",
        "market_chain", "market_min_liquidity_usd", "market_min_transfer_usd",
        "explorer_tx_url", "explorer_address_url",
        "explorer_token_url", "pending_native_transfers",
        "full_block_fallback_enabled",
    )
    payload = {
        "format": "crypto-address-monitor-user-backup-v3",
        "exported_at": int(time.time()),
        "user_id": int(user_id),
        "settings": {
            "poll_seconds": config.get("poll_seconds"),
            "evm_group": evm_group,
            "chains": {
                name: {key: chain[key] for key in safe_chain_keys if key in chain}
                for name, chain in config["chains"].items()
            },
        },
        "addresses": [
            {
                "chain": row["chain"], "address": row["address"],
                "label": row["label"], "enabled": bool(row["enabled"]),
                "watch_direction": row["watch_direction"],
                "created_at": row["created_at"],
            }
            for row in store.addresses(user_id=user_id)
        ],
    }
    return (json.dumps(payload, ensure_ascii=False, indent=2) + "\n").encode("utf-8")


def parse_user_backup(
    content: bytes, adapters: dict[str, Any], evm_group: list[str],
) -> list[dict[str, object]]:
    try:
        payload = json.loads(content.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("備份不是有效的 UTF-8 JSON") from exc
    if not isinstance(payload, dict) or payload.get("format") not in {
        "crypto-address-monitor-backup-v1",
        "crypto-address-monitor-user-backup-v2",
        "crypto-address-monitor-user-backup-v3",
    }:
        raise ValueError("不支援的備份格式")
    raw_addresses = payload.get("addresses")
    if not isinstance(raw_addresses, list) or len(raw_addresses) > 500:
        raise ValueError("備份地址清單無效或超過 500 筆")
    restored: dict[tuple[str, str], dict[str, object]] = {}
    now = int(time.time())
    for raw in raw_addresses:
        if not isinstance(raw, dict):
            raise ValueError("備份含有無效地址資料")
        chain = str(raw.get("chain", ""))
        if chain == "evm":
            if not evm_group:
                raise ValueError("目前沒有啟用的 EVM 鏈")
            address = adapters[evm_group[0]].normalize(str(raw.get("address", "")))
        elif chain in adapters:
            address = adapters[chain].normalize(str(raw.get("address", "")))
        else:
            raise ValueError(f"備份含有不支援的鏈：{chain}")
        label = " ".join(str(raw.get("label", "")).split())
        if not label or len(label) > 80:
            raise ValueError("備份中的標籤必須為 1 至 80 個字元")
        direction = str(raw.get("watch_direction", "both"))
        if direction not in {"both", "in", "out"}:
            raise ValueError("備份中的監控方向無效")
        enabled = raw.get("enabled", True)
        if type(enabled) is not bool and not (type(enabled) is int and enabled in (0, 1)):
            raise ValueError("備份中的啟用狀態必須為 true／false 或 0／1")
        try:
            created_at = int(raw.get("created_at", now))
        except (TypeError, ValueError):
            created_at = now
        restored[(chain, address)] = {
            "chain": chain, "address": address, "label": label,
            "enabled": bool(enabled),
            "watch_direction": direction,
            "created_at": max(0, min(created_at, now)),
        }
    return list(restored.values())
