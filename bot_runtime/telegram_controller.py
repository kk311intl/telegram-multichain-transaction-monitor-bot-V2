from __future__ import annotations
from contextlib import nullcontext
from .telegram_rate import TelegramThrottle

import html
import logging
import os
import time
from decimal import Decimal
from pathlib import Path
from typing import Any

from .backups import export_user_backup, parse_user_backup
from .store import Store
from .telegram import TelegramError


LOG = logging.getLogger("crypto-address-monitor")


class TelegramControllerMixin:
    @property
    def pending_input(self) -> dict[str, Any] | None:
        return self.pending_inputs.get(self.active_user_id)
    @pending_input.setter
    def pending_input(self, value: dict[str, Any] | None) -> None:
        if value is None:
            self.pending_inputs.pop(self.active_user_id, None)
        else:
            self.pending_inputs[self.active_user_id] = value
    @property
    def status_refresh(self) -> dict[str, Any] | None:
        return self.status_refreshes.get(self.active_user_id)
    @status_refresh.setter
    def status_refresh(self, value: dict[str, Any] | None) -> None:
        if value is None:
            self.status_refreshes.pop(self.active_user_id, None)
        else:
            self.status_refreshes[self.active_user_id] = value
    @property
    def latest_menu_message_id(self) -> int:
        return self.latest_menu_message_ids.get(self.active_user_id, 0)
    @latest_menu_message_id.setter
    def latest_menu_message_id(self, value: int) -> None:
        self.latest_menu_message_ids[self.active_user_id] = int(value)
    def send(
        self, text: str, reply_markup: dict[str, Any] | None = None,
        user_id: int | None = None, auto_delete: bool = True,
    ) -> int:
        # Telegram caps message text at 4096 characters. Split only between
        # complete lines so the per-line HTML tags remain balanced.
        chunks: list[str] = []
        chunk = ""
        for line in text.splitlines(keepends=True):
            if chunk and len(chunk) + len(line) > 3800:
                chunks.append(chunk.rstrip())
                chunk = ""
            chunk += line
        if chunk:
            chunks.append(chunk.rstrip())
        message_id = 0
        sent_message_ids: list[int] = []
        for index, item in enumerate(chunks):
            result = self.telegram.send(
                int(user_id or self.active_user_id), item,
                reply_markup if index == len(chunks) - 1 else None
            )
            if isinstance(result, dict) and isinstance(result.get("message_id"), int):
                message_id = int(result["message_id"])
                sent_message_ids.append(message_id)
        has_callbacks = any(
            "callback_data" in button
            for row in (reply_markup or {}).get("inline_keyboard", [])
            for button in row
        )
        has_force_reply = bool((reply_markup or {}).get("force_reply"))
        recipient = int(user_id or self.active_user_id)
        if message_id and has_callbacks:
            latest = max(self.latest_menu_message_ids.get(recipient, 0), message_id)
            self.latest_menu_message_ids[recipient] = latest
            self.store.set_meta(f"latest_menu_message_id:{recipient}", latest)
        if auto_delete:
            delete_at = int(time.time()) + self.settings.menu_seconds
            for sent_message_id in sent_message_ids:
                self.store.schedule_message_deletion(
                    recipient, sent_message_id, delete_at,
                )
        elif has_force_reply:
            LOG.warning("persistent force-reply message requested; refusing retention override")
        return message_id

    @staticmethod
    def _keyboard(rows: list[list[tuple[str, str]]]) -> dict[str, Any]:
        return {"inline_keyboard": [[
            {"text": text, "callback_data": data} for text, data in row
        ] for row in rows]}
    def main_keyboard(self) -> dict[str, Any]:
        return self._keyboard([
            [("➕ 新增地址", "menu:add"), ("📋 地址管理", "menu:list")],
            [("📊 運行狀態", "menu:status"), ("🛡 過濾記錄", "menu:filtered")],
            [("💾 備份管理", "menu:backup"), ("❓ 使用說明", "menu:help")],
        ])
    def backup_keyboard(self) -> dict[str, Any]:
        return self._keyboard([
            [("📤 匯出備份", "menu:export"), ("📥 匯入備份", "menu:import")],
            [("⬅️ 主選單", "menu:main")],
        ])
    def user_keyboard(self) -> dict[str, Any]:
        return self._keyboard([
            [("➕ 授權用戶", "user:authorize"), ("➖ 撤銷授權", "user:revoke")],
            [("關閉", "user:close")],
        ])
    def authorized_users_text(self) -> str:
        rows = self.store.authorized_users()
        lines = ["<b>用戶管理</b>", "選擇要執行的操作。", "", "<b>已授權用戶</b>"]
        lines.extend(
            f"• <code>{row['user_id']}</code>" + ("（所有者）" if row["user_id"] == self.owner else "")
            for row in rows
        )
        return "\n".join(lines)
    def edit_menu_message(
        self, message: dict[str, Any], text: str,
        reply_markup: dict[str, Any] | None = None, auto_delete: bool = True,
    ) -> None:
        chat_id = message.get("chat", {}).get("id")
        message_id = message.get("message_id")
        if chat_id is None or message_id is None:
            self.send(text, reply_markup)
            return
        payload: dict[str, Any] = {
            "chat_id": chat_id, "message_id": message_id, "text": text,
            "parse_mode": "HTML", "disable_web_page_preview": True,
        }
        if reply_markup:
            payload["reply_markup"] = reply_markup
        try:
            self.telegram.call("editMessageText", payload)
        except RuntimeError as exc:
            if "message is not modified" not in str(exc).lower():
                raise
        if auto_delete:
            self.store.schedule_message_deletion(
                int(chat_id), int(message_id), int(time.time()) + self.settings.menu_seconds,
            )
    def move_menu_to_bottom(
        self, state: dict[str, Any], text: str,
        reply_markup: dict[str, Any] | None = None,
    ) -> None:
        self._recall_flow_prompt(state)
        old = state.get("menu_message", {})
        if old.get("chat", {}).get("id") is not None and old.get("message_id") is not None:
            self.edit_menu_message(
                old, "此輸入流程已結束，請使用下方最新菜單。",
                {"inline_keyboard": []},
            )
        self.send(text, reply_markup)
    def _recall_flow_prompt(self, state: dict[str, Any] | None) -> None:
        if not state:
            return
        message_id = state.get("prompt_message_id")
        chat_id = state.get("prompt_chat_id", self.active_user_id)
        if not isinstance(message_id, int) or message_id <= 0:
            return
        try:
            self.telegram.call("deleteMessage", {
                "chat_id": int(chat_id), "message_id": message_id,
            })
        except TelegramError as exc:
            if exc.status not in {400, 403, 404}:
                LOG.warning("input prompt recall deferred: HTTP %s", exc.status or "network")
                return
        except Exception:
            LOG.exception("input prompt recall deferred")
            return
        self.store.finish_message_deletion(int(chat_id), message_id)
    def chains_text(self) -> str:
        ordered = self._ordered_chain_names()
        group_names = [self._chain_label(name) for name in ordered if name in self.evm_group]
        lines = ["<b>可用鏈</b>"]
        if group_names:
            lines.append(f"<code>EVM</code> — {' · '.join(map(html.escape, group_names))}")
        for name in ordered:
            if name not in self.evm_group:
                lines.append(html.escape(self._chain_label(name)))
        return "\n".join(lines)
    def _ordered_chain_names(self) -> list[str]:
        """Order UI entries by public mainnet launch date, oldest first."""
        configured_order = {name: index for index, name in enumerate(self.adapters)}

        def key(name: str) -> tuple[str, int]:
            launch_date = str(self.adapters[name].config.get("launch_date", "9999-12-31"))
            return launch_date, configured_order[name]

        return sorted(self.adapters, key=key)
    @staticmethod
    def _filter_reason(reason: str) -> str:
        return {
            "zero_value": "零價值交易",
            "goplus_fake_token": "GoPlus 標記為冒名 Token",
            "goplus_honeypot": "GoPlus 標記為蜜罐風險",
            "native_below_minimum": "原生幣金額低於門檻",
            "native_value_below_usd_minimum": "原生幣轉帳價值低於設定門檻",
            "token_not_above_minimum": "官方 USDC／USDT 低於 US$1（舊記錄）",
            "stablecoin_contract_mismatch": "冒用穩定幣名稱的非官方合約",
            "no_market_value": "DEX Screener 查無市場價值",
            "market_liquidity_below_minimum": "DEX 流動性低於設定門檻",
            "token_value_below_usd_minimum": "本次轉帳價值低於設定門檻",
            "tronscan_risk": "歷史來源標記為風險交易",
            "unknown_token": "未知 Token",
        }.get(reason, "其他風險規則")
    @staticmethod
    def _filter_warning(reason: str) -> str:
        return " ⚠️" if reason in {
            "no_market_value", "market_liquidity_below_minimum",
        } else ""
    def _display_time(self, timestamp: int | str) -> str:
        try:
            value = int(timestamp)
        except (TypeError, ValueError):
            value = 0
        minutes = self.settings.timezone_minutes
        sign = "+" if minutes >= 0 else "-"
        absolute = abs(minutes)
        hours, remainder = divmod(absolute, 60)
        suffix = f"UTC{sign}{hours}" + (f":{remainder:02d}" if remainder else "")
        return time.strftime(
            "%Y-%m-%d %H:%M:%S", time.gmtime(value + minutes * 60)
        ) + f" {suffix}"
    @staticmethod
    def _short_identifier(value: str) -> str:
        text = str(value)
        return text if len(text) <= 17 else f"{text[:8]}…{text[-8:]}"
    def _explorer_link(
        self, chain: str, kind: str, value: str, label: str, *, code_fallback: bool = False,
    ) -> str:
        config = self.config.get("chains", {}).get(chain, {})
        prefix = str(config.get(f"explorer_{kind}_url", ""))
        escaped_label = html.escape(str(label))
        if prefix and value:
            url = html.escape(prefix + str(value), quote=True)
            return f'<a href="{url}">{escaped_label}</a>'
        return f"<code>{escaped_label}</code>" if code_fallback else escaped_label
    def filtered_page(self, page: int = 0) -> tuple[str, dict[str, Any]]:
        page_size = self.settings.filter_page_size
        count = self.store.filtered_count(self.active_user_id)
        pages = max(1, (count + page_size - 1) // page_size)
        page = max(0, min(page, pages - 1))
        rows = self.store.recent_filtered(page_size, page * page_size, self.active_user_id)
        if not rows:
            text = "尚無被過濾的交易。"
        else:
            lines = []
            for row in rows:
                arrow = "↓ 收到" if row["direction"] == "in" else "↑ 支出"
                label = str(row["label"] or self._short_identifier(row["address"]))
                if label == row["address"]:
                    label = self._short_identifier(label)
                if len(label) > 80:
                    label = label[:77] + "…"
                symbol = html.escape(row["symbol"])
                if row["asset_id"] != "native":
                    symbol = self._explorer_link(
                        row["chain"], "token", row["asset_id"], row["symbol"],
                    )
                transaction = self._explorer_link(
                    row["chain"], "tx", row["txid"], self._short_identifier(row["txid"]),
                    code_fallback=True,
                )
                lines.append(
                    f"<b>{html.escape(self._chain_label(row['chain']))} "
                    f"{html.escape(self._filter_reason(row['filter_reason']))}</b>\n"
                    f"{html.escape(label)} {arrow} "
                    f"<b>{self._amount(row['amount_raw'], row['decimals'])} {symbol}"
                    f"{self._filter_warning(row['filter_reason'])}</b>\n"
                    f"交易：{transaction}\n"
                    f"時間：{self._display_time(row['block_timestamp'] or row['created_at'])}"
                )
            text = "\n\n".join(lines)
        navigation: list[tuple[str, str]] = []
        if page > 0:
            navigation.append(("◀️", f"filtered:{page - 1}"))
        if pages > 1:
            navigation.append((f"{page + 1}/{pages}", f"filtered:{page}"))
        if page + 1 < pages:
            navigation.append(("▶️", f"filtered:{page + 1}"))
        buttons = [navigation] if navigation else []
        buttons.append([("⬅️ 主選單", "menu:main")])
        return text, self._keyboard(buttons)
    def export_backup(self, user_id: int | None = None) -> bytes:
        selected_user = int(user_id or self.active_user_id)
        return export_user_backup(
            self.store, self.config, self.evm_group, selected_user,
        )
    def import_backup(self, user_id: int, content: bytes) -> int:
        restored = parse_user_backup(content, self.adapters, self.evm_group)
        return self.store.replace_addresses(user_id, restored)
    @staticmethod
    def _duration(seconds: int | float) -> str:
        value = max(0, int(seconds))
        days, value = divmod(value, 86400)
        hours, value = divmod(value, 3600)
        minutes, seconds = divmod(value, 60)
        parts = []
        if days:
            parts.append(f"{days}d")
        if hours or days:
            parts.append(f"{hours}h")
        if minutes or hours or days:
            parts.append(f"{minutes}m")
        parts.append(f"{seconds}s")
        return " ".join(parts)
    @staticmethod
    def _bytes(value: int) -> str:
        amount = float(max(0, value))
        for unit in ("B", "KiB", "MiB", "GiB"):
            if amount < 1024 or unit == "GiB":
                return f"{amount:.1f} {unit}"
            amount /= 1024
        return f"{amount:.1f} GiB"
    @staticmethod
    def _age(raw: str, now: int) -> str:
        try:
            timestamp = int(raw.split(":", 1)[0])
        except (TypeError, ValueError):
            return "尚無"
        return TelegramControllerMixin._duration(now - timestamp) + " 前"
    @staticmethod
    def _rss_bytes() -> int:
        try:
            for line in Path("/proc/self/status").read_text(encoding="ascii").splitlines():
                if line.startswith("VmRSS:"):
                    return int(line.split()[1]) * 1024
        except (OSError, ValueError, IndexError):
            pass
        return 0
    def _start_status_refresh(
        self, message: dict[str, Any], now: float | None = None,
    ) -> None:
        chat_id = message.get("chat", {}).get("id")
        message_id = message.get("message_id")
        if chat_id is None or message_id is None:
            return
        user_id = int(chat_id)
        latest = self.latest_menu_message_ids.get(user_id, 0)
        if int(message_id) < latest:
            return
        self.latest_menu_message_ids[user_id] = int(message_id)
        self.store.set_meta(f"latest_menu_message_id:{user_id}", int(message_id))
        current = time.monotonic() if now is None else now
        self.status_refreshes[user_id] = {
            "message": {"chat": {"id": chat_id}, "message_id": message_id},
            "user_id": user_id,
            "next_at": current + self.settings.menu_seconds,
            "expires_at": current + self.settings.menu_seconds,
        }
        self.store.schedule_message_deletion(
            user_id, int(message_id), int(time.time()) + self.settings.menu_seconds,
        )
    def _cancel_status_refresh(self, message: dict[str, Any] | None = None) -> None:
        user_id = self.active_user_id if message is None else int(
            message.get("chat", {}).get("id") or self.active_user_id
        )
        session = self.status_refreshes.get(user_id)
        if session is None:
            return
        if message is not None:
            active = session["message"]
            if (
                active.get("message_id") != message.get("message_id")
                or active.get("chat", {}).get("id") != message.get("chat", {}).get("id")
            ):
                return
        self.status_refreshes.pop(user_id, None)
    def _refresh_status_message(self, now: float | None = None) -> None:
        current = time.monotonic() if now is None else now
        for user_id, session in list(self.status_refreshes.items()):
            if not self.store.is_authorized(user_id):
                self.status_refreshes.pop(user_id, None)
                continue
            if current >= session["expires_at"]:
                self.status_refreshes.pop(user_id, None)
                continue
            if current < session["next_at"]:
                continue
            session["next_at"] = current + self.settings.menu_seconds
            try:
                self.edit_menu_message(
                    session["message"], self.status_text(user_id),
                    self._keyboard([[("⬅️ 主選單", "menu:main")]]),
                    auto_delete=False,
                )
            except TelegramThrottle:
                session["next_at"] = current
            except TelegramError as exc:
                if exc.status in {400, 403, 404}:
                    self.status_refreshes.pop(user_id, None)
                else:
                    LOG.warning("status auto-refresh deferred: HTTP %s", exc.status or "network")
            except Exception:
                LOG.exception("status auto-refresh failed")
    def _flush_message_deletions(self) -> None:
        for row in self.store.due_message_deletions()[:1]:
            chat_id = int(row["chat_id"])
            message_id = int(row["message_id"])
            try:
                self.telegram.call("deleteMessage", {
                    "chat_id": chat_id, "message_id": message_id,
                })
            except TelegramThrottle:
                continue
            except TelegramError as exc:
                if exc.status in {400, 403, 404}:
                    self.store.finish_message_deletion(chat_id, message_id)
                else:
                    delay = exc.retry_after or min(3600, 15 * (2 ** min(int(row["attempts"]), 8)))
                    self.store.defer_message_deletion(chat_id, message_id, delay, type(exc).__name__)
                continue
            except Exception as exc:
                self.store.defer_message_deletion(chat_id, message_id, 60, type(exc).__name__)
                continue
            self.store.finish_message_deletion(chat_id, message_id)
            session = self.status_refreshes.get(chat_id)
            if session and int(session["message"].get("message_id", 0)) == message_id:
                self.status_refreshes.pop(chat_id, None)
    def add_keyboard(self) -> dict[str, Any]:
        choices = [("EVM（全部）", "add:evm")] if self.evm_group else []
        choices.extend(
            (self._chain_label(name), f"add:{name}")
            for name in self._ordered_chain_names() if name in self.evm_group
        )
        for name in self._ordered_chain_names():
            if name not in self.evm_group:
                choices.append((self._chain_label(name), f"add:{name}"))
        rows = [choices[index:index + 2] for index in range(0, len(choices), 2)]
        rows.append([("⬅️ 主選單", "menu:main")])
        return self._keyboard(rows)
    def address_list_page(self, page: int = 0) -> tuple[str, dict[str, Any]]:
        rows = self.store.addresses(user_id=self.active_user_id)
        page_size = self.settings.address_page_size
        pages = max(1, (len(rows) + page_size - 1) // page_size)
        page = max(0, min(page, pages - 1))
        buttons: list[list[tuple[str, str]]] = []
        lines = [
            "<b>地址管理</b>\n點按地址可複製；選擇下方按鈕進行設定。",
            "↕️ 全部方向 · ⬇️ 只轉入 · ⬆️ 只轉出 · ⏸ 已暫停",
        ] if rows else ["尚未監控任何地址。"]
        for row in rows[page * page_size:(page + 1) * page_size]:
            lines.append(
                f"\n#{row['id']} · {html.escape(self._chain_label(row['chain']))}\n"
                f"<code>{html.escape(str(row['address']))}</code>"
            )
            label = str(row["label"])
            if len(label) > 24:
                label = label[:23] + "…"
            direction_icon = {"both": "↕️", "in": "⬇️", "out": "⬆️"}.get(
                row["watch_direction"], "↕️"
            )
            state_icon = "" if row["enabled"] else "⏸ "
            buttons.append([(
                f"{state_icon}#{row['id']} · {self._chain_label(row['chain'])} · "
                f"{label} · {direction_icon}",
                f"addr:{row['id']}",
            )])
        if pages > 1:
            navigation: list[tuple[str, str]] = []
            if page > 0:
                navigation.append(("◀️", f"page:{page - 1}"))
            navigation.append((f"{page + 1}/{pages}", f"page:{page}"))
            if page + 1 < pages:
                navigation.append(("▶️", f"page:{page + 1}"))
            buttons.append(navigation)
        buttons.append([("➕ 新增", "menu:add"), ("⬅️ 主選單", "menu:main")])
        return "\n".join(lines), self._keyboard(buttons)
    def address_detail(self, row: Any) -> tuple[str, dict[str, Any]]:
        direction_labels = {"both": "全部方向", "in": "只轉入", "out": "只轉出"}
        direction = direction_labels.get(row["watch_direction"], "全部方向")
        state = "啟用" if row["enabled"] else "已暫停"
        text = (
            f"<b>管理 #{row['id']} · {html.escape(str(row['label']))}</b>\n"
            f"鏈：<code>{html.escape(self._chain_label(row['chain']))}</code>\n"
            f"地址：<code>{html.escape(str(row['address']))}</code>\n"
            f"狀態：{state}\n方向：{direction}"
        )
        keyboard = self._keyboard([
            [(
                "⏸ 暫停監控" if row["enabled"] else "▶️ 恢復監控",
                f"enabled:{row['id']}:{0 if row['enabled'] else 1}",
            )],
            [("✅ 全部方向" if row["watch_direction"] == "both" else "全部方向", f"dir:{row['id']}:both")],
            [
                ("✅ 只轉入" if row["watch_direction"] == "in" else "只轉入", f"dir:{row['id']}:in"),
                ("✅ 只轉出" if row["watch_direction"] == "out" else "只轉出", f"dir:{row['id']}:out"),
            ],
            [("✏️ 改標籤", f"edit:{row['id']}"), ("🗑 刪除", f"delask:{row['id']}")],
            [("⬅️ 地址列表", "menu:list"), ("🏠 主選單", "menu:main")],
        ])
        return text, keyboard
    def _chain_label(self, name: str) -> str:
        if name == "evm":
            return "EVM"
        adapter = self.adapters.get(name)
        return str(adapter.config.get("display_name", name) if adapter else name)
    def _address_format_hint(self, name: str) -> str:
        kind = self.adapters[name].config['type'] if name in self.adapters else name
        if kind == "tron":
            return "TRON 主網 Base58Check 地址，例如 <code>T...</code>"
        if kind == 'bitcoin':
            return 'BTC 主網地址：<code>1...</code>、<code>3...</code> 或 <code>bc1...</code>'
        if kind == 'solana':
            return 'Solana 32-byte Base58 地址，大小寫須完全一致'
        return "0x 開頭的 20-byte EVM 十六進位地址，例如 <code>0x...</code>"
    def menu(self) -> str:
        return (
            f"<b>{html.escape(self.settings.title)}</b>\n\n"
            "請使用下方按鈕管理地址、監控方向與查看狀態。"
        )
    def usage_text(self) -> str:
        notice = html.escape(self.settings.usage_notice)
        return (notice+"\n" if notice else "") + f"單地址待發通知超過 {self.settings.pending_limit} 筆會自動移除。\n"

    def help_text(self) -> str:
        guidance = (
            "<b>使用說明</b>\n\n"
            f"{self.usage_text()}\n"
            "• 新增地址：選擇鏈、輸入地址，可加上標籤。\n"
            "• 地址管理：修改標籤、暫停或刪除，設定轉入／轉出方向。\n"
            "• 運行狀態：查看各鏈監控情況。\n"
            "• 過濾記錄：查看風險、低流動性及低價值交易。\n"
            "• 備份管理：匯出或匯入自己的地址與設定。\n\n"
            "交易上鏈後先顯示「交易未確認」，確認或失效時更新原消息。\n"
            f"非通知消息在 {self.settings.menu_seconds} 秒後回收，不置頂；交易及高頻地址移除通知保留。\n"
            "監控從啟用時開始，不補報歷史或停機期間的交易。\n\n"
            "/start 開啟主選單；未授權時取得 ID，交給管理員授權。"
        )
        return guidance + "\n\n" + self.asset_scope() + "\n\n" + self.chains_text()
    def handle_pending_text(self, message: dict[str, Any], text: str) -> None:
        state = self.pending_input
        if not state:
            self.send("請使用下方按鈕選擇功能。", self.main_keyboard())
            return
        if time.time() - float(state.get("created_at", 0)) > self.settings.menu_seconds:
            self.pending_input = None
            self.move_menu_to_bottom(state, "輸入流程已逾時，請重新操作。", self.main_keyboard())
            return
        if text.lower() in {"cancel", "取消"}:
            self.pending_input = None
            self.move_menu_to_bottom(state, "已取消。", self.main_keyboard())
            return
        action = state.get("action")
        self.pending_input = None
        if action in {"user_authorize", "user_revoke"}:
            if self.active_user_id != self.owner:
                raise ValueError("此操作僅限所有者使用")
            try:
                target = int(text.strip())
                if target <= 0:
                    raise ValueError
            except ValueError as exc:
                raise ValueError("User ID 必須是正整數") from exc
            if action == "user_authorize":
                added = self.store.authorize_user(target)
                result = f"User ID <code>{target}</code> 已{'授權' if added else '經在授權清單中'}。"
            elif target == self.owner:
                result = "不能撤銷所有者自己的權限。"
            else:
                removed = self.store.revoke_user(target)
                if removed:
                    self.pending_inputs.pop(target, None)
                    self.status_refreshes.pop(target, None)
                result = f"User ID <code>{target}</code> {'已撤銷授權' if removed else '不在授權清單中'}。"
            self.move_menu_to_bottom(
                state, result + "\n\n" + self.authorized_users_text(), self.user_keyboard(),
            )
            return
        if action == "add":
            parts = text.split(maxsplit=1)
            requested = str(state["chain"])
            address = parts[0]
            label = " ".join(parts[1].split()) if len(parts) == 2 else address
            if not label or len(label) > 80:
                raise ValueError("標籤必須為 1 至 80 個字元")
            if requested == "evm":
                if not self.evm_group:
                    raise ValueError("目前沒有啟用的 EVM 鏈")
                normalized = self.adapters[self.evm_group[0]].normalize(address)
                chain = "evm"
            else:
                normalized = self.adapters[requested].normalize(address)
                chain = requested
            self.store.add(chain, normalized, label, self.active_user_id)
            self.move_menu_to_bottom(
                state,
                f"已新增 <b>{html.escape(label)}</b>\n"
                f"鏈：<code>{html.escape(self._chain_label(chain))}</code>\n"
                f"地址：<code>{html.escape(self._short_identifier(normalized))}</code>\n"
                "預設：全部方向、全部資產；從目前位置開始，不回報舊交易。",
                self.main_keyboard(),
            )
            return
        if action == "edit":
            row_id = int(state["id"])
            label = " ".join(text.split())
            if not label or len(label) > 80 or not self.store.edit(
                row_id, label, self.active_user_id
            ):
                raise ValueError("找不到地址或標籤為空")
            row = self.store.address(row_id, self.active_user_id)
            if not row:
                raise ValueError("找不到該 ID")
            detail, keyboard = self.address_detail(row)
            self.move_menu_to_bottom(state, "標籤已修改。\n\n" + detail, keyboard)
            return
        raise ValueError("輸入流程已失效，請重新操作")
    def callback(self, query: dict[str, Any]) -> None:
        user_id = int(query.get("from", {}).get("id") or 0)
        self.active_user_id = user_id
        callback_id = str(query.get("id", ""))
        if not self.store.is_authorized(user_id):
            if callback_id:
                self.telegram.call("answerCallbackQuery", {
                    "callback_query_id": callback_id,
                    "text": "尚未授權，請使用 /start 取得授權說明。",
                    "show_alert": True,
                })
            return
        message = query.get("message", {})
        if message.get("chat", {}).get("type") != "private" or message.get("chat", {}).get("id") != user_id:
            return
        data = str(query.get("data", ""))
        if data.startswith("user:") and user_id != self.owner:
            if callback_id:
                self.telegram.call("answerCallbackQuery", {
                    "callback_query_id": callback_id,
                    "text": "此功能僅限所有者使用。",
                    "show_alert": True,
                })
            return
        if callback_id:
            try:
                self.telegram.call("answerCallbackQuery", {"callback_query_id": callback_id})
            except Exception:
                LOG.exception("unable to acknowledge callback")
        message_id = message.get("message_id")
        if isinstance(message_id, int) and message_id > self.latest_menu_message_id:
            self.latest_menu_message_id = message_id
            self.store.set_meta(f"latest_menu_message_id:{user_id}", message_id)
        show = lambda text, markup=None: self.edit_menu_message(message, text, markup)
        if data != "menu:status":
            self._cancel_status_refresh(message)
        if self.pending_input and (
            data in {"menu:main", "menu:backup", "user:menu", "user:list"}
            or data.startswith("addr:")
        ):
            state = self.pending_input
            self.pending_input = None
            self._recall_flow_prompt(state)
        try:
            if data == "menu:main":
                self.pending_input = None
                show(self.menu(), self.main_keyboard())
            elif data == "menu:help":
                self.pending_input = None
                show(self.help_text(), self._keyboard([[("⬅️ 主選單", "menu:main")]]))
            elif data == "menu:backup":
                self.pending_input = None
                show(
                    "<b>備份管理</b>\n\n匯出或匯入你自己的地址與監控設定；備份不含任何密鑰。",
                    self.backup_keyboard(),
                )
            elif data == "menu:add":
                self.pending_input = None
                show("<b>選擇要監控的鏈</b>", self.add_keyboard())
            elif data == "menu:list" or data.startswith("page:"):
                self.pending_input = None
                page = 0 if data == "menu:list" else int(data.split(":", 1)[1])
                show(*self.address_list_page(page))
            elif data == "menu:status":
                page = "status"
                text = self.status_text()
                show(text, self._keyboard([[("⬅️ 主選單", "menu:main")]]))
                self._start_status_refresh(message)
            elif data == "menu:filtered" or data.startswith("filtered:"):
                page = 0 if data == "menu:filtered" else int(data.split(":", 1)[1])
                text, keyboard = self.filtered_page(page)
                show(text, keyboard)
            elif data == "menu:export":
                self.pending_input = None
                filename = time.strftime("crypto-monitor-backup-%Y%m%d-%H%M%S.json")
                result = self.telegram.send_document(
                    user_id, filename, self.export_backup(user_id),
                    "你的地址與監控設定備份（不含任何 Key 或 RPC URL）",
                )
                if isinstance(result, dict) and isinstance(result.get("message_id"), int):
                    self.store.schedule_message_deletion(
                        user_id, int(result["message_id"]),
                        int(time.time()) + self.settings.menu_seconds,
                    )
                show("備份已在新消息中送出。", self.backup_keyboard())
            elif data == "menu:import":
                self.pending_input = {
                    "action": "import", "menu_message": message,
                    "created_at": time.time(),
                }
                show(
                    "請上傳本 Bot 匯出的 JSON 備份。匯入成功後會替換你目前的地址設定。",
                    self._keyboard([[("取消", "menu:backup")]]),
                )
            elif data in {"user:menu", "user:list"}:
                self.pending_input = None
                show(self.authorized_users_text(), self.user_keyboard())
            elif data in {"user:authorize", "user:revoke"}:
                action = data.split(":", 1)[1]
                self.pending_input = {
                    "action": f"user_{action}", "menu_message": message,
                    "created_at": time.time(),
                }
                verb = "授權" if action == "authorize" else "撤銷授權"
                show(
                    f"正在{verb}用戶，請在新消息輸入 Telegram User ID。",
                    self._keyboard([[('取消', 'user:menu')]]),
                )
                prompt_message_id = self.send(
                    f"請輸入要{verb}的 Telegram User ID。",
                    {"force_reply": True, "selective": True,
                     "input_field_placeholder": "Telegram User ID"},
                )
                if isinstance(prompt_message_id, int) and prompt_message_id > 0:
                    self.pending_input["prompt_message_id"] = prompt_message_id
                    self.pending_input["prompt_chat_id"] = user_id
            elif data == "user:close":
                self.pending_input = None
                show("用戶管理已關閉。", {"inline_keyboard": []})
            elif data.startswith("add:"):
                chain = data.split(":", 1)[1]
                if chain != "evm" and chain not in self.adapters:
                    raise ValueError("不支援這條鏈")
                self.pending_input = {
                    "action": "add", "chain": chain, "menu_message": message,
                    "created_at": time.time(),
                }
                show(
                    f"正在新增 <b>{html.escape(self._chain_label(chain))}</b> 地址，請在新消息輸入。",
                    self._keyboard([[("取消", "menu:main")]]),
                )
                prompt_message_id = self.send(
                    f"請輸入 <b>{html.escape(self._chain_label(chain))}</b> 地址，後面可加標籤。\n"
                    f"{self.usage_text()}"
                    f"格式：{self._address_format_hint(chain)}\n"
                    "標籤範例：<code>主錢包</code>",
                    {"force_reply": True, "selective": True,
                     "input_field_placeholder": "地址 可選標籤"},
                )
                if isinstance(prompt_message_id, int) and prompt_message_id > 0:
                    self.pending_input["prompt_message_id"] = prompt_message_id
                    self.pending_input["prompt_chat_id"] = user_id
            elif data.startswith(("addr:", "dir:", "enabled:", "scope:", "edit:", "delask:", "delete:")):
                self._address_callback(data, user_id, message, show)
            else:
                show("按鈕已失效，請返回主選單。", self.main_keyboard())
        except Exception as exc:
            LOG.exception("callback failed")
            self.pending_input = None
            show(f"操作失敗：<code>{html.escape(str(exc))}</code>", self.main_keyboard())
    def _address_callback(self, data, user_id, message, show):
        """Handle address actions after callback authorization; keep owner-scoped queries."""
        if data.startswith("addr:"):
            self.pending_input = None
            row = self.store.address(int(data.split(":", 1)[1]), user_id)
            if not row:
                raise ValueError("找不到該地址")
            detail, keyboard = self.address_detail(row)
            show(detail, keyboard)
        elif data.startswith("dir:"):
            _, row_id, direction = data.split(":", 2)
            if not self.store.set_watch_direction(int(row_id), direction, user_id):
                raise ValueError("找不到該地址")
            row = self.store.address(int(row_id), user_id)
            detail, keyboard = self.address_detail(row)
            show("監控方向已修改。\n\n" + detail, keyboard)
        elif data.startswith("enabled:"):
            _, row_id, enabled = data.split(":", 2)
            if not self.store.set_enabled(int(row_id), enabled == "1", user_id):
                raise ValueError("找不到該地址")
            row = self.store.address(int(row_id), user_id)
            detail, keyboard = self.address_detail(row)
            show(("監控已恢復。\n\n" if enabled == "1" else "監控已暫停。\n\n") + detail, keyboard)
        elif data.startswith("scope:"):
            _, row_id, _ = data.split(":", 2)
            row = self.store.address(int(row_id), user_id)
            if not row:
                raise ValueError("找不到該地址")
            detail, keyboard = self.address_detail(row)
            show("資產篩選功能已移除，地址固定監控全部資產。\n\n" + detail, keyboard)
        elif data.startswith("edit:"):
            row_id = int(data.split(":", 1)[1])
            if not self.store.address(row_id, user_id):
                raise ValueError("找不到該地址")
            self.pending_input = {
                "action": "edit", "id": row_id, "menu_message": message,
                "created_at": time.time(),
            }
            show("正在修改標籤，請在新消息輸入。", self._keyboard([[("取消", f"addr:{row_id}")]]))
            prompt_message_id = self.send(
                "請輸入新的地址標籤。",
                {"force_reply": True, "selective": True,
                 "input_field_placeholder": "新標籤"},
            )
            if isinstance(prompt_message_id, int) and prompt_message_id > 0:
                self.pending_input["prompt_message_id"] = prompt_message_id
                self.pending_input["prompt_chat_id"] = user_id
        elif data.startswith("delask:"):
            row_id = int(data.split(":", 1)[1])
            row = self.store.address(row_id, user_id)
            if not row:
                raise ValueError("找不到該地址")
            show(
                f"確定刪除 <b>{html.escape(str(row['label']))}</b>？",
                self._keyboard([[("確認刪除", f"delete:{row_id}"), ("取消", f"addr:{row_id}")]]),
            )
        elif data.startswith("delete:"):
            row_id = int(data.split(":", 1)[1])
            if not self.store.remove(row_id, user_id):
                raise ValueError("找不到該地址")
            self.pending_input = None
            text, keyboard = self.address_list_page()
            show("地址已刪除。\n\n" + text, keyboard)

    def command(self, message: dict[str, Any]) -> None:
        if message.get("chat", {}).get("type") != "private":
            return
        # Telegram service notices can be authored by the bot, not the chat user.
        # Only recall notices for our own tracked menus; never delete the pinned menu.
        pinned = message.get('pinned_message')
        if isinstance(pinned, dict):
            chat = int(message.get('chat', {}).get('id') or 0)
            notice = int(message.get('message_id') or 0)
            menu = int(pinned.get('message_id') or 0)
            tracked = self.store.db.execute(
                'SELECT 1 FROM telegram_deletions WHERE chat_id=? AND message_id=?',
                (chat,menu)).fetchone()
            own_menu = int(self.store.meta(f"latest_menu_message_id:{chat}","0"))
            if notice>0 and notice!=menu and menu==own_menu and self.store.is_authorized(chat) and tracked:
                self.store.schedule_message_deletion(chat,notice,0)
            return
        user_id = int(message.get("from", {}).get("id") or 0)
        if user_id <= 0 or int(message.get("chat", {}).get("id") or 0) != user_id:
            return
        self.active_user_id = user_id
        text = (message.get("text") or "").strip()
        self._cancel_status_refresh()
        command = text.split(maxsplit=1)[0].split("@", 1)[0].lower() if text.startswith("/") else ""
        chat_id = int(message.get("chat", {}).get("id") or user_id)
        message_id = int(message.get("message_id") or 0)
        if message_id > 0:
            self.store.schedule_message_deletion(
                chat_id, message_id, int(time.time()) + self.settings.menu_seconds,
            )
        if not self.store.is_authorized(user_id):
            if command == "/start":
                self.send(
                    "尚未獲得使用權限。你的 Telegram User ID：\n"
                    f"<code>{user_id}</code>\n\n"
                    "請把這個 User ID 發給 Bot 管理員，請管理員授權後再使用 /start。"
                )
            else:
                self.send("尚未獲得使用權限，請使用 /start 取得授權說明。")
            return
        document = message.get("document")
        if isinstance(document, dict):
            pending = self.pending_input
            if not pending or pending.get("action") != "import":
                self.send("請先從主選單進入「備份管理」並選擇匯入備份。", self.main_keyboard())
                return
            try:
                if int(document.get("file_size", 0) or 0) > 1_048_576:
                    raise ValueError("備份檔案不可超過 1 MiB")
                content = self.telegram.download_file(str(document.get("file_id", "")))
                restored = self.import_backup(user_id, content)
                self.pending_input = None
                self.move_menu_to_bottom(
                    pending, f"備份匯入完成，共恢復 {restored} 筆地址設定。",
                    self.backup_keyboard(),
                )
            except Exception as exc:
                if isinstance(exc, ValueError):
                    LOG.info("backup import rejected: %s", exc)
                else:
                    LOG.exception("backup import failed")
                self.pending_input = None
                self.move_menu_to_bottom(
                    pending, f"備份匯入失敗：<code>{html.escape(str(exc))}</code>",
                    self.backup_keyboard(),
                )
            return
        if not text.startswith("/"):
            pending = self.pending_input
            try:
                self.handle_pending_text(message, text)
            except Exception as exc:
                expected = isinstance(exc, ValueError) or "UNIQUE constraint" in str(exc)
                if expected:
                    LOG.info("pending input rejected: %s", exc)
                else:
                    LOG.exception("pending input failed")
                self.pending_input = None
                if "UNIQUE constraint" in str(exc):
                    reply = "這個地址已在該鏈的監控清單中。"
                else:
                    reply = f"操作失敗：<code>{html.escape(str(exc))}</code>"
                if pending and pending.get("menu_message"):
                    self.move_menu_to_bottom(pending, reply, self.main_keyboard())
                else:
                    self.send(reply, self.main_keyboard())
            return
        self.pending_input = None
        if command == "/start":
            self.send(self.menu(), self.main_keyboard())
        elif command == "/user":
            self.send(
                self.authorized_users_text() if user_id == self.owner else "此命令僅限所有者使用。",
                self.user_keyboard() if user_id == self.owner else None,
            )
        elif command == "/info":
            self.send(self.info_text())
        else:
            self.send("文字指令已停用，請使用下方按鈕操作。", self.main_keyboard())
    @staticmethod
    def _amount(raw: str | int, decimals: int) -> str:
        decimal_places = int(decimals)
        if not 0 <= decimal_places <= 255:
            raise ValueError("invalid token decimals")
        value = Decimal(str(raw)) / (Decimal(10) ** decimal_places)
        return f"{value:f}"
    def poll_telegram(self) -> None:
        offset = int(self.store.meta("telegram_offset", "0"))
        updates = self.telegram.call("getUpdates", {
            "offset": offset,
            "limit": 1,
            # Keep Telegram commands responsive.
            "timeout": 1 if self.status_refreshes else 2,
            "allowed_updates": ["message", "callback_query"],
        })
        rate = getattr(self.telegram, 'rate', None)
        if rate is not None and rate.remaining():
            # Keep the offset unchanged; Telegram retains the unacknowledged updates.
            return
        for update in updates:
            next_offset = int(update["update_id"]) + 1
            try:
                item = update.get("message") or update.get("callback_query") or {}
                user = (item.get("from") or {}).get("id")
                if rate is not None and rate.remaining(user):
                    # Do not run mutating commands for a flood-limited account.
                    # Acknowledge this update so other accounts remain responsive.
                    continue
                with rate.interactive(user) if rate is not None else nullcontext():
                    if "message" in update:
                        self.command(update["message"])
                    elif "callback_query" in update:
                        self.callback(update["callback_query"])
            except Exception:
                LOG.exception("Telegram update %s failed and was isolated", update["update_id"])
            finally:
                # A poison update must not block every later button press. Any
                # database mutation already committed by the handler stays idempotent.
                self.store.set_meta("telegram_offset", next_offset)
    @staticmethod
    def _assessment_warning(reason):
        reasons = set(str(reason or '').split('|'))
        security_failed = bool(reasons.intersection({
            'security_check_unavailable', 'security_unsupported',
            'security_unknown', 'security_cache_stale',
        }))
        market_failed = 'market_check_unavailable' in reasons
        return ('⚠️ 無法判定是否爲惡意/詐騙交易 注意甄別\n'
                if security_failed and market_failed else '')

    def _notification_text(self, row: Any, state: str | None = None) -> str:
        state = state or (
            "orphaned" if row["orphaned"] else
            ("filtered" if row["filtered"] else row["confirmation_state"])
        )
        title = {
            "pending": "交易未確認 ⏳",
            "confirmed": "交易已確認 ✅",
            "orphaned": "交易已失效（區塊重組）",
        }.get(state, "交易狀態未知")
        arrow = "↓ 收到" if row["direction"] == "in" else "↑ 支出"
        label = str(row["label"] or row["address"])
        if len(label) > 160:
            label = label[:157] + "…"
        watched = self._explorer_link(
            row["chain"], "address", row["address"], self._short_identifier(row["address"]),
            code_fallback=True,
        )
        counterparty = (
            self._explorer_link(
                row["chain"], "address", row["counterparty"],
                self._short_identifier(row["counterparty"]), code_fallback=True,
            )
            if row["counterparty"] else "交易內多方地址"
        )
        symbol = html.escape(row["symbol"])
        if row["asset_id"] != "native":
            symbol = self._explorer_link(
                row["chain"], "token", row["asset_id"], row["symbol"],
            )
        transaction = self._explorer_link(
            row["chain"], "tx", row["txid"], self._short_identifier(row["txid"]),
            code_fallback=True,
        )
        timestamp = row["block_timestamp"] or row["created_at"]
        amount = str(row["amount_raw"]) if not row["metadata_complete"] else self._amount(
            row["amount_raw"], row["decimals"]
        )
        if state == "filtered":
            return (
                f"<b>{html.escape(self._chain_label(row['chain']))} "
                f"{html.escape(self._filter_reason(row['filter_reason']))}</b>\n"
                f"{html.escape(label)} {arrow} <b>{amount} {symbol}"
                f"{self._filter_warning(row['filter_reason'])}</b>\n"
                f"交易：{transaction}\n"
                f"時間：{self._display_time(timestamp)}"
            )
        return (
                f"<b>{html.escape(self._chain_label(row['chain']))} {title}</b>\n"
                f"{self._assessment_warning(row['filter_reason'])}"
                f"{arrow} <b>{amount} {symbol}</b>\n"
                f"{html.escape(label)}：{watched}\n"
                f"對手：{counterparty}\n"
                f"交易：{transaction}\n"
                f"時間：{self._display_time(timestamp)}"
            )
    def flush_notifications(self, store: Store) -> None:
        self.notification_dispatcher.flush(store)
    def register_commands(self) -> None:
        self.telegram.call("setMyCommands", {"commands": [
            {"command": "start", "description": "開啟按鈕主選單"},
        ]})
        self.telegram.call("setMyCommands", {
            "scope": {"type": "chat", "chat_id": self.owner},
            "commands": [
            {"command": "start", "description": "開啟按鈕主選單"},
            {"command": "info", "description": "顯示固定技術設定"},
            {"command": "user", "description": "所有者管理授權用戶"},
        ]})
