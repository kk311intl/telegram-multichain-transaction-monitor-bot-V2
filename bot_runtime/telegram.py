from __future__ import annotations

import json
import logging
import time
import http.client
import io
import threading
import secrets
import urllib.error
import urllib.request
from typing import Any
from .telegram_rate import TelegramRate


TELEGRAM_TRANSPORT_ERRORS = (
    urllib.error.URLError,
    TimeoutError,
    http.client.HTTPException,
    ConnectionError,
    OSError,
    json.JSONDecodeError,
)


class TelegramError(RuntimeError):
    def __init__(self, message: str, status: int = 0, retry_after: int = 0):
        super().__init__(message)
        self.status = status
        self.retry_after = retry_after


class Telegram:
    def __init__(self, token: str, state_path=None, settings=None):
        self.base = f"https://api.telegram.org/bot{token}/"
        self.rate = TelegramRate(state_path,settings=settings)
        self.local = threading.local()

    def flood(self, method, retry, chat=None, description=''):
        retry = max(1, retry or 60)
        self.rate.cooldown(retry, chat=chat, poll=method=='getUpdates')
        token = self.base.split('/bot',1)[1].rstrip('/')
        description = str(description).replace(token,'[redacted]')[:240]
        record = {'at':time.time(),'method':method,'chat':chat,'retry_after':retry,'description':description}
        self.rate.record_flood(record)
        logging.getLogger('crypto-address-monitor').warning('Telegram flood method=%s chat=%s retry_after=%s description=%r',method,chat,retry,description)

    def _request(self, request):
        # Reuse TLS per worker. Never automatically retry an uncertain POST.
        connection = getattr(self.local, 'connection', None)
        if connection is None:
            connection = http.client.HTTPSConnection('api.telegram.org', timeout=20)
            self.local.connection = connection
        try:
            if connection.sock is None:
                connection.connect()  # TLS setup does not hold the shared write gate.
            self.rate.transmit(getattr(self.local, 'chat', None),
                lambda:connection.request('POST', request.selector, body=request.data, headers=dict(request.header_items())),
                poll=getattr(self.local,'poll',False),guard=getattr(self.local,'guard',None))
            response = connection.getresponse()
            body = response.read(8*1024*1024+1)
            if len(body) > 8*1024*1024:
                raise ValueError('Telegram response too large')
            if response.status >= 400:
                raise urllib.error.HTTPError(request.full_url, response.status, response.reason, response.headers, io.BytesIO(body))
            return io.BytesIO(body)
        except Exception:
            connection.close()
            self.local.connection = None
            raise

    def call(self, method: str, payload: dict[str, Any], guard=None) -> Any:
        chat = payload.get('chat_id',getattr(self.rate.local,'interactive_chat',None))
        self.local.chat = chat
        self.local.poll = method=='getUpdates'
        self.local.guard = guard
        self.rate.acquire(chat,
                          poll=method=='getUpdates')
        if guard is not None and not guard():
            from .telegram_rate import TelegramThrottle
            raise TelegramThrottle('recipient no longer authorized')
        request = urllib.request.Request(
            self.base + method,
            data=json.dumps(payload).encode(),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with self._request(request) as response:
                result = json.load(response)
        except urllib.error.HTTPError as exc:
            description = "HTTP error"
            retry_after = 0
            try:
                body = json.loads(exc.read().decode("utf-8", "replace"))
                description = str(body.get("description", description))[:240]
                retry_after = int((body.get("parameters") or {}).get("retry_after", 0) or 0)
            except Exception:
                pass
            if not retry_after:
                try:
                    retry_after = int(exc.headers.get("Retry-After", "0") or 0)
                except (TypeError, ValueError):
                    retry_after = 0
            if exc.code == 429:
                self.flood(method,retry_after,chat,description)
            raise TelegramError(
                f"Telegram {method} rejected request: {description}", exc.code, retry_after
            ) from exc
        except TELEGRAM_TRANSPORT_ERRORS as exc:
            raise TelegramError(f"Telegram {method} failed: {type(exc).__name__}") from exc
        if not result.get("ok"):
            status = int(result.get('error_code', 0))
            retry = int((result.get('parameters') or {}).get('retry_after', 0))
            if status == 429:
                self.flood(method,retry,chat,result.get('description',''))
            raise TelegramError(f"Telegram {method} rejected request", status, retry)
        return result.get("result")

    def send(
        self, chat_id: int, text: str,
        reply_markup: dict[str, Any] | None = None, guard=None,
    ) -> Any:
        payload: dict[str, Any] = {
            "chat_id": chat_id, "text": text, "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }
        if reply_markup:
            payload["reply_markup"] = reply_markup
        return self.call("sendMessage", payload, guard=guard)

    def send_document(
        self, chat_id: int, filename: str, content: bytes, caption: str,
    ) -> Any:
        self.local.chat = chat_id
        self.local.poll = False
        self.local.guard = None
        self.rate.acquire(chat_id)
        boundary = "CodexBoundary" + secrets.token_hex(12)
        parts = [
            f"--{boundary}\r\nContent-Disposition: form-data; name=\"chat_id\"\r\n\r\n{chat_id}\r\n".encode(),
            f"--{boundary}\r\nContent-Disposition: form-data; name=\"caption\"\r\n\r\n{caption}\r\n".encode("utf-8"),
            (
                f"--{boundary}\r\nContent-Disposition: form-data; name=\"document\"; "
                f"filename=\"{filename}\"\r\nContent-Type: application/json\r\n\r\n"
            ).encode(),
            content,
            f"\r\n--{boundary}--\r\n".encode(),
        ]
        request = urllib.request.Request(
            self.base + "sendDocument", data=b"".join(parts),
            headers={"Content-Type": f"multipart/form-data; boundary={boundary}"},
            method="POST",
        )
        try:
            with self._request(request) as response:
                result = json.load(response)
        except urllib.error.HTTPError as exc:
            retry = 0
            try:
                retry = int((json.loads(exc.read()).get('parameters') or {}).get('retry_after', 0))
            except (ValueError, TypeError, AttributeError):
                pass
            if exc.code == 429:
                self.flood('sendDocument',retry,chat_id)
            raise TelegramError("Telegram sendDocument rejected request", exc.code, retry) from exc
        except TELEGRAM_TRANSPORT_ERRORS as exc:
            raise TelegramError(f"Telegram sendDocument failed: {type(exc).__name__}") from exc
        if not result.get("ok"):
            status = int(result.get('error_code',0))
            retry = int((result.get('parameters') or {}).get('retry_after',0))
            if status == 429:
                self.flood('sendDocument',retry,chat_id,result.get('description',''))
            raise TelegramError("Telegram sendDocument rejected request",status,retry)
        return result.get("result")

    def download_file(self, file_id: str, max_bytes: int = 1_048_576) -> bytes:
        info = self.call("getFile", {"file_id": file_id})
        if not isinstance(info, dict) or not info.get("file_path"):
            raise TelegramError("Telegram getFile returned no path")
        request = urllib.request.Request(
            self.base.replace("/bot", "/file/bot", 1) + str(info["file_path"])
        )
        try:
            with urllib.request.urlopen(request, timeout=20) as response:
                declared = int(response.headers.get("Content-Length", "0") or 0)
                if declared > max_bytes:
                    raise ValueError("備份檔案不可超過 1 MiB")
                content = response.read(max_bytes + 1)
        except TELEGRAM_TRANSPORT_ERRORS[:-1] as exc:
            raise TelegramError(f"Telegram file download failed: {type(exc).__name__}") from exc
        if len(content) > max_bytes:
            raise ValueError("備份檔案不可超過 1 MiB")
        return content
