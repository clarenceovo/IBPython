"""Telegram logging handler — forwards WARNING+ logs to a Telegram chat.

Uses the Bot API (no extra deps).  Add to any stdlib logger::

    from src.transport.telegram_client import TelegramLogHandler

    handler = TelegramLogHandler(
        bot_token="123456:ABC-DEF...",
        chat_id="-1001234567890",
    )
    logging.getLogger().addHandler(handler)

All send calls are fire-and-forget with error logging to stderr — they never
block the event loop or the calling thread.
"""

from __future__ import annotations

import json
import logging
import threading
import urllib.error
import urllib.request
from typing import Any

__all__ = ["TelegramLogHandler"]

_TELEGRAM_API = "https://api.telegram.org/bot{token}/sendMessage"
_MAX_MSG_LEN = 4096  # Telegram hard limit


class TelegramLogHandler(logging.Handler):
    """Non-blocking ``logging.Handler`` that POSTs formatted records to Telegram.

    Parameters
    ----------
    bot_token:
        Bot token from ``@BotFather``.
    chat_id:
        Target chat / channel ID (integer or string).
    level:
        Minimum log level (default ``logging.WARNING``).
    fmt:
        Optional format string.  Defaults to a compact one-liner.
    min_interval:
        Minimum seconds between sends for the *same* message text (dedup).
        Set to ``0`` to disable.
    """

    def __init__(
        self,
        *,
        bot_token: str,
        chat_id: str | int,
        level: int = logging.WARNING,
        fmt: str | None = None,
        min_interval: float = 5.0,
    ) -> None:
        super().__init__(level)
        self._bot_token = bot_token
        self._chat_id = str(chat_id)
        self._url = _TELEGRAM_API.format(token=bot_token)
        self._min_interval = min_interval
        self._last_sent: dict[str, float] = {}
        self._lock = threading.Lock()

        if fmt is None:
            fmt = "<b>%(levelname)s</b> [%(name)s]\n%(message)s"
        self.setFormatter(logging.Formatter(fmt))

    # ------------------------------------------------------------------
    # logging.Handler interface
    # ------------------------------------------------------------------

    def emit(self, record: logging.LogRecord) -> None:
        """Format and send *record* to Telegram (fire-and-forget)."""
        try:
            text = self.format(record)
        except Exception:
            text = record.getMessage()

        # Dedup: skip if identical message was sent very recently.
        if self._min_interval > 0:
            import time as _time

            key = text[:128]
            now = _time.monotonic()
            with self._lock:
                last = self._last_sent.get(key, 0.0)
                if now - last < self._min_interval:
                    return
                self._last_sent[key] = now

            # Periodic purge of old keys (every ~100 emits)
            if len(self._last_sent) > 500:
                with self._lock:
                    cutoff = now - self._min_interval * 2
                    self._last_sent = {
                        k: v for k, v in self._last_sent.items() if v > cutoff
                    }

        # Fire-and-forget in a daemon thread — never block the caller.
        t = threading.Thread(
            target=self._send,
            args=(text,),
            daemon=True,
            name="tg-log",
        )
        t.start()

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _send(self, html: str) -> None:
        """POST to Telegram Bot API.  Runs in a background thread."""
        # Truncate to Telegram limit
        if len(html) > _MAX_MSG_LEN:
            html = html[: _MAX_MSG_LEN - 3] + "..."

        payload = json.dumps(
            {
                "chat_id": self._chat_id,
                "text": html,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            },
        ).encode("utf-8")

        req = urllib.request.Request(
            self._url,
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )

        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                if resp.status >= 400:
                    body = resp.read(256).decode("utf-8", errors="replace")
                    self._stderr(f"Telegram API error {resp.status}: {body}")
        except Exception as exc:
            self._stderr(f"Telegram send failed: {exc}")

    @staticmethod
    def _stderr(msg: str) -> None:
        logging.getLogger("telegram_client").warning("%s", msg)


# ------------------------------------------------------------------
# Async variant for use inside an event loop
# ------------------------------------------------------------------


class AsyncTelegramLogHandler(logging.Handler):
    """Async version that uses ``aiohttp`` (or fallback to sync) for sends.

    If ``aiohttp`` is not installed, falls back to the sync thread-based send.
    """

    def __init__(
        self,
        *,
        bot_token: str,
        chat_id: str | int,
        level: int = logging.WARNING,
        fmt: str | None = None,
        min_interval: float = 5.0,
    ) -> None:
        super().__init__(level)
        self._bot_token = bot_token
        self._chat_id = str(chat_id)
        self._min_interval = min_interval
        self._last_sent: dict[str, float] = {}
        self._session: Any = None  # aiohttp.ClientSession
        self._background_tasks: set = set()

        if fmt is None:
            fmt = "<b>%(levelname)s</b> [%(name)s]\n%(message)s"
        self.setFormatter(logging.Formatter(fmt))

    async def _get_session(self) -> Any:
        if self._session is None or self._session.closed:
            try:
                import aiohttp

                self._session = aiohttp.ClientSession()
            except ImportError:
                return None
        return self._session

    async def _async_send(self, html: str) -> None:
        session = await self._get_session()
        if session is None:
            return
        import aiohttp

        url = _TELEGRAM_API.format(token=self._bot_token)
        payload: dict[str, Any] = {
            "chat_id": self._chat_id,
            "text": html[:_MAX_MSG_LEN],
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }
        try:
            async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status >= 400:
                    body = await resp.text()
                    logging.getLogger("telegram_client").warning("API error %s: %s", resp.status, body)
        except Exception as exc:
            logging.getLogger("telegram_client").warning("send failed: %s", exc)

    def emit(self, record: logging.LogRecord) -> None:
        try:
            text = self.format(record)
        except Exception:
            text = record.getMessage()

        # Dedup
        if self._min_interval > 0:
            import time as _time

            key = text[:128]
            now = _time.monotonic()
            last = self._last_sent.get(key, 0.0)
            if now - last < self._min_interval:
                return
            self._last_sent[key] = now

        # Try scheduling on the running loop; fall back to sync thread.
        try:
            import asyncio

            loop = asyncio.get_running_loop()
            task = loop.create_task(self._async_send(text))
            # Prevent GC of fire-and-forget task
            self._background_tasks.add(task)
            task.add_done_callback(self._background_tasks.discard)
        except RuntimeError:
            # No running loop — use sync fallback
            TelegramLogHandler(
                bot_token=self._bot_token,
                chat_id=self._chat_id,
            )._send(text)

    async def close_session(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None
