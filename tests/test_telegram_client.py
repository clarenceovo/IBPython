"""Tests for TelegramLogHandler."""

from __future__ import annotations

import json
import logging
import time
from unittest.mock import MagicMock, patch

from src.transport.telegram_client import TelegramLogHandler


class TestTelegramLogHandler:
    def _make_handler(self, **kwargs):
        return TelegramLogHandler(
            bot_token="123:ABC",
            chat_id="-100123",
            min_interval=0,  # disable dedup in tests
            **kwargs,
        )

    def test_handler_is_logging_handler(self):
        h = self._make_handler()
        assert isinstance(h, logging.Handler)

    def test_format_includes_level_and_name(self):
        h = self._make_handler()
        record = logging.LogRecord(
            name="test.module",
            level=logging.ERROR,
            pathname="",
            lineno=0,
            msg="something broke",
            args=(),
            exc_info=None,
        )
        formatted = h.format(record)
        assert "ERROR" in formatted
        assert "test.module" in formatted
        assert "something broke" in formatted

    @patch("src.transport.telegram_client.TelegramLogHandler._send")
    def test_emit_fires_send_on_warning(self, mock_send):
        h = self._make_handler(level=logging.WARNING)
        logger = logging.getLogger("test_emit_warn")
        logger.addHandler(h)
        logger.setLevel(logging.WARNING)
        try:
            logger.warning("test message")
            # _send runs in a thread, give it a moment
            time.sleep(0.3)
            assert mock_send.called
        finally:
            logger.removeHandler(h)

    @patch("src.transport.telegram_client.TelegramLogHandler._send")
    def test_emit_ignores_debug(self, mock_send):
        h = self._make_handler(level=logging.WARNING)
        logger = logging.getLogger("test_emit_debug")
        logger.addHandler(h)
        logger.setLevel(logging.DEBUG)
        try:
            logger.debug("ignored")
            time.sleep(0.2)
            assert not mock_send.called
        finally:
            logger.removeHandler(h)

    def test_dedup_suppresses_repeat(self):
        h = TelegramLogHandler(
            bot_token="123:ABC",
            chat_id="-100123",
            min_interval=60.0,  # long dedup window
        )
        record = logging.LogRecord(
            name="x",
            level=logging.WARNING,
            pathname="",
            lineno=0,
            msg="repeat",
            args=(),
            exc_info=None,
        )
        with patch("src.transport.telegram_client.TelegramLogHandler._send") as mock_send:
            h.emit(record)  # first should fire
            h.emit(record)  # second should be deduped
            # Only the sync dedup check matters here — the thread is started for first
            assert mock_send.call_count == 1

    @patch("urllib.request.urlopen")
    def test_send_posts_to_telegram_api(self, mock_urlopen):
        mock_resp = MagicMock()
        mock_resp.status = 200
        mock_resp.__enter__ = MagicMock(return_value=mock_resp)
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_resp

        h = self._make_handler()
        h._send("<b>ERROR</b> test")

        assert mock_urlopen.called
        req = mock_urlopen.call_args[0][0]
        assert req.get_method() == "POST"
        body = json.loads(req.data)
        assert body["chat_id"] == "-100123"
        assert "ERROR" in body["text"]
        assert body["parse_mode"] == "HTML"

    @patch("urllib.request.urlopen")
    def test_send_truncates_long_messages(self, mock_urlopen):
        mock_resp = MagicMock()
        mock_resp.status = 200
        mock_resp.__enter__ = MagicMock(return_value=mock_resp)
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_resp

        h = self._make_handler()
        long_msg = "x" * 5000
        h._send(long_msg)

        body = json.loads(mock_urlopen.call_args[0][0].data)
        assert len(body["text"]) <= 4096
