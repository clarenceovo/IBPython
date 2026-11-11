"""Topic-based pub/sub broker for realtime feed streams.

Production model:

- A topic key identifies one upstream data stream: channel plus contract
  identity plus stream parameters (for example ticks on ES/FUT/CME with
  ALL_LAST).
- The first subscriber starts the upstream. The last unsubscribe stops it.
  Refcounting means N clients on one topic share one upstream subscription
  and one IBKR market-data line.
- Upstream events publish to the topic once. The broker fans the frame out
  to every subscriber queue. Each subscriber owns its queue, so one slow
  client drops only its own frames; fast clients are never blocked.
- Every published frame carries a per-topic sequence number. A client that
  sees a gap knows it dropped frames (its own queue overflowed).

The broker is transport-agnostic: subscribers are delivery callbacks plus an
integer id. The WebSocket router registers one callback per connection.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)

TopicKey = tuple[str, ...]

StartUpstream = Callable[[Callable[[Any], None]], Awaitable[Callable[[], Awaitable[None]]]]
"""Start the upstream stream; receives the topic publish callable and returns a cleanup coroutine."""


def topic_name(key: TopicKey) -> str:
    """Render a topic key as a dotted string for frames and logs."""
    return ".".join(str(part) for part in key)


def _now_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


@dataclass
class _Subscriber:
    subscriber_id: int
    deliver: Callable[[dict[str, Any]], None]


@dataclass
class _Topic:
    key: TopicKey
    channel: str
    meta: dict[str, Any] = field(default_factory=dict)
    seq: int = 0
    published: int = 0
    subscribers: dict[int, _Subscriber] = field(default_factory=dict)
    upstream_cleanup: Callable[[], Awaitable[None]] | None = None
    started_at: str = field(default_factory=_now_iso)
    next_subscriber_id: int = 1

    def status(self) -> dict[str, Any]:
        info: dict[str, Any] = {
            "topic": topic_name(self.key),
            "channel": self.channel,
            "subscribers": len(self.subscribers),
            "seq": self.seq,
            "published": self.published,
            "started_at": self.started_at,
        }
        info.update(self.meta)
        return info


class MarketDataBroker:
    """Fan-out registry that maps topics to refcounted upstream streams."""

    def __init__(self) -> None:
        self._topics: dict[TopicKey, _Topic] = {}
        self._lock = asyncio.Lock()

    # ------------------------------------------------------------------
    # Subscriber lifecycle
    # ------------------------------------------------------------------

    async def attach(
        self,
        key: TopicKey,
        *,
        channel: str,
        start_upstream: StartUpstream,
        deliver: Callable[[dict[str, Any]], None],
        meta: dict[str, Any] | None = None,
    ) -> tuple[int, int]:
        """Register one subscriber on a topic; start the upstream when first.

        Returns ``(subscriber_id, subscriber_count)`` after the attach.
        """
        async with self._lock:
            topic = self._topics.get(key)
            if topic is None:
                topic = _Topic(key=key, channel=channel, meta=dict(meta or {}))
                self._topics[key] = topic

        if not topic.subscribers:
            publish = self._make_publish(topic)
            topic.upstream_cleanup = await start_upstream(publish)
            logger.info("broker: upstream started topic=%s", topic_name(key))

        subscriber = _Subscriber(subscriber_id=0, deliver=deliver)
        async with self._lock:
            subscriber.subscriber_id = topic.next_subscriber_id
            topic.next_subscriber_id += 1
            topic.subscribers[subscriber.subscriber_id] = subscriber
        return subscriber.subscriber_id, len(topic.subscribers)

    async def detach(self, key: TopicKey, subscriber_id: int) -> int:
        """Remove one subscriber; stop the upstream when the last one leaves.

        Returns the remaining subscriber count. A topic with zero remaining
        subscribers is removed from the registry after its upstream stops.
        """
        async with self._lock:
            topic = self._topics.get(key)
            if topic is None:
                return 0
            topic.subscribers.pop(subscriber_id, None)
            remaining = len(topic.subscribers)
            if remaining:
                return remaining
            self._topics.pop(key, None)

        cleanup = topic.upstream_cleanup
        if cleanup is not None:
            try:
                await cleanup()
            except Exception:
                logger.warning("broker: upstream cleanup failed topic=%s", topic_name(key), exc_info=True)
        logger.info("broker: upstream stopped topic=%s (published=%d)", topic_name(key), topic.published)
        return 0

    # ------------------------------------------------------------------
    # Fan-out
    # ------------------------------------------------------------------

    def _make_publish(self, topic: _Topic) -> Callable[[Any], None]:
        def publish(data: Any) -> None:
            topic.seq += 1
            topic.published += 1
            frame = {
                "type": topic.channel,
                "topic": topic_name(topic.key),
                "seq": topic.seq,
                "data": data,
                "server_ts": _now_iso(),
            }
            for subscriber in list(topic.subscribers.values()):
                try:
                    subscriber.deliver(frame)
                except Exception:
                    logger.exception(
                        "broker: subscriber delivery failed topic=%s subscriber=%d",
                        topic_name(topic.key), subscriber.subscriber_id,
                    )

        return publish

    # ------------------------------------------------------------------
    # Introspection
    # ------------------------------------------------------------------

    def topic_status(self) -> list[dict[str, Any]]:
        """Snapshot every live topic with its subscriber count and sequence."""
        return [topic.status() for topic in self._topics.values()]

    def subscriber_count(self, key: TopicKey) -> int:
        topic = self._topics.get(key)
        return len(topic.subscribers) if topic is not None else 0

    def active_topic_count(self) -> int:
        return len(self._topics)

    def reset(self) -> None:
        """Drop all topic state without running cleanups. Test helper."""
        self._topics.clear()


# Single-process singleton. Sticky sessions are required for multi-worker
# deployments, exactly like the SSE subscription registry in streaming.py.
broker = MarketDataBroker()
