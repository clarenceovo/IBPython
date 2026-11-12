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
- Topic lifecycle transitions (upstream start on first attach, stop on last
  detach) are serialized under one broker lock. Fan-out publishing is
  lock-free and never waits on the lifecycle lock.

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
    starting: bool = False
    start_failed: bool = False
    start_evt: asyncio.Event = field(default_factory=asyncio.Event)

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
        # Per-key stop-in-flight guards: a dying topic's cleanup runs OUTSIDE
        # the lock, so a replacement attach must wait for it (feed stop calls
        # are keyed by symbol and would kill the replacement stream too).
        self._stopping: dict[TopicKey, asyncio.Event] = {}

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

        Registry mutation runs under the lock; the upstream start (network
        I/O) runs OUTSIDE it so one stalled IBKR call cannot freeze every
        other topic's lifecycle. Invariants are preserved with per-topic
        guards: concurrent attaches on one new topic wait on the creator's
        start result; a failed start rolls the topic back atomically.
        A replacement attach on a key whose stop is still in flight waits
        for the stop, then retries (feed stops are keyed by symbol).

        Returns ``(subscriber_id, subscriber_count)`` after the attach.
        """
        while True:
            async with self._lock:
                stop_evt = self._stopping.get(key)
                if stop_evt is None:
                    topic = self._topics.get(key)
                    if topic is None:
                        topic = _Topic(key=key, channel=channel, meta=dict(meta or {}))
                        self._topics[key] = topic
                        owner = True
                    else:
                        owner = False
                    topic.subscribers[topic.next_subscriber_id] = _Subscriber(
                        subscriber_id=topic.next_subscriber_id, deliver=deliver
                    )
                    subscriber_id = topic.next_subscriber_id
                    topic.next_subscriber_id += 1
                    if owner:
                        topic.starting = True
                        await_outside = None
                        break
                    elif topic.starting:
                        # Ride on the creator's in-flight start outside the lock.
                        await_outside = topic.start_evt
                        break
                    else:
                        return subscriber_id, len(topic.subscribers)
            if stop_evt is not None:
                await stop_evt.wait()
                continue

        if owner:
            try:
                cleanup = await start_upstream(self._make_publish(topic))
            except BaseException:
                async with self._lock:
                    self._topics.pop(key, None)
                    topic.starting = False
                    topic.start_failed = True
                    topic.start_evt.set()
                raise
            async with self._lock:
                topic.upstream_cleanup = cleanup
                topic.starting = False
                topic.start_evt.set()
                logger.info("broker: upstream started topic=%s", topic_name(key))
                return subscriber_id, len(topic.subscribers)

        # Rider path: wait for the creator's start to finish. (The owner path
        # always returns above; await_outside is only set for riders.)
        if await_outside is not None:
            await await_outside.wait()
        async with self._lock:
            alive = self._topics.get(key) is topic and subscriber_id in topic.subscribers
            if topic.start_failed or not alive:
                if alive:
                    topic.subscribers.pop(subscriber_id, None)
                    if not topic.subscribers:
                        self._topics.pop(key, None)
                raise RuntimeError(f"upstream start failed for topic {topic_name(key)}")
            return subscriber_id, len(topic.subscribers)

    async def detach(self, key: TopicKey, subscriber_id: int) -> int:
        """Remove one subscriber; stop the upstream when the last one leaves.

        Registry mutation runs under the lock; the upstream cleanup (network
        I/O) runs OUTSIDE it. A per-key in-flight guard makes replacement
        attaches wait for the stop so the old cleanup (keyed by symbol at
        the feed layer) cannot kill a freshly started replacement stream.

        Returns the remaining subscriber count. A topic with zero remaining
        subscribers is removed from the registry before its upstream stops.
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
            if key in self._stopping:
                return 0
            stop_evt = asyncio.Event()
            self._stopping[key] = stop_evt
            cleanup = topic.upstream_cleanup

        if cleanup is not None:
            try:
                await cleanup()
            except Exception:
                logger.warning("broker: upstream cleanup failed topic=%s", topic_name(key), exc_info=True)
        async with self._lock:
            if self._stopping.get(key) is stop_evt:
                self._stopping.pop(key, None)
            stop_evt.set()
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
