from __future__ import annotations

import asyncio
import logging
from collections import defaultdict, deque

logger = logging.getLogger(__name__)

INBOX_MAXLEN = 200
WAIT_TIMEOUT = 25.0


class InboxEngine:
    def __init__(self, store, maxlen: int = INBOX_MAXLEN):
        self.store = store
        self.maxlen = maxlen
        self._buffers: dict[tuple, deque] = defaultdict(
            lambda: deque(maxlen=self.maxlen)
        )
        self._events: dict[tuple, asyncio.Event] = defaultdict(asyncio.Event)
        self._lock = asyncio.Lock()

    async def restore_from_store(self) -> int:
        total = 0
        health = await self.store.health_check()
        for fname, stat in health.items():
            if stat["corrupt"] > 0:
                logger.error("CORRUPT records in %s: %d", fname, stat["corrupt"])
            prefix = "inbox_"
            body = fname.replace(prefix, "").replace(".jsonl", "")
            last_sep = body.rfind("_")
            if last_sep == -1:
                continue
            chat_id = int(body[:last_sep])
            topic_id = int(body[last_sep + 1:])
            msgs = await self.store.read_all(chat_id, topic_id)
            if msgs:
                key = (chat_id, topic_id)
                async with self._lock:
                    for msg in msgs:
                        self._buffers[key].append(msg)
                    self._events[key].set()
                total += len(msgs)
                logger.info(
                    "Restored %d messages for chat=%d topic=%d",
                    len(msgs), chat_id, topic_id,
                )
        return total

    async def handle(self, event) -> None:
        msg = event.message
        if not msg or not msg.text:
            return
        chat_id = msg.chat_id
        r = getattr(msg, "reply_to", None)
        topic_id = (
            getattr(r, "reply_to_top_id", None)
            or getattr(r, "reply_to_msg_id", None)
            or 0
        )
        entry = {
            "id": msg.id,
            "text": msg.text or "",
            "from": str(msg.sender_id),
            "ts": int(msg.date.timestamp()),
        }
        key = (chat_id, topic_id)
        await self.store.append(chat_id, topic_id, entry)
        async with self._lock:
            buf = self._buffers[key]
            if len(buf) == self.maxlen:
                logger.warning(
                    "inbox overflow chat=%d topic=%d dropping oldest",
                    chat_id, topic_id,
                )
            buf.append(entry)
            self._events[key].set()

    async def peek(self, chat_id: int, topic_id: int) -> list:
        key = (chat_id, topic_id)
        async with self._lock:
            return list(self._buffers.get(key, []))

    async def wait(
        self, chat_id: int, topic_id: int,
        timeout: float = WAIT_TIMEOUT,
    ) -> list:
        key = (chat_id, topic_id)
        ev = self._events[key]
        ev.clear()
        async with self._lock:
            existing = list(self._buffers.get(key, []))
            if existing:
                return existing
        try:
            await asyncio.wait_for(ev.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            return []
        async with self._lock:
            return list(self._buffers.get(key, []))

    async def ack(self, chat_id: int, topic_id: int, last_id: int) -> int:
        key = (chat_id, topic_id)
        async with self._lock:
            buf = self._buffers.get(key)
            if not buf:
                return 0
            before = len(buf)
            remaining = deque(
                (m for m in buf if m["id"] > last_id),
                maxlen=self.maxlen,
            )
            self._buffers[key] = remaining
        store_dropped = await self.store.ack(chat_id, topic_id, last_id)
        return store_dropped
