from __future__ import annotations

import logging
from collections import defaultdict, deque

logger = logging.getLogger(__name__)

INBOX_MAXLEN = 200


class InboxEngine:
    def __init__(self, maxlen: int = INBOX_MAXLEN):
        self.maxlen = maxlen
        self._buffers: dict[tuple[int, int], deque] = defaultdict(
            lambda: deque(maxlen=self.maxlen)
        )
        self._lock = __import__("asyncio").Lock()

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
            "id":   msg.id,
            "text": msg.text or "",
            "from": str(msg.sender_id),
            "ts":   int(msg.date.timestamp()),
        }
        key = (chat_id, topic_id)
        async with self._lock:
            buf = self._buffers[key]
            if len(buf) == self.maxlen:
                logger.warning(
                    "inbox overflow chat=%d topic=%d dropping oldest",
                    chat_id, topic_id
                )
            buf.append(entry)

    async def peek(self, chat_id: int, topic_id: int) -> list:
        key = (chat_id, topic_id)
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
                maxlen=self.maxlen
            )
            self._buffers[key] = remaining
            return before - len(remaining)
