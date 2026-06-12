from __future__ import annotations

import logging
from collections import deque
from datetime import datetime

from telethon.tl.types import Message

logger = logging.getLogger(__name__)

_inbox: deque[dict] = deque(maxlen=100)


class InboxMessage:
    def __init__(self, msg: Message):
        self.id = msg.id
        self.chat_id = msg.chat_id
        reply_to = getattr(msg, 'reply_to', None)
        if reply_to is not None:
            self.thread_id = getattr(reply_to, 'reply_to_top_id', 0) or getattr(reply_to, 'reply_to_msg_id', 0)
        else:
            self.thread_id = 0
        self.sender_id = msg.sender_id or 0
        self.text = msg.text or ""
        self.date = msg.date
        self.has_media = bool(msg.media)

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "chat_id": self.chat_id,
            "thread_id": self.thread_id,
            "sender_id": self.sender_id,
            "text": self.text,
            "date": self.date.isoformat() if self.date else None,
            "has_media": self.has_media,
        }


def push(msg: Message) -> None:
    _inbox.append(InboxMessage(msg).to_dict())


def get_pending() -> list[dict]:
    items = list(_inbox)
    _inbox.clear()
    return items


def peek_pending() -> int:
    return len(_inbox)
