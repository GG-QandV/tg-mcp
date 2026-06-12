from __future__ import annotations

import asyncio
import logging
import os
from collections import deque
from datetime import datetime
from pathlib import Path
from typing import Optional

from telethon import TelegramClient, events
from telethon.tl.types import Message
from xdg_base_dirs import xdg_state_home

logger = logging.getLogger(__name__)

TARGET_CHAT: int = int(os.environ.get("TG_TARGET_CHAT", "0"))
TARGET_THREAD: int = int(os.environ.get("TG_TARGET_THREAD", "0"))
MAX_INBOX: int = int(os.environ.get("TG_MAX_INBOX", "100"))

SESSION_DIR: Path = xdg_state_home() / "mcp-telegram"
SESSION_DIR.mkdir(parents=True, exist_ok=True)
SESSION_PATH: str = str(SESSION_DIR / "mcp_telegram_session")
for f in SESSION_DIR.glob("*.session-journal"):
    try:
        f.unlink()
    except OSError:
        pass

_persistent_client: TelegramClient | None = None
_inbox: deque[dict] = deque(maxlen=MAX_INBOX)
_listener_task: asyncio.Task | None = None


class InboxMessage:
    def __init__(self, msg: Message):
        self.id: int = msg.id
        self.chat_id: int = msg.chat_id
        reply_to = getattr(msg, 'reply_to', None)
        if reply_to is not None:
            self.thread_id: int = getattr(reply_to, 'reply_to_top_id', 0) or getattr(reply_to, 'reply_to_msg_id', 0)
        else:
            self.thread_id: int = 0
        self.sender_id: int = msg.sender_id or 0
        self.text: str = msg.text or ""
        self.date: datetime = msg.date
        self.has_media: bool = bool(msg.media)

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


async def start_listener() -> None:
    global _persistent_client, _listener_task

    if _persistent_client is not None:
        logger.info("Listener already running")
        return

    api_id = int(os.environ.get("TELEGRAM_API_ID", "0"))
    api_hash = os.environ.get("TELEGRAM_API_HASH", "")
    if not api_id or not api_hash:
        logger.error("TELEGRAM_API_ID and TELEGRAM_API_HASH must be set")
        return

    # Use same session as tools (mcp_telegram_session.session)
    _persistent_client = TelegramClient(
        SESSION_PATH, api_id, api_hash,
        base_logger="telethon_inbox",
        flood_sleep_threshold=0,
    )
    client = _persistent_client

    try:
        await client.start()
    except Exception as e:
        logger.error("Inbox listener start failed: %s", e)
        _persistent_client = None
        return

    logger.info("Inbox listener connected")

    @client.on(events.NewMessage)
    async def handle_new_message(event: events.NewMessage.Event) -> None:
        msg = event.message
        if not msg or not msg.text:
            return

        if TARGET_CHAT and msg.chat_id != TARGET_CHAT:
            return

        thread_id = 0
        reply_to = getattr(msg, 'reply_to', None)
        if reply_to is not None:
            if getattr(reply_to, 'forum_topic', False):
                thread_id = getattr(reply_to, 'reply_to_top_id', 0) or getattr(reply_to, 'reply_to_msg_id', 0)
            else:
                thread_id = getattr(reply_to, 'reply_to_top_id', 0)

        if TARGET_THREAD and thread_id != TARGET_THREAD:
            return

        _inbox.append(InboxMessage(msg).to_dict())
        logger.info("Inbox: stored msg %d from chat %s: %.60s",
                     msg.id, msg.chat_id, msg.text or "")

    _listener_task = asyncio.create_task(_keep_alive(client))
    logger.info("Inbox listener active for chat=%s thread=%s",
                TARGET_CHAT or "all", TARGET_THREAD or "all")


async def _keep_alive(client: TelegramClient) -> None:
    retries = 0
    while not asyncio.current_task().cancelled():
        try:
            await client.run_until_disconnected()
        except asyncio.CancelledError:
            logger.info("Inbox listener cancelled")
            break
        except Exception as e:
            retries += 1
            if retries > 5:
                logger.error("Inbox listener max retries exceeded: %s", e)
                break
            wait = min(2 ** retries, 60)
            logger.warning("Inbox listener disconnected (retry %d/5, wait %ds): %s", retries, wait, e)
            await asyncio.sleep(wait)
            try:
                await client.connect()
            except Exception as conn_err:
                logger.error("Inbox listener reconnect failed: %s", conn_err)
    logger.info("Inbox listener stopped")


async def stop_listener() -> None:
    global _persistent_client, _listener_task

    if _listener_task and not _listener_task.done():
        _listener_task.cancel()
        try:
            await _listener_task
        except asyncio.CancelledError:
            pass

    if _persistent_client:
        await _persistent_client.disconnect()
        _persistent_client = None

    _listener_task = None
    logger.info("Inbox listener stopped")


def get_pending() -> list[dict]:
    items = list(_inbox)
    _inbox.clear()
    return items


def peek_pending() -> int:
    return len(_inbox)
