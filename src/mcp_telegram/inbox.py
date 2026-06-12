"""
Inbox buffer for Telegram MCP Bridge.
Persistent Telethon connection with event handler for new messages.
"""
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

from .rate_limiter import create_rate_limiter_from_settings, get_rate_limiter

logger = logging.getLogger(__name__)

# Target chat and topic from environment
TARGET_CHAT: int = int(os.environ.get("TG_TARGET_CHAT", "0"))
TARGET_THREAD: int = int(os.environ.get("TG_TARGET_THREAD", "0"))
MAX_INBOX: int = int(os.environ.get("TG_MAX_INBOX", "100"))

SESSION_DIR: Path = xdg_state_home() / "mcp-telegram"
SESSION_DIR.mkdir(parents=True, exist_ok=True)

_persistent_client: TelegramClient | None = None
_inbox: deque[dict] = deque(maxlen=MAX_INBOX)
_listener_task: asyncio.Task | None = None


class InboxMessage:
    """Represents a message in the inbox buffer."""
    
    def __init__(self, msg: Message):
        self.id: int = msg.id
        self.chat_id: int = msg.chat_id
        self.thread_id: int = getattr(msg, 'reply_to', None) or 0
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
    """Start the persistent Telegram listener with inbox event handler.

    Uses its own TelegramClient (not shared with per-tool clients)
    to maintain a persistent connection for push notifications.
    """
    global _persistent_client, _listener_task

    if _persistent_client is not None:
        logger.info("Listener already running")
        return

    api_id = int(os.environ.get("TELEGRAM_API_ID", "0"))
    api_hash = os.environ.get("TELEGRAM_API_HASH", "")
    if not api_id or not api_hash:
        logger.error("TELEGRAM_API_ID and TELEGRAM_API_HASH must be set")
        return

    _persistent_client = TelegramClient(
        SESSION_DIR / "inbox_session",
        api_id,
        api_hash,
        base_logger="telethon_inbox",
        flood_sleep_threshold=0,
    )
    client = _persistent_client

    await client.start()
    logger.info("Telegram listener connected (started with update loop)")

    @client.on(events.NewMessage)
    async def handle_new_message(event: events.NewMessage.Event) -> None:
        msg = event.message
        if not msg or not msg.text:
            return

        # Filter by target chat
        # Telethon strips -100 prefix for broadcast/megagroup IDs
        chat_id = msg.chat_id
        if TARGET_CHAT and chat_id != TARGET_CHAT:
            return

        # Filter by target thread (topic)
        # In forum topics: reply_to_msg_id = topic root, reply_to_top_id = topic root (if replying)
        thread_id = 0
        reply_to = getattr(msg, 'reply_to', None)
        if reply_to is not None and hasattr(reply_to, 'forum_topic') and reply_to.forum_topic:
            thread_id = (getattr(reply_to, 'reply_to_top_id', None) or getattr(reply_to, 'reply_to_msg_id', 0))

        if TARGET_THREAD and thread_id != TARGET_THREAD:
            return

        # Store in inbox
        inbox_msg = InboxMessage(msg)
        _inbox.append(inbox_msg.to_dict())
        logger.info("Inbox: stored message %d from chat %s: %.60s",
                     msg.id, msg.chat_id, msg.text or "")

    # Register the handler
    client.add_event_handler(handle_new_message, events.NewMessage)

    # Keep connection alive
    _listener_task = asyncio.create_task(_keep_alive(client))

    logger.info("Inbox listener started for chat=%s thread=%s",
                TARGET_CHAT or "all", TARGET_THREAD or "all")


async def _keep_alive(client: TelegramClient) -> None:
    """Keep the client connected and handle reconnection."""
    try:
        await client.run_until_disconnected()
    except asyncio.CancelledError:
        logger.info("Inbox listener cancelled")
    except Exception as e:
        logger.error("Inbox listener disconnected: %s", e)
    finally:
        logger.info("Inbox listener stopped")


async def stop_listener() -> None:
    """Stop the persistent listener."""
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
    """Get and clear all pending inbox messages."""
    items = list(_inbox)
    _inbox.clear()
    return items


def peek_pending() -> int:
    """Get count of pending messages without consuming."""
    return len(_inbox)
