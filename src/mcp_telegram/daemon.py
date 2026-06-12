from __future__ import annotations

import asyncio
import logging
import sys
from pathlib import Path

from telethon import TelegramClient, events

from .inbox import InboxEngine
from .ipc_server import IPCServer, get_sock_path
from .telegram import TelegramSettings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def _check_stale_socket(sock_path: str) -> None:
    p = Path(sock_path)
    if not p.exists():
        return
    try:
        _, writer = await asyncio.wait_for(
            asyncio.open_unix_connection(sock_path), timeout=1.0
        )
        writer.close()
        logger.error("Another tgmcpd instance is already running")
        sys.exit(1)
    except (ConnectionRefusedError, OSError):
        logger.warning("Removing stale socket %s", sock_path)
        p.unlink(missing_ok=True)


async def main() -> None:
    cfg = TelegramSettings()
    sock_path = get_sock_path()

    await _check_stale_socket(sock_path)

    for f in Path(cfg.session_path).parent.glob("*.session-journal"):
        try:
            f.unlink()
        except OSError:
            pass

    client = TelegramClient(cfg.session_path, cfg.api_id, cfg.api_hash)
    await client.start()
    logger.info("Telegram connected")

    inbox = InboxEngine()
    client.add_event_handler(inbox.handle, events.NewMessage)

    ipc = IPCServer(inbox, client)

    try:
        await asyncio.gather(
            ipc.start(sock_path),
            client.run_until_disconnected(),
        )
    finally:
        Path(sock_path).unlink(missing_ok=True)
        logger.info("tgmcpd stopped, socket removed")


if __name__ == "__main__":
    asyncio.run(main())
