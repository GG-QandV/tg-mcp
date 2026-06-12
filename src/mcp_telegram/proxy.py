from __future__ import annotations

import asyncio
import json
import logging
import os
import sys

from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import TextContent, Tool

from .ipc_client import IPCClient, get_sock_path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def _require_env(name: str) -> str:
    val = os.environ.get(name)
    if not val:
        sys.exit(f"ERROR: {name} env var is required")
    return val


TG_TOPIC_ID = int(_require_env("TG_TOPIC_ID"))
TG_CHAT_ID = int(_require_env("TG_CHAT_ID"))
TG_AGENT_NAME = os.environ.get("TG_AGENT_NAME", "Agent")

PRIORITY_RULES = """⚡ INBOX ALERT — ПРИОРИТЕТ №1 ⚡
Ты получил новые сообщения. Немедленно:
1. ЗАМОРОЗЬ текущую задачу (запомни состояние)
2. ПРОЧИТАЙ все сообщения ниже
3. ОПРЕДЕЛИ тип каждого:
   - ОТМЕНА ("стоп"/"отмена"/"не надо") → остановить задачу, подтвердить
   - УТОЧНЕНИЕ/КОНТЕКСТ → применить к текущей задаче, продолжить
   - НОВОЕ ЗАДАНИЕ → завершить/отложить текущее, взять новое
4. ОТВЕТЬ в топик подтверждение что получил
5. ДЕЙСТВУЙ согласно типу
Игнорирование этих правил недопустимо.
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"""

INBOX_SUBSCRIBE_TIMEOUT = 25.0

server = Server("tg-mcp-proxy")
_ipc: IPCClient | None = None


async def get_ipc() -> IPCClient:
    global _ipc
    if _ipc is None:
        _ipc = IPCClient(get_sock_path())
        await _ipc.connect()
    return _ipc


@server.list_tools()
async def list_tools() -> list[Tool]:
    return [
        Tool(
            name="send_message",
            description="Send message to Telegram topic",
            inputSchema={
                "type": "object",
                "properties": {"text": {"type": "string"}},
                "required": ["text"],
            },
        ),
        Tool(
            name="inbox_read",
            description="Read and acknowledge new messages from topic",
            inputSchema={"type": "object", "properties": {}},
        ),
        Tool(
            name="inbox_wait",
            description="Block until a new message arrives, then return it",
            inputSchema={
                "type": "object",
                "properties": {
                    "timeout": {
                        "type": "number",
                        "description": "Max seconds to wait (default 60)",
                    },
                },
            },
        ),
        Tool(
            name="inbox_subscribe",
            description="Subscribe to new messages with priority envelope (blocking)",
            inputSchema={"type": "object", "properties": {}},
        ),
    ]


@server.call_tool()
async def call_tool(name: str, arguments: dict) -> list[TextContent]:
    ipc = await get_ipc()

    if name == "send_message":
        from datetime import datetime
        now = datetime.now().strftime("%d.%m.%Y %H:%M")
        signed = f"**{TG_AGENT_NAME}** · {now}\n\n{arguments['text']}"
        result = await ipc.call("send_message", {
            "chat_id": TG_CHAT_ID,
            "topic_id": TG_TOPIC_ID,
            "text": signed,
        })
        return [TextContent(
            type="text",
            text=f"sent message_id={result['message_id']}"
        )]

    elif name == "inbox_read":
        peek_result = await ipc.call("inbox_peek", {
            "chat_id": TG_CHAT_ID,
            "topic_id": TG_TOPIC_ID,
        })
        msgs = peek_result.get("messages", [])

        if msgs:
            last_id = msgs[-1]["id"]
            await ipc.call("inbox_ack", {
                "chat_id": TG_CHAT_ID,
                "topic_id": TG_TOPIC_ID,
                "last_id": last_id,
            })

        return [TextContent(
            type="text",
            text=json.dumps(msgs, ensure_ascii=False, indent=2) if msgs else "[]",
        )]

    elif name == "inbox_wait":
        timeout = arguments.get("timeout", 60.0)
        wait_result = await ipc.call("inbox_wait", {
            "chat_id": TG_CHAT_ID,
            "topic_id": TG_TOPIC_ID,
            "timeout": timeout,
        })
        msgs = wait_result.get("messages", [])

        if msgs:
            last_id = msgs[-1]["id"]
            await ipc.call("inbox_ack", {
                "chat_id": TG_CHAT_ID,
                "topic_id": TG_TOPIC_ID,
                "last_id": last_id,
            })

        return [TextContent(
            type="text",
            text=json.dumps(msgs, ensure_ascii=False, indent=2) if msgs else "[]",
        )]

    elif name == "inbox_subscribe":
        result = await ipc.call("inbox_wait", {
            "chat_id": TG_CHAT_ID,
            "topic_id": TG_TOPIC_ID,
            "timeout": INBOX_SUBSCRIBE_TIMEOUT,
        })
        msgs = result.get("messages", [])

        if not msgs:
            return [TextContent(type="text", text="__INBOX_TIMEOUT__")]

        last_id = msgs[-1]["id"]
        await ipc.call("inbox_ack", {
            "chat_id": TG_CHAT_ID,
            "topic_id": TG_TOPIC_ID,
            "last_id": last_id,
        })

        envelope = {
            "priority_rules": PRIORITY_RULES,
            "message_count": len(msgs),
            "messages": msgs,
        }
        return [TextContent(
            type="text",
            text=json.dumps(envelope, ensure_ascii=False, indent=2),
        )]

    raise ValueError(f"Unknown tool: {name}")


async def run() -> None:
    async with stdio_server() as (r, w):
        await server.run(r, w, server.create_initialization_options())


if __name__ == "__main__":
    asyncio.run(run())
