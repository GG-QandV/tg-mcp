import asyncio
import json
import pytest
from unittest.mock import AsyncMock
from tests.conftest import make_event


@pytest.mark.asyncio
async def test_dispatch_ping(mock_telegram_client, inbox):
    from src.mcp_telegram.ipc_server import IPCServer
    srv = IPCServer(inbox, mock_telegram_client)
    result = await srv._dispatch({"method": "ping", "params": {}})
    assert result == {"pong": True}


@pytest.mark.asyncio
async def test_dispatch_send_message(mock_telegram_client, inbox):
    from src.mcp_telegram.ipc_server import IPCServer
    srv = IPCServer(inbox, mock_telegram_client)
    result = await srv._dispatch({
        "method": "send_message",
        "params": {"chat_id": 100, "topic_id": 205, "text": "hello"},
    })
    assert result["message_id"] == 999
    mock_telegram_client.send_message.assert_called_once()


@pytest.mark.asyncio
async def test_dispatch_inbox_peek(mock_telegram_client, inbox):
    from src.mcp_telegram.ipc_server import IPCServer
    await inbox.handle(make_event(100, 205, 1, "test"))
    srv = IPCServer(inbox, mock_telegram_client)
    result = await srv._dispatch({
        "method": "inbox_peek",
        "params": {"chat_id": 100, "topic_id": 205},
    })
    assert len(result["messages"]) == 1


@pytest.mark.asyncio
async def test_dispatch_inbox_ack(mock_telegram_client, inbox):
    from src.mcp_telegram.ipc_server import IPCServer
    for i in range(1, 4):
        await inbox.handle(make_event(100, 205, i, f"m{i}"))
    srv = IPCServer(inbox, mock_telegram_client)
    result = await srv._dispatch({
        "method": "inbox_ack",
        "params": {"chat_id": 100, "topic_id": 205, "last_id": 2},
    })
    assert result["dropped"] == 2


@pytest.mark.asyncio
async def test_dispatch_unknown_method(mock_telegram_client, inbox):
    from src.mcp_telegram.ipc_server import IPCServer
    srv = IPCServer(inbox, mock_telegram_client)
    with pytest.raises(ValueError, match="Unknown method"):
        await srv._dispatch({"method": "nonexistent", "params": {}})


@pytest.mark.asyncio
async def test_concurrent_dispatch_not_blocked(mock_telegram_client, inbox):
    from src.mcp_telegram.ipc_server import IPCServer

    async def slow_send(*args, **kwargs):
        await asyncio.sleep(0.3)
        from unittest.mock import MagicMock
        return MagicMock(id=1)

    mock_telegram_client.send_message = slow_send
    srv = IPCServer(inbox, mock_telegram_client)

    t_start = asyncio.get_event_loop().time()

    send_task = asyncio.create_task(srv._dispatch({
        "method": "send_message",
        "params": {"chat_id": 100, "topic_id": 205, "text": "slow"},
    }))
    ping_task = asyncio.create_task(srv._dispatch({
        "method": "ping", "params": {},
    }))

    ping_result = await ping_task
    t_ping = asyncio.get_event_loop().time() - t_start

    assert ping_result == {"pong": True}
    assert t_ping < 0.1

    await send_task


@pytest.mark.asyncio
async def test_dispatch_inbox_wait_returns_on_message(mock_telegram_client, inbox):
    from src.mcp_telegram.ipc_server import IPCServer
    srv = IPCServer(inbox, mock_telegram_client)

    async def delayed_event():
        await asyncio.sleep(0.1)
        await inbox.handle(make_event(100, 205, 1, "trigger"))

    asyncio.create_task(delayed_event())
    result = await srv._dispatch({
        "method": "inbox_wait",
        "params": {"chat_id": 100, "topic_id": 205, "timeout": 5.0},
    })
    assert len(result["messages"]) == 1
    assert result["messages"][0]["id"] == 1


@pytest.mark.asyncio
async def test_dispatch_inbox_wait_timeout_returns_empty(mock_telegram_client, inbox):
    from src.mcp_telegram.ipc_server import IPCServer
    srv = IPCServer(inbox, mock_telegram_client)
    result = await srv._dispatch({
        "method": "inbox_wait",
        "params": {"chat_id": 100, "topic_id": 999, "timeout": 0.1},
    })
    assert result["messages"] == []


@pytest.mark.asyncio
async def test_dispatch_health_returns_status(mock_telegram_client, inbox):
    from src.mcp_telegram.ipc_server import IPCServer
    mock_telegram_client.is_connected = lambda: True
    srv = IPCServer(inbox, mock_telegram_client)
    result = await srv._dispatch({"method": "health", "params": {}})
    assert result["status"] == "ok"
    assert result["telegram"] is True
    assert isinstance(result["store"], dict)
    assert isinstance(result["ram_buffers"], dict)


@pytest.mark.asyncio
async def test_dispatch_inbox_wait_does_not_block_ping(tmp_sock_path, inbox_store):
    """inbox_wait в одном топике НЕ блокирует ping в другом соединении."""
    from src.mcp_telegram.inbox import InboxEngine
    from src.mcp_telegram.ipc_server import IPCServer
    from src.mcp_telegram.ipc_client import IPCClient

    inbox = InboxEngine(store=inbox_store, maxlen=10)
    mock_client = AsyncMock()

    server = IPCServer(inbox, mock_client)
    server_task = asyncio.create_task(server.start(tmp_sock_path))
    await asyncio.sleep(0.05)

    async def wait_client():
        c = IPCClient(tmp_sock_path)
        await c.connect()
        r = await c.call("inbox_wait", {
            "chat_id": 100, "topic_id": 999, "timeout": 5.0,
        })
        await c.close()
        return r

    async def ping_client():
        await asyncio.sleep(0.1)
        c = IPCClient(tmp_sock_path)
        await c.connect()
        t0 = asyncio.get_event_loop().time()
        r = await c.call("ping", {})
        elapsed = asyncio.get_event_loop().time() - t0
        await c.close()
        return r, elapsed

    wait_task = asyncio.create_task(wait_client())
    ping_result, ping_elapsed = await ping_client()

    assert ping_result == {"pong": True}
    assert ping_elapsed < 1.0

    wait_task.cancel()
    server_task.cancel()
