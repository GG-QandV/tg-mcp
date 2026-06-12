import asyncio
import json
import pytest
import pytest_asyncio
from unittest.mock import AsyncMock, MagicMock
from tests.conftest import make_event


@pytest_asyncio.fixture
async def running_server(tmp_sock_path, inbox, mock_telegram_client):
    from src.mcp_telegram.ipc_server import IPCServer
    srv = IPCServer(inbox, mock_telegram_client)
    task = asyncio.create_task(srv.start(tmp_sock_path))
    await asyncio.sleep(0.05)
    yield tmp_sock_path
    task.cancel()


@pytest_asyncio.fixture
async def running_server_with_store(tmp_path, mock_telegram_client):
    from src.mcp_telegram.inbox_store import InboxStore
    from src.mcp_telegram.inbox import InboxEngine
    from src.mcp_telegram.ipc_server import IPCServer, get_sock_path

    store = InboxStore(str(tmp_path / "store"))
    inbox = InboxEngine(store=store, maxlen=10)
    sock = str(tmp_path / "tgmcpd.sock")
    srv = IPCServer(inbox, mock_telegram_client)
    task = asyncio.create_task(srv.start(sock))
    await asyncio.sleep(0.05)
    yield sock, inbox, store
    task.cancel()


@pytest.mark.asyncio
async def test_message_triggers_inbox_subscribe(running_server, inbox):
    from src.mcp_telegram.ipc_client import IPCClient

    client = IPCClient(running_server)
    await client.connect()

    wait_task = asyncio.create_task(
        client.call("inbox_wait", {
            "chat_id": 100, "topic_id": 205, "timeout": 10.0,
        })
    )

    await asyncio.sleep(0.05)
    await inbox.handle(make_event(100, 205, 1, "hello from tg"))

    result = await asyncio.wait_for(wait_task, timeout=5.0)
    msgs = result["messages"]
    assert len(msgs) == 1
    assert msgs[0]["text"] == "hello from tg"
    assert msgs[0]["id"] == 1

    await client.close()


@pytest.mark.asyncio
async def test_message_survives_daemon_restart(tmp_path, mock_telegram_client):
    from src.mcp_telegram.inbox_store import InboxStore
    from src.mcp_telegram.inbox import InboxEngine

    store = InboxStore(str(tmp_path / "store"))
    inbox1 = InboxEngine(store=store, maxlen=10)

    await inbox1.handle(make_event(200, 310, 5, "survive restart"))

    inbox2 = InboxEngine(store=store, maxlen=10)
    restored = await inbox2.restore_from_store()
    assert restored == 1

    msgs = await inbox2.wait(200, 310, timeout=1.0)
    assert len(msgs) == 1
    assert msgs[0]["text"] == "survive restart"
    assert msgs[0]["id"] == 5


@pytest.mark.asyncio
async def test_concurrent_topics_isolated(running_server, inbox):
    from src.mcp_telegram.ipc_client import IPCClient

    client_205 = IPCClient(running_server)
    await client_205.connect()

    client_310 = IPCClient(running_server)
    await client_310.connect()

    wait_205 = asyncio.create_task(
        client_205.call("inbox_wait", {
            "chat_id": 100, "topic_id": 205, "timeout": 2.0,
        })
    )
    wait_310 = asyncio.create_task(
        client_310.call("inbox_wait", {
            "chat_id": 100, "topic_id": 310, "timeout": 2.0,
        })
    )

    await asyncio.sleep(0.05)
    await inbox.handle(make_event(100, 310, 10, "only for 310"))

    result_310 = await asyncio.wait_for(wait_310, timeout=5.0)
    assert len(result_310["messages"]) == 1
    assert result_310["messages"][0]["text"] == "only for 310"

    result_205 = await asyncio.wait_for(wait_205, timeout=5.0)
    assert result_205["messages"] == []

    await client_205.close()
    await client_310.close()


@pytest.mark.asyncio
async def test_ack_clears_store_and_buffer(inbox, inbox_store):
    await inbox.handle(make_event(100, 205, 1, "msg1"))
    await inbox.handle(make_event(100, 205, 2, "msg2"))

    assert len(await inbox.peek(100, 205)) == 2
    assert len(await inbox_store.read_all(100, 205)) == 2

    dropped = await inbox.ack(100, 205, 2)
    assert dropped == 2

    assert await inbox.peek(100, 205) == []
    assert await inbox_store.read_all(100, 205) == []
