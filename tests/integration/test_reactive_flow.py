import asyncio
import json
import pytest
import pytest_asyncio
from unittest.mock import AsyncMock, MagicMock
from tests.conftest import make_event


@pytest.mark.asyncio
async def test_bridge_pushes_message_to_opencode(inbox, respx_mock):
    from src.mcp_telegram.inbox_bridge import InboxBridge

    respx_mock.get("http://localhost:7777/session").respond(
        json=[{"id": "ses_test", "time": {"updated": 1000}}],
    )
    push_route = respx_mock.post("http://localhost:7777/session/ses_test/prompt_async").respond(
        status_code=204,
    )

    bridge = InboxBridge(inbox=inbox, topic_map=[(100, 205, 7777)], retry_max=1, retry_delay=0.05)
    bridge_task = asyncio.create_task(bridge.start())

    await inbox.handle(make_event(100, 205, 1, "hello from bridge"))
    await asyncio.sleep(0.3)

    assert push_route.called
    sent = json.loads(push_route.calls.last.request.content)
    prompt = json.loads(sent["prompt"])
    assert prompt["message_count"] == 1
    assert prompt["messages"][0]["text"] == "hello from bridge"

    remaining = await inbox.peek(100, 205)
    assert remaining == []

    bridge_task.cancel()


@pytest.mark.asyncio
async def test_bridge_handles_session_rediscovery(inbox, respx_mock):
    from src.mcp_telegram.inbox_bridge import InboxBridge

    respx_mock.get("http://localhost:7777/session").respond(
        json=[{"id": "ses_v2", "time": {"updated": 2000}}],
    )
    push_fail = respx_mock.post("http://localhost:7777/session/ses_old/prompt_async").respond(
        status_code=404,
    )
    push_ok = respx_mock.post("http://localhost:7777/session/ses_v2/prompt_async").respond(
        status_code=204,
    )

    bridge = InboxBridge(inbox=inbox, topic_map=[(100, 205, 7777)], retry_max=1, retry_delay=0.05)

    session_id = await bridge._get_session_id(7777)
    assert session_id == "ses_v2"

    ok = await bridge._push(7777, "ses_old", [{"id": 1, "text": "hi"}])
    assert ok is True
    assert push_fail.called
    assert push_ok.called


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

    msgs = await inbox2.wait(200, 310)
    assert len(msgs) == 1
    assert msgs[0]["text"] == "survive restart"
    assert msgs[0]["id"] == 5


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
