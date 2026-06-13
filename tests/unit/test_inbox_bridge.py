import asyncio
import json
import pytest
from unittest.mock import AsyncMock, MagicMock, PropertyMock, patch

from src.mcp_telegram.inbox_bridge import InboxBridge, PRIORITY_RULES


@pytest.fixture
def inbox():
    m = AsyncMock()
    m.wait = AsyncMock()
    m.ack = AsyncMock()
    return m


@pytest.mark.asyncio
async def test_empty_topic_map_logs_warning(inbox, caplog):
    bridge = InboxBridge(inbox=inbox, topic_map=[])
    with caplog.at_level("WARNING"):
        await bridge.start()
    assert "topic_map is empty" in caplog.text


@pytest.mark.asyncio
async def test_get_session_id_returns_most_recent(inbox):
    bridge = InboxBridge(inbox=inbox, topic_map=[(1, 2, 7777)])

    mock_resp = MagicMock()
    mock_resp.json = MagicMock(return_value=[
        {"id": "old", "time": {"updated": 100}},
        {"id": "new", "time": {"updated": 200}},
    ])

    with patch("httpx.AsyncClient") as mc:
        mc.return_value.__aenter__.return_value.get.return_value = mock_resp
        sid = await bridge._get_session_id(7777)

    assert sid == "new"


@pytest.mark.asyncio
async def test_get_session_id_empty_returns_none(inbox):
    bridge = InboxBridge(inbox=inbox, topic_map=[(1, 2, 7777)])

    mock_resp = MagicMock()
    mock_resp.json = MagicMock(return_value=[])

    with patch("httpx.AsyncClient") as mc:
        mc.return_value.__aenter__.return_value.get.return_value = mock_resp
        sid = await bridge._get_session_id(7777)

    assert sid is None


@pytest.mark.asyncio
async def test_get_session_id_connection_error_returns_none(inbox):
    bridge = InboxBridge(inbox=inbox, topic_map=[(1, 2, 7777)])

    with patch("httpx.AsyncClient") as mc:
        mc.return_value.__aenter__.return_value.get.side_effect = ConnectionError
        sid = await bridge._get_session_id(7777)

    assert sid is None


@pytest.mark.asyncio
async def test_push_success_returns_true(inbox):
    bridge = InboxBridge(inbox=inbox, topic_map=[(1, 2, 7777)])

    with patch("httpx.AsyncClient") as mc:
        mc.return_value.__aenter__.return_value.post.return_value = AsyncMock(status_code=204)
        ok = await bridge._push(7777, "ses_xxx", [{"id": 1, "text": "hi"}])

    assert ok is True


@pytest.mark.asyncio
async def test_push_404_rediscover_session(inbox):
    bridge = InboxBridge(inbox=inbox, topic_map=[(1, 2, 7777)], retry_max=1, retry_delay=0.05)

    async def post_side(*a, **kw):
        r = AsyncMock()
        r.status_code = 404
        return r

    async def get_side(*a, **kw):
        r = MagicMock()
        r.json = MagicMock(return_value=[{"id": "ses_new", "time": {"updated": 999}}])
        return r

    with patch("httpx.AsyncClient") as mc:
        mc.return_value.__aenter__.return_value.post = post_side
        mc.return_value.__aenter__.return_value.get = get_side
        ok = await bridge._push(7777, "ses_old", [{"id": 1, "text": "hi"}])

    assert ok is False


@pytest.mark.asyncio
async def test_push_retries_on_failure(inbox):
    bridge = InboxBridge(inbox=inbox, topic_map=[(1, 2, 7777)], retry_max=2, retry_delay=0.05)

    call_count = 0

    async def post_side(*a, **kw):
        nonlocal call_count
        call_count += 1
        raise ConnectionError("fail")

    with patch("httpx.AsyncClient") as mc:
        mc.return_value.__aenter__.return_value.post = post_side
        ok = await bridge._push(7777, "ses_x", [{"id": 1}])

    assert ok is False
    assert call_count == 2


@pytest.mark.asyncio
async def test_watch_ack_only_after_successful_push(inbox):
    bridge = InboxBridge(inbox=inbox, topic_map=[(1, 2, 7777)], retry_max=1, retry_delay=0.05)
    inbox.wait = AsyncMock(side_effect=[
        [{"id": 5, "text": "hi", "from": "1", "ts": 0}],
        asyncio.CancelledError(),
    ])

    mock_resp = MagicMock(status_code=204)

    with patch("httpx.AsyncClient") as mc:
        mc.return_value.__aenter__.return_value.get.return_value.json = MagicMock(
            return_value=[{"id": "ses_x", "time": {"updated": 999}}]
        )
        mc.return_value.__aenter__.return_value.post.return_value = mock_resp
        with pytest.raises(asyncio.CancelledError):
            await bridge.watch(1, 2, 7777)

    inbox.ack.assert_awaited_once_with(1, 2, 5)


@pytest.mark.asyncio
async def test_watch_no_ack_on_push_failure(inbox):
    bridge = InboxBridge(inbox=inbox, topic_map=[(1, 2, 7777)], retry_max=1, retry_delay=0.05)
    inbox.wait = AsyncMock(side_effect=[
        [{"id": 5, "text": "hi", "from": "1", "ts": 0}],
        asyncio.CancelledError(),
    ])

    with patch("httpx.AsyncClient") as mc:
        mc.return_value.__aenter__.return_value.get.return_value.json = MagicMock(
            return_value=[{"id": "ses_x", "time": {"updated": 999}}]
        )
        mc.return_value.__aenter__.return_value.post.side_effect = ConnectionError
        with pytest.raises(asyncio.CancelledError):
            await bridge.watch(1, 2, 7777)

    inbox.ack.assert_not_called()
