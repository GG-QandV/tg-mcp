import asyncio
import pytest
from unittest.mock import AsyncMock, patch

from src.mcp_telegram.inbox_bridge import InboxBridge, PRIORITY_RULES, _fmt_prompt


# ── helpers ──────────────────────────────────────────────────────────────────

def _make_proc(returncode=0):
    """Return a fake asyncio.subprocess.Process."""
    p = AsyncMock()
    p.returncode = returncode
    p.communicate = AsyncMock(return_value=(b"", b""))
    p.wait = AsyncMock(return_value=returncode)
    return p


@pytest.fixture
def inbox():
    m = AsyncMock()
    m.wait = AsyncMock()
    m.ack = AsyncMock()
    m.clear_ram_buffer = AsyncMock()
    return m


# ── _fmt_prompt ───────────────────────────────────────────────────────────────

def test_fmt_prompt_contains_count_and_text():
    msgs = [{"from": "42", "text": "hello", "id": 1, "ts": 0}]
    out = _fmt_prompt(msgs)
    assert "Новых сообщений: 1" in out
    assert "[42]: hello" in out


def test_fmt_prompt_replaces_newlines():
    msgs = [{"from": "u", "text": "line1\nline2", "id": 1, "ts": 0}]
    out = _fmt_prompt(msgs)
    assert "\n" not in out.split("Превью:")[1].split("\n")[0]


# ── start / empty topic_map ───────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_empty_topic_map_logs_warning(inbox, caplog):
    bridge = InboxBridge(inbox=inbox, topic_map=[])
    with caplog.at_level("WARNING"):
        await bridge.start()
    assert "topic_map is empty" in caplog.text


# ── _pane_exists ──────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_pane_exists_returns_true(inbox):
    bridge = InboxBridge(inbox=inbox, topic_map=[])
    proc = _make_proc(returncode=0)
    with patch("asyncio.create_subprocess_exec", return_value=proc):
        assert await bridge._pane_exists("agent:opencode") is True


@pytest.mark.asyncio
async def test_pane_exists_returns_false_on_nonzero(inbox):
    bridge = InboxBridge(inbox=inbox, topic_map=[])
    proc = _make_proc(returncode=1)
    with patch("asyncio.create_subprocess_exec", return_value=proc):
        assert await bridge._pane_exists("agent:opencode") is False


@pytest.mark.asyncio
async def test_pane_exists_returns_false_on_exception(inbox):
    bridge = InboxBridge(inbox=inbox, topic_map=[])
    with patch("asyncio.create_subprocess_exec", side_effect=OSError("no tmux")):
        assert await bridge._pane_exists("agent:opencode") is False


# ── _do_paste ─────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_do_paste_success(inbox):
    """load-buffer → paste-buffer → send-keys, all succeed → True."""
    bridge = InboxBridge(inbox=inbox, topic_map=[])
    procs = [_make_proc(0), _make_proc(0), _make_proc(0)]
    with patch("asyncio.create_subprocess_exec", side_effect=procs):
        assert await bridge._do_paste("agent:opencode", "hello world") is True


@pytest.mark.asyncio
async def test_do_paste_load_buffer_fails(inbox):
    bridge = InboxBridge(inbox=inbox, topic_map=[])
    # load-buffer fails → return False, no further calls
    p1 = _make_proc(returncode=1)
    p1.communicate = AsyncMock(return_value=(b"", b"err"))
    with patch("asyncio.create_subprocess_exec", side_effect=[p1]) as mock_exec:
        assert await bridge._do_paste("agent:opencode", "hi") is False
    assert mock_exec.call_count == 1


@pytest.mark.asyncio
async def test_do_paste_paste_buffer_fails(inbox):
    bridge = InboxBridge(inbox=inbox, topic_map=[])
    p1 = _make_proc(0)
    p2 = _make_proc(returncode=1)
    p2.communicate = AsyncMock(return_value=(b"", b"err"))
    with patch("asyncio.create_subprocess_exec", side_effect=[p1, p2]) as mock_exec:
        assert await bridge._do_paste("agent:opencode", "hi") is False
    assert mock_exec.call_count == 2


@pytest.mark.asyncio
async def test_do_paste_text_sent_to_load_buffer(inbox):
    """Проверяем что текст передаётся в stdin load-buffer."""
    bridge = InboxBridge(inbox=inbox, topic_map=[])
    procs = [_make_proc(0), _make_proc(0), _make_proc(0)]
    captured = []

    async def fake_exec(*args, **kwargs):
        captured.append(args)
        return procs[len(captured) - 1]

    with patch("asyncio.create_subprocess_exec", side_effect=fake_exec):
        await bridge._do_paste("agent:opencode", "my message")

    # первый вызов — load-buffer
    assert "load-buffer" in captured[0]
    # второй — paste-buffer -p -t agent:opencode (bracketed paste)
    assert "paste-buffer" in captured[1]
    assert "-p" in captured[1]
    assert "agent:opencode" in captured[1]
    # третий — send-keys Enter
    assert "send-keys" in captured[2]
    assert "Enter" in captured[2]


# ── _push_via_tmux (retry) ────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_push_via_tmux_success(inbox):
    bridge = InboxBridge(inbox=inbox, topic_map=[], retry_max=3)
    bridge._do_paste = AsyncMock(return_value=True)
    msgs = [{"id": 1, "text": "hi", "from": "u", "ts": 0}]
    assert await bridge._push_via_tmux("agent:opencode", msgs) is True
    bridge._do_paste.assert_awaited_once()


@pytest.mark.asyncio
async def test_push_via_tmux_retries_and_fails(inbox):
    bridge = InboxBridge(inbox=inbox, topic_map=[], retry_max=3, retry_delay=0.01)
    bridge._do_paste = AsyncMock(return_value=False)
    msgs = [{"id": 1, "text": "hi", "from": "u", "ts": 0}]
    assert await bridge._push_via_tmux("agent:opencode", msgs) is False
    assert bridge._do_paste.await_count == 3


@pytest.mark.asyncio
async def test_push_via_tmux_succeeds_on_second_attempt(inbox):
    bridge = InboxBridge(inbox=inbox, topic_map=[], retry_max=3, retry_delay=0.01)
    bridge._do_paste = AsyncMock(side_effect=[False, True])
    msgs = [{"id": 1, "text": "hi", "from": "u", "ts": 0}]
    assert await bridge._push_via_tmux("agent:opencode", msgs) is True
    assert bridge._do_paste.await_count == 2


# ── watch ─────────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_watch_ack_only_after_successful_push(inbox):
    bridge = InboxBridge(inbox=inbox, topic_map=[(1, 2, "agent:opencode")], retry_max=1)
    inbox.wait = AsyncMock(side_effect=[
        [{"id": 5, "text": "hi", "from": "1", "ts": 0}],
        asyncio.CancelledError(),
    ])
    bridge._pane_exists = AsyncMock(return_value=True)
    bridge._push_via_tmux = AsyncMock(return_value=True)

    with pytest.raises(asyncio.CancelledError):
        await bridge.watch(1, 2, "agent:opencode")

    inbox.ack.assert_awaited_once_with(1, 2, 5)
    inbox.clear_ram_buffer.assert_not_called()


@pytest.mark.asyncio
async def test_watch_no_ack_on_push_failure(inbox):
    bridge = InboxBridge(inbox=inbox, topic_map=[(1, 2, "agent:opencode")], retry_max=1)
    inbox.wait = AsyncMock(side_effect=[
        [{"id": 5, "text": "hi", "from": "1", "ts": 0}],
        asyncio.CancelledError(),
    ])
    bridge._pane_exists = AsyncMock(return_value=True)
    bridge._push_via_tmux = AsyncMock(return_value=False)

    with pytest.raises(asyncio.CancelledError):
        await bridge.watch(1, 2, "agent:opencode")

    inbox.ack.assert_not_called()
    inbox.clear_ram_buffer.assert_awaited_once_with(1, 2)


@pytest.mark.asyncio
async def test_watch_skips_push_when_pane_missing(inbox):
    """УМ-Л: pane не существует → clear_ram_buffer, нет push, нет ack."""
    bridge = InboxBridge(inbox=inbox, topic_map=[(1, 2, "agent:opencode")], retry_max=1)
    inbox.wait = AsyncMock(side_effect=[
        [{"id": 7, "text": "x", "from": "1", "ts": 0}],
        asyncio.CancelledError(),
    ])
    bridge._pane_exists = AsyncMock(return_value=False)
    bridge._push_via_tmux = AsyncMock()

    with pytest.raises(asyncio.CancelledError):
        await bridge.watch(1, 2, "agent:opencode")

    bridge._push_via_tmux.assert_not_called()
    inbox.ack.assert_not_called()
    inbox.clear_ram_buffer.assert_awaited_once_with(1, 2)


@pytest.mark.asyncio
async def test_watch_skips_empty_msgs(inbox):
    bridge = InboxBridge(inbox=inbox, topic_map=[(1, 2, "agent:opencode")], retry_max=1)
    inbox.wait = AsyncMock(side_effect=[
        [],
        asyncio.CancelledError(),
    ])
    bridge._pane_exists = AsyncMock(return_value=True)
    bridge._push_via_tmux = AsyncMock()

    with pytest.raises(asyncio.CancelledError):
        await bridge.watch(1, 2, "agent:opencode")

    bridge._pane_exists.assert_not_called()
    bridge._push_via_tmux.assert_not_called()
