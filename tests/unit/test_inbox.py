import logging
import pytest
from tests.conftest import make_event


@pytest.mark.asyncio
async def test_handle_stores_message(inbox):
    event = make_event(chat_id=100, topic_id=205, msg_id=1, text="hello")
    await inbox.handle(event)
    msgs = await inbox.peek(100, 205)
    assert len(msgs) == 1
    assert msgs[0]["text"] == "hello"
    assert msgs[0]["id"] == 1


@pytest.mark.asyncio
async def test_peek_does_not_drain(inbox):
    event = make_event(100, 205, 1, "hello")
    await inbox.handle(event)
    await inbox.peek(100, 205)
    msgs = await inbox.peek(100, 205)
    assert len(msgs) == 1


@pytest.mark.asyncio
async def test_ack_removes_up_to_last_id(inbox):
    for i in range(1, 6):
        await inbox.handle(make_event(100, 205, i, f"msg{i}"))

    dropped = await inbox.ack(100, 205, last_id=3)
    assert dropped == 3
    remaining = await inbox.peek(100, 205)
    assert [m["id"] for m in remaining] == [4, 5]


@pytest.mark.asyncio
async def test_ack_all(inbox):
    for i in range(1, 4):
        await inbox.handle(make_event(100, 205, i, f"msg{i}"))
    await inbox.ack(100, 205, last_id=999)
    assert await inbox.peek(100, 205) == []


@pytest.mark.asyncio
async def test_ack_empty_buffer(inbox):
    dropped = await inbox.ack(100, 205, last_id=1)
    assert dropped == 0


@pytest.mark.asyncio
async def test_peek_empty(inbox):
    assert await inbox.peek(999, 999) == []


@pytest.mark.asyncio
async def test_different_chats_isolated(inbox):
    await inbox.handle(make_event(chat_id=100, topic_id=0, msg_id=1, text="chat_A"))
    await inbox.handle(make_event(chat_id=200, topic_id=0, msg_id=2, text="chat_B"))

    msgs_a = await inbox.peek(100, 0)
    msgs_b = await inbox.peek(200, 0)

    assert len(msgs_a) == 1 and msgs_a[0]["text"] == "chat_A"
    assert len(msgs_b) == 1 and msgs_b[0]["text"] == "chat_B"


@pytest.mark.asyncio
async def test_different_topics_isolated(inbox):
    await inbox.handle(make_event(100, 205, 1, "topic_205"))
    await inbox.handle(make_event(100, 310, 2, "topic_310"))

    assert len(await inbox.peek(100, 205)) == 1
    assert len(await inbox.peek(100, 310)) == 1


@pytest.mark.asyncio
async def test_topic_id_zero_no_collision(inbox):
    await inbox.handle(make_event(100, 0, 1, "from_chat_100"))
    await inbox.handle(make_event(200, 0, 2, "from_chat_200"))

    assert await inbox.peek(100, 0) != await inbox.peek(200, 0)


@pytest.mark.asyncio
async def test_overflow_drops_oldest(inbox):
    for i in range(15):
        await inbox.handle(make_event(100, 205, i, f"msg{i}"))
    msgs = await inbox.peek(100, 205)
    assert len(msgs) == 10
    assert msgs[0]["id"] == 5


@pytest.mark.asyncio
async def test_overflow_logs_warning(inbox, caplog):
    with caplog.at_level(logging.WARNING, logger="src.mcp_telegram.inbox"):
        for i in range(12):
            await inbox.handle(make_event(100, 205, i, f"msg{i}"))
    assert any("overflow" in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_handle_reply_to_none(inbox):
    event = make_event(100, 0, 1, "no_thread")
    event.message.reply_to = None
    await inbox.handle(event)
    msgs = await inbox.peek(100, 0)
    assert len(msgs) == 1
