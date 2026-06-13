from __future__ import annotations

import asyncio
import json
import logging

import httpx

logger = logging.getLogger(__name__)

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


class InboxBridge:
    def __init__(
        self,
        inbox: "InboxEngine",
        topic_map: list[tuple[int, int, int]],
        retry_max: int = 3,
        retry_delay: float = 2.0,
    ) -> None:
        self.inbox = inbox
        self.topic_map = topic_map
        self.retry_max = retry_max
        self.retry_delay = retry_delay

    async def start(self) -> None:
        if not self.topic_map:
            logger.warning("topic_map is empty, bridge has no topics to watch")
            return
        tasks = [
            self.watch(chat_id, topic_id, port)
            for chat_id, topic_id, port in self.topic_map
        ]
        await asyncio.gather(*tasks)

    async def watch(self, chat_id: int, topic_id: int, opencode_port: int) -> None:
        while True:
            try:
                msgs = await self.inbox.wait(chat_id, topic_id)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error("inbox.wait crashed: %s", e)
                await asyncio.sleep(5)
                continue

            if not msgs:
                continue

            session_id = await self._get_session_id(opencode_port)
            if not session_id:
                logger.warning(
                    "No opencode session on port %d, msgs stay in store",
                    opencode_port,
                )
                await self.inbox.clear_ram_buffer(chat_id, topic_id)
                continue

            success = await self._push(opencode_port, session_id, msgs)
            if success:
                await self._ack(chat_id, topic_id, msgs[-1]["id"])
            else:
                logger.error("Push failed after retries, msgs stay in store")
                await self.inbox.clear_ram_buffer(chat_id, topic_id)

    async def _get_session_id(self, port: int) -> str | None:
        try:
            async with httpx.AsyncClient() as client:
                r = await client.get(
                    f"http://localhost:{port}/session",
                    timeout=3.0,
                )
                sessions = r.json()
                if not sessions:
                    return None
                sessions.sort(key=lambda s: s.get("time", {}).get("updated", 0), reverse=True)
                return sessions[0]["id"]
        except Exception as e:
            logger.warning("Cannot reach opencode API on port %d: %s", port, e)
            return None

    async def _push(self, port: int, session_id: str, msgs: list[dict]) -> bool:
        prompt = json.dumps({
            "priority_rules": PRIORITY_RULES,
            "message_count": len(msgs),
            "messages": msgs,
        }, ensure_ascii=False)

        current_session = session_id
        attempt = 0
        while attempt < self.retry_max:
            try:
                async with httpx.AsyncClient() as client:
                    r = await client.post(
                        f"http://localhost:{port}/session/{current_session}/prompt_async",
                        json={"parts": [{"type": "text", "text": prompt}]},
                        timeout=5.0,
                    )
                    if r.status_code == 204:
                        return True
                    if r.status_code == 404:
                        logger.warning(
                            "Session %s not found on port %d, rediscovering...",
                            current_session, port,
                        )
                        new_id = await self._get_session_id(port)
                        if new_id and new_id != current_session:
                            current_session = new_id
                            continue
                        return False
                    logger.warning(
                        "Push HTTP %d attempt %d/%d",
                        r.status_code, attempt + 1, self.retry_max,
                    )
            except Exception as e:
                logger.warning("Push error attempt %d/%d: %s", attempt + 1, self.retry_max, e)
            
            attempt += 1
            if attempt < self.retry_max:
                await asyncio.sleep(self.retry_delay)
        return False

    async def _ack(self, chat_id: int, topic_id: int, last_id: int) -> None:
        await self.inbox.ack(chat_id, topic_id, last_id)
