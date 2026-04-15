import asyncio
import os
import time
import logging

from telethon import TelegramClient, events

from .config import Settings
from .redis_gateway import RedisGateway

log = logging.getLogger(__name__)


class CollectorV2:
    def __init__(self):
        self.settings = Settings.from_env()
        self.api_id = int(os.environ["TG_API_ID"])
        self.api_hash = os.environ["TG_API_HASH"]
        self.phone = os.environ["TG_PHONE"]
        self.session_name = os.environ.get("TG_SESSION", "collector")
        self.client = TelegramClient(self.session_name, self.api_id, self.api_hash)
        self.redis = RedisGateway(
            self.settings.redis_url,
            self.settings.redis_stream_name,
            self.settings.redis_dedup_ttl,
        )

    def _record(self, msg, is_live: bool):
        if not msg or not msg.id or not msg.chat_id or not (msg.message or "").strip():
            return None
        return {
            "ts": int(msg.date.timestamp()) if msg.date else int(time.time()),
            "chat_id": int(msg.chat_id),
            "msg_id": int(msg.id),
            "text": msg.message,
            "sender": str(msg.sender_id or ""),
            "live": 1 if is_live else 0,
        }

    async def _process(self, msg, is_live: bool):
        record = self._record(msg, is_live)
        if not record:
            return
        if await self.redis.is_duplicate(record["chat_id"], record["msg_id"]):
            return
        await self.redis.publish(record)

    def start_live(self, chats: list[int]):
        @self.client.on(events.NewMessage(chats=chats if chats else None))
        async def handler(event):
            try:
                if event.is_channel:
                    await self._process(event.message, True)
            except Exception as e:
                log.error("live handler error: %s", e)

    async def backfill_dialogs(self, limit_per_dialog: int = 800):
        async for dialog in self.client.iter_dialogs(limit=200):
            if not getattr(dialog.entity, "broadcast", False):
                continue
            async for msg in self.client.iter_messages(dialog.entity, limit=limit_per_dialog, reverse=False):
                await self._process(msg, False)
                await asyncio.sleep(0.05)

    async def run(self):
        await self.client.start(phone=self.phone)
        log.info("collector started")
        self.start_live([])
        await self.backfill_dialogs()
        await self.client.run_until_disconnected()
