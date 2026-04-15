import asyncio
import logging

from telethon import TelegramClient, events

from .db import Postgres
from .config import Settings
from .redis_gateway import RedisGateway

log = logging.getLogger(__name__)


class SessionWorker:
    def __init__(self, session_row):
        self.settings = Settings.from_env()
        self.session = session_row
        self.client = TelegramClient(
            session_row["session_name"],
            session_row["api_id"],
            session_row["api_hash"],
        )
        self.redis = RedisGateway(
            self.settings.redis_url,
            self.settings.redis_stream_name,
            self.settings.redis_dedup_ttl,
        )
        self.db = Postgres(self.settings.pg_dsn)

    async def load_channels(self):
        async with self.db.pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT chat_id FROM channel_assignments WHERE session_id=$1",
                self.session["id"],
            )
            return [r["chat_id"] for r in rows]

    async def run(self):
        await self.db.connect()
        await self.client.start(phone=self.session["phone"])

        channels = await self.load_channels()
        log.info("session %s handling %d channels", self.session["session_name"], len(channels))

        @self.client.on(events.NewMessage(chats=channels if channels else None))
        async def handler(event):
            try:
                msg = event.message
                record = {
                    "ts": int(msg.date.timestamp()),
                    "chat_id": int(msg.chat_id),
                    "msg_id": int(msg.id),
                    "text": msg.message,
                    "sender": str(msg.sender_id or ""),
                    "live": 1,
                }

                if await self.redis.is_duplicate(record["chat_id"], record["msg_id"]):
                    return

                await self.redis.publish(record)
            except Exception as e:
                log.error("session handler error: %s", e)

        await self.client.run_until_disconnected()
