import asyncio
import logging

from .db import Postgres
from .config import Settings

log = logging.getLogger(__name__)


class Scheduler:
    def __init__(self):
        self.settings = Settings.from_env()
        self.db = Postgres(self.settings.pg_dsn)

    async def init(self):
        await self.db.connect()

    async def assign_channels(self):
        async with self.db.pool.acquire() as conn:
            sessions = await conn.fetch("SELECT id FROM sessions WHERE status='idle'")
            channels = await conn.fetch("SELECT DISTINCT chat_id FROM messages LIMIT 500")

            if not sessions:
                return

            idx = 0
            for ch in channels:
                sid = sessions[idx % len(sessions)]["id"]
                await conn.execute(
                    "INSERT INTO channel_assignments(chat_id, session_id) VALUES($1,$2) ON CONFLICT(chat_id) DO NOTHING",
                    ch["chat_id"], sid
                )
                idx += 1

            log.info("assigned %d channels", len(channels))

    async def run(self):
        await self.init()
        while True:
            try:
                await self.assign_channels()
                await asyncio.sleep(30)
            except Exception as e:
                log.error("scheduler error: %s", e)
                await asyncio.sleep(5)
