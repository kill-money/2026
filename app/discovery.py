import asyncio
import logging
from typing import List

from telethon import TelegramClient
from telethon.tl.functions.contacts import SearchRequest
from telethon.tl.types import Channel

from .db import Postgres
from .config import Settings

log = logging.getLogger(__name__)


class Discovery:
    def __init__(self, client: TelegramClient):
        self.client = client
        self.settings = Settings.from_env()
        self.db = Postgres(self.settings.pg_dsn)

    async def init(self):
        await self.db.connect()

    async def discover(self, keywords: List[str]):
        await self.init()

        for kw in keywords:
            try:
                result = await self.client(SearchRequest(q=kw, limit=50))
            except Exception as e:
                log.error("search failed for %s: %s", kw, e)
                continue

            for chat in result.chats:
                if not isinstance(chat, Channel):
                    continue

                if not chat.broadcast and not chat.megagroup:
                    continue

                username = getattr(chat, 'username', None)
                chat_id = getattr(chat, 'id', None)

                if not username and not chat_id:
                    continue

                async with self.db.pool.acquire() as conn:
                    await conn.execute(
                        """
                        INSERT INTO channel_targets
                        (chat_id, username, title, entity_type, source_keyword, status)
                        VALUES ($1,$2,$3,$4,$5,'discovered')
                        ON CONFLICT DO NOTHING
                        """,
                        chat_id,
                        username,
                        getattr(chat, 'title', None),
                        'channel' if chat.broadcast else 'group',
                        kw
                    )

            await asyncio.sleep(2)
