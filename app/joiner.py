import asyncio
import logging
from datetime import datetime, timedelta

from telethon import TelegramClient
from telethon.errors import FloodWaitError, ChannelPrivateError
from telethon.tl.functions.channels import JoinChannelRequest

from .db import Postgres
from .config import Settings

log = logging.getLogger(__name__)


class Joiner:
    def __init__(self, client: TelegramClient, session_name: str):
        self.client = client
        self.session_name = session_name
        self.settings = Settings.from_env()
        self.db = Postgres(self.settings.pg_dsn)

    async def init(self):
        await self.db.connect()

    async def fetch_targets(self, limit=20):
        async with self.db.pool.acquire() as conn:
            return await conn.fetch(
                """
                SELECT * FROM channel_targets
                WHERE status='discovered'
                AND (cooldown_until IS NULL OR cooldown_until < now())
                LIMIT $1
                """,
                limit
            )

    async def mark_result(self, target_id, status, error=None, cooldown=None, chat_id=None):
        async with self.db.pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE channel_targets
                SET status=$2,
                    last_error=$3,
                    cooldown_until=$4,
                    joined_at=CASE WHEN $2='joined' THEN now() ELSE joined_at END,
                    join_attempts = join_attempts + 1,
                    last_join_attempt_at=now()
                WHERE id=$1
                """,
                target_id,
                status,
                error,
                cooldown
            )

            if status == 'joined' and chat_id:
                await conn.execute(
                    """
                    INSERT INTO channel_assignments(chat_id, session_id)
                    SELECT $1, s.id FROM sessions s WHERE s.session_name=$2
                    ON CONFLICT DO NOTHING
                    """,
                    chat_id,
                    self.session_name
                )

    async def join_target(self, target):
        try:
            entity = None
            if target['username']:
                entity = await self.client.get_entity(target['username'])
            elif target['chat_id']:
                entity = await self.client.get_entity(target['chat_id'])
            else:
                return

            await self.client(JoinChannelRequest(entity))

            await self.mark_result(target['id'], 'joined', chat_id=entity.id)

            await asyncio.sleep(30)

        except FloodWaitError as e:
            cooldown = datetime.utcnow() + timedelta(seconds=e.seconds + 60)
            await self.mark_result(target['id'], 'cooldown', error=str(e), cooldown=cooldown)

        except ChannelPrivateError:
            await self.mark_result(target['id'], 'invalid', error='private')

        except Exception as e:
            await self.mark_result(target['id'], 'error', error=str(e))

    async def run(self):
        await self.init()
        while True:
            targets = await self.fetch_targets()
            for t in targets:
                await self.join_target(t)
            await asyncio.sleep(5)
