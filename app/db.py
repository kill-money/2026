import asyncpg
from typing import Iterable, Tuple


class Postgres:
    def __init__(self, dsn: str):
        self._dsn = dsn
        self.pool: asyncpg.Pool | None = None

    async def connect(self) -> None:
        self.pool = await asyncpg.create_pool(self._dsn, min_size=5, max_size=20)

    async def close(self) -> None:
        if self.pool:
            await self.pool.close()

    async def write_messages(self, rows: Iterable[Tuple]):
        if not rows:
            return
        assert self.pool is not None
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                await conn.executemany(
                    """
                    INSERT INTO messages (ts, chat_id, msg_id, phone, tag, confidence, sender, text, live)
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
                    ON CONFLICT (chat_id, msg_id) DO NOTHING
                    """,
                    list(rows),
                )
