from typing import Iterable, Tuple

from .db import Postgres
from .quality_models import NormalizedMessage


class QualityWriter:
    def __init__(self, db: Postgres):
        self.db = db

    async def write(self, items: Iterable[NormalizedMessage]):
        rows: list[Tuple] = []
        for m in items:
            rows.append((
                m.ts,
                m.chat_id,
                m.msg_id,
                m.phone,
                m.tag,
                m.confidence,
                m.sender,
                m.text,
                m.live,
            ))
        await self.db.write_messages(rows)
