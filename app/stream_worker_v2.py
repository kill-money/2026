import asyncio
import logging
from typing import List, Tuple

from redis.asyncio import Redis, ConnectionPool

from .config import Settings
from .db import Postgres
from .quality_models import RawMessage
from .quality_processor import QualityProcessor
from .quality_writer import QualityWriter

log = logging.getLogger(__name__)


class StreamWorkerV2:
    def __init__(self):
        self.settings = Settings.from_env()
        self.pool = ConnectionPool.from_url(self.settings.redis_url, decode_responses=True)
        self.redis = Redis(connection_pool=self.pool)
        self.db = Postgres(self.settings.pg_dsn)
        self.processor = QualityProcessor()
        self.writer = QualityWriter(self.db)
        self.stream = self.settings.redis_stream_name
        self.group = self.settings.redis_group_name
        self.consumer = "worker-1"
        self.running = True

    async def setup(self):
        await self.db.connect()
        try:
            await self.redis.xgroup_create(self.stream, self.group, id="0", mkstream=True)
        except Exception:
            pass

    async def _handle(self, messages: List[Tuple[str, dict]]):
        normalized = []
        ids = []
        for mid, data in messages:
            try:
                raw = RawMessage(
                    ts=int(data.get("ts", 0)),
                    chat_id=int(data.get("chat_id", 0)),
                    msg_id=int(data.get("msg_id", 0)),
                    text=data.get("text", ""),
                    sender=data.get("sender", ""),
                    live=int(data.get("live", 0)),
                )
                out = self.processor.process(raw)
                if out:
                    normalized.append(out)
                    ids.append(mid)
            except Exception as e:
                log.error("parse error: %s", e)
        if normalized:
            await self.writer.write(normalized)
            await self.redis.xack(self.stream, self.group, *ids)

    async def run(self):
        await self.setup()
        log.info("worker started")
        while self.running:
            try:
                res = await self.redis.xreadgroup(
                    groupname=self.group,
                    consumername=self.consumer,
                    streams={self.stream: ">"},
                    count=200,
                    block=5000,
                )
                if not res:
                    continue
                for _, msgs in res:
                    await self._handle(msgs)
            except Exception as e:
                log.error("worker error: %s", e)
                await asyncio.sleep(1)
