import json
import os
from typing import Dict, Any

from redis.asyncio import Redis, ConnectionPool


class RedisGateway:
    def __init__(self, url: str, stream: str, dedup_ttl: int):
        self.pool = ConnectionPool.from_url(url, decode_responses=True)
        self.client = Redis(connection_pool=self.pool)
        self.stream = stream
        self.ttl = dedup_ttl

    async def is_duplicate(self, chat_id: int, msg_id: int) -> bool:
        key = f"dedup:{chat_id}"
        added = await self.client.sadd(key, msg_id)
        if added == 1:
            await self.client.expire(key, self.ttl)
            return False
        return True

    async def publish(self, record: Dict[str, Any]) -> None:
        payload = {k: str(v) if not isinstance(v, (str, int, float)) else v for k, v in record.items()}
        await self.client.xadd(self.stream, payload, maxlen=500000, approximate=True)

    async def close(self):
        await self.client.aclose()
        await self.pool.disconnect()
