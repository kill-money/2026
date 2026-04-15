import os
from dataclasses import dataclass


def _required(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"missing required env: {name}")
    return value


@dataclass(frozen=True)
class Settings:
    pg_dsn: str
    redis_url: str
    redis_stream_name: str
    redis_group_name: str
    redis_dedup_ttl: int
    log_level: str

    @staticmethod
    def from_env() -> "Settings":
        return Settings(
            pg_dsn=_required("PG_DSN"),
            redis_url=_required("REDIS_URL"),
            redis_stream_name=os.environ.get("REDIS_STREAM_NAME", "ingest_stream"),
            redis_group_name=os.environ.get("REDIS_GROUP_NAME", "db_writer_group"),
            redis_dedup_ttl=int(os.environ.get("REDIS_DEDUP_TTL", "86400")),
            log_level=os.environ.get("LOG_LEVEL", "INFO"),
        )
