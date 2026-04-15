# Telegram Ingestion Platform V2

A production-oriented Telegram ingestion platform with:

- Telethon-based collectors
- Redis atomic deduplication
- Redis Streams for decoupled ingestion
- PostgreSQL for durable storage
- Phase 2 quality pipeline (raw -> normalized -> entities)
- Scheduler/session assignment scaffolding
- Prometheus metrics endpoints

## Components

- `app/collector_v2.py`: Telethon collector that publishes deduplicated records to Redis Stream
- `app/stream_worker_v2.py`: Stream worker consuming from Redis and writing through the Phase 2 pipeline
- `app/scheduler.py`: scheduler loop for session/channel assignment reconciliation
- `app/session_worker.py`: session heartbeat worker
- `app/quality_processor.py`: normalization, noise filtering, extraction, scoring
- `app/entity_resolver.py`: entity upsert + linking
- `sql/001_schema.sql`: PostgreSQL schema
- `infra/docker-compose.yml`: PostgreSQL, Redis, Prometheus

## Environment

Copy `.env.example` to `.env` and fill required values.

## Run

1. Start infrastructure:

```bash
cd infra && docker compose --env-file ../.env up -d
```

2. Apply schema to PostgreSQL.

3. Install app dependencies:

```bash
pip install -r requirements.txt
```

4. Start worker:

```bash
python -m app.main_worker
```

5. Start collector:

```bash
python -m app.main_collector
```

## Notes

- The system is designed for at-least-once delivery with idempotent database writes.
- Messages are ACKed only after successful pipeline handling.
- `XAUTOCLAIM` is used to recover stale pending messages.
