import asyncio
import logging

from .stream_worker_v2 import StreamWorkerV2

logging.basicConfig(level="INFO")


async def main():
    worker = StreamWorkerV2()
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
