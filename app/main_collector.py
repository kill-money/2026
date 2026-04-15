import asyncio
import logging

from .collector_v2 import CollectorV2

logging.basicConfig(level="INFO")


async def main():
    c = CollectorV2()
    await c.run()


if __name__ == "__main__":
    asyncio.run(main())
