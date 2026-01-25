# To run: $env:PYTHONPATH="src"; python -m etl.main
import asyncio
import logging
import aiohttp
from etl.api_client import fetch_json

def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

async def async_main() -> int:
    configure_logging()
    logger = logging.getLogger(__name__)

    # Reuse one session (connection pooling)
    async with aiohttp.ClientSession() as session:
        try:
            users = await fetch_json(session, "/users")
            logger.info("Fetch test OK. Fetched %d users.", len(users))
            return 0
        except Exception as e:
            logger.error("Fetch test failed: %s", e)
            return 1


def main() -> None:
    raise SystemExit(asyncio.run(async_main()))


if __name__ == "__main__":
    main()