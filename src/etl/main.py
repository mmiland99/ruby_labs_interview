#To run: $env:PYTHONPATH="src"; python -m etl.main
import asyncio
import logging
import aiohttp
from etl.fetchers import build_tree

def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

async def async_main() -> int:
    configure_logging()
    logger = logging.getLogger(__name__)

    async with aiohttp.ClientSession() as session:
        try:
            tree = await build_tree(session, max_concurrency=10)
            users_count = len(tree)
            posts_count = sum(len(u["posts"]) for u in tree)
            comments_count = sum(len(p["comments"]) for u in tree for p in u["posts"])

            logger.info("Tree built OK: users=%d posts=%d comments=%d", users_count, posts_count, comments_count)

            if tree:
                logger.info("Sample user_id=%s has %d posts", tree[0]["user"]["id"], len(tree[0]["posts"]))

            return 0
        except Exception as e:
            logger.error("Tree build failed: %s", e)
            return 1


def main() -> None:
    raise SystemExit(asyncio.run(async_main()))


if __name__ == "__main__":
    main()