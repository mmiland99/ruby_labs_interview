import asyncio
import logging
from typing import Any
import aiohttp
from etl.api_client import fetch_json
from etl.validators import validate_user, validate_post, validate_comment
from etl.transformers import latest_by_id

logger = logging.getLogger(__name__)

async def fetch_users(session: aiohttp.ClientSession) -> list[dict[str, Any]]:
    return await fetch_json(session, "/users")

async def fetch_posts_for_user(session: aiohttp.ClientSession, user_id: int) -> list[dict[str, Any]]:
    return await fetch_json(session, f"/users/{user_id}/posts")

async def fetch_comments_for_post(session: aiohttp.ClientSession, post_id: int) -> list[dict[str, Any]]:
    return await fetch_json(session, f"/posts/{post_id}/comments")

async def build_tree(
    session: aiohttp.ClientSession,
    max_concurrency: int = 10,
) -> list[dict[str, Any]]:

    sem = asyncio.Semaphore(max_concurrency)

    async def _limited(coro):
        async with sem:
            return await coro

    raw_users = await fetch_users(session)

    # User validation
    valid_users = [u for u in (validate_user(x) for x in raw_users) if u is not None]

    even_users = [u for u in valid_users if u["id"] % 2 == 0]

    logger.info("Users: fetched=%d, valid=%d, even=%d", len(raw_users), len(valid_users), len(even_users))

    # Fetch posts concurrently for even users
    post_lists = await asyncio.gather(
        *[_limited(fetch_posts_for_user(session, u["id"])) for u in even_users],
        return_exceptions=True,
    )

    results: list[dict[str, Any]] = []

    for user, posts_result in zip(even_users, post_lists):
        if isinstance(posts_result, Exception):
            logger.error("Failed fetching posts for user_id=%s: %s", user["id"], posts_result)
            continue

        # Validate posts, then take latest 5 by id (there were no dates)
        valid_posts = [p for p in (validate_post(x) for x in posts_result) if p is not None]
        latest_posts = latest_by_id(valid_posts, 5)

        # Fetch comments concurrently for selected posts (there were no dates either)
        comment_lists = await asyncio.gather(
            *[_limited(fetch_comments_for_post(session, p["id"])) for p in latest_posts],
            return_exceptions=True,
        )

        posts_out: list[dict[str, Any]] = []
        for post, comments_result in zip(latest_posts, comment_lists):
            if isinstance(comments_result, Exception):
                logger.error("Failed fetching comments for post_id=%s: %s", post["id"], comments_result)
                continue

            valid_comments = [c for c in (validate_comment(x) for x in comments_result) if c is not None]
            latest_comments = latest_by_id(valid_comments, 3)

            posts_out.append({"post": post, "comments": latest_comments})

        results.append({"user": user, "posts": posts_out})

    return results