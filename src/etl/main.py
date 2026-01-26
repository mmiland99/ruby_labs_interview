# To run (PowerShell): $env:PYTHONPATH="src"; python -m etl.main
import asyncio
import logging
import aiohttp
from etl.csv_writer import write_csv
from etl.fetchers import fetch_users, fetch_posts_for_user, fetch_comments_for_post
from etl.transformers import latest_by_id, to_csv_row
from etl.validators import validate_user, validate_post, validate_comment

def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

def collect_valid(items, validate_fn):
    # Return (valid_items, invalid_count).
    valid = []
    invalid = 0
    for x in items:
        vx = validate_fn(x)
        if vx is None:
            invalid += 1
        else:
            valid.append(vx)
    return valid, invalid


async def async_main() -> int:
    configure_logging()
    logger = logging.getLogger(__name__)

    sem = asyncio.Semaphore(10)

    async def limited(coro):
        async with sem:
            return await coro

    # Counters
    users_fetched = users_valid = users_even = users_invalid = 0
    posts_fetched = posts_valid = posts_invalid = posts_selected = posts_fetch_fail = 0
    comments_fetched = comments_valid = comments_invalid = comments_selected = comments_fetch_fail = 0

    async with aiohttp.ClientSession() as session:
        try:
            # 1) Users: fetch -> validate -> even filter
            raw_users = await fetch_users(session)
            users_fetched = len(raw_users)

            valid_users, users_invalid = collect_valid(raw_users, validate_user)
            users_valid = len(valid_users)

            even_users = [u for u in valid_users if u["id"] % 2 == 0]
            users_even = len(even_users)

            # 2) Posts: fetch concurrently, validate, latest 5
            posts_results = await asyncio.gather(
                *[limited(fetch_posts_for_user(session, u["id"])) for u in even_users],
                return_exceptions=True,
            )

            rows = []

            for user, pr in zip(even_users, posts_results):
                if isinstance(pr, Exception):
                    posts_fetch_fail += 1
                    logger.error("Posts fetch failed user_id=%s: %s", user["id"], pr)
                    continue

                posts_fetched += len(pr)
                valid_posts, inv_posts = collect_valid(pr, validate_post)
                posts_invalid += inv_posts
                posts_valid += len(valid_posts)

                selected_posts = latest_by_id(valid_posts, 5)
                posts_selected += len(selected_posts)

                # 3) Comments: fetching concurrently, validate, latest 3
                comments_results = await asyncio.gather(
                    *[limited(fetch_comments_for_post(session, p["id"])) for p in selected_posts],
                    return_exceptions=True,
                )

                for post, cr in zip(selected_posts, comments_results):
                    if isinstance(cr, Exception):
                        comments_fetch_fail += 1
                        logger.error("Comments fetch failed post_id=%s: %s", post["id"], cr)
                        continue

                    comments_fetched += len(cr)
                    valid_comments, inv_comments = collect_valid(cr, validate_comment)
                    comments_invalid += inv_comments
                    comments_valid += len(valid_comments)

                    selected_comments = latest_by_id(valid_comments, 3)
                    comments_selected += len(selected_comments)

                    for comment in selected_comments:
                        rows.append(to_csv_row(user, post, comment))

            # 4) Output
            fieldnames = [
                "user_id", "user_name",
                "post_id", "post_title",
                "comment_id", "comment_body", "comment_email",
            ]
            written = write_csv(rows, filepath="output.csv", fieldnames=fieldnames)

            # 5) Summary logs
            logger.info("Users: fetched=%d valid=%d invalid=%d even=%d", users_fetched, users_valid, users_invalid, users_even)
            logger.info("Posts: fetched=%d valid=%d invalid=%d selected=%d fetch_fail=%d", posts_fetched, posts_valid, posts_invalid, posts_selected, posts_fetch_fail)
            logger.info("Comments: fetched=%d valid=%d invalid=%d selected=%d fetch_fail=%d", comments_fetched, comments_valid, comments_invalid, comments_selected, comments_fetch_fail)
            logger.info("CSV: wrote %d rows -> output.csv", written)

            return 0

        except Exception as e:
            logger.error("Pipeline failed: %s", e)
            return 1

def main() -> None:
    raise SystemExit(asyncio.run(async_main()))

if __name__ == "__main__":
    main()