from typing import Any


def latest_by_id(items: list[dict[str, Any]], n: int) -> list[dict[str, Any]]:
    """
    Returns the N items with the highest 'id' values (in descending order).
    If some items don't have a valid int 'id', they are ignored.
    """
    valid = [x for x in items if isinstance(x.get("id"), int)]
    return sorted(valid, key=lambda x: x["id"], reverse=True)[:n]


def to_csv_row(
    user: dict[str, Any],
    post: dict[str, Any],
    comment: dict[str, Any],
) -> dict[str, Any]:
    """
    Flatten validated user/post/comment into one CSV row dict.
    Column spec (assignment): user(id,name), post(id,title), comment(id,body,email).
    """
    return {
        "user_id": user["id"],
        "user_name": user["name"],
        "post_id": post["id"],
        "post_title": post["title"],
        "comment_id": comment["id"],
        "comment_body": comment["body"],
        "comment_email": comment["email"],
    }