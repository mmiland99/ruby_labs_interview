import logging
from typing import Any, Optional

logger = logging.getLogger(__name__)


def _is_int(x: Any) -> bool:
    return isinstance(x, int) and not isinstance(x, bool)


def _is_str(x: Any) -> bool:
    return isinstance(x, str) and x.strip() != ""


def validate_user(user: dict[str, Any]) -> Optional[dict[str, Any]]:
    # Returns a normalized dict or None (log + skip).
    uid = user.get("id")
    name = user.get("name")

    if not _is_int(uid):
        logger.warning("Invalid user.id: %r (user=%r)", uid, user)
        return None
    if not _is_str(name):
        logger.warning("Invalid user.name: %r (user_id=%r)", name, uid)
        return None

    return {"id": uid, "name": name.strip()}


def validate_post(post: dict[str, Any]) -> Optional[dict[str, Any]]:

    # Required fields: id(int), userId(int), title(str)

    pid = post.get("id")
    uid = post.get("userId")
    title = post.get("title")

    if not _is_int(pid):
        logger.warning("Invalid post.id: %r (post=%r)", pid, post)
        return None
    if not _is_int(uid):
        logger.warning("Invalid post.userId: %r (post_id=%r)", uid, pid)
        return None
    if not _is_str(title):
        logger.warning("Invalid post.title: %r (post_id=%r)", title, pid)
        return None

    return {"id": pid, "userId": uid, "title": title.strip()}


def validate_comment(comment: dict[str, Any]) -> Optional[dict[str, Any]]:

    # Required fields: id(int), postId(int), body(str), email(str)
    cid = comment.get("id")
    pid = comment.get("postId")
    body = comment.get("body")
    email = comment.get("email")

    if not _is_int(cid):
        logger.warning("Invalid comment.id: %r (comment=%r)", cid, comment)
        return None
    if not _is_int(pid):
        logger.warning("Invalid comment.postId: %r (comment_id=%r)", pid, cid)
        return None
    if not _is_str(body):
        logger.warning("Invalid comment.body: %r (comment_id=%r)", body, cid)
        return None
    if not _is_str(email) or "@" not in email:
        logger.warning("Invalid comment.email: %r (comment_id=%r)", email, cid)
        return None

    clean_body = body.strip().replace("\r\n", " ").replace("\n", " ").replace("\r", " ")
    return {"id": cid, "postId": pid, "body": clean_body, "email": email.strip()}