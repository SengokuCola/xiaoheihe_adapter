from typing import Any, Dict, List, Tuple

import html
import re

from .vendor.models import Notification, ThreadContext

PLATFORM = "xiaoheihe"


def plain_text(text: str) -> str:
    return html.unescape(re.sub(r"<[^>]*>", "", text))


def group_id(notification: Notification) -> str:
    # 这里只生成外部群标识。真实 session_id 完全由宿主创建和解析。
    return f"post:{notification.post_id}:thread:{notification.root_comment_id or 'post'}"


def decode_current(notification: Notification, account: str, group: str, name: str) -> Dict[str, Any]:
    if not notification.sender_uid:
        raise ValueError("小黑盒通知缺少真实发送者 UID，拒绝伪造身份")
    current = plain_text(notification.content)
    if not current.strip():
        raise ValueError("通知没有可用文字，首版暂不支持纯图片消息")
    return {
        "platform": PLATFORM,
        "account_id": account,
        "scope": None,
        "message_id": f"xhh:{notification.post_id}:{notification.external_comment_id}",
        "timestamp": str(notification.created_at),
        "message_info": {
            "user_info": {"user_id": notification.sender_uid, "user_nickname": notification.sender_nickname},
            "group_info": {"group_id": group, "group_name": name},
            "additional_config": {"xiaoheihe_post_id": notification.post_id},
        },
        "raw_message": [{"type": "text", "data": current}],
        "is_at": True,
        "is_mentioned": True,
    }


def decode(notification: Notification, context: ThreadContext, account: str, limit: int) -> Dict[str, Any]:
    body = [f"[原帖：{plain_text(context.title)}]", plain_text(context.body)]
    # 限制树遍历与字符预算，避免长楼层无限扩张；网页内容始终作为用户提供的背景文字。
    pending = list(context.comments)
    visited = 0
    while pending and visited < 100:
        comment = pending.pop(0)
        visited += 1
        user = comment.get("user", {})
        nickname = user.get("username", user.get("nickname", "")) if isinstance(user, dict) else ""
        text = comment.get("text", comment.get("content", comment.get("comment_text", "")))
        body.append(f"{nickname}: {plain_text(str(text))}")
        children = comment.get("children", [])
        if isinstance(children, list):
            pending.extend(item for item in children if isinstance(item, dict))
    background = "\n".join(body)[:limit]
    message = decode_current(
        notification,
        account,
        group_id(notification),
        f"{plain_text(context.title)[:100]} / 楼层 {notification.root_comment_id or '原帖'}",
    )
    message["raw_message"][0]["data"] = f"{background}\n\n[当前消息]\n{message['raw_message'][0]['data']}"
    return message


def encode(message: Dict[str, Any], max_chars: int) -> Tuple[str, str, str, List[Dict[str, Any]]]:
    info = message["message_info"]
    group = info["group_info"]["group_id"]
    anchors: List[str] = []
    parts: List[str] = []
    images: List[Dict[str, Any]] = []
    for segment in message["raw_message"]:
        kind = segment["type"]
        if kind == "text":
            parts.append(segment["data"])
        elif kind == "reply":
            anchors.append(segment["data"]["target_message_id"])
        elif kind in {"image", "emoji"}:
            if not isinstance(segment.get("binary_data_base64"), str) or not segment["binary_data_base64"]:
                raise ValueError("图片或表情包缺少二进制内容")
            images.append(segment)
        else:
            raise ValueError(f"小黑盒评论不支持消息段 {kind}")
    if len(images) > 6:
        raise ValueError("一条评论最多发送 6 张图片或表情包")
    if len(set(anchors)) > 1:
        raise ValueError("同一评论不能引用多个目标")
    if anchors and message.get("reply_to") and anchors[0] != message["reply_to"]:
        raise ValueError("回复目标与引用消息不一致，拒绝猜测评论目标")
    anchor = anchors[0] if anchors else message.get("reply_to", "")
    if not anchor:
        raise ValueError("出站评论缺少回复锚点，无法确定应回复哪条评论")
    text = "".join(parts).strip()
    if (not text and not images) or len(text) > max_chars:
        raise ValueError(f"评论必须包含文字或图片，文字不超过 {max_chars} 个字符")
    return text, anchor, group, images
