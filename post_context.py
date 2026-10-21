"""保留帖子与评论的作者、回复关系和图片归属，构造宿主可识别的图文上下文。"""

from dataclasses import dataclass, field
from io import BytesIO
from typing import Any, Dict, List, Mapping, Optional, Tuple
from urllib.parse import urljoin, urlsplit

from PIL import Image, ImageOps

import asyncio
import base64
import hashlib
import httpx
import re

from .codec import plain_text
from .community_client import CommunityContext
from .vendor.models import RoutingTarget, ThreadContext
from .vendor.security import redact_log_text, resolve_public_host, validate_public_https_url

MAX_COMMENTS = 20
MAX_IMAGES = 6
MAX_DOWNLOAD_BYTES = 5 * 1024 * 1024
MAX_IMAGE_BYTES = 1024 * 1024


@dataclass
class ContextEntry:
    message_id: str
    text: str
    image_urls: List[str] = field(default_factory=list)
    segments: List[Dict[str, Any]] = field(default_factory=list)
    target: Optional[RoutingTarget] = None
    user_id: str = ""
    user_name: str = ""


def readable_text(text: str) -> str:
    return plain_text(re.sub(r"(?i)<br\s*/?>|</(?:p|div|li)>", "\n", text)).strip()


def first_text(values: Mapping[str, Any], keys: Tuple[str, ...]) -> str:
    for key in keys:
        value = values.get(key)
        if value is not None and str(value).strip():
            return str(value).strip()
    return ""


def identity(name: str, uid: str) -> str:
    nickname = readable_text(name).replace("\n", " ")[:100] or "昵称未提供"
    return f"{nickname}（UID {uid[:80]}）" if uid else f"{nickname}（UID 未提供）"


def image_urls(values: Mapping[str, Any]) -> List[str]:
    urls = []
    for key in ("images", "image_urls", "comment_a_images", "imgs"):
        raw = values.get(key, [])
        if isinstance(raw, (str, dict)):
            raw = [raw]
        if not isinstance(raw, list):
            raise ValueError(f"评论图片字段 {key} 不是数组")
        for item in raw:
            url = (
                first_text(item, ("url", "src", "original", "image_url", "large_url"))
                if isinstance(item, dict)
                else str(item)
            )
            if url:
                urls.append(url)
    return list(dict.fromkeys(urls))


def flatten_comments(comments: List[Dict[str, Any]]) -> Tuple[List[Tuple[Dict[str, Any], str]], bool]:
    pending = [(item, "") for item in reversed(comments)]
    rows = []
    seen = set()
    visited = 0
    while pending and visited < 200 and len(rows) < MAX_COMMENTS:
        raw, inherited_parent = pending.pop()
        visited += 1
        if not isinstance(raw, dict):
            raise ValueError("评论节点不是对象")
        comment = raw.get("comment", raw)
        if not isinstance(comment, dict):
            raise ValueError("评论正文节点不是对象")
        comment_id = first_text(comment, ("comment_id", "commentid", "comment_a_id", "id"))
        parent = first_text(comment, ("parent_comment_id", "parent_id", "reply_id")) or inherited_parent
        for source in (raw, comment) if raw is not comment else (raw,):
            for key in ("children", "sub_comments", "reply_list", "comments", "comment_list"):
                children = source.get(key, [])
                if not isinstance(children, list):
                    raise ValueError(f"评论回复字段 {key} 不是数组")
                pending.extend((child, comment_id) for child in reversed(children))
        if comment_id and comment_id in seen:
            continue
        if comment_id:
            seen.add(comment_id)
        rows.append((comment, parent))
    return rows, bool(pending)


def context_entries(context: ThreadContext, feed: Dict[str, Any], limit: int) -> List[ContextEntry]:
    author = feed["author"]
    # 详情和信息流都是平台给出的作者资料；不把帖子标题当作者，不虚构缺失 UID。
    author_name = context.author_name or author["nickname"]
    author_uid = context.author_uid or author["uid"]
    title = readable_text(context.title)[:200] or f"帖子 {context.post_id}"
    header = (
        f"[主动浏览的原帖：{title}]\n作者：{identity(author_name, author_uid)}\n"
        f"原帖链接：https://xiaoheihe.cn/app/bbs/link/{context.post_id}\n正文：\n"
    )
    comments, omitted = flatten_comments(context.comments)
    if not comments and isinstance(context, CommunityContext) and context.comment_count:
        raise ValueError(f"平台显示 {context.comment_count} 条评论，但评论首页返回空列表，请检查接口响应")
    body = readable_text(context.body)
    post_budget = max(0, (limit * 3 // 5 if comments else limit - 150) - len(header))
    text = header + body[:post_budget]
    if len(body) > post_budget:
        text += "\n[原帖正文因长度限制截取]"
    if not body:
        text += "[接口未返回文字正文]"
    if not comments:
        text += "\n[当前评论区为空]" if isinstance(context, CommunityContext) and context.comment_count == 0 else (
            "\n[评论首页未返回内容，无法确认评论区是否为空]"
        )
    else:
        text += "\n[下面为本次接口返回的评论节选]"
    entries = [ContextEntry(
        f"browse:{context.post_id}", text, list(dict.fromkeys(context.image_urls)),
        target=RoutingTarget("default", context.post_id), user_id=author_uid, user_name=author_name,
    )]
    remaining = max(0, limit - len(text) - 150)
    roots: Dict[str, str] = {}
    for index, (comment, parent) in enumerate(comments):
        user = comment.get("user", comment.get("sender", {}))
        if not isinstance(user, dict):
            raise ValueError("评论发言者资料不是对象")
        name = first_text(user, ("nickname", "username", "name"))
        uid = first_text(user, ("uid", "heybox_id", "heyboxid", "user_id", "userid", "id"))
        comment_id = first_text(comment, ("comment_id", "commentid", "comment_a_id", "id"))
        if not comment_id:
            raise ValueError("评论缺少真实评论 ID，无法登记回复目标")
        root = first_text(comment, ("root_comment_id", "root_id"))
        if root in {"", "-1", "0"}:
            root = roots.get(parent, "") if parent and parent not in {"-1", "0"} else comment_id
        if not root:
            raise ValueError(f"评论 {comment_id} 的根楼层未知，无法登记楼中楼目标")
        roots[comment_id] = root
        relation = f"回复评论 #{parent}" if parent and parent not in {"-1", "0"} else "原帖下的评论"
        label = f"[评论 #{comment_id or ('本地序号' + str(index + 1))} · {relation}]\n发言者：{identity(name, uid)}\n"
        content = readable_text(first_text(comment, ("content", "text", "comment_text", "comment_a_text")))
        if remaining < len(label) + 30:
            omitted = True
            break
        budget = min(800, remaining - len(label) - 20)
        row = label + (content[:budget] if content else "[无文字正文，可能含图片]")
        if len(content) > budget:
            row += "\n[评论正文截取]"
        entries.append(
            ContextEntry(
                f"browse:{context.post_id}:comment:{comment_id}", row, image_urls(comment),
                target=RoutingTarget("default", context.post_id, root, comment_id), user_id=uid, user_name=name,
            )
        )
        remaining -= len(row)
    if omitted:
        entries[0].text += f"\n[评论按接口顺序节选，最多 {MAX_COMMENTS} 条，受文字预算限制，非完整评论区]"
    if not body and not any(entry.image_urls for entry in entries) and len(entries) == 1:
        raise ValueError("帖子未返回正文、图片或可读取的评论")
    return entries


def image_segment(binary: bytes) -> Dict[str, Any]:
    with Image.open(BytesIO(binary)) as source:
        if source.format not in {"JPEG", "PNG", "GIF", "WEBP"}:
            raise ValueError("不支持的图片格式")
        if source.width * source.height > 20_000_000:
            raise ValueError("图片像素超过 2000 万")
        # 将大图和动图首帧压成有界预览，在工作线程中处理，避免占用事件循环及 IPC 帧。
        source.seek(0)
        preview = ImageOps.exif_transpose(source)
        preview.thumbnail((1280, 1280))
        if preview.mode in {"RGBA", "LA"} or "transparency" in preview.info:
            rgba = preview.convert("RGBA")
            rgb = Image.new("RGB", rgba.size, "white")
            rgb.paste(rgba, mask=rgba.getchannel("A"))
        else:
            rgb = preview.convert("RGB")
        output = BytesIO()
        rgb.save(output, format="JPEG", quality=80, optimize=True)
        data = output.getvalue()
    if len(data) > MAX_IMAGE_BYTES:
        raise ValueError("图片预览超过 1 MB")
    return {
        "type": "image",
        "data": "[图片已附加；纯文字模式无法读取该图片，请勿猜测内容]",
        "hash": hashlib.sha256(data).hexdigest(),
        "binary_data_base64": base64.b64encode(data).decode("ascii"),
    }


async def load_image(client: httpx.AsyncClient, url: str) -> Dict[str, Any]:
    for _ in range(4):
        url = validate_public_https_url(url)
        hostname = urlsplit(url).hostname
        if hostname is None or not any(
            hostname == suffix or hostname.endswith("." + suffix) for suffix in ("max-c.com", "xiaoheihe.cn")
        ):
            raise ValueError("图片不属于小黑盒媒体域名")
        await resolve_public_host(hostname)
        async with client.stream("GET", url) as response:
            if response.is_redirect:
                url = urljoin(url, response.headers["location"])
                continue
            response.raise_for_status()
            chunks = []
            size = 0
            async for chunk in response.aiter_bytes():
                size += len(chunk)
                if size > MAX_DOWNLOAD_BYTES:
                    raise ValueError("图片下载超过 5 MB")
                chunks.append(chunk)
        return await asyncio.to_thread(image_segment, b"".join(chunks))
    raise ValueError("图片重定向次数过多")


async def build_context(
    context: ThreadContext, feed: Dict[str, Any], limit: int
) -> Tuple[List[ContextEntry], List[str]]:
    entries = await asyncio.to_thread(context_entries, context, feed, limit)
    if any(not entry.user_id for entry in entries):
        raise ValueError("原帖或评论缺少真实作者 UID，无法插入可回复的用户消息")
    slots = []
    seen_urls = set()
    for entry in entries:
        entry.segments = [{"type": "text", "data": entry.text}]
        for number, url in enumerate(entry.image_urls, 1):
            if url in seen_urls:
                entry.segments.append({"type": "text", "data": f"[本条图片 {number} 与前文图片相同]"})
                continue
            seen_urls.add(url)
            if len(slots) >= MAX_IMAGES:
                entry.segments.append({"type": "text", "data": f"[本条图片 {number} 未加载：本次最多 {MAX_IMAGES} 张]"})
                continue
            label = f"[以上发言者所附图片 {number}]"
            entry.segments.append({"type": "text", "data": label})
            position = len(entry.segments)
            entry.segments.append({})
            slots.append((entry, position, url, number))
    failures = []
    semaphore = asyncio.Semaphore(3)
    if slots:
        # TLS 客户端初始化会同步读取证书；纯文字帖不创建媒体连接，证书读取也放到线程中。
        media_client = await asyncio.to_thread(
            httpx.AsyncClient,
            timeout=10,
            trust_env=False,
            follow_redirects=False,
            headers={"Referer": "https://www.xiaoheihe.cn/"},
        )
        async with media_client as client:

            async def download(entry: ContextEntry, position: int, url: str, number: int) -> None:
                async with semaphore:
                    try:
                        entry.segments[position] = await asyncio.wait_for(load_image(client, url), timeout=15)
                    except Exception as exc:
                        detail = f"图片 {number} 读取失败：{type(exc).__name__}: {redact_log_text(str(exc))}"
                        failures.append(detail)
                        entry.segments[position] = {"type": "text", "data": f"[{detail}；图片内容不可见，请勿猜测]"}

            await asyncio.gather(*(download(*slot) for slot in slots))
    for entry in entries:
        entry.text = "\n".join(segment["data"] for segment in entry.segments if segment["type"] == "text")
    if slots and not any(segment["type"] == "image" for entry in entries for segment in entry.segments):
        if not readable_text(context.body) and len(entries) == 1:
            raise ValueError("纯图片帖的图片均未能读取，停止本次判断")
    return entries, failures
