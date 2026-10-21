from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from typing import Any, Dict, List, Optional, Tuple

import asyncio
import re

from .browse_store import BrowseStore
from .codec import PLATFORM, plain_text
from .community_client import CommunityContext, contains_video
from .digest import DigestSettings, DigestStore
from .post_context import build_context
from .storage import Store
from .vendor.api_client import CredentialInvalidError, RateLimitedError, XiaoheiheApiClient
from .vendor.endpoints import EndpointName
from .vendor.models import RoutingTarget
from .vendor.parsers import parse_feed
from .vendor.security import redact_log_text


class BrowseSettings(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    enabled: bool = False
    topic_ids: List[str] = Field(default_factory=list, max_length=20)
    interval_seconds: int = Field(default=300, ge=60, le=86400)
    posts_per_round: int = Field(default=2, ge=1, le=5)
    summary: DigestSettings = Field(default_factory=DigestSettings)

    @field_validator("topic_ids")
    @classmethod
    def validate_topics(cls, values: List[str]) -> List[str]:
        if any(not value.isascii() or not value.isdigit() or len(value) > 32 for value in values):
            raise ValueError("分区 ID 必须是 1–32 位数字")
        return list(dict.fromkeys(values))

    @model_validator(mode="after")
    def validate_enabled(self) -> "BrowseSettings":
        if self.enabled and not self.topic_ids:
            raise ValueError("请先选择至少一个浏览分区")
        return self


def parse_catalog(payload: Dict[str, Any]) -> List[Dict[str, str]]:
    body = payload.get("result", payload.get("data", payload))
    if not isinstance(body, dict) or "topics_list" not in body:
        raise ValueError("小黑盒分区目录缺少 topics_list 字段")
    pending = [(body["topics_list"], "")]
    topics: Dict[str, Dict[str, str]] = {}
    while pending:
        node, group = pending.pop()
        if isinstance(node, list):
            pending.extend((item, group) for item in reversed(node))
        elif isinstance(node, dict):
            topic_id = str(node.get("topic_id", node.get("id", "")))
            name = str(node.get("name", node.get("title", node.get("topic_name", ""))))
            if topic_id.isascii() and topic_id.isdigit() and name:
                topics[topic_id] = {"id": topic_id, "name": name, "group": group}
            elif name:
                group = name
            pending.extend((value, group) for value in node.values() if isinstance(value, (dict, list)))
    if not topics:
        raise ValueError("小黑盒分区目录为空，请检查接口响应")
    return list(topics.values())


def require_success(result: Dict[str, Any]) -> Dict[str, Any]:
    if not result.get("success"):
        raise RuntimeError(str(result.get("error", "宿主未接受主动浏览任务")))
    return result


class Browser:
    def __init__(self, ctx: Any, store: Store, send_lock: asyncio.Lock) -> None:
        self.ctx = ctx
        self.store = store
        self.db = BrowseStore(store)
        self.digest = DigestStore(store)
        self.send_lock = send_lock
        self.lock = asyncio.Lock()
        self.errors: Dict[str, str] = {}

    async def settings(self, account: str) -> BrowseSettings:
        raw = await self.db.settings(account)
        return BrowseSettings.model_validate(raw) if raw is not None else BrowseSettings()

    async def snapshot(self, account: str) -> Dict[str, Any]:
        return {
            "settings": (await self.settings(account)).model_dump(),
            "topics": await self.db.catalog(account),
            "status": await self.db.status(account),
            "error": self.errors.get(account, ""),
            "account_id": account,
            "summary_status": await self.digest.snapshot(account),
        }

    async def refresh_catalog(self, client: XiaoheiheApiClient, account: str) -> None:
        response = await client._request(EndpointName.TOPIC_INDEX, params={"type": "list"})
        topics = await asyncio.to_thread(parse_catalog, response.payload)
        await self.db.save_catalog(account, topics)

    async def existing_stream(self, account: str, post: str) -> Optional[Dict[str, str]]:
        saved = await self.db.post_stream(account, post)
        if saved is not None:
            return saved
        # SDK 已将宿主响应解包为聊天流列表。
        streams = await self.ctx.chat.get_group_streams(platform=PLATFORM)
        streams = [stream for stream in streams if stream["account_id"] == account
                   and stream["platform"] == PLATFORM and stream["group_id"].startswith(f"post:{post}:thread:")]
        if not streams:
            return None
        # 旧版本可能为根楼层建过多个流；固定选定一个真实流，后续不再随通知楼层切换。
        stream = min(streams, key=lambda item: (item["group_id"] != f"post:{post}:thread:browse", item["group_id"]))
        name = stream["group_name"] or f"帖子 {post}"
        await self.db.save_stream(account, post, stream["stream_id"], stream["group_id"], name)
        return {"stream_id": stream["stream_id"], "group_id": stream["group_id"], "group_name": name}

    async def save(self, account: str, raw: Dict[str, Any]) -> None:
        settings = BrowseSettings.model_validate(raw)
        known = {topic["id"] for topic in await self.db.catalog(account)}
        if settings.enabled and set(settings.topic_ids) - known:
            raise ValueError("所选分区不在当前账号的分区目录中，请刷新分区后重试")
        if settings.summary.enabled and settings.summary.topic_id not in known:
            raise ValueError("总结帖发布分区不在当前账号目录中，请刷新分区后重试")
        async with self.lock:
            async with self.send_lock:
                await self.db.save(account, settings.model_dump())
        self.errors.pop(account, None)

    async def prepare_outbound(self, account: str, message: Dict[str, Any]) -> Tuple[Dict[str, Any], Optional[str]]:
        group = message["message_info"]["group_info"]["group_id"]
        match = re.fullmatch(r"post:([0-9]+):thread:browse", group)
        if match is None:
            return message, None
        post = match[1]
        anchor = f"browse:{post}"
        if await self.db.origin(account, anchor) != post:
            raise ValueError("该主动浏览会话没有已登记的帖子目标")
        # 保留模型选择的明确 target；原帖与评论分别查登记路由，仍统一经过主动配额闸门。
        return message, post

    async def due(self, client: XiaoheiheApiClient, account: str, context_limit: int, reply_limit: int) -> None:
        async with self.lock:
            settings = await self.settings(account)
            if not settings.enabled or not await self.db.claim_scan(account, settings.interval_seconds):
                return
            candidates: Dict[str, Dict[str, Any]] = {}
            failures = []
            for topic in settings.topic_ids:
                try:
                    response = await client._request(
                        EndpointName.TOPIC_FEED,
                        params={
                            "topic_id": topic,
                            "offset": 0,
                            "limit": 10,
                            "lastval": "",
                            "dw": 720,
                        },
                    )
                    page = parse_feed(response.payload)
                    for item in page.items:
                        post = item["post_id"]
                        if post and item["author"]["uid"] != account and not contains_video(item):
                            candidates[post] = item
                except (CredentialInvalidError, RateLimitedError):
                    raise
                except Exception as exc:
                    failures.append(f"分区 {topic}：{redact_log_text(str(exc))}")
            self.errors[account] = "；".join(failures)
            # 多分区相同帖子只判断一次，每轮仍按配置控制提交数量。
            submitted = 0
            for item in sorted(candidates.values(), key=lambda post: post["created_at"], reverse=True):
                if submitted >= settings.posts_per_round:
                    break
                post = item["post_id"]
                if await self.store.seen(account, f"video-post:{post}") or not await self.db.can_consider(
                    account, post
                ):
                    continue
                message_id = f"browse:{post}"
                group = f"post:{post}:thread:browse"
                await self.store.record_activity(
                    account,
                    "browse",
                    message_id,
                    post,
                    group,
                    plain_text(str(item.get("title", ""))) or f"帖子 {post}",
                    item["author"]["nickname"],
                    plain_text(item["content"]),
                    "processing",
                )
                try:
                    context = await client.fetch_thread_context(post)
                    # 信息流可能缺少视频标记；详情再检查一次，视频不进入模型也不计入浏览统计。
                    if isinstance(context, CommunityContext) and context.has_video:
                        await self.store.mark_seen(account, f"video-post:{post}")
                        await self.store.update_activity(account, "browse", message_id, "skipped", "已排除含视频的帖子")
                        continue
                    if not await self.db.claim_post(account, post):
                        continue
                    submitted += 1
                    title = plain_text(context.title) or f"帖子 {post}"
                    # 背景与主动意图分开提交；作者、图片和评论保留实际来源，不伪装成用户 @。
                    entries, image_failures = await build_context(context, item, context_limit)
                    await self.store.update_activity(
                        account,
                        "browse",
                        message_id,
                        "processing",
                        title=title,
                        body="\n\n".join(entry.text for entry in entries),
                    )
                    async with self.send_lock:
                        existing = await self.existing_stream(account, post)
                        if existing is None:
                            session = require_success(
                                await self.ctx.chat.open_session(
                                    platform=PLATFORM, chat_type="group", group_id=group, account_id=account,
                                )
                            )
                            stream = session["stream_id"]
                            await self.db.save_stream(account, post, stream, group, title)
                        else:
                            stream, group = existing["stream_id"], existing["group_id"]
                        await self.store.update_activity(account, "browse", message_id, "processing", group=group)
                        target = RoutingTarget("default", post)
                        await self.store.save_route(account, message_id, target, group)
                        await self.db.save_anchor(account, message_id, post)
                        for entry in entries:
                            if entry.target is None:
                                raise ValueError("浏览消息缺少明确的帖子或评论路由")
                            await self.store.save_route(account, entry.message_id, entry.target, group)
                            await self.db.save_anchor(account, entry.message_id, post)
                            appended = require_success(
                                await self.ctx.maisaka.context.append(
                                    stream,
                                    entry.segments,
                                    visible_text=entry.text,
                                    message_id=entry.message_id,
                                    source_kind="xiaoheihe_browse",
                                    replyable=True,
                                    user_info={"user_id": entry.user_id, "user_nickname": entry.user_name or entry.user_id},
                                )
                            )
                            if appended.get("replyable") is not True:
                                raise ValueError("宿主未提供可回复的用户消息支持，请更新主程序并重启")
                        task = require_success(
                            await self.ctx.maisaka.proactive.trigger(
                                stream,
                                "你正在自主浏览小黑盒帖子。结合自己的性格、兴趣和帖子内容，自行决定是否参与。"
                                "请区分原帖作者和评论者、图片所属发言者；图片读取失败时不要猜测其内容。",
                                reason="按用户配置浏览小黑盒分区",
                                metadata={"post_id": post, "anchor": message_id},
                            )
                        )
                        # 提交期间保持出站锁，确保帖子与评论路由先于快速生成的回复完成登记。
                        await self.db.save_anchor(account, task["task_id"], post)
                        await self.digest.collect(account, post, "\n\n".join(entry.text for entry in entries), stream)
                        await self.store.update_activity(
                            account,
                            "browse",
                            message_id,
                            "considering",
                            "已提交麦麦自主判断，未发送不代表已决定跳过"
                            + ("；" + "；".join(image_failures) if image_failures else ""),
                            title=title,
                        )
                        if image_failures:
                            self.errors[account] = "；".join(image_failures)
                except Exception as exc:
                    await self.store.update_activity(account, "browse", message_id, "error", redact_log_text(str(exc)))
                    self.errors[account] = redact_log_text(str(exc))
                    if isinstance(exc, (CredentialInvalidError, RateLimitedError)):
                        raise
