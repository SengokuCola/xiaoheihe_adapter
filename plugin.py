from contextlib import suppress
from typing import Any, ClassVar, Dict, List, Optional, Type

from maibot_sdk import API, MaiBotPlugin, MessageGateway, PluginConfigBase

import asyncio

from .auth import LoginService
from .activity_records import ActivityRecords
from .browse import Browser, require_success
from .codec import PLATFORM, decode, decode_current, encode, group_id, plain_text
from .community_client import CommentRejectedError, CommunityClient
from .digest import DigestPublisher
from .media_upload import parse_image, upload_image
from .settings import AdapterSettings
from .storage import Store, load_credentials
from .vendor.api_client import CredentialInvalidError, RateLimitedError, XiaoheiheApiClient, XiaoheiheApiError
from .vendor.models import Credentials, Notification, NotificationType, RoutingTarget
from .vendor.security import redact_log_text

GATEWAY = "xiaoheihe_community"


class XiaoheiheAdapter(MaiBotPlugin):
    config_model: ClassVar[Optional[Type[PluginConfigBase]]] = AdapterSettings

    def __init__(self) -> None:
        super().__init__()
        self._worker: Optional[asyncio.Task] = None
        self._digest_worker: Optional[asyncio.Task] = None
        self._publisher: Optional[DigestPublisher] = None
        self._client: Optional[XiaoheiheApiClient] = None
        self._store: Optional[Store] = None
        self._account = ""
        self._ready = False
        self._lifecycle_lock = asyncio.Lock()
        self._send_lock = asyncio.Lock()
        self._login: Optional[LoginService] = None
        self._profile: Dict[str, Any] = {}
        self._connection_error = ""
        self._browser: Optional[Browser] = None

    async def on_load(self) -> None:
        self._store = Store(self.ctx.paths.data_dir)
        await self._store.initialize()
        self._browser = Browser(self.ctx, self._store, self._send_lock)
        await self._browser.db.initialize()
        await self._browser.digest.initialize()
        self._publisher = DigestPublisher(self.ctx, self._browser, self._send_lock)
        self._login = LoginService(self.ctx.paths.data_dir, self._on_login)
        if self.config.plugin.enabled:
            await self._start_if_authenticated()

    async def on_unload(self) -> None:
        async with self._lifecycle_lock:
            if self._login is not None:
                await self._login.cancel()
            await self._stop()

    async def on_config_update(self, scope: str, config_data: Dict[str, Any], version: str) -> None:
        del version
        if scope != "self":
            return
        async with self._lifecycle_lock:
            if self._login is not None:
                await self._login.cancel()
            await self._stop()
            self.set_plugin_config(config_data)
            if self.config.plugin.enabled:
                await self._start_if_authenticated()

    async def _start_if_authenticated(self) -> None:
        try:
            credentials = await asyncio.to_thread(load_credentials, self.ctx.paths.data_dir)
        except FileNotFoundError:
            self._profile = {}
            self._account = ""
            return
        self._profile = credentials.public_dict()
        self._account = credentials.uid
        self._connection_error = ""
        self._worker = asyncio.create_task(self._run(), name="xiaoheihe-community")

    async def _on_login(self, credentials: Credentials) -> None:
        async with self._lifecycle_lock:
            await self._stop()
            self._profile = credentials.public_dict()
            if self.config.plugin.enabled:
                await self._start_if_authenticated()

    def login_service(self) -> LoginService:
        if self._login is None:
            raise RuntimeError("小黑盒登录服务尚未初始化")
        return self._login

    @API("community_status", description="读取小黑盒账号、连接和扫码状态，不触发网络请求", version="1")
    async def community_status(self) -> Dict[str, Any]:
        return {
            **self.login_service().snapshot(),
            "account_name": self._profile.get("nickname", "未登录"),
            "account_id": self._profile.get("uid", "—"),
            "has_account": bool(self._profile),
            "connection": "已连接" if self._ready else ("未连接" if self._profile else "等待登录"),
            "connection_error": self._connection_error,
        }

    @API("community_activity", description="读取当前账号的互动统计及收发记录", version="1")
    async def community_activity(self, page: int = 1) -> Dict[str, Any]:
        return await self.store().activity(self._account, page)

    @API("community_posts", description="按帖子查看浏览与评论记录，支持搜索和筛选", version="1")
    async def community_posts(self, page: int = 1, search: str = "", status: str = "all") -> Dict[str, Any]:
        return await ActivityRecords(self.store()).posts(self._account, page, search, status)

    @API("community_post_records", description="读取当前账号一篇帖子的全部互动时间线，分页返回", version="1")
    async def community_post_records(
        self, account_id: str, post_id: str, page: int = 1, kind: str = "all",
    ) -> Dict[str, Any]:
        if not self._account or self._account != account_id:
            raise ValueError("登录账号已变化，请刷新记录")
        return await ActivityRecords(self.store()).thread(self._account, post_id, page, kind)

    @API("community_record_detail", description="展开单条记录的完整浏览素材、评论正文和处理说明", version="1")
    async def community_record_detail(
        self, account_id: str, post_id: str, direction: str, message: str,
    ) -> Dict[str, Any]:
        if not self._account or self._account != account_id:
            raise ValueError("登录账号已变化，请刷新记录")
        return await ActivityRecords(self.store()).event(self._account, post_id, direction, message)

    def browser(self) -> Browser:
        if self._browser is None:
            raise RuntimeError("主动浏览服务尚未初始化")
        return self._browser

    @API("community_browse_status", description="读取主动浏览配置、分区目录及今日统计", version="1")
    async def community_browse_status(self) -> Dict[str, Any]:
        return await self.browser().snapshot(self._account)

    @API("community_browse_topics", description="从小黑盒读取真实分区目录，不发表评论", version="1")
    async def community_browse_topics(self) -> Dict[str, Any]:
        async with self._lifecycle_lock:
            if not self._ready or self._client is None:
                raise ValueError("请先在小黑盒社区页面登录并连接")
            await self.browser().refresh_catalog(self._client, self._account)
        return await self.community_browse_status()

    @API("community_browse_save", description="保存当前账号的主动浏览分区和互动频率，立即生效", version="1")
    async def community_browse_save(self, settings: Dict[str, Any], account_id: str) -> Dict[str, Any]:
        async with self._lifecycle_lock:
            if not self._account or self._account != account_id:
                raise ValueError("登录账号已变化，请刷新页面后重试")
            await self.browser().save(self._account, settings)
        return await self.community_browse_status()

    @API("community_browse_reset", description="重置当前账号今日浏览统计，保留每帖评论限制及帖子去重", version="1")
    async def community_browse_reset(self, account_id: str) -> Dict[str, Any]:
        async with self._lifecycle_lock:
            if not self._account or self._account != account_id:
                raise ValueError("登录账号已变化，请刷新页面后重试")
            async with self.browser().lock:
                await self.browser().db.reset_today(self._account)
        return await self.community_browse_status()

    @API("community_digest_action", description="重试失败总结或跳过本轮，发送未知结果不自动重发", version="1")
    async def community_digest_action(self, account_id: str, action: str) -> Dict[str, Any]:
        if action not in {"retry", "discard"}:
            raise ValueError("未知总结操作")
        async with self._lifecycle_lock:
            if not self._account or self._account != account_id:
                raise ValueError("登录账号已变化，请刷新页面后重试")
            if self._publisher is None or self._client is None or not self._ready:
                raise ValueError("请先连接小黑盒账号")
            await self._publisher.action(self._account, action)
        return await self.community_browse_status()

    @API("community_login", description="生成小黑盒登录二维码并开始扫码轮询", version="1")
    async def community_login(self) -> Dict[str, Any]:
        async with self._lifecycle_lock:
            # 更换账号前停掉旧消息连接，避免扫码期间仍用旧账号发评论。
            await self._stop()
            await self.login_service().start()
        return await self.community_status()

    @API("community_cancel_login", description="取消小黑盒扫码并恢复已保存账号的连接", version="1")
    async def community_cancel_login(self) -> Dict[str, Any]:
        async with self._lifecycle_lock:
            await self.login_service().cancel()
            await self._stop()
            if self.config.plugin.enabled:
                await self._start_if_authenticated()
        return await self.community_status()

    @API("community_reconnect", description="使用已保存的小黑盒账号重新连接", version="1")
    async def community_reconnect(self) -> Dict[str, Any]:
        async with self._lifecycle_lock:
            await self.login_service().cancel()
            await self._stop()
            if self.config.plugin.enabled:
                await self._start_if_authenticated()
        return await self.community_status()

    def store(self) -> Store:
        if self._store is None:
            raise RuntimeError("小黑盒存储尚未初始化")
        return self._store

    async def _state(self, ready: bool) -> None:
        self._ready = False
        accepted = await self.ctx.gateway.update_state(
            gateway_name=GATEWAY, ready=ready, platform=PLATFORM, account_id=self._account
        )
        if ready and not accepted:
            raise RuntimeError("宿主未接受小黑盒网关")
        self._ready = ready

    async def _stop(self) -> None:
        self._ready = False
        if self._digest_worker is not None:
            self._digest_worker.cancel()
            with suppress(asyncio.CancelledError):
                await self._digest_worker
            self._digest_worker = None
        if self._worker is not None:
            self._worker.cancel()
            with suppress(asyncio.CancelledError):
                await self._worker
            self._worker = None
        # 等待正在进行的 HTTP 发送完成，再关闭客户端。
        async with self._send_lock:
            if self._client is not None:
                await self._client.close()
                self._client = None

    async def _run(self) -> None:
        try:
            credentials = await asyncio.to_thread(load_credentials, self.ctx.paths.data_dir)
            self._account = credentials.uid
            self._client = CommunityClient("default", credentials=credentials)
            self._digest_worker = asyncio.create_task(self._run_digest(), name="xiaoheihe-digest")
            while True:
                try:
                    await self._client.check_credentials()
                    await self._state(True)
                    self._connection_error = ""
                    for kind in (NotificationType.MENTION, NotificationType.REPLY):
                        await self._poll(kind)
                    await self.browser().due(
                        self._client,
                        self._account,
                        self.config.community.context_max_chars,
                        self.config.community.reply_max_chars,
                    )
                    await asyncio.sleep(self.config.community.poll_interval)
                except CredentialInvalidError:
                    raise
                except XiaoheiheApiError as exc:
                    await self._state(False)
                    self._connection_error = f"{exc.category}（HTTP {exc.status_code}），稍后重连"
                    self.ctx.logger.warning("小黑盒请求失败: category=%s HTTP=%s", exc.category, exc.status_code)
                    await asyncio.sleep(max(self.config.community.poll_interval, exc.retry_after or 0))
        except asyncio.CancelledError:
            raise
        except CredentialInvalidError:
            self._connection_error = "登录凭证已失效，请在本页重新扫码。"
            self.ctx.logger.error("小黑盒凭证失效，连接已停止；请重新扫码并重新启用插件")
        except Exception as exc:
            # 清除 URL 查询和凭证后保留具体诊断，便于定位失败原因。
            self.ctx.logger.error("小黑盒连接停止: %s: %s", type(exc).__name__, redact_log_text(str(exc)))
            self._connection_error = redact_log_text(str(exc))
        finally:
            if self._digest_worker is not None:
                self._digest_worker.cancel()
                with suppress(asyncio.CancelledError):
                    await self._digest_worker
                self._digest_worker = None
            await self._state(False)

    async def _run_digest(self) -> None:
        while True:
            if self._ready and self._client is not None and self._publisher is not None:
                try:
                    await self._publisher.due(self._client, self._account)
                except Exception as exc:
                    self.ctx.logger.error("小黑盒总结任务检查失败: %s", redact_log_text(str(exc)))
            await asyncio.sleep(self.config.community.poll_interval)

    async def _poll(self, kind: NotificationType) -> None:
        if self._client is None:
            raise RuntimeError("小黑盒客户端尚未初始化")
        store = self.store()
        cursor = await store.cursor(self._account, kind.value)
        pending: List[Notification] = []
        newest = cursor or 0
        # 首次只记录当前通知基线；之后必须读到旧边界才能推进游标，分页上限不静默漏消息。
        for page_number in range(1, self.config.community.max_pages + 1):
            page = await self._client.fetch_notifications(kind, page=page_number)
            notifications = [item["notification"] for item in page.items]
            ids = [int(item.notification_id) for item in notifications]
            newest = max([newest, *ids])
            if cursor is None:
                await store.set_cursor(self._account, kind.value, newest)
                self.ctx.logger.info("小黑盒 %s 历史通知基线已建立", kind.value)
                return
            pending.extend(item for item in notifications if int(item.notification_id) > cursor)
            if not page.has_more or any(value <= cursor for value in ids):
                break
        else:
            raise RuntimeError("小黑盒通知分页达到上限，尚未读到旧边界；请增大 max_pages")
        for notification in sorted(pending, key=lambda item: int(item.notification_id)):
            key = f"{notification.post_id}:{notification.external_comment_id}"
            if notification.sender_uid == self._account:
                await store.mark_seen(self._account, key)
                continue
            counted_reply = await self.browser().db.note_notification(self._account, notification)
            if await store.seen(self._account, key):
                continue
            message_id = f"xhh:{notification.post_id}:{notification.external_comment_id}"
            await store.record_activity(
                self._account,
                "in",
                message_id,
                notification.post_id,
                group_id(notification),
                f"帖子 {notification.post_id} / 楼层 {notification.root_comment_id or '原帖'}",
                notification.sender_nickname or notification.sender_uid,
                plain_text(notification.content),
                "processing",
            )
            try:
                existing = await self.browser().existing_stream(self._account, notification.post_id)
                if existing is None:
                    context = await self._client.fetch_thread_context(
                        notification.post_id, root_comment_id=notification.root_comment_id
                    )
                    message = decode(notification, context, self._account, self.config.community.context_max_chars)
                else:
                    message = decode_current(notification, self._account, existing["group_id"], existing["group_name"])
                target_group = message["message_info"]["group_info"]["group_id"]
                await store.save_route(self._account, message_id, notification.route, target_group)
                if counted_reply:
                    await self.browser().db.save_anchor(self._account, message_id, notification.post_id)
                if existing is not None and kind is NotificationType.MENTION:
                    # 已入流的 @ 只是一条可回复的用户消息，不走新入站或主动召唤。
                    appended = require_success(await self.ctx.maisaka.context.append(
                        existing["stream_id"], message["raw_message"], message_id=message_id,
                        visible_text=plain_text(notification.content), source_kind="xiaoheihe_mention",
                        replyable=True, user_info=message["message_info"]["user_info"],
                    ))
                    if appended.get("replyable") is not True:
                        raise ValueError("宿主未提供可回复的用户消息支持")
                    accepted = True
                else:
                    accepted = await self.ctx.gateway.route_message(
                        gateway_name=GATEWAY, message=message, external_message_id=message_id, dedupe_key=message_id,
                    )
                await store.update_activity(
                    self._account,
                    "in",
                    message_id,
                    "accepted" if accepted else "blocked",
                    "" if accepted else "请检查宿主适配器策略及日志",
                    title=message["message_info"]["group_info"]["group_name"],
                    group=target_group,
                )
            except Exception as exc:
                await store.update_activity(self._account, "in", message_id, "error", redact_log_text(str(exc)))
                raise
            if not accepted:
                self.ctx.logger.info("宿主未接收小黑盒通知，请检查适配器策略: %s", notification.notification_id)
            await store.mark_seen(self._account, key)
        await store.set_cursor(self._account, kind.value, newest)

    @MessageGateway(
        name=GATEWAY,
        route_type="duplex",
        platform=PLATFORM,
        protocol="xiaoheihe_community",
        description="小黑盒社区 @、评论回复与图文评论双向网关",
        timeout_ms=180000,
    )
    async def send_comment(
        self,
        message: Dict[str, Any],
        route: Optional[Dict[str, Any]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        del route, metadata, kwargs
        async with self._send_lock:
            if not self._ready or self._client is None:
                return {"success": False, "error": "小黑盒网关未就绪"}
            try:
                if message.get("platform") != PLATFORM or message.get("account_id") != self._account:
                    raise ValueError("出站账号或平台与网关不一致")
                message, proactive_post = await self.browser().prepare_outbound(self._account, message)
                text, anchor, group, images = encode(message, self.config.community.reply_max_chars)
                target = await self.store().route(self._account, anchor, group)
                if proactive_post is None:
                    proactive_post = await self.browser().db.origin(self._account, anchor)
                if await self.browser().db.is_mention_anchor(self._account, target.post_id, anchor):
                    proactive_post = None
                if proactive_post is not None and target.post_id != proactive_post:
                    raise ValueError("回复目标不属于当前主动浏览的帖子")
                message_id = message["message_id"]
                if not message_id:
                    raise ValueError("出站消息缺少稳定 ID")
                if not await self.store().claim_send(self._account, message_id):
                    previous = await self.store().sent(self._account, message_id)
                    assert previous is not None
                    return {"success": True, "external_message_id": previous["external_id"]}
            except (KeyError, ValueError) as exc:
                return {"success": False, "error": str(exc)}
            comment_requested = False
            try:
                await self.store().record_activity(
                    self._account,
                    "out",
                    message_id,
                    target.post_id,
                    group,
                    message["message_info"]["group_info"].get("group_name", f"帖子 {target.post_id}"),
                    self._profile.get("nickname", "麦麦"),
                    text + (f"\n[附带 {len(images)} 张图片或表情包]" if images else ""),
                    "sending",
                    anchor,
                )
                if proactive_post is not None:
                    settings = await self.browser().settings(self._account)
                    try:
                        if not settings.enabled and (group == f"post:{proactive_post}:thread:browse" or anchor.startswith("browse:")):
                            raise ValueError("主动浏览已关闭，本次主动评论已取消")
                        await self.browser().db.begin_comment(
                            self._account,
                            proactive_post,
                        )
                    except ValueError as exc:
                        await self.store().finish_send(self._account, message_id, "blocked")
                        await self.store().update_activity(self._account, "out", message_id, "blocked", str(exc))
                        return {"success": False, "error": str(exc)}
                if images:
                    parsed = await asyncio.to_thread(lambda: [parse_image(segment) for segment in images])
                    urls = [await upload_image(self._client, image) for image in parsed]
                    await self.store().update_activity(
                        self._account, "out", message_id, "sending",
                        body=text + "\n" + "\n".join(f"[图片] {url}" for url in urls),
                    )
                    comment_requested = True
                    result = await self._client.send_media_comment(target, text, urls)
                else:
                    comment_requested = True
                    result = await self._client.send_comment(target, text)
                if not result.confirmed or not result.external_comment_id:
                    raise RuntimeError("服务端没有返回明确的评论回执")
                external_id = f"xhh:{target.post_id}:{result.external_comment_id}"
                if proactive_post is not None:
                    counted = await self.browser().db.complete_comment(self._account, message_id, proactive_post, external_id)
                    if counted:
                        await self.browser().db.save_anchor(self._account, external_id, proactive_post)
                else:
                    await self.store().finish_send(self._account, message_id, "sent", external_id)
                reply_target = RoutingTarget(
                    profile_id="default",
                    post_id=target.post_id,
                    root_comment_id=target.root_comment_id or result.external_comment_id,
                    parent_comment_id=result.external_comment_id,
                )
                await self.store().save_route(self._account, external_id, reply_target, group)
                await self.store().update_activity(self._account, "out", message_id, "sent", f"评论回执：{external_id}")
                if proactive_post is not None:
                    await self.store().update_activity(
                        self._account,
                        "browse",
                        f"browse:{proactive_post}",
                        "participated",
                        f"已发送评论：{external_id}",
                    )
                return {"success": True, "external_message_id": external_id}
            except (CommentRejectedError, CredentialInvalidError, RateLimitedError) as exc:
                error = redact_log_text(str(exc))
                await self.store().finish_send(self._account, message_id, "error")
                await self.store().update_activity(self._account, "out", message_id, "error", error)
                if isinstance(exc, CredentialInvalidError):
                    await self._state(False)
                return {"success": False, "error": error}
            except asyncio.CancelledError:
                # 取消时保留发送中的记录，只有确认成功的记录参与去重。
                raise
            except Exception as exc:
                state = "unknown" if comment_requested else "error"
                await self.store().finish_send(self._account, message_id, state)
                await self.store().update_activity(
                    self._account,
                    "out",
                    message_id,
                    state,
                    f"{redact_log_text(str(exc))}；"
                    + ("评论结果未确认" if comment_requested else "未提交评论"),
                )
                if isinstance(exc, CredentialInvalidError):
                    await self._state(False)
                self.ctx.logger.error("小黑盒发送失败: %s，comment_requested=%s", type(exc).__name__, comment_requested)
                return {"success": False, "error": (
                    f"评论结果未确认：{redact_log_text(str(exc))}" if comment_requested
                    else f"图片处理或上传失败，未提交评论：{redact_log_text(str(exc))}"
                )}


def create_plugin() -> XiaoheiheAdapter:
    return XiaoheiheAdapter()
