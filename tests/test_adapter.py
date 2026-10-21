from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import asyncio
import logging
import sys
import time

import httpx
import pytest

# 同时支持从插件独立仓库和麦麦根目录运行。
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from xiaoheihe_adapter.codec import decode, encode, group_id
from xiaoheihe_adapter import plugin as plugin_module
from xiaoheihe_adapter.community_client import CommentRejectedError
from xiaoheihe_adapter.browse import Browser, BrowseSettings
from xiaoheihe_adapter.plugin import XiaoheiheAdapter
from xiaoheihe_adapter.storage import Store, load_credentials, save_credentials
from xiaoheihe_adapter.vendor.api_client import SendUncertainError, XiaoheiheApiClient
from xiaoheihe_adapter.vendor.models import (
    ApiPage,
    Credentials,
    Notification,
    NotificationType,
    SendResult,
    ThreadContext,
)


def notification(number=10, kind=NotificationType.MENTION):
    return Notification(
        profile_id="default",
        external_event_id=str(number),
        external_comment_id=str(number),
        notification_id=str(number),
        event_type=kind,
        sender_uid="200",
        sender_nickname="盒友",
        post_id="123",
        root_comment_id="5",
        parent_comment_id=str(number),
        content="你好",
        created_at=1000,
    )


def context():
    return ThreadContext("123", "游戏讨论", "原帖内容", "300", "楼主", [])


async def adapter(tmp_path):
    plugin = XiaoheiheAdapter()
    plugin.set_plugin_config(plugin.get_default_config())
    plugin._ctx = SimpleNamespace(
        logger=logging.getLogger("xhh-test"),
        gateway=SimpleNamespace(route_message=AsyncMock(return_value=True), update_state=AsyncMock(return_value=True)),
        chat=SimpleNamespace(get_group_streams=AsyncMock(return_value=[]), open_session=AsyncMock()),
        maisaka=SimpleNamespace(
            context=SimpleNamespace(append=AsyncMock(return_value={"success": True, "replyable": True})),
            proactive=SimpleNamespace(trigger=AsyncMock()),
        ),
    )
    plugin._account = "100"
    plugin._ready = True
    plugin._store = Store(tmp_path)
    await plugin._store.initialize()
    plugin._browser = Browser(plugin.ctx, plugin._store, plugin._send_lock)
    await plugin._browser.db.initialize()
    await plugin._browser.digest.initialize()
    plugin._client = SimpleNamespace(
        fetch_notifications=AsyncMock(),
        fetch_thread_context=AsyncMock(return_value=context()),
        send_comment=AsyncMock(return_value=SendResult("999", True)),
    )
    return plugin


def test_first_poll_only_establishes_baseline(tmp_path):
    async def check():
        plugin = await adapter(tmp_path)
        plugin._client.fetch_notifications.return_value = ApiPage([{"notification": notification()}])
        await plugin._poll(NotificationType.MENTION)
        assert await plugin.store().cursor("100", "mention") == 10
        plugin.ctx.gateway.route_message.assert_not_called()

    asyncio.run(check())


def test_pagination_and_cross_type_deduplication(tmp_path):
    async def check():
        plugin = await adapter(tmp_path)
        await plugin.store().set_cursor("100", "mention", 5)
        await plugin.store().set_cursor("100", "reply", 5)
        plugin._client.fetch_notifications.side_effect = [
            ApiPage([{"notification": notification(12)}], has_more=True),
            ApiPage([{"notification": notification(10)}, {"notification": notification(5)}]),
            ApiPage([{"notification": notification(10, NotificationType.REPLY)}]),
        ]
        await plugin._poll(NotificationType.MENTION)
        await plugin._poll(NotificationType.REPLY)
        assert plugin.ctx.gateway.route_message.await_count == 2
        ids = [call.kwargs["message"]["message_id"] for call in plugin.ctx.gateway.route_message.call_args_list]
        assert ids == ["xhh:123:10", "xhh:123:12"]
        assert await plugin.store().cursor("100", "mention") == 12
        activity = await plugin.store().activity("100")
        assert activity["received"] == 2
        assert activity["replied"] == 0
        assert activity["conversations"] == 1
        assert all(item["state"] == "accepted" for item in activity["items"])

    asyncio.run(check())


async def seed_browsed_post(plugin, count=1):
    await plugin.browser().db.save("100", BrowseSettings(enabled=True, topic_ids=["1"]).model_dump())
    await plugin.browser().db.claim_post("100", "123")
    for index in range(count):
        message_id = f"seed:{index}"
        await plugin.store().claim_send("100", message_id)
        await plugin.store().record_activity(
            "100", "out", message_id, "123", "post:123:thread:browse", "标题", "麦麦", "正文", "sending", "browse:123",
        )
        await plugin.browser().db.complete_comment("100", message_id, "123", f"xhh:123:seed-{index}")


def test_normal_reply_adds_credit_and_uses_it_while_mention_is_independent(tmp_path):
    async def check():
        plugin = await adapter(tmp_path)
        await seed_browsed_post(plugin, 3)
        await plugin.store().set_cursor("100", "reply", 5)
        n = replace(notification(12, NotificationType.REPLY), created_at=time.time() + 10, raw={"userid_b": "100"})
        plugin._client.fetch_notifications.return_value = ApiPage([{"notification": n}])
        await plugin._poll(NotificationType.REPLY)
        assert await plugin.browser().db.post_budget("100", "123") == {"used": 3, "limit": 6}
        assert await plugin.browser().db.origin("100", "xhh:123:12") == "123"
        assert (await plugin.send_comment(outgoing(n)))["success"]
        assert await plugin.browser().db.post_budget("100", "123") == {"used": 4, "limit": 6}
        await plugin.store().set_cursor("100", "mention", 5)
        mention = replace(notification(13, NotificationType.MENTION), created_at=time.time() + 10)
        plugin._client.fetch_notifications.return_value = ApiPage([{"notification": mention}])
        await plugin._poll(NotificationType.MENTION)
        assert await plugin.browser().db.origin("100", "xhh:123:13") is None
        assert (await plugin.send_comment(outgoing(mention, "out:mention")))["success"]
        assert await plugin.browser().db.post_budget("100", "123") == {"used": 4, "limit": 6}
        assert plugin.ctx.gateway.route_message.await_count == 2
    asyncio.run(check())


@pytest.mark.parametrize("mention_first", [True, False])
def test_same_comment_cross_type_notifies_host_once_and_mention_never_adds_quota(tmp_path, mention_first):
    async def check():
        plugin = await adapter(tmp_path)
        await seed_browsed_post(plugin)
        for kind in (NotificationType.MENTION, NotificationType.REPLY):
            await plugin.store().set_cursor("100", kind.value, 5)
        n = replace(notification(12), created_at=time.time() + 10, raw={"userid_b": "100"})
        kinds = (NotificationType.MENTION, NotificationType.REPLY) if mention_first else (
            NotificationType.REPLY, NotificationType.MENTION,
        )
        for kind in kinds:
            plugin._client.fetch_notifications.return_value = ApiPage([{"notification": replace(n, event_type=kind)}])
            await plugin._poll(kind)
        plugin.ctx.gateway.route_message.assert_awaited_once()
        assert await plugin.browser().db.post_budget("100", "123") == {"used": 1, "limit": 3}
        assert await plugin.browser().db.origin("100", "xhh:123:12") is None
    asyncio.run(check())


def test_mention_in_existing_post_only_appends_user_message_without_waking_or_opening(tmp_path):
    async def check():
        plugin = await adapter(tmp_path)
        await seed_browsed_post(plugin, 3)
        group = "post:123:thread:browse"
        await plugin.browser().db.save_stream("100", "123", "real-stream", group, "原来的帖子流")
        await plugin.browser().db.save_anchor("100", "browse:123", "123")
        await plugin.store().set_cursor("100", "mention", 5)
        n = replace(notification(12, NotificationType.MENTION), created_at=time.time() + 10)
        plugin._client.fetch_notifications.return_value = ApiPage([{"notification": n}])
        await plugin._poll(NotificationType.MENTION)
        plugin.ctx.gateway.route_message.assert_not_called()
        plugin.ctx.chat.open_session.assert_not_called()
        plugin.ctx.maisaka.proactive.trigger.assert_not_called()
        plugin._client.fetch_thread_context.assert_not_called()
        appended = plugin.ctx.maisaka.context.append.call_args
        assert appended.args == ("real-stream", [{"type": "text", "data": n.content}])
        assert appended.kwargs["message_id"] == "xhh:123:12" and appended.kwargs["replyable"]
        assert appended.kwargs["user_info"]["user_id"] == "200"
        route = await plugin.store().route("100", "xhh:123:12", group)
        assert route.parent_comment_id == "12"
        message = outgoing(n)
        message["message_info"]["group_info"]["group_id"] = group
        assert (await plugin.send_comment(message))["success"]
        assert await plugin.browser().db.post_budget("100", "123") == {"used": 3, "limit": 3}
        activity = await plugin.store().activity("100")
        incoming = next(item for item in activity["items"] if item["direction"] == "in")
        assert incoming["group_id"] == group
    asyncio.run(check())


def test_existing_post_flow_is_discovered_by_account_and_reused_for_new_root_reply(tmp_path):
    async def check():
        plugin = await adapter(tmp_path)
        group = "post:123:thread:old-root"
        plugin.ctx.chat.get_group_streams.return_value = [
            {"stream_id": "wrong-account", "account_id": "999", "platform": "xiaoheihe", "group_id": group, "group_name": "其他账号"},
            {"stream_id": "old-stream", "account_id": "100", "platform": "xiaoheihe", "group_id": group, "group_name": "旧帖子流"},
        ]
        await plugin.store().set_cursor("100", "mention", 5)
        n = notification(12, NotificationType.MENTION)
        plugin._client.fetch_notifications.return_value = ApiPage([{"notification": n}])
        await plugin._poll(NotificationType.MENTION)
        assert plugin.ctx.maisaka.context.append.call_args.args[0] == "old-stream"
        plugin.ctx.gateway.route_message.assert_not_called()
        plugin.ctx.chat.open_session.assert_not_called()
        await plugin.store().set_cursor("100", "reply", 12)
        new_reply = replace(notification(13, NotificationType.REPLY), root_comment_id="new-root")
        plugin._client.fetch_notifications.return_value = ApiPage([{"notification": new_reply}])
        await plugin._poll(NotificationType.REPLY)
        routed = plugin.ctx.gateway.route_message.call_args.kwargs["message"]
        assert routed["message_info"]["group_info"]["group_id"] == group
        assert routed["raw_message"] == [{"type": "text", "data": new_reply.content}]
        saved = await plugin.store().route("100", "xhh:123:13", group)
        assert saved.root_comment_id == "new-root" and saved.parent_comment_id == "13"
        plugin.ctx.chat.get_group_streams.assert_awaited_once()
    asyncio.run(check())


def test_pagination_limit_preserves_cursor(tmp_path):
    async def check():
        plugin = await adapter(tmp_path)
        plugin.config.community.max_pages = 1
        await plugin.store().set_cursor("100", "mention", 5)
        plugin._client.fetch_notifications.return_value = ApiPage([{"notification": notification(12)}], has_more=True)
        with pytest.raises(RuntimeError, match="分页达到上限"):
            await plugin._poll(NotificationType.MENTION)
        assert await plugin.store().cursor("100", "mention") == 5
        plugin.ctx.gateway.route_message.assert_not_called()

    asyncio.run(check())


def outgoing(item, message_id="out:1"):
    return {
        "platform": "xiaoheihe",
        "account_id": "100",
        "message_id": message_id,
        "reply_to": f"xhh:123:{item.external_comment_id}",
        "message_info": {"group_info": {"group_id": group_id(item)}},
        "raw_message": [{"type": "text", "data": "收到"}],
    }


def test_exact_reply_target_and_idempotent_send(tmp_path):
    async def check():
        plugin = await adapter(tmp_path)
        first, second = notification(10), notification(12)
        for item in (first, second):
            await plugin.store().save_route("100", f"xhh:123:{item.external_comment_id}", item.route, group_id(item))
        message = outgoing(first)
        first_result = await plugin.send_comment(message)
        second_result = await plugin.send_comment(message)
        assert first_result == second_result == {"success": True, "external_message_id": "xhh:123:999"}
        plugin._client.send_comment.assert_awaited_once()
        assert plugin._client.send_comment.call_args.args[0].parent_comment_id == "10"
        message["message_id"] = "out:2"
        message["message_info"]["group_info"]["group_id"] = "another-thread"
        assert not (await plugin.send_comment(message))["success"]
        assert plugin._client.send_comment.await_count == 1
        activity = await plugin.store().activity("100")
        assert activity["replied"] == 1
        assert activity["pagination"]["total"] == 1
        assert activity["items"][0]["body"] == "收到"
        assert activity["items"][0]["anchor"] == "xhh:123:10"

    asyncio.run(check())


def test_unknown_send_can_retry_after_restart_and_success_is_deduplicated(tmp_path):
    async def check():
        plugin = await adapter(tmp_path)
        item = notification()
        await plugin.store().save_route("100", "xhh:123:10", item.route, group_id(item))
        plugin._client.send_comment.side_effect = SendUncertainError("timeout")
        assert not (await plugin.send_comment(outgoing(item)))["success"]
        restarted = await adapter(tmp_path)
        assert (await restarted.send_comment(outgoing(item)))["success"]
        assert (await restarted.send_comment(outgoing(item)))["success"]
        restarted._client.send_comment.assert_awaited_once()
        activity = await restarted.store().activity("100")
        assert activity["replied"] == 1
        assert activity["attention"] == 0
        assert activity["items"][0]["state"] == "sent"

    asyncio.run(check())


def test_explicit_api_rejection_is_recorded_as_error_not_unknown(tmp_path):
    async def check():
        plugin = await adapter(tmp_path)
        item = notification()
        await plugin.store().save_route("100", "xhh:123:10", item.route, group_id(item))
        plugin._client.send_comment.side_effect = CommentRejectedError("缺失参数", category="upstream_rejected")
        result = await plugin.send_comment(outgoing(item))
        assert result == {"success": False, "error": "缺失参数"}
        assert (await plugin.store().sent("100", outgoing(item)["message_id"]))["state"] == "error"
        assert (await plugin.store().activity("100"))["items"][0]["state"] == "error"
    asyncio.run(check())


def test_no_identity_fabrication_and_no_media_silently_dropped():
    item = notification()
    item.sender_uid = ""
    with pytest.raises(ValueError, match="真实发送者"):
        decode(item, context(), "100", 6000)
    message = outgoing(notification())
    message["raw_message"].append({"type": "image", "data": "image"})
    with pytest.raises(ValueError, match="缺少二进制内容"):
        encode(message, 500)


@pytest.mark.parametrize("upload_failed", [False, True])
def test_media_upload_before_comment_and_failure_does_not_post_or_retry(tmp_path, monkeypatch, upload_failed):
    async def check():
        plugin = await adapter(tmp_path)
        plugin._client.send_media_comment = AsyncMock(return_value=SendResult("999", True))
        uploader = AsyncMock(return_value="https://imgheybox.max-c.com/test.png")
        if upload_failed:
            uploader.side_effect = RuntimeError("上传失败")
        monkeypatch.setattr(plugin_module, "upload_image", uploader)
        monkeypatch.setattr(plugin_module, "parse_image", lambda segment: segment)
        item = notification()
        await plugin.store().save_route("100", "xhh:123:10", item.route, group_id(item))
        message = outgoing(item)
        message["raw_message"] = [{"type": "emoji", "data": "表情", "binary_data_base64": "test-only"}]
        result = await plugin.send_comment(message)
        assert result["success"] is not upload_failed
        plugin._client.send_comment.assert_not_called()
        activity = (await plugin.store().activity("100"))["items"][0]
        if upload_failed:
            plugin._client.send_media_comment.assert_not_called()
            assert "未提交评论" in result["error"]
            assert activity["state"] == "error"
            assert not (await plugin.send_comment(message))["success"]
            assert uploader.await_count == 2
        else:
            call = plugin._client.send_media_comment.call_args
            assert call.args[0].parent_comment_id == "10"
            assert call.args[1] == ""
            assert call.args[2] == ["https://imgheybox.max-c.com/test.png"]
            assert "https://imgheybox.max-c.com/test.png" in activity["body"]
    asyncio.run(check())


def test_credentials_round_trip(tmp_path):
    credentials = Credentials("default", "100", "麦麦", {"pkey": "test-only"})
    save_credentials(tmp_path, credentials)
    assert load_credentials(tmp_path) == credentials


def test_activity_is_paginated_persistent_and_account_scoped(tmp_path):
    async def check():
        store = Store(tmp_path)
        await store.initialize()
        for index in range(51):
            await store.record_activity(
                "100", "in", str(index), "123", "thread-1", "游戏讨论", "盒友", "你好", "accepted"
            )
        await store.record_activity(
            "200", "in", "private", "456", "thread-2", "其他账号", "另一位盒友", "不应显示", "error"
        )
        reopened = Store(tmp_path)
        first = await reopened.activity("100")
        second = await reopened.activity("100", 2)
        assert first["received"] == 51
        assert first["attention"] == 0
        assert first["conversations"] == 1
        assert first["pagination"] == {"page": 1, "pages": 2, "total": 51}
        assert len(first["items"]) == 50
        assert len(second["items"]) == 1
        assert all(item["title"] == "游戏讨论" for item in first["items"])
        assert (await reopened.activity("100", 999))["pagination"]["page"] == 2

    asyncio.run(check())


def test_http_post_timeout_never_retries():
    async def check():
        calls = []

        def handler(request):
            calls.append(request)
            assert request.url.host == "workshopapi.xiaoheihe.cn"
            assert b"reply_id=10" in request.content
            raise httpx.ReadTimeout("timeout", request=request)

        async with httpx.AsyncClient(
            base_url="https://api.xiaoheihe.cn", transport=httpx.MockTransport(handler)
        ) as http:
            client = XiaoheiheApiClient(
                "default",
                credentials=Credentials("default", "100", "麦麦", {"pkey": "test"}),
                client=http,
                min_request_interval_seconds=0,
                max_retries=3,
            )
            with pytest.raises(SendUncertainError):
                await client.send_comment(notification().route, "收到")
        assert len(calls) == 1

    asyncio.run(check())
