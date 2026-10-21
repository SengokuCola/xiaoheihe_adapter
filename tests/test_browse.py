from maibot_sdk.context import PluginContext
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import asyncio
import logging
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from xiaoheihe_adapter.browse import Browser, BrowseSettings, parse_catalog
from xiaoheihe_adapter.community_client import CommunityContext
from xiaoheihe_adapter.plugin import XiaoheiheAdapter
from xiaoheihe_adapter.storage import Store
from xiaoheihe_adapter.vendor.endpoints import EndpointName
from xiaoheihe_adapter.vendor.models import RoutingTarget, SendResult, ThreadContext


async def setup(tmp_path):
    ctx = SimpleNamespace(
        logger=logging.getLogger("browse-test"),
        chat=SimpleNamespace(
            open_session=AsyncMock(return_value={"success": True, "stream_id": "host-session"}),
            get_group_streams=AsyncMock(return_value=[]),
        ),
        maisaka=SimpleNamespace(
            context=SimpleNamespace(append=AsyncMock(return_value={"success": True, "replyable": True})),
            proactive=SimpleNamespace(trigger=AsyncMock(return_value={"success": True, "task_id": "proactive:task"})),
        ),
    )
    store = Store(tmp_path)
    await store.initialize()
    browser = Browser(ctx, store, asyncio.Lock())
    await browser.db.initialize()
    await browser.digest.initialize()
    await browser.db.save_catalog(
        "100", [{"id": "1", "name": "游戏", "group": "社区"}, {"id": "2", "name": "数码", "group": "社区"}]
    )
    return browser, ctx


def test_settings_and_real_catalog_validation():
    assert not BrowseSettings().enabled
    with pytest.raises(ValueError, match="至少一个"):
        BrowseSettings(enabled=True)
    with pytest.raises(ValueError):
        BrowseSettings(topic_ids=["fake-topic"])
    assert parse_catalog(
        {
            "result": {
                "topics_list": [
                    {
                        "name": "游戏",
                        "children": [{"topic_id": 12, "name": "单机游戏"}, {"topic_id": 12, "name": "单机游戏"}],
                    }
                ]
            }
        }
    ) == [{"id": "12", "name": "单机游戏", "group": "游戏"}]
    with pytest.raises(ValueError, match="topics_list"):
        parse_catalog({"result": {"unknown": []}})


def test_browse_uses_native_decision_and_does_not_send_directly(tmp_path):
    async def check():
        browser, ctx = await setup(tmp_path)
        client = SimpleNamespace(
            _request=AsyncMock(
                return_value=SimpleNamespace(
                    payload={
                        "result": {
                            "links": [
                                {
                                    "linkid": "123",
                                    "title": "游戏讨论",
                                    "description": "一篇帖子",
                                    "create_at": 100,
                                    "user": {"userid": "200", "nickname": "盒友"},
                                },
                            ]
                        }
                    }
                )
            ),
            fetch_thread_context=AsyncMock(
                return_value=ThreadContext(
                    "123",
                    "游戏讨论",
                    "完整帖子",
                    "200",
                    "盒友",
                    [
                        {"id": "88", "user": {"userid": "300", "username": "评论者"}, "text": "评论内容"},
                    ],
                )
            ),
            send_comment=AsyncMock(),
        )
        await browser.due(client, "100", 6000, 500)
        client._request.assert_not_called()
        settings = BrowseSettings(enabled=True, topic_ids=["1", "2"])
        await browser.save("100", settings.model_dump())
        await browser.due(client, "100", 6000, 500)
        assert client._request.await_count == 2
        assert all(call.args[0] is EndpointName.TOPIC_FEED for call in client._request.call_args_list)
        ctx.maisaka.proactive.trigger.assert_awaited_once()
        appended = ctx.maisaka.context.append.call_args_list
        assert len(appended) == 2
        assert "作者：盒友（UID 200）" in appended[0].kwargs["visible_text"]
        assert "发言者：评论者（UID 300）" in appended[1].kwargs["visible_text"]
        assert "评论内容" in appended[1].kwargs["visible_text"]
        assert ctx.chat.open_session.call_args.kwargs["group_id"] == "post:123:thread:browse"
        assert "自行决定是否参与" in ctx.maisaka.proactive.trigger.call_args.args[1]
        client.send_comment.assert_not_called()
        assert await browser.db.origin("100", "proactive:task") == "123"
        route = await browser.store.route("100", "browse:123", "post:123:thread:browse")
        assert route.post_id == "123" and not route.parent_comment_id
        comment_route = await browser.store.route("100", "browse:123:comment:88", "post:123:thread:browse")
        assert comment_route.root_comment_id == "88" and comment_route.parent_comment_id == "88"
        assert appended[1].kwargs["replyable"] is True
        assert appended[1].kwargs["user_info"] == {"user_id": "300", "user_nickname": "评论者"}
        await browser.due(client, "100", 6000, 500)
        assert client._request.await_count == 2  # 调度间隔持久化，页面查询不会加速浏览。
        await browser.store.execute("DELETE FROM browse_schedule WHERE account='100'")
        await browser.due(client, "100", 6000, 500)
        ctx.maisaka.proactive.trigger.assert_awaited_once()  # 同一帖子不会重复判断。
        activity = await browser.store.activity("100")
        assert activity["items"][0]["state"] == "considering"
        assert "评论内容" in activity["items"][0]["body"]
        assert activity["replied"] == 0

    asyncio.run(check())


def test_daily_counts_do_not_limit_browsing_or_comment_attempts(tmp_path):
    async def check():
        browser, _ = await setup(tmp_path)
        # 已浏览并尝试评论 205 个不同帖子，超过被删除的两个每日配置上限。
        await browser.store.execute(
            "WITH RECURSIVE posts(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM posts WHERE n<205) "
            "INSERT INTO browse_jobs(account,post,created_at,reserved_at) "
            "SELECT '100',CAST(n AS TEXT),strftime('%s','now'),strftime('%s','now') FROM posts"
        )
        assert await browser.db.can_consider("100", "206")
        assert await browser.db.claim_post("100", "206")
        await browser.db.begin_comment("100", "206")
        reopened, _ = await setup(tmp_path)
        assert not await reopened.db.can_consider("100", "206")
        assert not await reopened.db.claim_post("100", "206")
        await reopened.db.begin_comment("100", "206")
        assert await reopened.db.claim_post("200", "206")
        await reopened.db.begin_comment("200", "206")
        status = await reopened.db.status("100")
        assert status["today_decisions"] == 206
        assert status["today_attempts"] == 206

    asyncio.run(check())


@pytest.mark.parametrize("has_stream", [False, True])
def test_existing_stream_uses_real_sdk_response_normalization(tmp_path, has_stream):
    async def check():
        browser, _ = await setup(tmp_path)
        stream = {
            "stream_id": "real-stream", "account_id": "100", "platform": "xiaoheihe",
            "group_id": "post:123:thread:browse", "group_name": "帖子讨论",
        }
        rpc = AsyncMock(return_value={"success": True, "streams": [stream] if has_stream else []})
        browser.ctx = PluginContext("maibot-team.xiaoheihe-adapter", rpc_call=rpc)
        expected = {key: stream[key] for key in ("stream_id", "group_id", "group_name")} if has_stream else None
        assert await browser.existing_stream("100", "123") == expected
        assert rpc.call_args.args == ("cap.call", "maibot-team.xiaoheihe-adapter", {
            "capability": "chat.get_group_streams", "args": {"platform": "xiaoheihe"},
        })
        assert await browser.db.post_stream("100", "123") == expected

    asyncio.run(check())


def test_browsing_reuses_post_flow_created_by_earlier_mention(tmp_path):
    async def check():
        browser, ctx = await setup(tmp_path)
        group = "post:123:thread:old-root"
        ctx.chat.get_group_streams.return_value = [{
            "stream_id": "existing-real-stream", "account_id": "100", "platform": "xiaoheihe",
            "group_id": group, "group_name": "已有帖子流",
        }]
        await browser.save("100", BrowseSettings(enabled=True, topic_ids=["1"]).model_dump())
        client = SimpleNamespace(
            _request=AsyncMock(return_value=SimpleNamespace(payload={"result": {"links": [{
                "linkid": "123", "title": "原帖", "description": "正文", "user": {"userid": "200", "username": "作者"},
            }]}})),
            fetch_thread_context=AsyncMock(return_value=ThreadContext("123", "原帖", "正文", "200", "作者", [
                {"commentid": "8", "text": "评论", "user": {"userid": "300", "username": "评论者"}},
            ])),
        )
        await browser.due(client, "100", 6000, 500)
        ctx.chat.open_session.assert_not_called()
        assert ctx.maisaka.proactive.trigger.call_args.args[0] == "existing-real-stream"
        assert all(call.args[0] == "existing-real-stream" for call in ctx.maisaka.context.append.call_args_list)
        route = await browser.store.route("100", "browse:123:comment:8", group)
        assert route.parent_comment_id == "8"
        assert await browser.db.origin("100", "browse:123:comment:8") == "123"
        assert (await browser.store.activity("100"))["items"][0]["group_id"] == group
    asyncio.run(check())


def test_deleted_setting_is_removed_from_saved_preferences(tmp_path):
    async def check():
        browser, _ = await setup(tmp_path)
        await browser.db.save("100", {
            **BrowseSettings().model_dump(), "min_comment_gap_seconds": 1800,
            "daily_comments": 99, "daily_decisions": 200,
        })
        await browser.db.initialize()
        saved = await browser.db.settings("100")
        assert not {"min_comment_gap_seconds", "daily_comments", "daily_decisions"}.intersection(saved)
        assert await browser.settings("100") == BrowseSettings()
    asyncio.run(check())


def test_video_posts_skip_details_or_model_without_spending_budget(tmp_path):
    async def check():
        browser, ctx = await setup(tmp_path)
        await browser.save("100", BrowseSettings(enabled=True, topic_ids=["1"]).model_dump())
        client = SimpleNamespace(
            _request=AsyncMock(return_value=SimpleNamespace(payload={"result": {"links": [
                {"linkid": "1", "has_video": 1, "user": {"userid": "200"}},
                {"linkid": "2", "user": {"userid": "200"}},
                {"linkid": "3", "user": {"userid": "200"}},
            ]}})),
            fetch_thread_context=AsyncMock(side_effect=[
                CommunityContext("2", "详情含视频", "正文", "200", "盒友", [], has_video=True),
                CommunityContext("3", "文字帖子", "正文", "200", "盒友", [], comment_count=0),
            ]),
        )
        await browser.due(client, "100", 6000, 500)
        assert [call.args[0] for call in client.fetch_thread_context.call_args_list] == ["2", "3"]
        assert (await browser.db.status("100"))["today_decisions"] == 1
        ctx.maisaka.proactive.trigger.assert_awaited_once()
        assert ctx.chat.open_session.call_args.kwargs["group_id"] == "post:3:thread:browse"
        assert await browser.store.seen("100", "video-post:2")
        await browser.store.execute("DELETE FROM browse_schedule WHERE account='100'")
        await browser.due(client, "100", 6000, 500)
        assert client.fetch_thread_context.await_count == 2
    asyncio.run(check())


def test_reset_today_only_resets_statistics_without_clearing_deduplication(tmp_path):
    async def check():
        browser, _ = await setup(tmp_path)
        for post in ("1", "2"):
            assert await browser.db.claim_post("100", post)
        assert await browser.db.claim_post("200", "other")
        await browser.db.begin_comment("100", "1")
        await browser.db.reset_today("100")
        await browser.db.reset_today("100")
        restarted, _ = await setup(tmp_path)
        status = await restarted.db.status("100")
        assert status["today_decisions"] == 0
        assert status["today_attempts"] == 1
        assert (await restarted.db.status("200"))["today_decisions"] == 1
        assert not await restarted.db.claim_post("100", "1")
        assert await restarted.db.claim_post("100", "3")
        assert (await restarted.db.status("100"))["today_decisions"] == 1
        await restarted.db.begin_comment("100", "2")
        assert (await restarted.db.status("100"))["today_attempts"] == 2
        await restarted.store.execute(
            "INSERT INTO browse_resets VALUES (?, ?, ?)", ("100", restarted.db.day_start() - 86400, 999)
        )
        assert (await restarted.db.status("100"))["today_decisions"] == 1

    asyncio.run(check())


def test_send_gate_cannot_be_bypassed_by_changing_quote_and_respects_disable(tmp_path):
    async def check():
        browser, ctx = await setup(tmp_path)
        plugin = XiaoheiheAdapter()
        plugin._ctx = ctx
        plugin.set_plugin_config(plugin.get_default_config())
        plugin._account = "100"
        plugin._ready = True
        plugin._store = browser.store
        plugin._browser = browser
        plugin._send_lock = browser.send_lock
        plugin._client = SimpleNamespace(send_comment=AsyncMock(side_effect=[SendResult(str(i), True) for i in range(900, 904)]))
        await browser.save("100", BrowseSettings(enabled=True, topic_ids=["1"]).model_dump())
        for post in ("123", "456"):
            assert await browser.db.claim_post("100", post)
            await browser.db.save_anchor("100", f"browse:{post}", post)
            await browser.store.save_route(
                "100", f"browse:{post}", RoutingTarget("default", post), f"post:{post}:thread:browse"
            )
        message = {
            "platform": "xiaoheihe",
            "account_id": "100",
            "message_id": "out:1",
            "reply_to": "changed-quote",
            "message_info": {"group_info": {"group_id": "post:123:thread:browse", "group_name": "游戏讨论"}},
            "raw_message": [
                {"type": "text", "data": "有趣的想法"},
                {"type": "reply", "data": {"target_message_id": "another-quote"}},
            ],
        }
        assert not (await plugin.send_comment(message))["success"]  # 未登记目标不能悄悄改成主帖。
        plugin._client.send_comment.assert_not_called()
        message["reply_to"] = "browse:123"
        message["raw_message"] = [{"type": "text", "data": "有趣的想法"}]
        assert (await plugin.send_comment(message))["success"]
        assert plugin._client.send_comment.call_args.args[0].post_id == "123"
        assert not plugin._client.send_comment.call_args.args[0].parent_comment_id
        message["message_id"] = "out:2"
        assert (await plugin.send_comment(message))["success"]
        message["message_id"] = "out:3"
        assert (await plugin.send_comment(message))["success"]
        message["message_id"] = "out:4"
        assert not (await plugin.send_comment(message))["success"]
        await browser.save("100", BrowseSettings(topic_ids=["1"]).model_dump())
        message["message_id"] = "out:disabled"
        message["reply_to"] = "browse:456"
        message["message_info"]["group_info"]["group_id"] = "post:456:thread:browse"
        assert not (await plugin.send_comment(message))["success"]
        assert plugin._client.send_comment.await_count == 3
        # 被动回复仍可发送，不受主动开关和主动额度影响。
        message["message_id"] = "out:passive"
        message["reply_to"] = "incoming"
        message["raw_message"] = [{"type": "text", "data": "回复盒友"}]
        message["message_info"]["group_info"]["group_id"] = "post:456:thread:7"
        await browser.store.save_route(
            "100", "incoming", RoutingTarget("default", "456", "7", "8"), "post:456:thread:7"
        )
        assert (await plugin.send_comment(message))["success"]
        assert plugin._client.send_comment.await_count == 4

    asyncio.run(check())


@pytest.mark.parametrize("root,parent", [("", ""), ("7", "7"), ("7", "8")])
def test_reply_target_selects_post_comment_or_nested_reply(tmp_path, root, parent):
    async def check():
        browser, ctx = await setup(tmp_path)
        plugin = XiaoheiheAdapter()
        plugin._ctx = ctx
        plugin.set_plugin_config(plugin.get_default_config())
        plugin._account = "100"
        plugin._ready = True
        plugin._store = browser.store
        plugin._browser = browser
        plugin._send_lock = browser.send_lock
        plugin._client = SimpleNamespace(send_comment=AsyncMock(side_effect=[SendResult(str(i), True) for i in range(900, 903)]))
        await browser.save("100", BrowseSettings(enabled=True, topic_ids=["1"]).model_dump())
        await browser.db.claim_post("100", "123")
        await browser.db.save_anchor("100", "browse:123", "123")
        group = "post:123:thread:browse"
        anchor = f"browse:123:comment:{parent}" if parent else "browse:123"
        await browser.store.save_route("100", anchor, RoutingTarget("default", "123", root, parent), group)
        message = {
            "platform": "xiaoheihe", "account_id": "100", "message_id": "out:target", "reply_to": anchor,
            "message_info": {"group_info": {"group_id": group}},
            "raw_message": [{"type": "text", "data": "回复对应发言"}],
        }
        # 重建 Store 后仍读取同一条精确路由，不依赖进程内的最后一条消息。
        plugin._store = Store(tmp_path)
        await browser.store.save_route("100", "wrong-post", RoutingTarget("default", "456", "7", "8"), group)
        message["reply_to"] = "wrong-post"
        assert not (await plugin.send_comment(message))["success"]
        plugin._client.send_comment.assert_not_called()
        message["reply_to"] = anchor
        assert (await plugin.send_comment(message))["success"]
        target = plugin._client.send_comment.call_args.args[0]
        assert (target.post_id, target.root_comment_id, target.parent_comment_id) == ("123", root, parent)
        message["message_id"] = "out:second"
        assert (await plugin.send_comment(message))["success"]
        message["message_id"] = "out:third"
        assert (await plugin.send_comment(message))["success"]
        message["message_id"] = "out:fourth"
        assert not (await plugin.send_comment(message))["success"]
        assert plugin._client.send_comment.await_count == 3
    asyncio.run(check())


def test_failed_proactive_send_can_retry_and_only_success_blocks_new_calls(tmp_path):
    async def check():
        browser, ctx = await setup(tmp_path)
        plugin = XiaoheiheAdapter()
        plugin._ctx = ctx
        plugin.set_plugin_config(plugin.get_default_config())
        plugin._account = "100"
        plugin._ready = True
        plugin._store = browser.store
        plugin._browser = browser
        plugin._send_lock = browser.send_lock
        plugin._client = SimpleNamespace(send_comment=AsyncMock(side_effect=[
            RuntimeError("发送失败"), SendResult("999", True), SendResult("1000", True), SendResult("1001", True),
        ]))
        await browser.save("100", BrowseSettings(enabled=True, topic_ids=["1"]).model_dump())
        await browser.db.claim_post("100", "123")
        await browser.db.save_anchor("100", "browse:123", "123")
        group = "post:123:thread:browse"
        await browser.store.save_route("100", "browse:123", RoutingTarget("default", "123"), group)
        message = {
            "platform": "xiaoheihe", "account_id": "100", "message_id": "out:retry", "reply_to": "browse:123",
            "message_info": {"group_info": {"group_id": group}},
            "raw_message": [{"type": "text", "data": "回复"}],
        }
        assert not (await plugin.send_comment(message))["success"]
        assert (await plugin.send_comment(message))["success"]
        message["message_id"] = "out:next"
        assert (await plugin.send_comment(message))["success"]
        message["message_id"] = "out:third"
        assert (await plugin.send_comment(message))["success"]
        message["message_id"] = "out:fourth"
        result = await plugin.send_comment(message)
        assert not result["success"] and "3/3" in result["error"]
        assert plugin._client.send_comment.await_count == 4
    asyncio.run(check())
