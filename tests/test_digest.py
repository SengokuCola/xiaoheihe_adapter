from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock
from urllib.parse import parse_qs

import asyncio
import httpx
import json
import logging
import sys
import time

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from xiaoheihe_adapter.browse import Browser, BrowseSettings
from xiaoheihe_adapter.comment_signing import comment_query
from xiaoheihe_adapter.community_client import CommentRejectedError, CommunityClient
from xiaoheihe_adapter.digest import DigestDraft, DigestPublisher, DigestSettings, material_excerpts
from xiaoheihe_adapter.storage import Store
from xiaoheihe_adapter.vendor.api_client import SendUncertainError
from xiaoheihe_adapter.vendor.models import Credentials


async def setup(tmp_path, settings=None):
    ctx = SimpleNamespace(
        logger=logging.getLogger("digest-test"),
        call_capability=AsyncMock(return_value={"success": True, "text": json.dumps({
            "title": "逛社区的一点感想", "paragraphs": ["最近看到了不同的想法。", "我自己的想法是再多听听。"],
        }, ensure_ascii=False)}),
    )
    store = Store(tmp_path)
    await store.initialize()
    browser = Browser(ctx, store, asyncio.Lock())
    await browser.db.initialize()
    await browser.digest.initialize()
    config = settings if settings is not None else DigestSettings(enabled=True, every_posts=2, topic_id="1")
    await browser.db.save("100", BrowseSettings(summary=config).model_dump())
    publisher = DigestPublisher(ctx, browser, browser.send_lock)
    client = SimpleNamespace(publish_post=AsyncMock(return_value="987"))
    return publisher, browser, ctx, client


def test_count_batch_uses_replyer_and_only_successful_comments(tmp_path):
    async def check():
        publisher, browser, ctx, client = await setup(tmp_path)
        await browser.digest.collect("100", "10", "作者甲的原帖和评论者乙", "actual-stream")
        await browser.digest.collect("100", "10", "重复材料", "actual-stream")
        await publisher.due(client, "100")
        ctx.call_capability.assert_not_called()
        await browser.digest.collect("100", "11", "作者丙的原帖", "another-stream")
        for state, body in (("sent", "我确实发过的评论"), ("error", "没有发成功的内容")):
            await browser.store.record_activity("100", "out", state, "10", "group", "标题", "麦麦", body, state)
        await publisher.due(client, "100")
        call = ctx.call_capability.call_args
        assert call.args == ("maisaka.reply.generate",)
        assert call.kwargs["stream_id"] == "another-stream"
        assert "我确实发过的评论" in call.kwargs["reference"]
        assert "没有发成功的内容" not in call.kwargs["reference"]
        assert "作者甲" in call.kwargs["reference"]
        client.publish_post.assert_awaited_once_with(
            "逛社区的一点感想", ["最近看到了不同的想法。", "我自己的想法是再多听听。"], "1",
        )
        snapshot = await browser.digest.snapshot("100")
        assert snapshot["pending_posts"] == 0
        assert snapshot["latest"]["state"] == "sent" and snapshot["latest"]["post"] == "987"
        await browser.digest.initialize()
        await publisher.due(client, "100")
        client.publish_post.assert_awaited_once()
        assert (await browser.digest.snapshot("other"))["latest"] is None
    asyncio.run(check())


def test_period_only_fires_when_there_are_new_reads_and_period_elapsed(tmp_path):
    async def check():
        publisher, browser, _, client = await setup(
            tmp_path, DigestSettings(enabled=True, trigger="interval", interval_hours=1, topic_id="1"),
        )
        await publisher.due(client, "100")
        await browser.digest.collect("100", "10", "原帖", "stream")
        await publisher.due(client, "100")
        client.publish_post.assert_not_called()
        await browser.store.execute("UPDATE digest_reads SET created_at=?", (time.time() - 3601,))
        await publisher.due(client, "100")
        client.publish_post.assert_awaited_once()
        await publisher.due(client, "100")
        client.publish_post.assert_awaited_once()
    asyncio.run(check())


@pytest.mark.parametrize("uncertain", [False, True])
def test_publication_failure_retains_draft_and_never_republishes_automatically(tmp_path, uncertain):
    async def check():
        publisher, browser, ctx, client = await setup(tmp_path)
        for post in ("10", "11"):
            await browser.digest.collect("100", post, "原帖", "stream")
        client.publish_post.side_effect = (
            SendUncertainError("响应超时", category="send_unknown") if uncertain else
            CommentRejectedError("缺少参数", category="upstream_rejected")
        )
        await publisher.due(client, "100")
        latest = (await browser.digest.snapshot("100"))["latest"]
        assert latest["state"] == ("unknown" if uncertain else "failed")
        assert latest["draft"]["title"] == "逛社区的一点感想"
        await publisher.due(client, "100")
        client.publish_post.assert_awaited_once()
        if uncertain:
            with pytest.raises(ValueError, match="不确定"):
                await publisher.action("100", "retry")
            await publisher.action("100", "discard")
            assert (await browser.digest.snapshot("100"))["latest"]["state"] == "discarded"
        else:
            client.publish_post.side_effect = None
            await publisher.action("100", "retry")
            await publisher.due(client, "100")
            assert client.publish_post.await_count == 2
            ctx.call_capability.assert_awaited_once()
            assert (await browser.digest.snapshot("100"))["latest"]["state"] == "sent"
    asyncio.run(check())


def test_invalid_generation_and_disable_during_generation_do_not_publish(tmp_path):
    async def check():
        publisher, browser, ctx, client = await setup(tmp_path)
        for post in ("10", "11"):
            await browser.digest.collect("100", post, "原帖", "stream")
        ctx.call_capability.return_value = {"success": True, "text": "这不是 JSON"}
        await publisher.due(client, "100")
        assert (await browser.digest.snapshot("100"))["latest"]["state"] == "error"
        client.publish_post.assert_not_called()
        async def disable(*args, **kwargs):
            await browser.db.save("100", BrowseSettings().model_dump())
            return {"success": True, "text": '{"title":"标题","paragraphs":["一","二"]}'}
        ctx.call_capability.side_effect = disable
        await publisher.action("100", "retry")
        await publisher.due(client, "100")
        client.publish_post.assert_not_called()
        latest = (await browser.digest.snapshot("100"))["latest"]
        assert latest["state"] == "error" and latest["draft"]["paragraphs"] == ["一", "二"]
    asyncio.run(check())


def test_restart_keeps_uncertain_publication_and_recovers_interrupted_generation(tmp_path):
    async def check():
        _, browser, _, _ = await setup(tmp_path)
        for account, state in (("a", "sending"), ("b", "generating")):
            await browser.store.execute(
                "INSERT INTO digest_runs(account,id,created_at,state,topic) VALUES (?, 'batch', ?, ?, '1')",
                (account, time.time(), state),
            )
        await browser.digest.initialize()
        assert (await browser.digest.snapshot("a"))["latest"]["state"] == "unknown"
        assert (await browser.digest.snapshot("b"))["latest"]["state"] == "error"
    asyncio.run(check())


@pytest.mark.parametrize("topic,tag", [("58144", "28"), ("1", "27")])
@pytest.mark.parametrize("top_level", [False, True])
def test_publish_form_keeps_separate_paragraphs_and_signs_actual_path(topic, tag, top_level):
    async def check():
        def handler(request):
            assert request.url.path == "/bbs/app/api/link/post"
            query = dict(request.url.params)
            assert query == comment_query(int(query["_time"]), query["nonce"], path=request.url.path)
            form = parse_qs(request.content.decode())
            assert form["title"] == ["标题 & +"]
            assert form["topic_ids"] == [topic] and form["link_tag"] == [tag]
            assert form["post_type"] == ["1"]
            assert json.loads(form["text"][0]) == [{"type": "text", "text": "第一段"}, {"type": "text", "text": "第二段"}]
            payload = {"status": "ok", "link_id": 123} if top_level else {
                "status": "ok", "result": {"link_id": "123"},
            }
            return httpx.Response(200, json=payload)
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
            client = CommunityClient("default", client=http, credentials=Credentials("default", "100", "麦麦", {"pkey":"test"}), min_request_interval_seconds=0)
            assert await client.publish_post("标题 & +", ["第一段", "第二段"], topic) == "123"
    asyncio.run(check())


def test_invalid_or_empty_drafts_are_rejected():
    for payload in ({"title":"", "paragraphs":["一", "二"]}, {"title":"标题", "paragraphs":["一"]}):
        with pytest.raises(ValueError):
            DigestDraft.model_validate(payload)


@pytest.mark.parametrize("count", [10, 100])
def test_material_budget_includes_sent_comments_not_just_post_body(count):
    excerpts = material_excerpts([{
        "post": str(index), "body": "原帖作者与正文" * 1000,
        "my_comments": ["旧评论" * 1000, "已发送评论" * 1000],
    } for index in range(count)])
    assert len(excerpts) == count
    assert sum(len(item["body_excerpt"]) + len(item["my_comments_excerpt"]) for item in excerpts) <= 2400
    assert all(item["body_excerpt"] and item["my_comments_excerpt"] for item in excerpts)
