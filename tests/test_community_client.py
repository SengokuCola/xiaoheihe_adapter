from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import asyncio
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from xiaoheihe_adapter.community_client import CommunityClient, contains_video, parse_community_context
from xiaoheihe_adapter.post_context import context_entries
from xiaoheihe_adapter.vendor.parsers import ResponseShapeError
from xiaoheihe_adapter.vendor.models import RoutingTarget


def payload(video=0, comments=None, count=2):
    return {
        "result": {
            "link": {"linkid": "123", "title": "标题", "text": "正文", "has_video": video, "comment_num": count},
            "comments": comments if comments is not None else [{"comment": [
                {"commentid": 11, "text": "评论", "user": {"userid": "1", "username": "甲"}},
                {"commentid": 12, "text": "回复", "user": {"userid": "2", "username": "乙"}},
            ]}],
        }
    }


def test_grouped_comments_keep_authors_and_reply_relationship():
    context = parse_community_context(payload(), "123")
    entries = context_entries(context, {"author": {"uid": "3", "nickname": "作者"}}, 6000)
    assert "发言者：甲（UID 1）" in entries[1].text
    assert "发言者：乙（UID 2）" in entries[2].text
    assert "回复评论 #11" in entries[2].text


def test_request_explicitly_loads_first_page_and_root_thread():
    async def check():
        client = CommunityClient("default")
        client._request = AsyncMock(return_value=SimpleNamespace(payload=payload()))
        try:
            context = await client.fetch_thread_context("123", root_comment_id="11")
            assert context.comments[0]["commentid"] == 11
            calls = client._request.call_args_list
            assert len(calls) == 2
            assert all(call.kwargs["params"]["page"] == 1 for call in calls)
            assert all(call.kwargs["params"]["limit"] == 20 for call in calls)
            assert "root_comment_id" not in calls[0].kwargs["params"]
            assert calls[1].kwargs["params"]["root_comment_id"] == "11"
        finally:
            await client.close()
    asyncio.run(check())


def test_empty_comments_are_not_silently_treated_as_empty_community():
    feed = {"author": {"uid": "3", "nickname": "作者"}}
    with pytest.raises(ValueError, match="平台显示 2 条评论"):
        context_entries(parse_community_context(payload(comments=[]), "123"), feed, 6000)
    assert "当前评论区为空" in context_entries(
        parse_community_context(payload(comments=[], count=0), "123"), feed, 6000
    )[0].text
    raw = payload()
    del raw["result"]["comments"]
    with pytest.raises(ResponseShapeError, match="缺少评论列表"):
        parse_community_context(raw, "123")


@pytest.mark.parametrize("post", [
    {"has_video": 1}, {"has_video": "1"}, {"video_url": "https://example.com/clip.mp4"},
    {"text": '[{"type":"video","url":"clip.mp4"}]'}, {"text": '<video src="clip.mp4"></video>'},
])
def test_video_detection(post):
    assert contains_video(post)
    assert not contains_video({"has_video": "0", "text": "文字与配图", "video_url": ""})


@pytest.mark.parametrize("root,parent", [("", ""), ("7", "7"), ("7", "8")])
def test_comment_request_keeps_post_root_and_parent(root, parent):
    async def check():
        client = CommunityClient("default")
        client._request = AsyncMock(return_value=SimpleNamespace(payload={"status": "ok", "result": {"commentid": "9"}}))
        try:
            await client.send_comment(RoutingTarget("default", "123", root, parent), "评论正文")
            body = client._request.call_args.kwargs["form_body"]
            assert body["link_id"] == "123"
            assert body["root_id"] == (root or "-1")
            assert body["reply_id"] == (parent or "-1")
        finally:
            await client.close()
    asyncio.run(check())
