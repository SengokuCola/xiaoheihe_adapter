from io import BytesIO
from pathlib import Path
from unittest.mock import AsyncMock

from PIL import Image

import asyncio
import base64
import httpx
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from xiaoheihe_adapter import post_context
from xiaoheihe_adapter.post_context import build_context, context_entries, image_segment, load_image
from xiaoheihe_adapter.vendor.models import ThreadContext

FEED = {"author": {"uid": "200", "nickname": "原帖作者"}}
IMAGE = "https://imgheybox.max-c.com/post/test.png"


def png() -> bytes:
    output = BytesIO()
    Image.new("RGB", (20, 10), "red").save(output, format="PNG")
    return output.getvalue()


def test_author_comment_names_and_nested_reply_relations():
    context = ThreadContext(
        "123",
        "标题",
        "<p>正文第一段</p><p>第二段</p>",
        "",
        "",
        [
            {
                "id": "1",
                "user": {"username": "甲", "userid": "300"},
                "text": "评论正文",
                "children": [
                    {"comment_id": "2", "sender": {"nickname": "乙", "uid": "400"}, "content": "回复正文"},
                ],
            },
        ],
    )
    entries = context_entries(context, FEED, 6000)
    assert len(entries) == 3
    assert "作者：原帖作者（UID 200）" in entries[0].text
    assert "正文第一段\n第二段" in entries[0].text
    assert "发言者：甲（UID 300）" in entries[1].text
    assert "回复评论 #1" in entries[2].text
    assert "发言者：乙（UID 400）" in entries[2].text
    assert entries[2].message_id == "browse:123:comment:2"
    assert entries[0].target.parent_comment_id == ""
    assert entries[1].target.root_comment_id == "1"
    assert entries[1].target.parent_comment_id == "1"
    assert entries[2].target.root_comment_id == "1"
    assert entries[2].target.parent_comment_id == "2"


def test_post_and_comment_images_keep_their_owners(monkeypatch):
    async def check():
        loader = AsyncMock(return_value=image_segment(png()))
        monkeypatch.setattr(post_context, "load_image", loader)
        context = ThreadContext(
            "123",
            "标题",
            "正文",
            "200",
            "原帖作者",
            [
                {
                    "id": "1",
                    "user": {"uid": "300", "nickname": "评论作者"},
                    "text": "评论图片",
                    "images": [{"url": IMAGE + "?comment=1"}],
                },
            ],
            image_urls=[IMAGE],
        )
        entries, failures = await build_context(context, FEED, 6000)
        assert not failures
        assert loader.await_count == 2
        assert "原帖作者" in entries[0].text
        assert "评论作者" in entries[1].text
        for entry in entries:
            segments = [segment for segment in entry.segments if segment["type"] == "image"]
            assert len(segments) == 1
            with Image.open(BytesIO(base64.b64decode(segments[0]["binary_data_base64"]))) as image:
                assert image.format == "JPEG"
        assert "以上发言者所附图片" in entries[1].text

    asyncio.run(check())


def test_image_budget_and_failed_downloads_are_explicit(monkeypatch):
    async def check():
        loader = AsyncMock(return_value=image_segment(png()))
        monkeypatch.setattr(post_context, "load_image", loader)
        context = ThreadContext(
            "123", "标题", "正文", "200", "原帖作者", [], image_urls=[IMAGE + str(index) for index in range(7)]
        )
        entries, _ = await build_context(context, FEED, 6000)
        assert loader.await_count == 6
        assert "本次最多 6 张" in entries[0].text
        loader.side_effect = ValueError("图片下载失败")
        context.image_urls = [IMAGE]
        entries, failures = await build_context(context, FEED, 6000)
        assert failures
        assert "图片内容不可见，请勿猜测" in entries[0].text
        assert not any(segment["type"] == "image" for segment in entries[0].segments)
        context.body = ""
        with pytest.raises(ValueError, match="纯图片帖"):
            await build_context(context, FEED, 6000)

    asyncio.run(check())


def test_media_download_rejects_non_cdn_and_private_redirects(monkeypatch):
    async def check():
        monkeypatch.setattr(post_context, "resolve_public_host", AsyncMock(return_value={"1.1.1.1"}))
        requests = []

        def handler(request):
            requests.append(request)
            if request.url.path == "/redirect":
                return httpx.Response(302, headers={"location": "https://127.0.0.1/private"})
            return httpx.Response(200, content=png(), headers={"content-type": "image/png"})

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            result = await load_image(client, IMAGE)
            assert result["type"] == "image"
            with pytest.raises(ValueError, match="媒体域名"):
                await load_image(client, "https://example.com/image.png")
            assert len(requests) == 1
            with pytest.raises(ValueError, match="内网"):
                await load_image(client, "https://imgheybox.max-c.com/redirect")
            assert len(requests) == 2

    asyncio.run(check())


def test_comment_budget_does_not_hide_that_the_list_is_partial():
    context = ThreadContext(
        "123",
        "标题",
        "正文",
        "200",
        "原帖作者",
        [
            {"id": str(index), "user": {"uid": str(index), "nickname": "评论者"}, "text": "长评论" * 400}
            for index in range(30)
        ],
    )
    entries = context_entries(context, FEED, 1200)
    assert "非完整评论区" in entries[0].text
    assert "评论正文截取" in entries[-1].text
    assert sum(len(entry.text) for entry in entries) <= 1200
