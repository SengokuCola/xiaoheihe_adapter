from pathlib import Path
from urllib.parse import parse_qs

import asyncio
import httpx
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from xiaoheihe_adapter.comment_signing import comment_query
from xiaoheihe_adapter.community_client import CommentRejectedError, CommunityClient
from xiaoheihe_adapter.vendor.api_client import SendUncertainError
from xiaoheihe_adapter.vendor.models import Credentials, RoutingTarget


def test_comment_signature_matches_web_client_fixture():
    # 固定时间和随机数的网页 JavaScript 输出，避免只测试 Python 自身构造规则。
    query = comment_query(1780000000, "07A55BB6F976EA0AFF33C4BDE4F6FAFD")
    assert query["hkey"] == "TU3XT07"
    assert query["_time"] == "1780000000"
    assert query["_rnd"] == "15:6b0a07059a5332516ce48ab7ae6b857cb84b051969563a93ce43b7689636c915"


@pytest.mark.parametrize("root,parent", [("", ""), ("7", "7"), ("7", "8")])
@pytest.mark.parametrize("with_image", [False, True])
def test_actual_comment_request_uses_web_contract(root, parent, with_image):
    async def check():
        calls = []

        def handler(request):
            calls.append(request)
            assert request.method == "POST"
            assert request.url.host == "api.xiaoheihe.cn"
            assert request.url.path == "/bbs/app/comment/create"
            query = dict(request.url.params)
            assert query == comment_query(int(query["_time"]), query["nonce"])
            assert query["app"] == "heybox" and query["web_version"] == "3.0"
            assert "heybox_id" not in query and "device_id" not in query
            assert "pkey=fixture-cookie" in request.headers["cookie"]
            form = parse_qs(request.content.decode(), keep_blank_values=True)
            assert form["link_id"] == ["123"]
            assert form["root_id"] == [root or "-1"]
            assert form["reply_id"] == [parent or "-1"]
            assert form["text"] == ["测试 & + %\n第二行"]
            if with_image:
                assert form["imgs"] == ["https://imgheybox.max-c.com/test.gif"]
            else:
                assert "imgs" not in form
            return httpx.Response(200, json={"status": "ok", "result": {"commentid": "9"}})

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
            client = CommunityClient(
                "default",
                credentials=Credentials("default", "100", "麦麦", {"pkey": "fixture-cookie"}),
                client=http,
                min_request_interval_seconds=0,
            )
            route = RoutingTarget("default", "123", root, parent)
            if with_image:
                result = await client.send_media_comment(
                    route, "测试 & + %\n第二行", ["https://imgheybox.max-c.com/test.gif"]
                )
            else:
                result = await client.send_comment(route, "测试 & + %\n第二行")
            assert result.external_comment_id == "9"
        assert len(calls) == 1

    asyncio.run(check())


@pytest.mark.parametrize("timeout", [False, True])
def test_explicit_rejection_is_distinct_from_timeout_and_not_retried(timeout):
    async def check():
        calls = []

        def handler(request):
            calls.append(request)
            if timeout:
                raise httpx.ReadTimeout("timeout", request=request)
            return httpx.Response(200, json={"status": "failed", "msg": "缺失参数"})

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
            client = CommunityClient(
                "default",
                credentials=Credentials("default", "100", "麦麦", {"pkey": "fixture"}),
                client=http,
                min_request_interval_seconds=0,
            )
            with pytest.raises(SendUncertainError if timeout else CommentRejectedError):
                await client.send_comment(RoutingTarget("default", "123"), "测试")
        assert len(calls) == 1

    asyncio.run(check())
