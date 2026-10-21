from io import BytesIO
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

from PIL import Image

import asyncio
import base64
import httpx
import json
import sys
import time

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from xiaoheihe_adapter import media_upload
from xiaoheihe_adapter.codec import encode
from xiaoheihe_adapter.community_client import CommunityClient
from xiaoheihe_adapter.media_upload import parse_image, upload_image
from xiaoheihe_adapter.vendor.models import RoutingTarget


def segment(kind="image", animated=False):
    output = BytesIO()
    first = Image.new("RGB", (10, 10), "red")
    if animated:
        first.save(
            output, format="GIF", save_all=True, append_images=[Image.new("RGB", (10, 10), "blue")], duration=100
        )
    else:
        first.save(output, format="PNG")
    return {"type": kind, "data": "图片说明", "binary_data_base64": base64.b64encode(output.getvalue()).decode("ascii")}


def test_parse_text_image_and_animated_emoji_without_flattening_animation():
    emoji = segment("emoji", True)
    image = parse_image(emoji)
    assert image.mimetype == "image/gif"
    with Image.open(BytesIO(image.data)) as decoded:
        assert decoded.n_frames == 2
    message = {
        "reply_to": "target",
        "message_info": {"group_info": {"group_id": "group"}},
        "raw_message": [
            {"type": "text", "data": "正文"},
            segment(),
            emoji,
        ],
    }
    text, anchor, group, images = encode(message, 500)
    assert (text, anchor, group, len(images)) == ("正文", "target", "group", 2)
    message["raw_message"] = [emoji]
    assert encode(message, 500)[0] == ""
    with pytest.raises(ValueError, match="Base64"):
        parse_image({"binary_data_base64": "bad!"})
    with pytest.raises(ValueError, match="5 MB"):
        parse_image({"binary_data_base64": "A" * (7 * 1024 * 1024)})


@pytest.mark.parametrize("fail_put", [False, True])
def test_upload_contract_and_no_callback_after_failed_storage_upload(monkeypatch, fail_put):
    async def check():
        image = parse_image(segment())
        now = int(time.time())
        api = SimpleNamespace(
            _upload_request=AsyncMock(
                side_effect=[
                    {"keys": ["web/bbs/image.png"], "bucket": "heybox-1234", "region": "ap-shanghai"},
                    {
                        "credentials": {
                            "tmpSecretId": "temp-id",
                            "tmpSecretKey": "temp-secret",
                            "sessionToken": "temp-token",
                        },
                        "startTime": now,
                        "expiredTime": now + 300,
                    },
                    {"preview_urls": ["https://imgheybox.max-c.com/web/bbs/image.png"]},
                ]
            )
        )
        requests = []

        def handler(request):
            requests.append(request)
            assert request.method == "PUT"
            assert request.url.host == "heybox-1234.cos.ap-shanghai.myqcloud.com"
            assert request.content == image.data
            assert request.headers["x-cos-security-token"] == "temp-token"
            assert request.headers["content-type"] == "image/png"
            assert "q-sign-algorithm=sha1" in request.headers["authorization"]
            assert "cookie" not in request.headers
            return httpx.Response(500 if fail_put else 200)

        original = httpx.AsyncClient
        monkeypatch.setattr(
            media_upload.httpx,
            "AsyncClient",
            lambda **kwargs: original(transport=httpx.MockTransport(handler), **kwargs),
        )
        if fail_put:
            with pytest.raises(RuntimeError, match="HTTP 500"):
                await upload_image(api, image)
            assert api._upload_request.await_count == 2
        else:
            assert await upload_image(api, image) == "https://imgheybox.max-c.com/web/bbs/image.png"
            calls = api._upload_request.call_args_list
            assert [call.args[0] for call in calls] == ["info/v2", "token/v2", "callback/v2"]
            assert json.loads(calls[0].args[1]["file_infos"])[0]["width"] == 10
            assert calls[-1].args[2] == {"is_finished": "true"}
        assert len(requests) == 1

    asyncio.run(check())


def test_media_comment_preserves_reply_target_and_all_images():
    async def check():
        client = CommunityClient("default")
        client._request = AsyncMock(
            return_value=SimpleNamespace(payload={"status": "ok", "result": {"commentid": "9"}})
        )
        urls = ["https://imgheybox.max-c.com/a.png", "https://imgheybox.max-c.com/b.gif"]
        try:
            result = await client.send_media_comment(RoutingTarget("default", "123", "7", "8"), "", urls)
            assert result.external_comment_id == "9"
            body = client._request.call_args.kwargs["form_body"]
            assert (body["link_id"], body["root_id"], body["reply_id"]) == ("123", "7", "8")
            assert body["imgs"] == ";".join(urls)
            assert body["text"] == ""
            with pytest.raises(ValueError, match="公开发送"):
                await client.send_media_comment(RoutingTarget("default", "123"), "", ["https://example.com/private"])
        finally:
            await client.close()

    asyncio.run(check())
