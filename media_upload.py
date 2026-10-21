"""图片和表情包的 COS 上传；临时凭证仅在内存中使用。

协议参考：https://github.com/AaronZ345/xiaoheihe-cli/blob/main/xhh_operator/media.py
"""

from dataclasses import dataclass
from io import BytesIO
from typing import Any, Dict
from urllib.parse import quote, urlsplit

from PIL import Image

import asyncio
import base64
import hashlib
import hmac
import httpx
import json
import re
import time

MAX_BYTES = 5 * 1024 * 1024
UPLOAD_PATH = "/bbs/app/api/qcloud/cos/upload/"


@dataclass(frozen=True)
class UploadImage:
    data: bytes
    mimetype: str
    extension: str
    width: int
    height: int

    def file_info(self) -> Dict[str, Any]:
        return {
            "name": hashlib.sha256(self.data).hexdigest() + "." + self.extension,
            "mimetype": self.mimetype,
            "fsize": len(self.data),
            "width": self.width,
            "height": self.height,
        }


def parse_image(segment: Dict[str, Any]) -> UploadImage:
    encoded = segment["binary_data_base64"]
    if len(encoded) > (MAX_BYTES + 2) // 3 * 4:
        raise ValueError("单张图片或表情包不得超过 5 MB")
    try:
        binary = base64.b64decode(encoded, validate=True)
    except ValueError as exc:
        raise ValueError("图片或表情包不是有效的 Base64") from exc
    if not binary or len(binary) > MAX_BYTES:
        raise ValueError("图片为空或超过 5 MB")
    try:
        with Image.open(BytesIO(binary)) as image:
            formats = {
                "JPEG": ("image/jpeg", "jpg"),
                "PNG": ("image/png", "png"),
                "GIF": ("image/gif", "gif"),
                "WEBP": ("image/webp", "webp"),
            }
            if image.format not in formats or image.width * image.height > 20_000_000:
                raise ValueError("图片格式不支持或像素超过 2000 万")
            mimetype, extension = formats[image.format]
            width, height = image.size
            image.verify()
    except (OSError, Image.DecompressionBombError) as exc:
        raise ValueError("图片或表情包无法解码") from exc
    # 不重新编码，保留 GIF/WebP 动画和透明度。
    return UploadImage(binary, mimetype, extension, width, height)


def public_media_url(value: Any) -> str:
    if not isinstance(value, str):
        raise ValueError("图床回执缺少图片地址")
    parsed = urlsplit(value)
    host = parsed.hostname or ""
    if (
        parsed.scheme != "https"
        or parsed.username
        or parsed.password
        or parsed.port not in {None, 443}
        or not any(
            host == domain or host.endswith("." + domain) for domain in ("max-c.com", "xiaoheihe.cn", "myqcloud.com")
        )
        or any(word in parsed.query.lower() for word in ("secret", "token", "signature", "q-sign", "q-ak"))
    ):
        raise ValueError("图床回执不是可公开发送的图片地址")
    return value


def cos_headers(credentials: Dict[str, str], host: str, path: str, mime: str, start: int, end: int) -> Dict[str, str]:
    period = f"{start};{end}"
    signed = {"content-type": mime, "host": host}
    canonical = "&".join(quote(key, safe="-_.~") + "=" + quote(signed[key], safe="-_.~") for key in sorted(signed))
    request_hash = hashlib.sha1(f"put\n{path}\n\n{canonical}\n".encode()).hexdigest()
    key = hmac.new(credentials["tmpSecretKey"].encode(), period.encode(), hashlib.sha1).hexdigest()
    signature = hmac.new(key.encode(), f"sha1\n{period}\n{request_hash}\n".encode(), hashlib.sha1).hexdigest()
    return {
        **signed,
        "x-cos-security-token": credentials["sessionToken"],
        "Authorization": f"q-sign-algorithm=sha1&q-ak={credentials['tmpSecretId']}&q-sign-time={period}"
        f"&q-key-time={period}&q-header-list=content-type;host&q-url-param-list=&q-signature={signature}",
    }


async def upload_image(client: Any, image: UploadImage) -> str:
    info = await client._upload_request(
        "info/v2", {"file_infos": json.dumps([image.file_info()]), "scope": "bbs", "need_cache": "0"}
    )
    keys, bucket, region = info.get("keys"), info.get("bucket"), info.get("region")
    if (
        not isinstance(keys, list)
        or len(keys) != 1
        or not isinstance(keys[0], str)
        or not keys[0]
        or not isinstance(bucket, str)
        or not re.fullmatch(r"[a-z0-9-]+-\d+", bucket)
        or not isinstance(region, str)
        or not re.fullmatch(r"[a-z]+-[a-z0-9-]+", region)
    ):
        raise ValueError("图床上传信息字段不符合协议")
    key = keys[0]
    if any(char in key for char in "?#\\") or any(part in {".", ".."} for part in key.split("/")):
        raise ValueError("图床上传对象路径无效")
    token = await client._upload_request(
        "token/v2",
        {
            "bucket": bucket,
            "keys": json.dumps(keys),
            "mimetypes": json.dumps([image.mimetype]),
            "is_multipart_upload": "0",
        },
    )
    credentials = token.get("credentials")
    if not isinstance(credentials, dict) or not all(
        isinstance(credentials.get(name), str) and credentials[name]
        for name in ("tmpSecretId", "tmpSecretKey", "sessionToken")
    ):
        raise ValueError("图床临时凭证字段不符合协议")
    start, end = int(token["startTime"]), int(token["expiredTime"])
    if not start < end or end <= time.time():
        raise ValueError("图床临时凭证已失效")
    host = f"{bucket}.cos.{region}.myqcloud.com"
    path = "/" + key.lstrip("/")
    headers = cos_headers(credentials, host, path, image.mimetype, start, end)
    # COS 请求使用独立客户端，不携带小黑盒账号 Cookie；不重试结果不确定的上传。
    storage = await asyncio.to_thread(httpx.AsyncClient, timeout=30, trust_env=False, follow_redirects=False)
    async with storage:
        try:
            response = await storage.put(
                "https://" + host + quote(path, safe="/-_.~"), headers=headers, content=image.data
            )
        except httpx.HTTPError as exc:
            raise RuntimeError("图床上传连接失败，未提交评论") from exc
    if not response.is_success:
        raise RuntimeError(f"图床上传失败：HTTP {response.status_code}")
    result = await client._upload_request("callback/v2", {"keys": json.dumps(keys)}, {"is_finished": "true"})
    urls = result.get("preview_urls")
    if not isinstance(urls, list) or len(urls) != 1:
        raise ValueError("图床上传完成回执缺少图片地址")
    return public_media_url(urls[0])
