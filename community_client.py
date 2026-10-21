"""在保留上游协议代码的基础上适配社区详情、视频标记与评论楼层。"""

from dataclasses import asdict, dataclass
from typing import Any, Dict, List, Mapping, Optional

import httpx
import json

from .media_upload import UPLOAD_PATH, public_media_url
from .comment_signing import COMMENT_PATH, comment_query
from .vendor.api_client import (
    WEB_CLIENT_PARAMS, ApiResponse, CredentialInvalidError, RateLimitedError, SendUncertainError,
    XiaoheiheApiClient, XiaoheiheApiError, _is_relogin_response, _merge_post_and_thread_context, _upstream_error,
)
from .vendor.endpoints import EndpointName
from .vendor.models import RoutingTarget, SendResult, ThreadContext
from .vendor.parsers import ResponseShapeError, parse_send_result, parse_thread_context


def contains_video(post: Mapping[str, Any]) -> bool:
    if str(post.get("has_video", "")).lower() in {"1", "true"} or post.get("video_url"):
        return True
    if post.get("video") or post.get("videos"):
        return True
    content = post.get("text", post.get("content", []))
    if isinstance(content, str):
        try:
            content = json.loads(content)
        except json.JSONDecodeError:
            return "<video" in content.lower()
    if isinstance(content, list):
        return any(
            isinstance(block, dict) and (block.get("type") == "video" or contains_video(block))
            for block in content
        )
    return False


@dataclass(slots=True)
class CommunityContext(ThreadContext):
    has_video: bool = False
    comment_count: Optional[int] = None


def parse_community_context(payload: Mapping[str, Any], post_id: str) -> CommunityContext:
    context = parse_thread_context(payload, post_id)
    body = payload.get("result", payload.get("data", payload))
    post = body.get("post", body.get("link", body))
    if not any(key in body for key in ("comments", "comment_list", "children")):
        raise ResponseShapeError("帖子详情缺少评论列表字段")
    raw_comments = body.get("comments", body.get("comment_list", body.get("children")))
    if not isinstance(raw_comments, list) or not all(isinstance(group, dict) for group in raw_comments):
        raise ResponseShapeError("评论列表包含非对象节点")
    comments = []
    for group in context.comments:
        nodes = group.get("comment", group)
        if isinstance(nodes, list):
            if not all(isinstance(node, dict) for node in nodes):
                raise ResponseShapeError("评论分组包含非对象节点")
            # 一个分组的第一条是根楼层，后续条目是该楼的回复。
            if nodes:
                root = dict(nodes[0])
                root["children"] = list(root.get("children", [])) + nodes[1:]
                comments.append(root)
        elif isinstance(nodes, dict):
            comments.append(dict(nodes))
        else:
            raise ResponseShapeError("评论分组不是数组或对象")
    count = post.get("comment_num")
    values = asdict(context)
    values["comments"] = comments
    return CommunityContext(
        **values, has_video=contains_video(post), comment_count=int(count) if count is not None else None
    )


class CommentRejectedError(XiaoheiheApiError):
    """服务端已明确拒绝评论，区别于连接中断后的未知结果。"""


class CommunityClient(XiaoheiheApiClient):
    async def _request(
        self, name: EndpointName, *, params: Optional[Dict[str, Any]] = None,
        json_body: Optional[Dict[str, Any]] = None, form_body: Optional[Dict[str, Any]] = None,
    ) -> ApiResponse:
        if name is not EndpointName.CREATE_COMMENT:
            return await super()._request(name, params=params, json_body=json_body, form_body=form_body)
        if form_body is None or params is not None or json_body is not None:
            raise ValueError("评论创建只接受标准表单")
        return await self._write_form(COMMENT_PATH, form_body, "评论")

    async def publish_post(self, title: str, paragraphs: List[str], topic_id: str) -> str:
        if not title.strip() or len(title) > 80:
            raise ValueError("帖子标题必须为 1–80 字")
        if not topic_id.isascii() or not topic_id.isdigit():
            raise ValueError("发帖必须指定有效分区 ID")
        if not 2 <= len(paragraphs) <= 12 or any(not text.strip() for text in paragraphs):
            raise ValueError("帖子必须包含 2–12 段非空正文")
        if sum(map(len, paragraphs)) > 10000:
            raise ValueError("帖子正文超过 10000 字")
        response = await self._write_form("/bbs/app/api/link/post", {
            "title": title, "desc": paragraphs[0][:100], "post_type": "1",
            "text": json.dumps([{"type": "text", "text": text} for text in paragraphs], ensure_ascii=False),
            "topic_ids": topic_id, "link_tag": "28" if topic_id == "58144" else "27",
        }, "发帖")
        # 发帖接口直接在顶层返回 link_id；部分客户端契约使用 result 包装。
        if "link_id" in response.payload:
            post_id = str(response.payload["link_id"])
        elif isinstance(response.payload.get("result"), dict):
            result = response.payload["result"]
            post_id = str(result.get("link_id", result.get("linkid", "")))
        else:
            raise SendUncertainError("发帖成功响应未提供帖子 ID", category="send_unknown")
        if not post_id.isascii() or not post_id.isdigit():
            raise SendUncertainError("发帖响应未提供有效帖子 ID", category="send_unknown")
        return post_id

    async def _write_form(self, path: str, form_body: Dict[str, Any], operation: str) -> ApiResponse:
        await self.start()
        if self.credentials is None:
            raise CredentialInvalidError("账号未登录", category="credential_invalid")
        if self._client is None:
            raise RuntimeError("小黑盒客户端未初始化")
        await self._limiter.wait()
        try:
            response = await self._client.post(
                "https://api.xiaoheihe.cn" + path,
                params=comment_query(path=path), data=form_body,
                headers={"Origin": "https://www.xiaoheihe.cn", "Referer": "https://www.xiaoheihe.cn/",
                         "Content-Type": "application/x-www-form-urlencoded;charset=utf-8"},
            )
        except httpx.HTTPError as exc:
            raise SendUncertainError(f"{operation}请求连接中断或超时", category="send_unknown") from exc
        if response.status_code == 401:
            raise CredentialInvalidError(f"{operation}接口要求重新登录", category="credential_invalid")
        if response.status_code == 429:
            raise RateLimitedError(f"{operation}接口请求频率受限", category="rate_limited")
        if response.status_code >= 500:
            raise SendUncertainError(f"{operation}接口返回 HTTP {response.status_code}", category="send_unknown")
        if not response.is_success:
            raise CommentRejectedError(f"{operation}接口返回 HTTP {response.status_code}", category="http")
        payload = response.json()
        if not isinstance(payload, dict):
            raise SendUncertainError(f"{operation}接口响应不是对象", category="send_unknown")
        if _is_relogin_response(payload):
            raise CredentialInvalidError(f"{operation}接口要求重新登录", category="credential_invalid")
        if error := _upstream_error(payload):
            raise CommentRejectedError(error, category="upstream_rejected")
        return ApiResponse(payload=payload, cookies={}, headers=dict(response.headers))

    async def _upload_request(
        self, action: str, form: Dict[str, str], params: Optional[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        if action not in {"info/v2", "token/v2", "callback/v2"}:
            raise ValueError("未知图床上传操作")
        await self.start()
        if self.credentials is None:
            raise CredentialInvalidError("图片上传需要登录", category="credential_invalid")
        if self._client is None:
            raise RuntimeError("小黑盒客户端未初始化")
        path = UPLOAD_PATH + action
        query = {**WEB_CLIENT_PARAMS, "web_version": "3.0", **(params or {})}
        signed = self._signer.sign(
            "POST", path, params=query, json_body=form,
            signing_key=self.credentials.signing_key, device_id=self._device_id,
        )
        await self._limiter.wait()
        try:
            response = await self._client.post(
                "https://api.xiaoheihe.cn" + path, params=signed.params, data=form,
                headers={**signed.headers, "Referer": "https://www.xiaoheihe.cn/", "Origin": "https://www.xiaoheihe.cn"},
            )
        except httpx.HTTPError as exc:
            raise XiaoheiheApiError("图床接口连接失败，未提交评论", category="upload") from exc
        if response.status_code == 401:
            raise CredentialInvalidError("图床接口拒绝登录凭证", category="credential_invalid")
        if response.status_code == 429:
            raise RateLimitedError("图床接口请求频率受限", category="rate_limited")
        if not response.is_success:
            raise XiaoheiheApiError(f"图床接口返回 HTTP {response.status_code}", category="upload")
        payload = response.json()
        if not isinstance(payload, dict):
            raise ResponseShapeError("图床接口响应不是对象")
        if _is_relogin_response(payload):
            raise CredentialInvalidError("图床接口要求重新登录", category="credential_invalid")
        if payload.get("status") != "ok":
            raise XiaoheiheApiError("图床接口未返回成功状态", category="upload")
        result = payload.get("result")
        if not isinstance(result, dict):
            raise ResponseShapeError("图床接口缺少 result 对象")
        return result

    async def send_media_comment(self, route: RoutingTarget, content: str, urls: List[str]) -> SendResult:
        body = {
            "is_cy": "0", "link_id": route.post_id, "reply_id": route.parent_comment_id or "-1",
            "root_id": route.root_comment_id or "-1", "text": content,
            "imgs": ";".join(public_media_url(url) for url in urls),
        }
        response = await self._request(EndpointName.CREATE_COMMENT, form_body=body)
        return parse_send_result(response.payload)

    async def _load_context(self, post_id: str, root: str = "") -> CommunityContext:
        params: Dict[str, Any] = {"link_id": post_id, "h_src": "", "page": 1, "limit": 20}
        if root:
            params["root_comment_id"] = root
        response = await self._request(EndpointName.POST_TREE, params=params)
        return parse_community_context(response.payload, post_id)

    async def fetch_thread_context(
        self, post_id: str, *, root_comment_id: str = "", post_context: Optional[ThreadContext] = None
    ) -> ThreadContext:
        if post_context is None:
            post_context = await self._load_context(post_id)
        if not root_comment_id:
            return post_context
        thread = await self._load_context(post_id, root_comment_id)
        return _merge_post_and_thread_context(post_context, thread)
