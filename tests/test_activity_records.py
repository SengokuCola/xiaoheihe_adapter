from pathlib import Path

import asyncio
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from xiaoheihe_adapter.activity_records import ActivityRecords
from xiaoheihe_adapter.storage import Store
from xiaoheihe_adapter.vendor.models import RoutingTarget


async def setup(tmp_path):
    store = Store(tmp_path)
    await store.initialize()
    return store, ActivityRecords(store)


def test_post_groups_include_all_roots_and_correct_title_without_cross_account_data(tmp_path):
    async def check():
        store, records = await setup(tmp_path)
        for direction, message, group, title, state in (
            ("browse", "browse:123", "browse", "真实原帖标题", "participated"),
            ("in", "xhh:123:8", "root-8", "楼层会话名", "accepted"),
            ("out", "sent", "root-9", "", "sent"),
            ("out", "blocked", "root-8", "", "blocked"),
        ):
            await store.record_activity("100", direction, message, "123", group, title, "发言者", "正文", state)
        await store.record_activity("200", "in", "other", "123", "other", "其他账号的标题", "别人", "秘密", "error")
        result = await records.posts("100")
        assert result["overview"] == {"posts": 1, "received": 1, "replied": 1, "attention": 1}
        post = result["items"][0]
        assert post["title"] == "真实原帖标题"
        assert post["records"] == 4
        assert post["url"] == "https://xiaoheihe.cn/app/bbs/link/123"
        thread = await records.thread("100", "123")
        assert [item["message"] for item in thread["items"]] == ["browse:123", "xhh:123:8", "sent", "blocked"]
        assert thread["items"][-1]["state_label"] == "发送被限制"
        assert all("account" not in item for item in thread["items"])
        assert (await records.posts("999"))["overview"]["posts"] == 0
        with pytest.raises(ValueError, match="当前账号"):
            await records.thread("999", "123")
    asyncio.run(check())


def test_thread_pagination_and_filters_reach_all_records_with_stable_time_order(tmp_path):
    async def check():
        store, records = await setup(tmp_path)
        for index in range(65):
            await store.record_activity("100", "out", str(index), "123", "g", "帖子", "麦麦", str(index), "sent")
        last = await records.thread("100", "123", 999999)
        assert last["pagination"] == {"page": 3, "pages": 3, "total": 65}
        assert [item["body"] for item in last["items"]] == [str(index) for index in range(60, 65)]
        all_ids = []
        for page in range(1, 4):
            all_ids.extend(item["message"] for item in (await records.thread("100", "123", page))["items"])
        assert all_ids == [str(index) for index in range(65)]
        empty = await records.thread("100", "123", kind="in")
        assert empty["items"] == [] and empty["pagination"]["total"] == 0
        assert (await records.thread("100", "123", kind="out"))["pagination"]["total"] == 65
    asyncio.run(check())


def test_search_is_literal_and_status_filters_apply_to_whole_post(tmp_path):
    async def check():
        store, records = await setup(tmp_path)
        await store.record_activity("100", "browse", "a", "1", "g", "进度100%_完成", "作者甲", "正文", "considering")
        await store.record_activity("100", "out", "b", "2", "g", "普通标题", "麦麦", "回帖", "sent")
        await store.record_activity("100", "in", "c", "3", "g", "错误帖子", "评论者", "评论", "blocked")
        assert [item["post_id"] for item in (await records.posts("100", search="%_"))["items"]] == ["1"]
        assert (await records.posts("100", search="作者甲"))["pagination"]["total"] == 1
        assert (await records.posts("100", search="' OR 1=1 --"))["items"] == []
        assert [item["post_id"] for item in (await records.posts("100", status="replied"))["items"]] == ["2"]
        assert [item["post_id"] for item in (await records.posts("100", status="attention"))["items"]] == ["3"]
    asyncio.run(check())


def test_full_content_and_reply_target_can_be_read_only_in_the_correct_post(tmp_path):
    async def check():
        store, records = await setup(tmp_path)
        body = "原帖作者与评论素材" * 1000
        await store.record_activity("100", "browse", "browse:1", "1", "g", "标题", "作者", body, "considering")
        await store.save_route("100", "browse:1", RoutingTarget("default", "1"), "g")
        await store.save_route("100", "comment:8", RoutingTarget("default", "1", "7", "8"), "g")
        for message, anchor in (("top", "browse:1"), ("child", "comment:8")):
            await store.record_activity("100", "out", message, "1", "g", "", "麦麦", "回复", "sent", anchor)
        thread = await records.thread("100", "1")
        assert thread["items"][0]["has_more"] and len(thread["items"][0]["body"]) == 400
        assert [item["reply_label"] for item in thread["items"][1:]] == ["回复主帖", "回复评论 #8"]
        full = await records.event("100", "1", "browse", "browse:1")
        assert full["item"]["body"] == body and not full["item"]["has_more"]
        with pytest.raises(ValueError, match="不存在"):
            await records.event("100", "2", "browse", "browse:1")
        with pytest.raises(ValueError, match="不存在"):
            await records.event("200", "1", "browse", "browse:1")
    asyncio.run(check())


def test_post_list_pagination_is_by_post_and_uses_latest_activity(tmp_path):
    async def check():
        store, records = await setup(tmp_path)
        for index in range(21):
            await store.record_activity("100", "browse", str(index), str(index), "g", f"帖子{index}", "作者", "正文", "considering")
        first = await records.posts("100")
        assert first["pagination"] == {"page": 1, "pages": 2, "total": 21}
        assert len(first["items"]) == 20 and first["items"][0]["post_id"] == "20"
        second = await records.posts("100", 999999)
        assert second["pagination"]["page"] == 2 and second["items"][0]["post_id"] == "0"
    asyncio.run(check())


@pytest.mark.parametrize("page", [0, -1, True])
def test_invalid_page_is_rejected(tmp_path, page):
    async def check():
        _, records = await setup(tmp_path)
        with pytest.raises(ValueError):
            await records.posts("100", page)
    asyncio.run(check())
