from dataclasses import replace
from pathlib import Path

import asyncio
import sys
import time

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from xiaoheihe_adapter.browse_store import BrowseStore
from xiaoheihe_adapter.storage import Store
from xiaoheihe_adapter.vendor.models import Notification, NotificationType


async def setup(tmp_path):
    store = Store(tmp_path)
    await store.initialize()
    db = BrowseStore(store)
    await db.initialize()
    await db.claim_post("100", "123")
    return store, db


def reply(comment="50"):
    return Notification(
        "default",
        comment,
        comment,
        comment,
        NotificationType.REPLY,
        "200",
        "盒友",
        "123",
        "1",
        comment,
        "回复麦麦",
        time.time() + 10,
        raw={"userid_b": "100"},
    )


async def success(store, db, index, anchor="browse:123", group="post:123:thread:browse"):
    message = f"out:{index}"
    await store.claim_send("100", message)
    await store.record_activity("100", "out", message, "123", group, "标题", "麦麦", "正文", "sending", anchor)
    return await db.complete_comment("100", message, "123", f"xhh:123:{index}")


def test_three_successes_then_each_new_reply_adds_three_once(tmp_path):
    async def check():
        store, db = await setup(tmp_path)
        for i in range(3):
            await db.begin_comment("100", "123")
            await success(store, db, i)
        with pytest.raises(ValueError, match="3/3"):
            await db.begin_comment("100", "123")
        n = reply()
        assert await db.note_notification("100", n)
        assert await db.note_notification("100", n)
        assert await db.post_budget("100", "123") == {"used": 3, "limit": 6}
        for i in range(3, 6):
            await db.begin_comment("100", "123")
            await success(store, db, i)
        with pytest.raises(ValueError, match="6/6"):
            await db.begin_comment("100", "123")
        assert await db.note_notification("100", reply("51"))
        assert await db.post_budget("100", "123") == {"used": 6, "limit": 9}
        restarted = BrowseStore(store)
        await restarted.initialize()
        assert await restarted.post_budget("100", "123") == {"used": 6, "limit": 9}
        assert await restarted.post_budget("200", "123") == {"used": 0, "limit": 3}

    asyncio.run(check())


@pytest.mark.parametrize("mention_first", [False, True])
def test_mention_and_reply_for_same_comment_do_not_stack(mention_first, tmp_path):
    async def check():
        store, db = await setup(tmp_path)
        await success(store, db, 1)
        n = reply()
        mention = replace(n, event_type=NotificationType.MENTION)
        if mention_first:
            assert not await db.note_notification("100", mention)
            assert not await db.note_notification("100", n)
        else:
            assert await db.note_notification("100", n)
            anchor = "xhh:123:50"
            await db.save_anchor("100", anchor, "123")
            await success(store, db, 2, anchor, "post:123:thread:1")
            assert not await db.note_notification("100", mention)
            assert await db.origin("100", anchor) is None
        assert await db.post_budget("100", "123") == {"used": 1, "limit": 3}

    asyncio.run(check())


def test_late_mention_during_send_does_not_consume_active_quota(tmp_path):
    async def check():
        store, db = await setup(tmp_path)
        await success(store, db, 1)
        n = reply()
        assert await db.note_notification("100", n)
        await db.begin_comment("100", "123")
        assert not await db.note_notification("100", replace(n, event_type=NotificationType.MENTION))
        assert not await success(store, db, 2, "xhh:123:50", "post:123:thread:1")
        assert (await store.sent("100", "out:2"))["state"] == "sent"
        assert await db.post_budget("100", "123") == {"used": 1, "limit": 3}

    asyncio.run(check())


def test_only_later_replies_to_bot_grant_credit_and_legacy_success_is_imported(tmp_path):
    async def check():
        store, db = await setup(tmp_path)
        assert not await db.note_notification("100", reply())
        await store.claim_send("100", "legacy")
        await store.record_activity(
            "100", "out", "legacy", "123", "post:123:thread:browse", "标题", "麦麦", "正文", "sent"
        )
        await store.finish_send("100", "legacy", "sent", "xhh:123:1")
        await db.initialize()
        await db.initialize()
        assert await db.post_budget("100", "123") == {"used": 1, "limit": 3}
        for n in (
            replace(reply(), sender_uid="100"),
            replace(reply(), sender_uid=""),
            replace(reply(), created_at=1),
            replace(reply(), raw={"userid_b": "300"}),
            replace(reply(), raw={}),
            replace(reply(), post_id="456"),
        ):
            assert not await db.note_notification("100", n)
        assert await db.post_budget("100", "123") == {"used": 1, "limit": 3}

    asyncio.run(check())
