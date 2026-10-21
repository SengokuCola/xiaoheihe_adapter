from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import asyncio
import json
import sqlite3
import time

from .storage import Store
from .vendor.models import Notification, NotificationType

BASE_POST_COMMENTS = 3
REPLY_COMMENT_BONUS = 3


class BrowseStore:
    def __init__(self, store: Store) -> None:
        self.store = store

    async def initialize(self) -> None:
        for statement in (
            "CREATE TABLE IF NOT EXISTS browse_preferences (account TEXT PRIMARY KEY, settings TEXT)",
            "CREATE TABLE IF NOT EXISTS browse_catalog (account TEXT PRIMARY KEY, topics TEXT)",
            "CREATE TABLE IF NOT EXISTS browse_schedule (account TEXT PRIMARY KEY, next_scan REAL)",
            "CREATE TABLE IF NOT EXISTS browse_jobs (account TEXT, post TEXT, created_at REAL, reserved_at REAL DEFAULT 0, "
            "PRIMARY KEY(account, post))",
            "CREATE TABLE IF NOT EXISTS browse_anchors (account TEXT, anchor TEXT, post TEXT, PRIMARY KEY(account, anchor))",
            "CREATE TABLE IF NOT EXISTS browse_resets (account TEXT, day REAL, decisions INTEGER, PRIMARY KEY(account, day))",
            "CREATE TABLE IF NOT EXISTS browse_comment_successes (account TEXT, message TEXT, post TEXT, "
            "external_id TEXT, sent_at REAL, PRIMARY KEY(account, message), UNIQUE(account, external_id))",
            "CREATE TABLE IF NOT EXISTS browse_reply_events (account TEXT, post TEXT, comment TEXT, "
            "is_mention INTEGER, credited INTEGER, created_at REAL, PRIMARY KEY(account, post, comment))",
            "CREATE TABLE IF NOT EXISTS browse_post_streams (account TEXT, post TEXT, stream TEXT, group_id TEXT, "
            "group_name TEXT, PRIMARY KEY(account, post))",
        ):
            await self.store.execute(statement)
        await self.store.execute(
            "UPDATE browse_preferences SET settings=json_remove(settings, "
            "'$.min_comment_gap_seconds', '$.daily_comments', '$.daily_decisions') "
            "WHERE json_type(settings, '$.min_comment_gap_seconds') IS NOT NULL "
            "OR json_type(settings, '$.daily_comments') IS NOT NULL "
            "OR json_type(settings, '$.daily_decisions') IS NOT NULL"
        )
        await self.store.execute(
            "INSERT OR IGNORE INTO browse_comment_successes "
            "SELECT s.account,s.message,a.post_id,s.external_id,a.created_at FROM sends s "
            "JOIN activity a ON a.account=s.account AND a.message=s.message AND a.direction='out' "
            "WHERE s.state='sent' AND a.group_id='post:' || a.post_id || ':thread:browse'"
        )

    async def settings(self, account: str) -> Optional[Dict[str, Any]]:
        row = await self.store.execute(
            "SELECT settings FROM browse_preferences WHERE account=?", (account,), fetch=True
        )
        return json.loads(row[0]) if row else None

    async def save(self, account: str, settings: Dict[str, Any]) -> None:
        await self.store.execute(
            "INSERT OR REPLACE INTO browse_preferences VALUES (?, ?)",
            (account, json.dumps(settings, ensure_ascii=False)),
        )
        await self.store.execute("DELETE FROM browse_schedule WHERE account=?", (account,))

    async def catalog(self, account: str) -> List[Dict[str, str]]:
        row = await self.store.execute("SELECT topics FROM browse_catalog WHERE account=?", (account,), fetch=True)
        return json.loads(row[0]) if row else []

    async def save_catalog(self, account: str, topics: List[Dict[str, str]]) -> None:
        await self.store.execute(
            "INSERT OR REPLACE INTO browse_catalog VALUES (?, ?)", (account, json.dumps(topics, ensure_ascii=False))
        )

    async def claim_scan(self, account: str, interval: int) -> bool:
        return await asyncio.to_thread(self._claim_scan, account, interval)

    def _claim_scan(self, account: str, interval: int) -> bool:
        with sqlite3.connect(self.store.path, timeout=15) as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT next_scan FROM browse_schedule WHERE account=?", (account,)).fetchone()
            now = time.time()
            if row is not None and row[0] > now:
                return False
            db.execute("INSERT OR REPLACE INTO browse_schedule VALUES (?, ?)", (account, now + interval))
            return True

    @staticmethod
    def day_start() -> float:
        return datetime.now(timezone(timedelta(hours=8))).replace(hour=0, minute=0, second=0, microsecond=0).timestamp()

    async def claim_post(self, account: str, post: str) -> bool:
        return bool(await self.store.execute(
            "INSERT OR IGNORE INTO browse_jobs(account, post, created_at) VALUES (?, ?, ?)",
            (account, post, time.time()),
        ))

    async def can_consider(self, account: str, post: str) -> bool:
        # 只预检帖子去重；详情通过视频筛选后再由 claim_post 原子登记。
        row = await self.store.execute(
            "SELECT 1 FROM browse_jobs WHERE account=? AND post=?", (account, post), fetch=True,
        )
        return row is None

    @staticmethod
    def _decision_count(db: sqlite3.Connection, account: str, day: float) -> int:
        return db.execute(
            "SELECT COUNT(*) - COALESCE((SELECT decisions FROM browse_resets WHERE account=? AND day=?), 0) "
            "FROM browse_jobs WHERE account=? AND created_at>=?",
            (account, day, account, day),
        ).fetchone()[0]

    async def reset_today(self, account: str) -> None:
        day = self.day_start()
        # 只记录当天计数基线，不删除帖子去重、评论配额或历史互动。
        # 单条写语句与 claim_post 的事务串行化，重置期间新增的帖子也不会丢计数。
        await self.store.execute(
            "INSERT INTO browse_resets(account, day, decisions) "
            "SELECT ?, ?, COUNT(*) FROM browse_jobs WHERE account=? AND created_at>=? "
            "ON CONFLICT(account, day) DO UPDATE SET decisions=excluded.decisions",
            (account, day, account, day),
        )

    async def save_anchor(self, account: str, anchor: str, post: str) -> None:
        await self.store.execute("INSERT OR REPLACE INTO browse_anchors VALUES (?, ?, ?)", (account, anchor, post))

    async def origin(self, account: str, anchor: str) -> Optional[str]:
        row = await self.store.execute(
            "SELECT post FROM browse_anchors WHERE account=? AND anchor=?", (account, anchor), fetch=True
        )
        return row[0] if row else None

    async def save_stream(self, account: str, post: str, stream: str, group: str, name: str) -> None:
        await self.store.execute(
            "INSERT OR REPLACE INTO browse_post_streams VALUES (?, ?, ?, ?, ?)", (account, post, stream, group, name)
        )

    async def post_stream(self, account: str, post: str) -> Optional[Dict[str, str]]:
        row = await self.store.execute(
            "SELECT stream,group_id,group_name FROM browse_post_streams WHERE account=? AND post=?",
            (account, post),
            fetch=True,
        )
        return {"stream_id": row[0], "group_id": row[1], "group_name": row[2]} if row else None

    async def is_mention_anchor(self, account: str, post: str, anchor: str) -> bool:
        row = await self.store.execute(
            "SELECT 1 FROM browse_reply_events WHERE account=? AND post=? AND is_mention=1 "
            "AND ?='xhh:' || post || ':' || comment",
            (account, post, anchor),
            fetch=True,
        )
        return row is not None

    async def begin_comment(self, account: str, post: str) -> None:
        await asyncio.to_thread(self._begin_comment, account, post)

    def _begin_comment(self, account: str, post: str) -> None:
        # 全部主动/续聊发送由插件发送锁串行化，额度只统计确认成功的评论。
        with sqlite3.connect(self.store.path, timeout=15) as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute(
                "SELECT 1 FROM browse_jobs WHERE account=? AND post=?", (account, post)
            ).fetchone()
            if row is None:
                raise ValueError("该帖子没有有效浏览任务")
            budget = self._post_budget(db, account, post)
            if budget["used"] >= budget["limit"]:
                raise ValueError(f"该帖子主动评论成功次数已达上限（{budget['used']}/{budget['limit']}）")
            # 仅记录今天尝试评论过的帖子，供页面统计，不作为发送限制。
            db.execute("UPDATE browse_jobs SET reserved_at=? WHERE account=? AND post=?", (time.time(), account, post))

    @staticmethod
    def _post_budget(db: sqlite3.Connection, account: str, post: str) -> Dict[str, int]:
        used = db.execute(
            "SELECT COUNT(*) FROM browse_comment_successes WHERE account=? AND post=?", (account, post)
        ).fetchone()[0]
        credits = db.execute(
            "SELECT COALESCE(SUM(credited),0) FROM browse_reply_events WHERE account=? AND post=?", (account, post)
        ).fetchone()[0]
        return {"used": used, "limit": BASE_POST_COMMENTS + credits * REPLY_COMMENT_BONUS}

    async def post_budget(self, account: str, post: str) -> Dict[str, int]:
        return await asyncio.to_thread(self._read_post_budget, account, post)

    def _read_post_budget(self, account: str, post: str) -> Dict[str, int]:
        with sqlite3.connect(self.store.path, timeout=15) as db:
            return self._post_budget(db, account, post)

    async def complete_comment(self, account: str, message: str, post: str, external: str) -> bool:
        return await asyncio.to_thread(self._complete_comment, account, message, post, external)

    def _complete_comment(self, account: str, message: str, post: str, external: str) -> bool:
        with sqlite3.connect(self.store.path, timeout=15) as db:
            db.execute("BEGIN IMMEDIATE")
            db.execute(
                "UPDATE sends SET state='sent',external_id=? WHERE account=? AND message=? AND state!='sent'",
                (external, account, message),
            )
            if db.execute(
                "SELECT 1 FROM activity a JOIN browse_reply_events e "
                "ON a.account=e.account AND a.post_id=e.post AND a.anchor='xhh:' || e.post || ':' || e.comment "
                "WHERE a.account=? AND a.direction='out' AND a.message=? AND e.is_mention=1",
                (account, message),
            ).fetchone():
                return False
            db.execute(
                "INSERT OR IGNORE INTO browse_comment_successes VALUES (?, ?, ?, ?, ?)",
                (account, message, post, external, time.time()),
            )
            return True

    async def note_notification(self, account: str, notification: Notification) -> bool:
        return await asyncio.to_thread(self._note_notification, account, notification)

    def _note_notification(self, account: str, notification: Notification) -> bool:
        """返回该普通回复是否参与帖子额度；@ 优先，跨通知类型只记一次。"""
        if not notification.sender_uid or notification.sender_uid == account:
            return False
        post, comment = notification.post_id, notification.external_comment_id
        with sqlite3.connect(self.store.path, timeout=15) as db:
            db.execute("BEGIN IMMEDIATE")
            if not db.execute("SELECT 1 FROM browse_jobs WHERE account=? AND post=?", (account, post)).fetchone():
                return False
            if notification.event_type is NotificationType.MENTION:
                db.execute(
                    "INSERT INTO browse_reply_events VALUES (?, ?, ?, 1, 0, ?) "
                    "ON CONFLICT(account,post,comment) DO UPDATE SET is_mention=1,credited=0",
                    (account, post, comment, notification.created_at),
                )
                # 即使 @ 通知晚到，也撤回同一评论此前记入的加额和普通回复消耗。
                anchor = f"xhh:{post}:{comment}"
                db.execute(
                    "DELETE FROM browse_comment_successes WHERE account=? AND message IN "
                    "(SELECT message FROM activity WHERE account=? AND direction='out' AND anchor=?)",
                    (account, account, anchor),
                )
                db.execute("DELETE FROM browse_anchors WHERE account=? AND anchor=?", (account, anchor))
                return False
            if (
                notification.event_type is not NotificationType.REPLY
                or str(notification.raw.get("userid_b", "")) != account
            ):
                return False
            first = db.execute(
                "SELECT MIN(sent_at) FROM browse_comment_successes WHERE account=? AND post=?", (account, post)
            ).fetchone()[0]
            if first is None or notification.created_at < int(first):
                return False
            db.execute(
                "INSERT OR IGNORE INTO browse_reply_events VALUES (?, ?, ?, 0, 1, ?)",
                (account, post, comment, notification.created_at),
            )
            return bool(
                db.execute(
                    "SELECT credited FROM browse_reply_events WHERE account=? AND post=? AND comment=?",
                    (account, post, comment),
                ).fetchone()[0]
            )

    async def status(self, account: str) -> Dict[str, Any]:
        return await asyncio.to_thread(self._status, account)

    def _status(self, account: str) -> Dict[str, Any]:
        with sqlite3.connect(self.store.path, timeout=15) as db:
            db.execute("BEGIN")
            day = self.day_start()
            decisions = self._decision_count(db, account, day)
            attempts = db.execute(
                "SELECT COUNT(*) FROM browse_jobs WHERE account=? AND reserved_at>=?", (account, day)
            ).fetchone()[0]
            schedule = db.execute("SELECT next_scan FROM browse_schedule WHERE account=?", (account,)).fetchone()
        return {"today_decisions": decisions, "today_attempts": attempts, "next_scan": schedule[0] if schedule else 0}
