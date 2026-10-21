from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Optional

import asyncio
import json
import sqlite3
import time

from .vendor.models import Credentials, RoutingTarget
from .vendor.security import atomic_write_json


def load_credentials(directory: Path) -> Credentials:
    with (directory / "credentials.json").open(encoding="utf-8") as handle:
        payload = json.load(handle)
    credentials = Credentials(**payload)
    if not credentials.uid or not credentials.cookies:
        raise ValueError("凭证缺少账号 ID 或 Cookie，请重新扫码")
    return credentials


def save_credentials(directory: Path, credentials: Credentials) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    atomic_write_json(directory / "credentials.json", asdict(credentials))


class Store:
    def __init__(self, directory: Path) -> None:
        self.path = directory / "community.sqlite3"

    async def execute(self, sql: str, parameters: tuple = (), *, fetch: bool = False) -> Any:
        return await asyncio.to_thread(self._execute, sql, parameters, fetch)

    def _execute(self, sql: str, parameters: tuple, fetch: bool) -> Any:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(self.path, timeout=15) as db:
            cursor = db.execute(sql, parameters)
            return cursor.fetchone() if fetch else cursor.rowcount

    async def initialize(self) -> None:
        for statement in (
            "CREATE TABLE IF NOT EXISTS cursors (account TEXT, kind TEXT, value INTEGER, PRIMARY KEY(account, kind))",
            "CREATE TABLE IF NOT EXISTS events (account TEXT, event_key TEXT, PRIMARY KEY(account, event_key))",
            "CREATE TABLE IF NOT EXISTS routes (account TEXT, message TEXT, target TEXT, group_id TEXT, "
            "PRIMARY KEY(account, message))",
            "CREATE TABLE IF NOT EXISTS activity (account TEXT, direction TEXT, message TEXT, created_at REAL, "
            "post_id TEXT, group_id TEXT, title TEXT, sender TEXT, body TEXT, state TEXT, detail TEXT, anchor TEXT, "
            "PRIMARY KEY(account, direction, message))",
            "CREATE INDEX IF NOT EXISTS activity_recent ON activity(account, created_at DESC)",
            "CREATE INDEX IF NOT EXISTS activity_post ON activity(account, post_id, created_at)",
            "CREATE TABLE IF NOT EXISTS sends (account TEXT, message TEXT, state TEXT, external_id TEXT, "
            "PRIMARY KEY(account, message))",
        ):
            await self.execute(statement)

    async def cursor(self, account: str, kind: str) -> Optional[int]:
        row = await self.execute("SELECT value FROM cursors WHERE account=? AND kind=?", (account, kind), fetch=True)
        return row[0] if row else None

    async def set_cursor(self, account: str, kind: str, value: int) -> None:
        await self.execute("INSERT OR REPLACE INTO cursors VALUES (?, ?, ?)", (account, kind, value))

    async def seen(self, account: str, key: str) -> bool:
        return bool(
            await self.execute("SELECT 1 FROM events WHERE account=? AND event_key=?", (account, key), fetch=True)
        )

    async def mark_seen(self, account: str, key: str) -> None:
        await self.execute("INSERT OR IGNORE INTO events VALUES (?, ?)", (account, key))

    async def save_route(self, account: str, message: str, target: RoutingTarget, group: str) -> None:
        await self.execute(
            "INSERT OR REPLACE INTO routes VALUES (?, ?, ?, ?)", (account, message, json.dumps(asdict(target)), group)
        )

    async def route(self, account: str, message: str, group: str) -> RoutingTarget:
        row = await self.execute(
            "SELECT target, group_id FROM routes WHERE account=? AND message=?", (account, message), fetch=True
        )
        if row is None or row[1] != group:
            raise ValueError("回复锚点不存在或不属于当前楼层，拒绝猜测评论目标")
        return RoutingTarget(**json.loads(row[0]))

    async def claim_send(self, account: str, message: str) -> bool:
        return bool(await self.execute(
            "INSERT INTO sends VALUES (?, ?, 'sending', '') "
            "ON CONFLICT(account, message) DO UPDATE SET state='sending', external_id='' WHERE sends.state!='sent'",
            (account, message),
        ))

    async def finish_send(self, account: str, message: str, state: str, external_id: str = "") -> None:
        await self.execute(
            "UPDATE sends SET state=?, external_id=? WHERE account=? AND message=? AND state!='sent'",
            (state, external_id, account, message),
        )

    async def sent(self, account: str, message: str) -> Optional[Dict[str, str]]:
        row = await self.execute(
            "SELECT state, external_id FROM sends WHERE account=? AND message=?", (account, message), fetch=True
        )
        return {"state": row[0], "external_id": row[1]} if row else None

    async def record_activity(
        self,
        account: str,
        direction: str,
        message: str,
        post: str,
        group: str,
        title: str,
        sender: str,
        body: str,
        state: str,
        anchor: str = "",
    ) -> None:
        # 同一消息重试只更新处理状态，保留首次出现时间；不记录 Cookie 或完整接口响应。
        await self.execute(
            "INSERT INTO activity VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '', ?) "
            "ON CONFLICT(account, direction, message) DO UPDATE SET state=excluded.state, detail=''",
            (account, direction, message, time.time(), post, group, title, sender, body, state, anchor),
        )

    async def update_activity(
        self,
        account: str,
        direction: str,
        message: str,
        state: str,
        detail: str = "",
        title: Optional[str] = None,
        body: Optional[str] = None,
        group: Optional[str] = None,
    ) -> None:
        await self.execute(
            "UPDATE activity SET state=?, detail=?, title=COALESCE(?, title), body=COALESCE(?, body), group_id=COALESCE(?, group_id) "
            "WHERE account=? AND direction=? AND message=?",
            (state, detail, title, body, group, account, direction, message),
        )

    async def activity(self, account: str, page: int = 1) -> Dict[str, Any]:
        if type(page) is not int or page < 1:
            raise ValueError("页码必须是正整数")
        return await asyncio.to_thread(self._activity, account, page)

    def _activity(self, account: str, page: int) -> Dict[str, Any]:
        with sqlite3.connect(self.path, timeout=15) as db:
            db.row_factory = sqlite3.Row
            # 一个读事务内取得统计和分页，避免正在收发消息时页数与记录数不一致。
            db.execute("BEGIN")
            counts = db.execute(
                "SELECT COUNT(*) AS total, "
                "COUNT(CASE WHEN direction='in' THEN 1 END) AS received, "
                "COUNT(CASE WHEN direction='out' AND state='sent' THEN 1 END) AS replied, "
                "COUNT(CASE WHEN state IN ('error', 'unknown') THEN 1 END) AS attention, "
                "COUNT(DISTINCT group_id) AS conversations FROM activity WHERE account=?",
                (account,),
            ).fetchone()
            total = counts["total"]
            pages = max(1, (total + 49) // 50)
            page = min(page, pages)
            rows = db.execute(
                "SELECT * FROM activity WHERE account=? ORDER BY created_at DESC, rowid DESC LIMIT 50 OFFSET ?",
                (account, (page - 1) * 50),
            ).fetchall()
        states = {
            "processing": "正在处理",
            "accepted": "已交给麦麦",
            "blocked": "宿主未接收",
            "error": "处理失败",
            "sending": "发送中或中断待核对",
            "sent": "已发送",
            "unknown": "发送结果待核对",
            "considering": "已提交麦麦自主判断",
            "participated": "已主动参与评论",
            "skipped": "已排除",
        }
        items = []
        for row in rows:
            item = dict(row)
            item.pop("account")
            item["time"] = datetime.fromtimestamp(row["created_at"], timezone(timedelta(hours=8))).strftime(
                "%m-%d %H:%M:%S"
            )
            item["direction_label"] = {"in": "收到评论", "out": "麦麦回复", "browse": "主动浏览"}[row["direction"]]
            item["state_label"] = states[row["state"]]
            item["preview"] = row["body"][:100]
            items.append(item)
        return {
            "received": counts["received"],
            "replied": counts["replied"],
            "attention": counts["attention"],
            "conversations": counts["conversations"],
            "items": items,
            "pagination": {"page": page, "pages": pages, "total": total},
        }
