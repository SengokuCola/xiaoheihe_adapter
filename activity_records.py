"""按真实帖子聚合互动记录，提供分页时间线和按需读取的完整内容。"""

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

import asyncio
import json
import sqlite3

from .storage import Store

POST_PAGE_SIZE = 20
EVENT_PAGE_SIZE = 30
ATTENTION_STATES = "('error','unknown','blocked','sending')"
STATE_LABELS = {
    "processing": "正在处理", "accepted": "已交给麦麦", "error": "处理失败",
    "sending": "发送中 / 待核对", "sent": "已发送", "unknown": "发送结果待核对",
    "considering": "已提交自主判断", "participated": "已浏览并参与评论", "skipped": "已跳过",
}
GROUPS = f"""
SELECT post_id, COUNT(*) AS records, MAX(created_at) AS last_at,
       SUM(direction='browse') AS browsed, SUM(direction='in') AS received,
       SUM(direction='out' AND state='sent') AS replied,
       SUM(state IN {ATTENTION_STATES}) AS attention
FROM activity WHERE account=? AND post_id<>'' GROUP BY post_id
"""


def display_time(stamp: float) -> str:
    return datetime.fromtimestamp(stamp, timezone(timedelta(hours=8))).strftime("%Y-%m-%d %H:%M:%S")


def pagination(page: int, total: int, size: int) -> Dict[str, int]:
    pages = max(1, (total + size - 1) // size)
    return {"page": min(page, pages), "pages": pages, "total": total}


def validate_page(page: int) -> None:
    if type(page) is not int or page < 1:
        raise ValueError("页码必须是正整数")


class ActivityRecords:
    def __init__(self, store: Store) -> None:
        self.store = store

    async def posts(self, account: str, page: int = 1, search: str = "", status: str = "all") -> Dict[str, Any]:
        validate_page(page)
        if not isinstance(search, str) or len(search) > 200:
            raise ValueError("搜索内容不能超过 200 字")
        if status not in {"all", "replied", "attention"}:
            raise ValueError("未知帖子筛选条件")
        return await asyncio.to_thread(self._posts, account, page, search.strip(), status)

    def _posts(self, account: str, page: int, search: str, status: str) -> Dict[str, Any]:
        with sqlite3.connect(self.store.path, timeout=15) as db:
            db.row_factory = sqlite3.Row
            db.execute("BEGIN")
            overview = dict(db.execute(
                "WITH grouped AS (" + GROUPS + ") SELECT COUNT(*) AS posts, "
                "COALESCE(SUM(received),0) AS received, COALESCE(SUM(replied),0) AS replied, "
                "COALESCE(SUM(attention),0) AS attention FROM grouped", (account,),
            ).fetchone())
            where = "1=1"
            parameters: List[Any] = [account]
            if status == "replied":
                where += " AND g.replied>0"
            elif status == "attention":
                where += " AND g.attention>0"
            if search:
                # 搜索按字面值匹配，% 和 _ 不扩张为通配符。
                pattern = "%" + search.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_") + "%"
                where += (
                    " AND (g.post_id LIKE ? ESCAPE '\\' OR EXISTS (SELECT 1 FROM activity a "
                    "WHERE a.account=? AND a.post_id=g.post_id "
                    "AND (a.title LIKE ? ESCAPE '\\' OR a.sender LIKE ? ESCAPE '\\')))"
                )
                parameters.extend([pattern, account, pattern, pattern])
            prefix = "WITH grouped AS (" + GROUPS + ") "
            total = db.execute(prefix + "SELECT COUNT(*) FROM grouped g WHERE " + where, parameters).fetchone()[0]
            pager = pagination(page, total, POST_PAGE_SIZE)
            rows = db.execute(
                prefix + "SELECT g.* FROM grouped g WHERE " + where +
                " ORDER BY g.last_at DESC,g.post_id DESC LIMIT ? OFFSET ?",
                (*parameters, POST_PAGE_SIZE, (pager["page"] - 1) * POST_PAGE_SIZE),
            ).fetchall()
            items = [self._post(db, account, dict(row)) for row in rows]
        return {"account_id": account, "overview": overview, "items": items, "pagination": pager}

    @staticmethod
    def _post(db: sqlite3.Connection, account: str, item: Dict[str, Any]) -> Dict[str, Any]:
        post = item["post_id"]
        title = db.execute(
            "SELECT title FROM activity WHERE account=? AND post_id=? AND TRIM(title)<>'' "
            "ORDER BY CASE direction WHEN 'browse' THEN 0 ELSE 1 END,created_at DESC,rowid DESC LIMIT 1",
            (account, post),
        ).fetchone()
        latest = db.execute(
            "SELECT direction,state,body,sender FROM activity WHERE account=? AND post_id=? "
            "ORDER BY created_at DESC,rowid DESC LIMIT 1", (account, post),
        ).fetchone()
        item.update(
            title=title[0] if title else "", last_time=display_time(item["last_at"]),
            preview=latest["body"][:120], last_direction=latest["direction"],
            url="https://xiaoheihe.cn/app/bbs/link/" + post,
        )
        return item

    async def thread(self, account: str, post: str, page: int = 1, kind: str = "all") -> Dict[str, Any]:
        self._validate_post(post)
        validate_page(page)
        if kind not in {"all", "browse", "in", "out"}:
            raise ValueError("未知记录类型")
        return await asyncio.to_thread(self._thread, account, post, page, kind)

    def _thread(self, account: str, post: str, page: int, kind: str) -> Dict[str, Any]:
        with sqlite3.connect(self.store.path, timeout=15) as db:
            db.row_factory = sqlite3.Row
            db.execute("BEGIN")
            grouped = db.execute("SELECT * FROM (" + GROUPS + ") WHERE post_id=?", (account, post)).fetchone()
            if grouped is None:
                raise ValueError("当前账号没有这个帖子的记录")
            header = self._post(db, account, dict(grouped))
            where = "a.account=? AND a.post_id=?"
            args: List[Any] = [account, post]
            if kind != "all":
                where += " AND a.direction=?"
                args.append(kind)
            total = db.execute("SELECT COUNT(*) FROM activity a WHERE " + where, args).fetchone()[0]
            pager = pagination(page, total, EVENT_PAGE_SIZE)
            rows = db.execute(
                "SELECT a.*,r.target AS reply_target FROM activity a LEFT JOIN routes r "
                "ON r.account=a.account AND r.message=a.anchor AND r.group_id=a.group_id WHERE " + where +
                " ORDER BY a.created_at ASC,a.rowid ASC LIMIT ? OFFSET ?",
                (*args, EVENT_PAGE_SIZE, (pager["page"] - 1) * EVENT_PAGE_SIZE),
            ).fetchall()
            items = [self._event(dict(row), full=False) for row in rows]
        return {"account_id": account, "post": header, "items": items, "pagination": pager}

    async def event(self, account: str, post: str, direction: str, message: str) -> Dict[str, Any]:
        self._validate_post(post)
        if direction not in {"browse", "in", "out"} or not isinstance(message, str) or not 1 <= len(message) <= 200:
            raise ValueError("无效记录标识")
        return await asyncio.to_thread(self._read_event, account, post, direction, message)

    def _read_event(self, account: str, post: str, direction: str, message: str) -> Dict[str, Any]:
        with sqlite3.connect(self.store.path, timeout=15) as db:
            db.row_factory = sqlite3.Row
            row = db.execute(
                "SELECT a.*,r.target AS reply_target FROM activity a LEFT JOIN routes r "
                "ON r.account=a.account AND r.message=a.anchor AND r.group_id=a.group_id "
                "WHERE a.account=? AND a.post_id=? AND a.direction=? AND a.message=?",
                (account, post, direction, message),
            ).fetchone()
            if row is None:
                raise ValueError("当前帖子中不存在这条记录")
            return {"account_id": account, "item": self._event(dict(row), full=True)}

    @staticmethod
    def _validate_post(post: str) -> None:
        if not isinstance(post, str) or not post.isascii() or not post.isdigit() or len(post) > 32:
            raise ValueError("无效帖子 ID")

    @staticmethod
    def _event(item: Dict[str, Any], *, full: bool) -> Dict[str, Any]:
        item.pop("account")
        target = item.pop("reply_target")
        direction = item["direction"]
        item["time"] = display_time(item["created_at"])
        item["direction_label"] = {"in": "收到评论", "out": "麦麦回帖", "browse": "浏览记录"}[direction]
        item["state_label"] = (
            "发送被限制" if direction == "out" else "未交给麦麦"
        ) if item["state"] == "blocked" else STATE_LABELS[item["state"]]
        item["reply_label"] = ""
        if direction == "out" and target is not None:
            route = json.loads(target)
            parent = route["parent_comment_id"]
            item["reply_label"] = "回复评论 #" + parent if parent else "回复主帖"
        limit = 400 if direction == "browse" else 2000
        item["has_more"] = not full and (len(item["body"]) > limit or len(item["detail"]) > 1200)
        if not full:
            item["body"] = item["body"][:limit]
            item["detail"] = item["detail"][:1200]
        return item
