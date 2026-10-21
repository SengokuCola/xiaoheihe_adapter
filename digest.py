"""将实际浏览材料按批次交给 Replyer，生成并发布多段总结帖。"""

from pydantic import BaseModel, ConfigDict, Field, model_validator
from typing import Any, Dict, List, Literal, Optional

import asyncio
import json
import sqlite3
import time
import uuid

from .storage import Store
from .vendor.api_client import CredentialInvalidError, RateLimitedError
from .vendor.security import redact_log_text
from .community_client import CommentRejectedError


class DigestSettings(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    enabled: bool = False
    trigger: Literal["count", "interval", "either"] = "count"
    every_posts: int = Field(default=10, ge=1, le=100)
    interval_hours: int = Field(default=24, ge=1, le=168)
    topic_id: str = Field(default="", max_length=32)
    model_name: str = Field(default="", max_length=200)

    @model_validator(mode="after")
    def validate_topic(self) -> "DigestSettings":
        if self.enabled and (not self.topic_id.isascii() or not self.topic_id.isdigit()):
            raise ValueError("请为总结帖选择发布分区")
        return self


class DigestDraft(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    title: str = Field(min_length=1, max_length=80)
    paragraphs: List[str] = Field(min_length=2, max_length=12)

    @model_validator(mode="after")
    def validate_text(self) -> "DigestDraft":
        self.title = self.title.strip()
        self.paragraphs = [text.strip() for text in self.paragraphs]
        if not self.title or any(not text for text in self.paragraphs):
            raise ValueError("总结标题和每段正文都不能为空")
        if sum(map(len, self.paragraphs)) > 10000:
            raise ValueError("总结正文超过 10000 字")
        return self


def material_excerpts(materials: List[Dict[str, Any]]) -> List[Dict[str, str]]:
    """为 8K 上下文的 Replyer 留出人设、格式要求和 2048 tokens 输出空间。"""
    if not materials:
        raise ValueError("本轮总结没有浏览材料")
    per_post = 2400 // len(materials)
    excerpts = []
    for item in materials:
        comments = "\n".join(item["my_comments"][-3:])
        comment_limit = min(len(comments), per_post // 3)
        excerpts.append({
            "post": item["post"], "body_excerpt": item["body"][:per_post - comment_limit],
            "my_comments_excerpt": comments[:comment_limit],
        })
    return excerpts


class DigestStore:
    def __init__(self, store: Store) -> None:
        self.store = store

    async def initialize(self) -> None:
        await self.store.execute(
            "CREATE TABLE IF NOT EXISTS digest_reads (account TEXT, post TEXT, body TEXT, stream TEXT, "
            "created_at REAL, batch TEXT NOT NULL DEFAULT '', PRIMARY KEY(account, post))"
        )
        await self.store.execute(
            "CREATE TABLE IF NOT EXISTS digest_runs (account TEXT, id TEXT, created_at REAL, state TEXT, "
            "topic TEXT, draft TEXT NOT NULL DEFAULT '', post TEXT NOT NULL DEFAULT '', error TEXT NOT NULL DEFAULT '', "
            "PRIMARY KEY(account, id))"
        )
        # 进程中断后保留准确的发送边界，发送中的任务不能自动再次发布。
        await self.store.execute(
            "UPDATE digest_runs SET state=CASE state WHEN 'sending' THEN 'unknown' ELSE 'error' END, "
            "error='上次任务因插件停止而中断' WHERE state IN ('generating', 'sending')"
        )

    async def collect(self, account: str, post: str, body: str, stream: str) -> None:
        await self.store.execute(
            "INSERT OR IGNORE INTO digest_reads(account, post, body, stream, created_at) VALUES (?, ?, ?, ?, ?)",
            (account, post, body, stream, time.time()),
        )

    async def snapshot(self, account: str) -> Dict[str, Any]:
        return await asyncio.to_thread(self._snapshot, account)

    def _snapshot(self, account: str) -> Dict[str, Any]:
        with sqlite3.connect(self.store.path, timeout=15) as db:
            db.row_factory = sqlite3.Row
            db.execute("BEGIN")
            count, first = db.execute(
                "SELECT COUNT(*), MIN(created_at) FROM digest_reads WHERE account=? AND batch=''", (account,),
            ).fetchone()
            row = db.execute(
                "SELECT * FROM digest_runs WHERE account=? ORDER BY created_at DESC, rowid DESC LIMIT 1", (account,),
            ).fetchone()
            latest = dict(row) if row else None
            if latest:
                latest.pop("account")
                latest["draft"] = json.loads(latest["draft"]) if latest["draft"] else None
            return {"pending_posts": count, "first_read_at": first, "latest": latest}

    async def claim(self, account: str, settings: DigestSettings) -> Optional[str]:
        return await asyncio.to_thread(self._claim, account, settings)

    def _claim(self, account: str, settings: DigestSettings) -> Optional[str]:
        with sqlite3.connect(self.store.path, timeout=15) as db:
            db.execute("BEGIN IMMEDIATE")
            previous = db.execute(
                "SELECT state, created_at FROM digest_runs WHERE account=? ORDER BY created_at DESC, rowid DESC LIMIT 1",
                (account,),
            ).fetchone()
            if previous and previous[0] not in {"sent", "discarded"}:
                return None
            count, first = db.execute(
                "SELECT COUNT(*), MIN(created_at) FROM digest_reads WHERE account=? AND batch=''", (account,),
            ).fetchone()
            if not settings.enabled or not count:
                return None
            time_due = time.time() >= max(first, previous[1] if previous else first) + settings.interval_hours * 3600
            count_due = count >= settings.every_posts
            if not ((settings.trigger != "interval" and count_due) or (settings.trigger != "count" and time_due)):
                return None
            batch = uuid.uuid4().hex
            db.execute("INSERT INTO digest_runs(account, id, created_at, state, topic) VALUES (?, ?, ?, 'generating', ?)",
                       (account, batch, time.time(), settings.topic_id))
            limit = settings.every_posts if settings.trigger == "count" or count_due else 100
            db.execute(
                "UPDATE digest_reads SET batch=? WHERE account=? AND post IN "
                "(SELECT post FROM digest_reads WHERE account=? AND batch='' ORDER BY created_at, rowid LIMIT ?)",
                (batch, account, account, limit),
            )
            return batch

    async def materials(self, account: str, batch: str) -> List[Dict[str, Any]]:
        return await asyncio.to_thread(self._materials, account, batch)

    def _materials(self, account: str, batch: str) -> List[Dict[str, Any]]:
        with sqlite3.connect(self.store.path, timeout=15) as db:
            db.row_factory = sqlite3.Row
            rows = db.execute(
                "SELECT post, body, stream FROM digest_reads WHERE account=? AND batch=? ORDER BY created_at, rowid",
                (account, batch),
            ).fetchall()
            materials = []
            for row in rows:
                replies = db.execute(
                    "SELECT body FROM activity WHERE account=? AND post_id=? AND direction='out' AND state='sent' "
                    "ORDER BY created_at", (account, row["post"]),
                ).fetchall()
                materials.append({**dict(row), "my_comments": [reply[0] for reply in replies]})
            return materials

    async def update(self, account: str, batch: str, state: str, *, draft: str = "", post: str = "", error: str = "") -> None:
        await self.store.execute(
            "UPDATE digest_runs SET state=?, draft=CASE WHEN ?='' THEN draft ELSE ? END, post=?, error=? "
            "WHERE account=? AND id=?", (state, draft, draft, post, error, account, batch),
        )


class DigestPublisher:
    def __init__(self, ctx: Any, browser: Any, send_lock: asyncio.Lock) -> None:
        self.ctx = ctx
        self.browser = browser
        self.db = browser.digest
        self.send_lock = send_lock
        self.lock = asyncio.Lock()

    async def due(self, client: Any, account: str) -> None:
        async with self.lock:
            settings = (await self.browser.settings(account)).summary
            if not settings.enabled:
                return
            latest = (await self.db.snapshot(account))["latest"]
            if latest and latest["state"] == "retry":
                await self._publish(client, account, latest["id"], latest["draft"])
                return
            batch = await self.db.claim(account, settings)
            if batch is not None:
                await self._publish(client, account, batch)

    async def action(self, account: str, action: str) -> None:
        async with self.lock:
            latest = (await self.db.snapshot(account))["latest"]
            if latest is None or latest["state"] not in {"error", "failed", "unknown"}:
                raise ValueError("没有等待处理的失败任务")
            if action == "discard":
                await self.db.update(account, latest["id"], "discarded")
                return
            if action != "retry" or latest["state"] == "unknown":
                raise ValueError("发送结果不确定，请先在小黑盒核对；本轮可跳过，不能直接重发")
            await self.db.update(account, latest["id"], "retry")

    async def _publish(self, client: Any, account: str, batch: str, saved_draft: Optional[Dict[str, Any]] = None) -> None:
        sending = False
        try:
            if saved_draft is None:
                await self.db.update(account, batch, "generating")
                materials = await self.db.materials(account, batch)
                excerpts = material_excerpts(materials)
                # 写作目标与输出格式分别传入；人格由宿主 Replyer 注入。
                reference = (
                    "写一篇小黑盒浏览感想。挑一两个感兴趣的话题，写自己的看法。"
                    "正文约200–400字，分2–4段。\n浏览素材：\n"
                    + json.dumps(excerpts, ensure_ascii=False)
                )
                result = await self.ctx.call_capability(
                    "maisaka.reply.generate", stream_id=materials[-1]["stream"], reference=reference,
                    model_name=(await self.browser.settings(account)).summary.model_name,
                    format_instruction='输出 JSON：{"title":"帖子标题","paragraphs":["第一段","第二段"]}。',
                    timeout_ms=180000,
                )
                if not result["success"]:
                    raise RuntimeError(result["error"])
                draft = DigestDraft.model_validate_json(result["text"])
                await self.db.update(account, batch, "ready", draft=draft.model_dump_json())
            else:
                draft = DigestDraft.model_validate(saved_draft)
            async with self.send_lock:
                # 生成可能耗时较长，发布前重新读取开关和发布分区。
                settings = (await self.browser.settings(account)).summary
                if not settings.enabled:
                    raise ValueError("主动发帖已关闭，本轮内容已保留")
                await self.db.store.execute(
                    "UPDATE digest_runs SET topic=? WHERE account=? AND id=?", (settings.topic_id, account, batch),
                )
                await self.db.update(account, batch, "sending")
                sending = True
                post = await client.publish_post(draft.title, draft.paragraphs, settings.topic_id)
                await self.db.update(account, batch, "sent", post=post)
        except asyncio.CancelledError:
            await self.db.update(account, batch, "unknown" if sending else "error", error="插件停止，本轮任务已中断")
            raise
        except Exception as exc:
            state = "failed" if isinstance(exc, (CommentRejectedError, CredentialInvalidError, RateLimitedError)) else (
                "unknown" if sending else "error"
            )
            error = redact_log_text(str(exc))
            await self.db.update(account, batch, state, error=error)
            self.ctx.logger.error("小黑盒总结发帖失败: %s: %s", type(exc).__name__, error)
