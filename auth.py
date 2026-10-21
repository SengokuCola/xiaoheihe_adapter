"""页面扫码会话；查询只返回内存快照，登录轮询在独立任务中执行。"""

from contextlib import suppress
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, Optional

import asyncio
import base64
import io
import time

import qrcode

from .storage import save_credentials
from .vendor.api_client import XiaoheiheApiClient
from .vendor.models import Credentials, LoginState
from .vendor.security import redact_log_text


def qr_image(content: str) -> str:
    output = io.BytesIO()
    qrcode.make(content).save(output, format="PNG")
    image = "data:image/png;base64," + base64.b64encode(output.getvalue()).decode("ascii")
    if len(image) > 24000:
        raise ValueError("登录二维码超过页面图片大小限制，请重新生成")
    return image


class LoginService:
    def __init__(self, directory: Path, on_login: Callable[[Credentials], Awaitable[None]]) -> None:
        self.directory = directory
        self.on_login = on_login
        self._task: Optional[asyncio.Task] = None
        self._state = "idle"
        self._message = "点击生成二维码，使用小黑盒 App 扫码并确认。"
        self._image = ""
        self._expires_at = 0.0

    def snapshot(self) -> Dict[str, Any]:
        remaining = max(0, int(self._expires_at - time.time()))
        active = self._state in {"waiting_scan", "scanned_waiting_confirm"} and remaining > 0
        return {
            "login_state": self._state,
            "login_message": self._message,
            "qr_image": self._image if active else "",
            "remaining_seconds": remaining if active else 0,
            "login_active": self._state == "requesting_qr" or active,
        }

    async def start(self) -> None:
        await self.cancel()
        self._state = "requesting_qr"
        self._message = "正在生成登录二维码…"
        self._task = asyncio.create_task(self._run(), name="xiaoheihe-qr-login")

    async def cancel(self) -> None:
        if self._task is not None:
            self._task.cancel()
            with suppress(asyncio.CancelledError):
                await self._task
            self._task = None
        self._image = ""
        self._expires_at = 0.0
        self._state = "idle"
        self._message = "点击生成二维码，使用小黑盒 App 扫码并确认。"

    async def _run(self) -> None:
        client = XiaoheiheApiClient("default")
        try:
            session = await client.request_qr()
            self._expires_at = session.expires_at
            self._image = await asyncio.to_thread(qr_image, session.qr_content)
            self._state = "waiting_scan"
            self._message = "请使用小黑盒 App 扫码。"
            while time.time() < session.expires_at:
                await asyncio.sleep(3)
                state, _, credentials = await client.check_qr(session)
                self._state = state.value
                if state is LoginState.SUCCESS:
                    if credentials is None:
                        raise RuntimeError("登录成功但没有收到账号凭证")
                    await client.check_credentials()
                    await asyncio.to_thread(save_credentials, self.directory, credentials)
                    self._image = ""
                    self._expires_at = 0.0
                    await self.on_login(credentials)
                    self._message = "登录成功，正在连接小黑盒社区。"
                    return
                if state is LoginState.SCANNED_WAITING_CONFIRM:
                    self._message = "已扫码，请在小黑盒 App 中确认登录。"
                elif state is LoginState.WAITING_SCAN:
                    self._message = "等待扫码，请使用小黑盒 App 扫码。"
                elif state in {LoginState.EXPIRED, LoginState.FAILED}:
                    break
            self._state = "expired"
            self._message = "二维码已过期或登录失败，请重新生成。"
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._state = "failed"
            self._message = f"登录失败：{redact_log_text(str(exc))}"
        finally:
            self._image = ""
            await client.close()
