"""在麦麦根目录运行：uv run python -m plugins.xiaoheihe_adapter.login --data-dir <插件数据目录>。"""

from pathlib import Path

import argparse
import asyncio
import time

from .storage import save_credentials
from .vendor.api_client import XiaoheiheApiClient
from .vendor.models import LoginState
from .vendor.security import redact_log_text


async def login(directory: Path) -> None:
    import qrcode

    client = XiaoheiheApiClient("default")
    image = directory / "login.png"
    try:
        session = await client.request_qr()
        await asyncio.to_thread(directory.mkdir, parents=True, exist_ok=True)
        await asyncio.to_thread(lambda: qrcode.make(session.qr_content).save(image))
        print(f"请用小黑盒 App 扫码并确认登录，二维码图片：{image.resolve()}")
        while time.time() < session.expires_at:
            await asyncio.sleep(3)
            state, _, credentials = await client.check_qr(session)
            if state is LoginState.SUCCESS:
                if credentials is None:
                    raise RuntimeError("登录成功但缺少凭证")
                await client.check_credentials()
                await asyncio.to_thread(save_credentials, directory, credentials)
                print(f"登录成功：{credentials.nickname}（{credentials.uid}），可在 WebUI 启用插件。")
                return
            if state in {LoginState.EXPIRED, LoginState.FAILED}:
                raise RuntimeError("二维码失效或登录失败，请重新运行扫码命令")
        raise RuntimeError("二维码已过期，请重新运行扫码命令")
    finally:
        await client.close()
        await asyncio.to_thread(image.unlink, missing_ok=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="小黑盒社区适配器扫码登录；请先停用插件再更换账号")
    parser.add_argument("--data-dir", type=Path, required=True, help="宿主授予插件的 data_dir")
    args = parser.parse_args()
    try:
        asyncio.run(login(args.data_dir.resolve()))
    except Exception as exc:
        # HTTP 错误对象可能含二维码/签名，输出诊断前清除 URL 查询和凭证。
        parser.exit(1, f"登录失败（{type(exc).__name__}）：{redact_log_text(str(exc))}\n")


if __name__ == "__main__":
    main()
