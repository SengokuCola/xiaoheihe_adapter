from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import asyncio
import base64
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from xiaoheihe_adapter import auth
from xiaoheihe_adapter.auth import LoginService
from xiaoheihe_adapter.plugin import XiaoheiheAdapter
from xiaoheihe_adapter.storage import load_credentials
from xiaoheihe_adapter.vendor.models import Credentials, LoginState, QRLoginSession


def test_page_login_saves_credentials_connects_and_clears_qr(tmp_path, monkeypatch):
    async def check():
        credentials = Credentials("default", "100", "麦麦", {"pkey": "private-test-cookie"})
        session = QRLoginSession("default", "test", "https://example.com/login?test=1", time.time() + 60)
        callback = AsyncMock()
        service = LoginService(tmp_path, callback)

        async def check_qr(_):
            snapshot = service.snapshot()
            assert snapshot["login_active"]
            image = base64.b64decode(snapshot["qr_image"].split(",")[1])
            assert image.startswith(b"\x89PNG")
            assert len(snapshot["qr_image"]) <= 24000
            return LoginState.SUCCESS, "", credentials

        client = SimpleNamespace(
            request_qr=AsyncMock(return_value=session),
            check_qr=check_qr,
            check_credentials=AsyncMock(),
            close=AsyncMock(),
        )
        monkeypatch.setattr(auth, "XiaoheiheApiClient", lambda _: client)
        monkeypatch.setattr(auth.asyncio, "sleep", AsyncMock())
        await service._run()
        assert load_credentials(tmp_path) == credentials
        callback.assert_awaited_once_with(credentials)
        assert service.snapshot()["login_state"] == "success"
        assert service.snapshot()["qr_image"] == ""
        assert "private-test-cookie" not in str(service.snapshot())
        assert not list(tmp_path.glob("*.png"))
        client.close.assert_awaited_once()

    asyncio.run(check())


def test_cancel_login_closes_pending_client_and_does_not_save(tmp_path, monkeypatch):
    async def check():
        started = asyncio.Event()

        async def request_qr():
            started.set()
            await asyncio.Event().wait()

        client = SimpleNamespace(request_qr=request_qr, close=AsyncMock())
        monkeypatch.setattr(auth, "XiaoheiheApiClient", lambda _: client)
        service = LoginService(tmp_path, AsyncMock())
        await service.start()
        await started.wait()
        await service.cancel()
        client.close.assert_awaited_once()
        assert service.snapshot()["login_state"] == "idle"
        assert not service.snapshot()["login_active"]
        assert not (tmp_path / "credentials.json").exists()

    asyncio.run(check())


def test_no_saved_account_waits_for_sidebar_login(tmp_path):
    async def check():
        plugin = XiaoheiheAdapter()
        plugin.set_plugin_config(plugin.get_default_config())
        plugin._ctx = SimpleNamespace(paths=SimpleNamespace(data_dir=tmp_path))
        await plugin.on_load()
        assert plugin._worker is None
        snapshot = await plugin.community_status()
        assert snapshot["connection"] == "等待登录"
        assert not snapshot["has_account"]
        assert snapshot["qr_image"] == ""
        assert not (tmp_path / "credentials.json").exists()
        await plugin.on_unload()

    asyncio.run(check())
