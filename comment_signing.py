"""网页评论写接口参数，协议参考 xiaoheihe-cli/xhh_operator/signing.py。"""

from typing import Dict, Optional

import hashlib
import hmac
import secrets
import time

from .vendor.request_signing import generate_hkey

COMMENT_PATH = "/bbs/app/comment/create"
# 网页客户端公开的版本 15 协议常量，不是账号凭证。
WORKSHOP_KEY = b"Z7mFG4tQp9Ws2LxB8H"  # gitleaks:allow


def comment_query(
    now: Optional[int] = None, nonce: Optional[str] = None, *, path: str = COMMENT_PATH,
) -> Dict[str, str]:
    stamp = int(time.time()) if now is None else now
    request_nonce = secrets.token_hex(16).upper() if nonce is None else nonce
    message = WORKSHOP_KEY + f"{request_nonce}{stamp}:{request_nonce}".encode()
    digest = hmac.new(WORKSHOP_KEY, message, hashlib.sha256).hexdigest()
    return {
        "app": "heybox",
        "os_type": "web",
        "x_app": "heybox_website",
        "x_client_type": "web",
        "x_os_type": "Windows",
        "x_client_version": "",
        "client_type": "web",
        "web_version": "3.0",
        "version": "999.0.4",
        "_time": str(stamp),
        "nonce": request_nonce,
        # 网页算法使用 time+1 生成 hkey，而 _time 与 _rnd 保留原始时间。
        "hkey": generate_hkey(path, stamp + 1, request_nonce),
        "_rnd": "15:" + digest,
    }
