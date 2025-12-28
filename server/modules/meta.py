from typing import Optional
import time
import json
import requests
from .http import safe_post_json

from .config import LOGISTICS_API_URL, TAGS_API_URL, APP_KEY, APP_SECRET
from .sign import md5_sign


def _sign(app_key: str, app_secret: str, timestamp: str) -> str:
    # The existing meta endpoints align with md5 signature style in main.py
    return md5_sign(app_key, app_secret, timestamp)


def get_logistics(
    request_timeout_seconds: int = 15,
    app_key: Optional[str] = None,
    app_secret: Optional[str] = None,
    api_url: Optional[str] = None,
) -> Optional[dict]:
    timestamp = str(int(time.time()))
    ak = app_key or APP_KEY
    sk = app_secret or APP_SECRET
    url = api_url or LOGISTICS_API_URL
    sign = _sign(ak, sk, timestamp)
    files = {
        "app_key": (None, ak),
        "timestamp": (None, timestamp),
        "sign": (None, sign),
    }
    return safe_post_json(url, files=files, timeout=request_timeout_seconds)


def get_tags(
    request_timeout_seconds: int = 15,
    app_key: Optional[str] = None,
    app_secret: Optional[str] = None,
    api_url: Optional[str] = None,
) -> Optional[dict]:
    timestamp = str(int(time.time()))
    ak = app_key or APP_KEY
    sk = app_secret or APP_SECRET
    url = api_url or TAGS_API_URL
    sign = _sign(ak, sk, timestamp)
    files = {
        "app_key": (None, ak),
        "timestamp": (None, timestamp),
        "sign": (None, sign),
    }
    return safe_post_json(url, files=files, timeout=request_timeout_seconds)


