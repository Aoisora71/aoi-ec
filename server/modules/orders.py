from typing import List, Optional, Dict, Any
import time
import os
import requests
from .http import safe_post_json

from .config import (
    APP_KEY,
    APP_SECRET,
    CREATE_ORDER_API_URL,
    UPDATE_ORDER_STATUS_API_URL,
    CANCEL_ORDER_API_URL,
    ORDER_LIST_API_URL,
    ORDER_DETAIL_API_URL,
    STOCK_LIST_API_URL,
    CREATE_PORDER_API_URL,
    UPDATE_PORDER_STATUS_API_URL,
    CANCEL_PORDER_API_URL,
    PORDER_LIST_API_URL,
    PORDER_DETAIL_API_URL,
    LOGISTICS_TRACK_API_URL,
)
from .sign import md5_sign


def generate_sign(app_key: str, app_secret: str, timestamp: str) -> str:
    return md5_sign(app_key, app_secret, timestamp)


def create_order(
    purchase_order: str,
    status: str,
    goods: List[dict],
    logistics_id: Optional[str] = None,
    remark: Optional[str] = None,
    request_timeout_seconds: int = 15,
    app_key: Optional[str] = None,
    app_secret: Optional[str] = None,
    api_url: Optional[str] = None,
) -> Optional[dict]:
    timestamp = str(int(time.time()))
    resolved_app_key = app_key or APP_KEY
    resolved_app_secret = app_secret or APP_SECRET
    resolved_api_url = api_url or CREATE_ORDER_API_URL
    sign = generate_sign(resolved_app_key, resolved_app_secret, timestamp)

    files: Dict[str, Any] = {
        "app_key": (None, resolved_app_key),
        "timestamp": (None, timestamp),
        "sign": (None, sign),
        "purchase_order": (None, purchase_order),
        "status": (None, status),
    }

    if logistics_id is not None:
        files["logistics_id"] = (None, logistics_id)
    if remark is not None:
        files["remark"] = (None, remark)

    for i, good in enumerate(goods):
        prefix = f"goods[{i}]"
        files[f"{prefix}[link]"] = (None, good["link"])  # required
        files[f"{prefix}[price]"] = (None, str(good["price"]))
        files[f"{prefix}[num]"] = (None, str(good["num"]))
        if "pic" in good:
            files[f"{prefix}[pic]"] = (None, good["pic"]) 
        if "remark" in good:
            files[f"{prefix}[remark]"] = (None, good["remark"]) 
        if "fba" in good:
            files[f"{prefix}[fba]"] = (None, good["fba"]) 
        if "asin" in good:
            files[f"{prefix}[asin]"] = (None, good["asin"]) 
        if "props" in good:
            for j, prop in enumerate(good["props"]):
                files[f"{prefix}[props][{j}][key]"] = (None, prop["key"]) 
                files[f"{prefix}[props][{j}][value]"] = (None, prop["value"]) 
        if "option" in good:
            for j, opt in enumerate(good["option"]):
                files[f"{prefix}[option][{j}][name]"] = (None, opt["name"]) 
                files[f"{prefix}[option][{j}][num]"] = (None, str(opt["num"])) 
        if "tags" in good:
            for j, tag in enumerate(good["tags"]):
                files[f"{prefix}[tags][{j}][type]"] = (None, tag["type"]) 
                files[f"{prefix}[tags][{j}][no]"] = (None, tag["no"]) 
                if "goods_no" in tag:
                    files[f"{prefix}[tags][{j}][goods_no]"] = (None, tag["goods_no"]) 

    data = safe_post_json(resolved_api_url, files=files, timeout=request_timeout_seconds)
    if data is None:
        return None

    if not data.get("success", False):
        print(" Create order API failed:", data)
        return None
    return data.get("data")


def update_order_status(
    order_sn: str,
    status: str,
    request_timeout_seconds: int = 15,
    app_key: Optional[str] = None,
    app_secret: Optional[str] = None,
    api_url: Optional[str] = None,
) -> Optional[dict]:
    timestamp = str(int(time.time()))
    resolved_app_key = app_key or APP_KEY
    resolved_app_secret = app_secret or APP_SECRET
    resolved_api_url = api_url or UPDATE_ORDER_STATUS_API_URL
    sign = generate_sign(resolved_app_key, resolved_app_secret, timestamp)
    files = {
        "app_key": (None, resolved_app_key),
        "timestamp": (None, timestamp),
        "sign": (None, sign),
        "order_sn": (None, order_sn),
        "status": (None, status),
    }
    return safe_post_json(resolved_api_url, files=files, timeout=request_timeout_seconds)


def cancel_order(
    order_sn: str,
    request_timeout_seconds: int = 15,
    app_key: Optional[str] = None,
    app_secret: Optional[str] = None,
    api_url: Optional[str] = None,
) -> Optional[dict]:
    timestamp = str(int(time.time()))
    resolved_app_key = app_key or APP_KEY
    resolved_app_secret = app_secret or APP_SECRET
    resolved_api_url = api_url or CANCEL_ORDER_API_URL
    sign = generate_sign(resolved_app_key, resolved_app_secret, timestamp)
    files = {
        "app_key": (None, resolved_app_key),
        "timestamp": (None, timestamp),
        "sign": (None, sign),
        "order_sn": (None, order_sn),
    }
    return safe_post_json(resolved_api_url, files=files, timeout=request_timeout_seconds)


def get_order_list(
    page: int = 1,
    page_size: int = 10,
    request_timeout_seconds: int = 15,
    app_key: Optional[str] = None,
    app_secret: Optional[str] = None,
    api_url: Optional[str] = None,
) -> Optional[dict]:
    timestamp = str(int(time.time()))
    resolved_app_key = app_key or APP_KEY
    resolved_app_secret = app_secret or APP_SECRET
    resolved_api_url = api_url or ORDER_LIST_API_URL
    sign = generate_sign(resolved_app_key, resolved_app_secret, timestamp)
    files = {
        "app_key": (None, resolved_app_key),
        "timestamp": (None, timestamp),
        "sign": (None, sign),
        "page": (None, str(page)),
        "pageSize": (None, str(page_size)),
    }
    return safe_post_json(resolved_api_url, files=files, timeout=request_timeout_seconds)


def get_order_detail(
    order_sn: str,
    request_timeout_seconds: int = 15,
    app_key: Optional[str] = None,
    app_secret: Optional[str] = None,
    api_url: Optional[str] = None,
) -> Optional[dict]:
    timestamp = str(int(time.time()))
    resolved_app_key = app_key or APP_KEY
    resolved_app_secret = app_secret or APP_SECRET
    resolved_api_url = api_url or ORDER_DETAIL_API_URL
    sign = generate_sign(resolved_app_key, resolved_app_secret, timestamp)
    files = {
        "app_key": (None, resolved_app_key),
        "timestamp": (None, timestamp),
        "sign": (None, sign),
        "order_sn": (None, order_sn),
    }
    return safe_post_json(resolved_api_url, files=files, timeout=request_timeout_seconds)


def get_stock_list(
    request_timeout_seconds: int = 15,
    app_key: Optional[str] = None,
    app_secret: Optional[str] = None,
    api_url: Optional[str] = None,
) -> Optional[dict]:
    timestamp = str(int(time.time()))
    resolved_app_key = app_key or APP_KEY
    resolved_app_secret = app_secret or APP_SECRET
    resolved_api_url = api_url or STOCK_LIST_API_URL
    sign = generate_sign(resolved_app_key, resolved_app_secret, timestamp)
    files = {
        "app_key": (None, resolved_app_key),
        "timestamp": (None, timestamp),
        "sign": (None, sign),
    }
    return safe_post_json(resolved_api_url, files=files, timeout=request_timeout_seconds)


def create_porder(
    status: str,
    logistics_id: str,
    porder_detail: List[dict],
    client_remark: Optional[str] = None,
    receiver_address: Optional[dict] = None,
    importer_address: Optional[dict] = None,
    porder_file: Optional[List[dict]] = None,
    request_timeout_seconds: int = 15,
    app_key: Optional[str] = None,
    app_secret: Optional[str] = None,
    api_url: Optional[str] = None,
) -> Optional[dict]:
    timestamp = str(int(time.time()))
    resolved_app_key = app_key or APP_KEY
    resolved_app_secret = app_secret or APP_SECRET
    resolved_api_url = api_url or CREATE_PORDER_API_URL
    sign = generate_sign(resolved_app_key, resolved_app_secret, timestamp)
    files: Dict[str, Any] = {
        "app_key": (None, resolved_app_key),
        "timestamp": (None, timestamp),
        "sign": (None, sign),
        "status": (None, status),
        "logistics_id": (None, logistics_id),
    }
    if client_remark:
        files["client_remark"] = (None, client_remark)
    for i, item in enumerate(porder_detail):
        prefix = f"porder_detail[{i}]"
        files[f"{prefix}[order_sn]"] = (None, item["order_sn"]) 
        if "sorting" in item:
            files[f"{prefix}[sorting]"] = (None, str(item["sorting"])) 
        files[f"{prefix}[num]"] = (None, str(item["num"])) 
        if "client_remark" in item:
            files[f"{prefix}[client_remark]"] = (None, item["client_remark"]) 
        if "porder_detail_tag" in item:
            for j, tag in enumerate(item["porder_detail_tag"]):
                tprefix = f"{prefix}[porder_detail_tag][{j}]"
                for key in ["type", "no", "goods_no", "text_line_one", "text_line_two"]:
                    if key in tag:
                        files[f"{tprefix}[{key}]"] = (None, str(tag[key])) 
    if isinstance(receiver_address, dict):
        for key, value in receiver_address.items():
            files[f"receiver_address[{key}]"] = (None, str(value)) 
    if isinstance(importer_address, dict):
        for key, value in importer_address.items():
            files[f"importer_address[{key}]"] = (None, str(value)) 
    if porder_file:
        for i, pf in enumerate(porder_file):
            fprefix = f"porder_file[{i}]"
            if "name" in pf:
                files[f"{fprefix}[name]"] = (None, pf["name"]) 
            if "file" in pf and pf["file"]:
                try:
                    files[f"{fprefix}[file]"] = (os.path.basename(pf["file"]), open(pf["file"], "rb"))
                except Exception:
                    files[f"{fprefix}[file]"] = (None, pf["file"]) 
    try:
        data = safe_post_json(resolved_api_url, files=files, timeout=request_timeout_seconds)
    finally:
        for key, val in list(files.items()):
            if isinstance(val, tuple) and len(val) == 2 and hasattr(val[1], "close"):
                try:
                    val[1].close()
                except Exception:
                    pass
    return data


def update_porder_status(
    porder_sn: str,
    status: str,
    request_timeout_seconds: int = 15,
    app_key: Optional[str] = None,
    app_secret: Optional[str] = None,
    api_url: Optional[str] = None,
) -> Optional[dict]:
    timestamp = str(int(time.time()))
    resolved_app_key = app_key or APP_KEY
    resolved_app_secret = app_secret or APP_SECRET
    resolved_api_url = api_url or UPDATE_PORDER_STATUS_API_URL
    sign = generate_sign(resolved_app_key, resolved_app_secret, timestamp)
    files = {
        "app_key": (None, resolved_app_key),
        "timestamp": (None, timestamp),
        "sign": (None, sign),
        "porder_sn": (None, porder_sn),
        "status": (None, status),
    }
    return safe_post_json(resolved_api_url, files=files, timeout=request_timeout_seconds)


def cancel_porder(
    porder_sn: str,
    request_timeout_seconds: int = 15,
    app_key: Optional[str] = None,
    app_secret: Optional[str] = None,
    api_url: Optional[str] = None,
) -> Optional[dict]:
    timestamp = str(int(time.time()))
    resolved_app_key = app_key or APP_KEY
    resolved_app_secret = app_secret or APP_SECRET
    resolved_api_url = api_url or CANCEL_PORDER_API_URL
    sign = generate_sign(resolved_app_key, resolved_app_secret, timestamp)
    files = {
        "app_key": (None, resolved_app_key),
        "timestamp": (None, timestamp),
        "sign": (None, sign),
        "porder_sn": (None, porder_sn),
    }
    return safe_post_json(resolved_api_url, files=files, timeout=request_timeout_seconds)


def get_porder_list(
    page: int = 1,
    page_size: int = 10,
    porder_sn: Optional[str] = None,
    request_timeout_seconds: int = 15,
    app_key: Optional[str] = None,
    app_secret: Optional[str] = None,
    api_url: Optional[str] = None,
) -> Optional[dict]:
    timestamp = str(int(time.time()))
    resolved_app_key = app_key or APP_KEY
    resolved_app_secret = app_secret or APP_SECRET
    resolved_api_url = api_url or PORDER_LIST_API_URL
    sign = generate_sign(resolved_app_key, resolved_app_secret, timestamp)
    files = {
        "app_key": (None, resolved_app_key),
        "timestamp": (None, timestamp),
        "sign": (None, sign),
        "page": (None, str(page)),
        "pageSize": (None, str(page_size)),
    }
    if porder_sn:
        files["porder_sn"] = (None, porder_sn)
    return safe_post_json(resolved_api_url, files=files, timeout=request_timeout_seconds)


def get_porder_detail(
    porder_sn: str,
    request_timeout_seconds: int = 15,
    app_key: Optional[str] = None,
    app_secret: Optional[str] = None,
    api_url: Optional[str] = None,
) -> Optional[dict]:
    timestamp = str(int(time.time()))
    resolved_app_key = app_key or APP_KEY
    resolved_app_secret = app_secret or APP_SECRET
    resolved_api_url = api_url or PORDER_DETAIL_API_URL
    sign = generate_sign(resolved_app_key, resolved_app_secret, timestamp)
    files = {
        "app_key": (None, resolved_app_key),
        "timestamp": (None, timestamp),
        "sign": (None, sign),
        "porder_sn": (None, porder_sn),
    }
    return safe_post_json(resolved_api_url, files=files, timeout=request_timeout_seconds)


def get_logistics_track(
    express_no: str,
    request_timeout_seconds: int = 15,
    app_key: Optional[str] = None,
    app_secret: Optional[str] = None,
    api_url: Optional[str] = None,
) -> Optional[dict]:
    timestamp = str(int(time.time()))
    resolved_app_key = app_key or APP_KEY
    resolved_app_secret = app_secret or APP_SECRET
    resolved_api_url = api_url or LOGISTICS_TRACK_API_URL
    sign = generate_sign(resolved_app_key, resolved_app_secret, timestamp)
    files = {
        "app_key": (None, resolved_app_key),
        "timestamp": (None, timestamp),
        "sign": (None, sign),
        "express_no": (None, express_no),
    }
    return safe_post_json(resolved_api_url, files=files, timeout=request_timeout_seconds)


