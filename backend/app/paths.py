"""专用数据目录解析：统一数据湖、归档和持久化层的落盘位置。

默认仍然是 ``backend/data_store``，但允许通过 ``APP_DATA_STORE_DIR``
（或 ``DATA_STORE_DIR``）指定一个独立的运行目录。测试和部署演练可以把
归档、数据湖和 JSON 仓库一起放到临时目录里，避免互相污染。
"""
from __future__ import annotations

import os
from pathlib import Path


def data_store_root() -> Path:
    """返回数据落盘根目录。"""
    configured = os.environ.get("APP_DATA_STORE_DIR") or os.environ.get("DATA_STORE_DIR")
    if configured:
        return Path(configured).expanduser().resolve()
    return Path(__file__).resolve().parent.parent / "data_store"


def archive_root() -> Path:
    return data_store_root() / "archive"


def lake_root() -> Path:
    return data_store_root() / "lake"
