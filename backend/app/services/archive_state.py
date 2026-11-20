"""归档资产登记、恢复回执与恢复清单的持久化层。

状态统一放在 ``<archive_root>/_state`` 下，与归档批次目录分开：

* ``assets.json``：每个归档文件（旧版 zlib / gzip v2）的版本、摘要和校验状态；
* ``receipts.json`` 与 ``receipts/<id>.json``：每次分区恢复的实际文件与结果；
* ``manifests/<batch_id>.json``：按批次回写恢复清单；
* ``lake_ledger.json``：已恢复事件的全局索引，用于上线/回滚重复恢复时去重。

所有写入都是“先写临时文件再 rename”，进程内用锁串行化。
"""
from __future__ import annotations

import json
import os
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from app.paths import archive_root

_LOCK = threading.RLock()
_STATE_SCHEMA_VERSION = 1


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _state_dir() -> Path:
    path = archive_root() / "_state"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _receipt_dir() -> Path:
    path = _state_dir() / "receipts"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _manifest_dir() -> Path:
    path = _state_dir() / "manifests"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _read_json(path: Path, default: dict[str, Any]) -> dict[str, Any]:
    if not path.exists():
        return dict(default)
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            return data
    except (OSError, json.JSONDecodeError):
        pass
    return dict(default)


def _write_json(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2, default=str)
    os.replace(tmp, path)


def _assets_path() -> Path:
    return _state_dir() / "assets.json"


def _receipts_path() -> Path:
    return _state_dir() / "receipts.json"


def _ledger_path() -> Path:
    return _state_dir() / "lake_ledger.json"


def _load_assets() -> dict[str, Any]:
    data = _read_json(_assets_path(), {"schema_version": _STATE_SCHEMA_VERSION, "assets": []})
    if not isinstance(data.get("assets"), list):
        data["assets"] = []
    data.setdefault("schema_version", _STATE_SCHEMA_VERSION)
    return data


def list_assets(batch_id: str | None = None, partition: str | None = None,
                include_history: bool = True) -> list[dict[str, Any]]:
    with _LOCK:
        assets = list(_load_assets()["assets"])
    if batch_id:
        assets = [a for a in assets if a.get("batch_id") == batch_id]
    if partition:
        assets = [a for a in assets if a.get("partition") == partition]
    if not include_history:
        latest: dict[tuple[str, str, str], dict[str, Any]] = {}
        for asset in assets:
            key = (str(asset.get("batch_id")), str(asset.get("partition")), str(asset.get("format")))
            current = latest.get(key)
            if current is None or int(asset.get("version", 0)) > int(current.get("version", 0)):
                latest[key] = asset
        assets = list(latest.values())
    return sorted(
        assets,
        key=lambda a: (
            str(a.get("batch_id")), str(a.get("partition")), str(a.get("format")),
            int(a.get("version", 0)),
        ),
    )


def get_asset(asset_id: str) -> dict[str, Any] | None:
    for asset in list_assets():
        if asset.get("asset_id") == asset_id:
            return asset
    return None


def next_version(batch_id: str, partition: str, archive_date: str, fmt: str) -> int:
    versions = [
        int(a.get("version", 0))
        for a in list_assets(batch_id=batch_id, partition=partition)
        if a.get("format") == fmt and a.get("archive_date") == archive_date
    ]
    return (max(versions) if versions else 0) + 1


def upsert_asset(asset: dict[str, Any]) -> dict[str, Any]:
    """按 ``asset_id`` 插入或更新资产，返回持久化后的副本。"""
    with _LOCK:
        data = _load_assets()
        assets: list[dict[str, Any]] = data["assets"]
        record = dict(asset)
        record.setdefault("asset_id", "AST-" + uuid.uuid4().hex[:16])
        record.setdefault("created_at", now_iso())
        record["updated_at"] = now_iso()
        for index, existing in enumerate(assets):
            if existing.get("asset_id") == record["asset_id"]:
                merged = {**existing, **record}
                assets[index] = merged
                _write_json(_assets_path(), data)
                return dict(merged)
        assets.append(record)
        _write_json(_assets_path(), data)
        return dict(record)


def register_asset(asset: dict[str, Any]) -> dict[str, Any]:
    asset = dict(asset)
    asset.setdefault("version", 1)
    asset.setdefault("status", "unverified")
    return upsert_asset(asset)


def _load_receipts() -> dict[str, Any]:
    data = _read_json(_receipts_path(), {"schema_version": _STATE_SCHEMA_VERSION, "receipts": []})
    if not isinstance(data.get("receipts"), list):
        data["receipts"] = []
    return data


def add_receipt(receipt: dict[str, Any]) -> dict[str, Any]:
    """持久化一条分区回执，同时写单文件便于回滚后按 ID 查找。"""
    record = dict(receipt)
    record.setdefault("receipt_id", "RCPT-" + uuid.uuid4().hex[:16])
    record.setdefault("created_at", now_iso())
    with _LOCK:
        data = _load_receipts()
        data["receipts"].append(record)
        _write_json(_receipts_path(), data)
        _write_json(_receipt_dir() / f"{record['receipt_id']}.json", record)
    return dict(record)


def list_receipts(batch_id: str | None = None, partition: str | None = None,
                  limit: int = 100) -> list[dict[str, Any]]:
    with _LOCK:
        receipts = list(_load_receipts()["receipts"])
    if batch_id:
        receipts = [r for r in receipts if r.get("batch_id") == batch_id]
    if partition:
        receipts = [r for r in receipts if r.get("partition") == partition]
    receipts.sort(key=lambda r: str(r.get("created_at", "")), reverse=True)
    return receipts[:limit]


def get_receipt(receipt_id: str) -> dict[str, Any] | None:
    path = _receipt_dir() / f"{receipt_id}.json"
    if path.exists():
        data = _read_json(path, {})
        if data:
            return data
    for receipt in list_receipts(limit=1_000_000):
        if receipt.get("receipt_id") == receipt_id:
            return receipt
    return None


def manifest_path(batch_id: str) -> Path:
    return _manifest_dir() / f"{batch_id}.json"


def load_manifest(batch_id: str) -> dict[str, Any]:
    data = _read_json(
        manifest_path(batch_id),
        {
            "schema_version": _STATE_SCHEMA_VERSION,
            "batch_id": batch_id,
            "created_at": now_iso(),
            "updated_at": now_iso(),
            "restore_runs": [],
            "partitions": {},
        },
    )
    data.setdefault("batch_id", batch_id)
    data.setdefault("restore_runs", [])
    data.setdefault("partitions", {})
    return data


def save_manifest(batch_id: str, manifest: dict[str, Any]) -> Path:
    path = manifest_path(batch_id)
    manifest = dict(manifest)
    manifest["batch_id"] = batch_id
    manifest["schema_version"] = _STATE_SCHEMA_VERSION
    manifest["updated_at"] = now_iso()
    with _LOCK:
        _write_json(path, manifest)
    return path


def _load_ledger() -> dict[str, Any]:
    data = _read_json(_ledger_path(), {"schema_version": _STATE_SCHEMA_VERSION, "entries": {}})
    if not isinstance(data.get("entries"), dict):
        data["entries"] = {}
    return data


def ledger_get(partition: str, event_id: str) -> dict[str, Any] | None:
    with _LOCK:
        entries = _load_ledger()["entries"]
    entry = entries.get(f"{partition}|{event_id}")
    return dict(entry) if isinstance(entry, dict) else None


def ledger_all(partition: str | None = None) -> dict[str, dict[str, Any]]:
    with _LOCK:
        entries = _load_ledger()["entries"]
    result: dict[str, dict[str, Any]] = {}
    for key, entry in entries.items():
        if not isinstance(entry, dict):
            continue
        if partition and not key.startswith(f"{partition}|"):
            continue
        result[key] = dict(entry)
    return result


def ledger_put_many(entries: dict[str, dict[str, Any]]) -> None:
    if not entries:
        return
    with _LOCK:
        data = _load_ledger()
        data["entries"].update(entries)
        _write_json(_ledger_path(), data)
