"""归档迁移与恢复接口：迁移、校验、恢复入口、回执和恢复清单查询。"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from app.services.archive_migration import ArchiveMigrationService
from app.services.archive_state import list_assets
from app.services.archive_v2 import ArchiveError

router = APIRouter(prefix="/api/archive", tags=["归档迁移与恢复"])


class MigrateRequest(BaseModel):
    partitions: list[str] = Field(default_factory=list)
    verify: bool = True
    force: bool = False
    requester: str = ""


class VerifyRequest(BaseModel):
    partitions: list[str] = Field(default_factory=list)


class RestoreRequest(BaseModel):
    batch_id: str
    partitions: list[str] = Field(default_factory=list)
    requester: str = ""
    mode: str = "auto"


def _service() -> ArchiveMigrationService:
    return ArchiveMigrationService()


def _guard(action):
    try:
        return action()
    except ArchiveError as exc:
        status = 404 if exc.reason == "batch_not_found" else 400
        raise HTTPException(
            status_code=status,
            detail={"reason": exc.reason, "message": exc.message, **exc.detail},
        ) from exc


@router.get("/batches")
def list_archive_batches() -> dict[str, Any]:
    """列出归档批次、分区和格式。"""
    service = _service()
    return {"ok": True, "batches": service.list_batches()}


@router.post("/restore")
def restore_partitions(payload: RestoreRequest) -> dict[str, Any]:
    """恢复入口：按分区恢复归档数据，返回每个分区的回执。"""
    service = _service()
    result = _guard(lambda: service.restore(
        payload.batch_id, payload.partitions,
        requester=payload.requester, mode=payload.mode,
    ))
    return result


@router.get("/receipts")
def list_receipts(
    batch_id: str | None = None,
    partition: str | None = None,
    limit: int = Query(default=100, ge=1, le=1000),
) -> dict[str, Any]:
    service = _service()
    receipts = service.list_receipts(batch_id=batch_id, partition=partition, limit=limit)
    return {"ok": True, "items": receipts, "total": len(receipts)}


@router.get("/receipts/{receipt_id}")
def get_receipt(receipt_id: str) -> dict[str, Any]:
    receipt = _service().get_receipt(receipt_id)
    if receipt is None:
        raise HTTPException(status_code=404, detail=f"回执 {receipt_id} 不存在")
    return {"ok": True, "receipt": receipt}


@router.get("/receipts/{receipt_id}/rows")
def get_receipt_rows(receipt_id: str) -> dict[str, Any]:
    result = _service().receipt_rows(receipt_id)
    if not result["ok"]:
        raise HTTPException(status_code=404, detail=result["message"])
    return result


@router.get("/receipts/{receipt_id}/legacy")
def get_receipt_legacy(receipt_id: str) -> dict[str, Any]:
    """旧版恢复环境视图：按回执 ID 返回原压缩包名和数据文本。"""
    result = _service().legacy_receipt_view(receipt_id)
    if not result["ok"]:
        raise HTTPException(status_code=404, detail=result["message"])
    return result


@router.get("/source-row")
def get_source_row_receipt(
    partition: str, event_id: str,
) -> dict[str, Any]:
    result = _service().source_row_receipt(partition, event_id)
    if not result["ok"]:
        raise HTTPException(status_code=404, detail=result["message"])
    return result


@router.get("/lake")
def query_lake(
    partition: str,
    date: str | None = None,
    receipt_id: str | None = None,
    limit: int = Query(default=200, ge=1, le=1000),
) -> dict[str, Any]:
    # 直接调用（脚本/测试）时 limit 可能是 Query 默认对象，这里兜底为整数。
    size = limit if isinstance(limit, int) else 200
    return _service().query_lake(partition, date=date, receipt_id=receipt_id, limit=size)


@router.get("/assets")
def list_archive_assets(
    batch_id: str | None = None,
    partition: str | None = None,
    include_history: bool = True,
) -> dict[str, Any]:
    assets = list_assets(batch_id=batch_id, partition=partition, include_history=include_history)
    return {"ok": True, "items": assets, "total": len(assets)}


@router.post("/{batch_id}/migrate")
def migrate_batch(batch_id: str, payload: MigrateRequest) -> dict[str, Any]:
    """把旧版 zlib 分区迁移为带你分区摘要的 gzip v2，可重复重试。"""
    service = _service()
    result = _guard(lambda: service.migrate_batch(
        batch_id, payload.partitions or None,
        verify=payload.verify, force=payload.force, requester=payload.requester,
    ))
    return result


@router.post("/{batch_id}/verify")
def verify_batch(batch_id: str, payload: VerifyRequest) -> dict[str, Any]:
    service = _service()
    return _guard(lambda: service.verify_batch(batch_id, payload.partitions or None))


@router.get("/{batch_id}/inspect")
def inspect_archive(batch_id: str, partition: str) -> dict[str, Any]:
    """检查持久化 gzip v2 的分区、日期、事件身份与摘要。"""
    service = _service()
    return _guard(lambda: service.inspect_archive(batch_id, partition))


@router.get("/{batch_id}/manifest")
def get_manifest(batch_id: str) -> dict[str, Any]:
    service = _service()
    manifest = _guard(lambda: service.get_manifest(batch_id))
    return {"ok": True, "manifest": manifest}


@router.get("/{batch_id}/assets")
def get_batch_assets(batch_id: str) -> dict[str, Any]:
    service = _service()
    batch_dir = _guard(lambda: service._batch_dir(batch_id))
    assets = list_assets(batch_id=batch_dir.name)
    return {"ok": True, "items": assets, "total": len(assets)}
