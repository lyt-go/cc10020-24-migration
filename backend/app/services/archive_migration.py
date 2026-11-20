"""归档压缩格式迁移与分区恢复服务。

这条链路覆盖归档升级部署的完整过程：

1. 扫描归档批次里的旧版 ``.zlib`` / ``.json`` 分区文件；
2. 迁移为带分区摘要的 gzip v2 文件，并登记资产版本（迁移本身可重试）；
3. 独立校验 gzip v2 的结构和摘要，只有校验通过的资产才允许被恢复；
4. 恢复入口按分区选择“已校验 gzip v2 → 可读旧版压缩”的读取路径，
   把回执（实际使用的文件、版本、摘要、事件身份）持久化；
5. 恢复数据写入数据湖的原分区，按事件身份去重，保证上线/回滚重复执行
   不会多灌同一条源记录；
6. 每次恢复结果回写批次恢复清单，部署过程中即可确认恢复了哪些源记录。

服务本身不缓存状态：资产登记、回执、清单和湖内索引都落在
``<archive_root>/_state``，因此进程重启或回滚到旧版恢复环境后仍可回查。
"""
from __future__ import annotations

import os
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from app.paths import archive_root
from app.services import archive_state
from app.services.archive_v2 import (
    GZIP_FORMAT,
    GZIP_FORMAT_VERSION,
    LEGACY_ZLIB_FORMAT,
    ArchiveError,
    archive_suffix_format,
    build_gzip_payload,
    canonical_partition,
    compute_partition_digest,
    date_to_lake_partition,
    digest_label,
    event_identity,
    file_sha256,
    is_state_file,
    normalize_archive_date,
    parse_date,
    partition_from_path,
    read_gzip_v2,
    read_legacy_archive,
    safe_name,
    sha256_hex,
    unique_event_ids,
    verify_gzip_payload,
    write_gzip_v2,
)
from app.services.base import BaseModuleService

_RUN_LOCK = threading.RLock()

GZIP_USABLE_STATUS = "verified"
LEGACY_OK_STATUS = "legacy_readable"


class ArchiveMigrationService(BaseModuleService):
    """归档迁移、校验、恢复与恢复清单服务。"""

    MODULE = "event_archive"
    ENTITY = "归档策略"
    OPERATION_TERM = "归档"
    STATUS_ORDER = ["草稿", "待校验", "已生效", "已停用"]

    def __init__(self) -> None:
        super().__init__()
        self._history: list[dict[str, Any]] = []

    # ──────────────────────────────────────────────
    # 批次与文件发现
    # ──────────────────────────────────────────────
    def _root(self) -> Path:
        root = archive_root()
        root.mkdir(parents=True, exist_ok=True)
        return root

    @staticmethod
    def _normalize_batch_id(value: str) -> str:
        text = str(value or "").strip().strip("/\\")
        for prefix in ("archives_", "archive_", "batch_", "arch_"):
            if text.casefold().startswith(prefix):
                text = text[len(prefix):]
                break
        parsed = parse_date(text)
        if parsed:
            return parsed.replace("-", "")
        return safe_name(text, "batch").casefold()

    def list_batches(self) -> list[dict[str, Any]]:
        """列出所有归档批次及其分区/文件概况。"""
        root = self._root()
        batches: list[dict[str, Any]] = []
        for child in sorted(root.iterdir()):
            if not child.is_dir() or child.name.startswith(("_", ".")):
                continue
            files = self._scan_files(child)
            partitions: dict[str, int] = {}
            for item in files:
                partitions[item["partition"]] = partitions.get(item["partition"], 0) + 1
            batches.append({
                "batch_id": child.name,
                "path": str(child),
                "archive_date": self._batch_date(child.name, child),
                "file_count": len(files),
                "partitions": sorted(partitions),
                "formats": sorted({item["format"] for item in files}),
            })
        return batches

    def _batch_dir(self, batch_id: str) -> Path:
        root = self._root()
        wanted = self._normalize_batch_id(batch_id)
        for child in sorted(root.iterdir()):
            if not child.is_dir() or child.name.startswith(("_", ".")):
                continue
            if child.name == batch_id or self._normalize_batch_id(child.name) == wanted:
                return child
        raise ArchiveError("batch_not_found", f"归档批次 {batch_id} 不存在")

    def _batch_date(self, batch_name: str, batch_dir: Path | None = None) -> str:
        for candidate in (batch_name, batch_dir.name if batch_dir else ""):
            parsed = parse_date(candidate)
            if parsed:
                return parsed
        return datetime.now(timezone.utc).date().isoformat()

    def _scan_files(self, batch_dir: Path, batch_id: str | None = None) -> list[dict[str, Any]]:
        """扫描批次内的归档文件，推断实际分区、格式和日期。"""
        batch_id = batch_id or batch_dir.name
        batch_date = self._batch_date(batch_id, batch_dir)
        files: list[dict[str, Any]] = []
        for root, dirs, names in os.walk(batch_dir):
            dirs[:] = sorted(d for d in dirs if not d.startswith(("_", ".")))
            for name in sorted(names):
                if name.startswith(".") or name.endswith(".tmp") or is_state_file(name):
                    continue
                fmt = archive_suffix_format(name)
                if fmt is None:
                    continue
                path = Path(root) / name
                rel = path.relative_to(batch_dir).as_posix()
                partition, date = partition_from_path(rel, fallback_date=batch_date)
                files.append({
                    "file": name,
                    "relative_path": rel,
                    "path": str(path.resolve()),
                    "format": fmt,
                    "partition": partition,
                    "archive_date": date or batch_date,
                    "size_bytes": path.stat().st_size if path.exists() else 0,
                    "batch_id": batch_id,
                })
        return files

    # ──────────────────────────────────────────────
    # 资产登记
    # ──────────────────────────────────────────────
    @staticmethod
    def _asset_id(batch_id: str, partition: str, fmt: str, version: int) -> str:
        material = f"{batch_id}|{partition}|{fmt}|v{version}"
        return "AST-" + sha256_hex(material)[:20]

    def _asset_path(self, asset: dict[str, Any]) -> Path:
        path = Path(str(asset.get("path") or ""))
        if path.exists():
            return path
        rel = asset.get("relative_path")
        batch_id = str(asset.get("batch_id") or "")
        if rel and batch_id:
            candidate = archive_root() / batch_id / str(rel)
            if candidate.exists():
                return candidate
        return path

    def _same_file_asset(self, file_info: dict[str, Any], sha: str) -> dict[str, Any] | None:
        """同批次、同分区、同文件名且同摘要 → 复用已有资产（保持版本稳定）。"""
        wanted = digest_label(sha) if sha else ""
        for asset in archive_state.list_assets(
            batch_id=file_info["batch_id"], partition=file_info["partition"],
        ):
            if (
                asset.get("file") == file_info["file"]
                and wanted
                and asset.get("compressed_sha256") == wanted
            ):
                return asset
        return None

    def _register_file_asset(self, file_info: dict[str, Any]) -> dict[str, Any]:
        """登记（或复用）一个归档文件资产，保留历史版本。"""
        path = Path(file_info["path"])
        try:
            sha = file_sha256(path)
        except OSError:
            sha = ""
        existing = self._same_file_asset(file_info, sha) if sha else None
        if existing is not None:
            return existing

        fmt = file_info["format"]
        asset_format = GZIP_FORMAT if fmt == "gzip" else (
            LEGACY_ZLIB_FORMAT if fmt == LEGACY_ZLIB_FORMAT else "json"
        )
        version = archive_state.next_version(
            file_info["batch_id"], file_info["partition"], file_info["archive_date"], asset_format,
        )
        asset: dict[str, Any] = {
            "asset_id": self._asset_id(file_info["batch_id"], file_info["partition"], asset_format, version),
            "batch_id": file_info["batch_id"],
            "partition": file_info["partition"],
            "archive_date": file_info["archive_date"],
            "format": asset_format,
            "format_version": GZIP_FORMAT_VERSION if fmt == "gzip" else 1,
            "file": file_info["file"],
            "relative_path": file_info["relative_path"],
            "path": file_info["path"],
            "size_bytes": file_info["size_bytes"],
            "version": version,
            "compressed_sha256": digest_label(sha) if sha else "",
            "payload_sha256": "",
            "event_ids": [],
            "record_count": 0,
            "status": "discovered",
            "source": "filesystem",
        }

        if fmt == "gzip":
            try:
                info = read_gzip_v2(path)
                payload = info["payload"]
                asset["payload_sha256"] = digest_label(info["payload_sha256"])
                asset["event_ids"] = unique_event_ids(
                    [e for e in payload.get("events", []) if isinstance(e, dict)]
                )
                asset["record_count"] = len(payload.get("events", []) or [])
                asset["partition_digest"] = payload.get("partition_digest")
                asset["status"] = "discovered"  # 文件存在不等于校验通过
            except ArchiveError as exc:
                asset["status"] = "corrupt"
                asset["error"] = exc.message
        else:
            try:
                info = read_legacy_archive(path)
                asset["status"] = LEGACY_OK_STATUS
                asset["payload_sha256"] = info["payload_sha256"]
                asset["event_ids"] = info["event_ids"]
                asset["record_count"] = info["record_count"]
            except ArchiveError as exc:
                asset["status"] = "legacy_corrupt"
                asset["error"] = exc.message

        return archive_state.register_asset(asset)

    def sync_assets(self, batch_id: str) -> list[dict[str, Any]]:
        """把批次里的现有文件登记为资产（幂等，不改变已校验状态）。"""
        batch_dir = self._batch_dir(batch_id)
        self._sync_batch_files(batch_dir, batch_dir.name)
        return archive_state.list_assets(batch_id=batch_dir.name)

    def _sync_batch_files(self, batch_dir: Path, batch_id: str) -> None:
        for file_info in self._scan_files(batch_dir, batch_id):
            expected_format = GZIP_FORMAT if file_info["format"] == "gzip" else file_info["format"]
            known = any(
                a.get("file") == file_info["file"] and
                (a.get("format") == expected_format or expected_format == LEGACY_ZLIB_FORMAT)
                for a in archive_state.list_assets(
                    batch_id=batch_id, partition=file_info["partition"],
                )
            )
            if not known:
                self._register_file_asset(file_info)

    # ──────────────────────────────────────────────
    # 迁移
    # ──────────────────────────────────────────────
    def migrate_batch(
        self,
        batch_id: str,
        partitions: Iterable[str] | None = None,
        *,
        verify: bool = True,
        force: bool = False,
        requester: str = "",
    ) -> dict[str, Any]:
        """把旧版压缩数据迁移为 gzip v2 归档。

        重试语义：

        * 已存在同摘要的已校验 gzip 资产时直接复用，不产生新版本；
        * 上一次残留的未校验版本不会被当作已迁移结果，重试会生成新版本；
        * 迁移失败的分区保留旧版资产与失败原因，可再次回查与重试。
        """
        batch_dir = self._batch_dir(batch_id)
        actual_batch = batch_dir.name
        files = self._scan_files(batch_dir, actual_batch)

        legacy_by_partition: dict[str, list[dict[str, Any]]] = {}
        for item in files:
            if item["format"] in (LEGACY_ZLIB_FORMAT, "json"):
                legacy_by_partition.setdefault(item["partition"], []).append(item)

        requested = [canonical_partition(p) for p in partitions] if partitions else sorted(legacy_by_partition)
        requested = list(dict.fromkeys(requested))

        results: list[dict[str, Any]] = []
        for partition in requested:
            result = self._migrate_partition(
                actual_batch, batch_dir, partition,
                legacy_by_partition.get(partition, []),
                verify=verify, force=force,
            )
            results.append(result)

        # 迁移完成后也登记未参与迁移的旧版/新版文件，便于回查存量版本。
        self._sync_batch_files(batch_dir, actual_batch)

        summary = {
            "ok": all(r.get("status") not in ("failed", "corrupt") for r in results),
            "batch_id": actual_batch,
            "archive_date": self._batch_date(actual_batch, batch_dir),
            "requester": requester,
            "verify": verify,
            "results": results,
            "migrated": sum(1 for r in results if r.get("status") == "migrated"),
            "reused": sum(1 for r in results if r.get("status") == "reused"),
            "failed": sum(1 for r in results if r.get("status") == "failed"),
            "assets": archive_state.list_assets(batch_id=actual_batch),
        }
        self._history.append({
            "action": "migrate", "batch_id": actual_batch,
            "timestamp": datetime.now(timezone.utc).isoformat(), "summary": summary,
        })
        return summary

    def _migrate_partition(
        self,
        batch_id: str,
        batch_dir: Path,
        partition: str,
        legacy_files: list[dict[str, Any]],
        *,
        verify: bool,
        force: bool,
    ) -> dict[str, Any]:
        rejected: list[dict[str, Any]] = []
        readable: list[tuple[dict[str, Any], dict[str, Any]]] = []
        for item in legacy_files:
            # 先登记旧版资产，回执和清单才能回查存量版本。
            self._register_file_asset(item)
            try:
                info = read_legacy_archive(item["path"])
            except ArchiveError as exc:
                rejected.append({
                    "file": item["file"], "format": item["format"],
                    "reason": exc.reason, "detail": exc.message,
                })
                continue
            readable.append((item, info))

        if not readable:
            # 旧文件可能在迁移完成后被清理或损坏；已有通过校验的 gzip v2
            # 仍视为迁移完成，重试不应把它降级为失败。
            reusable = self._find_verified_asset(batch_id, partition)
            if reusable is not None:
                return {
                    "partition": partition,
                    "status": "reused",
                    "reason": "already_verified_legacy_unavailable",
                    "message": "旧版压缩文件不可读，但已有通过校验的 gzip v2 资产",
                    "asset": reusable,
                    "source_files": [],
                    "rejected": rejected,
                }
            reason = rejected[0]["reason"] if rejected else "no_legacy_source"
            return {
                "partition": partition,
                "status": "failed",
                "reason": reason,
                "message": "没有可读的旧版压缩数据，迁移未完成",
                "source_files": [],
                "rejected": rejected,
            }

        events: list[dict[str, Any]] = []
        seen: set[str] = set()
        source_files: list[dict[str, Any]] = []
        dates: list[str] = []
        for item, info in readable:
            for record in info["records"]:
                identity = event_identity(record)
                if identity in seen:
                    continue
                seen.add(identity)
                events.append(record)
            source_files.append({
                "file": item["file"],
                "relative_path": item["relative_path"],
                "format": info["source_format"],
                "archive_date": item["archive_date"],
                "compressed_sha256": info["compressed_sha256"],
                "payload_sha256": info["payload_sha256"],
                "record_count": info["record_count"],
                "event_ids": info["event_ids"],
            })
            dates.append(item["archive_date"])

        archive_date = sorted(dates)[0] if dates else self._batch_date(batch_id, batch_dir)
        digest = compute_partition_digest(partition, archive_date, events)

        # 已校验且摘要一致 → 重试直接复用，不制造重复版本。
        if not force:
            reusable = self._find_verified_asset(batch_id, partition)
            if reusable is not None and reusable.get("partition_digest") == digest:
                return {
                    "partition": partition,
                    "status": "reused",
                    "reason": "already_verified",
                    "message": "已存在通过校验的同摘要 gzip v2 资产，直接复用",
                    "asset": reusable,
                    "source_files": source_files,
                    "rejected": rejected,
                }

        version = archive_state.next_version(batch_id, partition, archive_date, GZIP_FORMAT)
        file_name = f"{safe_name(partition)}_{archive_date.replace('-', '')}.v{version}.gzip"
        target = batch_dir / file_name
        payload = build_gzip_payload(
            batch_id=batch_id,
            partition=partition,
            archive_date=archive_date,
            events=events,
            source_files=source_files,
        )
        written = write_gzip_v2(target, payload)

        asset = archive_state.register_asset({
            "asset_id": self._asset_id(batch_id, partition, GZIP_FORMAT, version),
            "batch_id": batch_id,
            "partition": partition,
            "archive_date": archive_date,
            "format": GZIP_FORMAT,
            "format_version": GZIP_FORMAT_VERSION,
            "file": file_name,
            "relative_path": file_name,
            "path": written["path"],
            "size_bytes": written["size_bytes"],
            "version": version,
            "compressed_sha256": digest_label(written["compressed_sha256"]),
            "payload_sha256": digest_label(written["payload_sha256"]),
            "partition_digest": digest,
            "event_ids": payload["event_ids"],
            "record_count": payload["record_count"],
            "status": "unverified",
            "source": "migration",
            "source_files": source_files,
            "created_by": "migrate",
        })

        status = "migrated"
        verification: dict[str, Any] | None = None
        if verify:
            verification = self._verify_asset(asset)
            asset = verification["asset"]
            if not verification["ok"]:
                status = "failed"

        return {
            "partition": partition,
            "status": status,
            "reason": "ok" if status == "migrated" else "verification_failed",
            "message": (
                "迁移完成并通过校验" if status == "migrated"
                else "gzip v2 已生成但校验未通过"
            ),
            "asset": asset,
            "verification": verification,
            "source_files": source_files,
            "rejected": rejected,
            "gzip_file": file_name,
        }

    # ──────────────────────────────────────────────
    # 校验
    # ──────────────────────────────────────────────
    def _find_verified_asset(self, batch_id: str, partition: str) -> dict[str, Any] | None:
        """返回分区内最新的已校验 gzip v2 资产。"""
        candidates = [
            a for a in archive_state.list_assets(batch_id=batch_id, partition=partition)
            if a.get("format") == GZIP_FORMAT and a.get("status") == GZIP_USABLE_STATUS
        ]
        if not candidates:
            return None
        return max(candidates, key=lambda a: int(a.get("version", 0)))

    def verify_batch(
        self, batch_id: str, partitions: Iterable[str] | None = None,
    ) -> dict[str, Any]:
        """校验批次内的 gzip v2 资产，只有通过的资产才可用于恢复。"""
        batch_dir = self._batch_dir(batch_id)
        actual_batch = batch_dir.name
        self.sync_assets(actual_batch)
        wanted = {canonical_partition(p) for p in partitions} if partitions else None

        results: list[dict[str, Any]] = []
        for asset in archive_state.list_assets(batch_id=actual_batch, include_history=True):
            if asset.get("format") != GZIP_FORMAT:
                continue
            if wanted is not None and asset.get("partition") not in wanted:
                continue
            if asset.get("status") == "superseded" and not _is_latest(asset):
                continue
            verification = self._verify_asset(asset)
            results.append({
                "partition": asset.get("partition"),
                "file": asset.get("file"),
                "asset_id": asset.get("asset_id"),
                "version": asset.get("version"),
                "status": verification["asset"].get("status"),
                "ok": verification["ok"],
                "errors": verification["errors"],
                "digest": verification["computed_digest"],
            })

        return {
            "ok": all(r["ok"] for r in results) if results else True,
            "batch_id": actual_batch,
            "results": results,
            "verified": sum(1 for r in results if r["ok"]),
            "failed": sum(1 for r in results if not r["ok"]),
        }

    def _verify_asset(self, asset: dict[str, Any]) -> dict[str, Any]:
        """按文件字节、载荷结构和分区摘要逐项校验一个 gzip v2 资产。"""
        path = self._asset_path(asset)
        errors: list[str] = []
        computed_digest = ""
        payload: dict[str, Any] | None = None

        if not path.exists():
            asset = archive_state.upsert_asset({
                **asset, "status": "missing", "verification_errors": ["文件不存在"],
                "verified_at": None,
            })
            return {"ok": False, "asset": asset, "errors": ["文件不存在"], "computed_digest": ""}

        try:
            info = read_gzip_v2(path)
            payload = info["payload"]
        except ArchiveError as exc:
            asset = archive_state.upsert_asset({
                **asset, "status": "corrupt", "verification_errors": [exc.message],
                "verified_at": None,
            })
            return {"ok": False, "asset": asset, "errors": [exc.message], "computed_digest": ""}

        try:
            current_sha = digest_label(info["compressed_sha256"])
        except Exception:  # pragma: no cover - 防御性兜底
            current_sha = ""
        if asset.get("compressed_sha256") and current_sha != asset.get("compressed_sha256"):
            errors.append("文件字节与登记摘要不一致")

        verification = verify_gzip_payload(payload)
        errors.extend(verification["errors"])
        computed_digest = verification["computed_digest"]

        if payload.get("partition") != asset.get("partition"):
            errors.append(
                f"载荷分区 {payload.get('partition')} 与资产分区 {asset.get('partition')} 不一致"
            )
        payload_date = normalize_archive_date(payload.get("archive_date"))
        if asset.get("archive_date") and payload_date != normalize_archive_date(asset["archive_date"]):
            errors.append(
                f"载荷日期 {payload_date} 与资产日期 {asset.get('archive_date')} 不一致"
            )
        if payload.get("archive_batch") not in (None, asset.get("batch_id")):
            errors.append("载荷批次与资产批次不一致")

        ok = not errors
        updated = archive_state.upsert_asset({
            **asset,
            "status": "verified" if ok else "verification_failed",
            "partition_digest": verification["stored_digest"] or computed_digest,
            "payload_sha256": digest_label(info["payload_sha256"]),
            "event_ids": verification["event_ids"],
            "record_count": verification["record_count"],
            "verification_errors": errors,
            "verified_at": datetime.now(timezone.utc).isoformat() if ok else None,
        })

        if ok:
            # 同分区更早的已校验版本标记为 superseded，仍保留可回查。
            for other in archive_state.list_assets(
                batch_id=updated.get("batch_id"), partition=updated.get("partition"),
            ):
                if (
                    other.get("format") == GZIP_FORMAT
                    and other.get("asset_id") != updated.get("asset_id")
                    and int(other.get("version", 0)) < int(updated.get("version", 0))
                    and other.get("status") == "verified"
                ):
                    archive_state.upsert_asset({
                        **other, "status": "superseded",
                        "superseded_by": updated.get("asset_id"),
                    })

        return {
            "ok": ok,
            "asset": updated,
            "errors": errors,
            "computed_digest": computed_digest,
            "stored_digest": verification["stored_digest"],
        }

    # ──────────────────────────────────────────────
    # 恢复
    # ──────────────────────────────────────────────
    def restore(
        self,
        batch_id: str,
        partitions: Iterable[str],
        *,
        requester: str = "",
        mode: str = "auto",
    ) -> dict[str, Any]:
        """从归档恢复指定分区。

        ``mode``：

        * ``auto``：优先使用已校验 gzip v2，未迁移/未校验时回退到可读旧版；
        * ``gzip``：只允许已校验 gzip v2；
        * ``legacy``：模拟旧版恢复环境，只读旧版压缩数据（用于回滚演练）。
        """
        with _RUN_LOCK:
            return self._restore_locked(batch_id, partitions, requester=requester, mode=mode)

    def _restore_locked(
        self, batch_id: str, partitions: Iterable[str], *, requester: str, mode: str,
    ) -> dict[str, Any]:
        requested = [canonical_partition(p) for p in partitions]
        requested = list(dict.fromkeys(requested))
        if not requested:
            raise ArchiveError("invalid_request", "至少需要指定一个分区")

        mode = (mode or "auto").casefold()
        if mode not in ("auto", "gzip", "legacy"):
            raise ArchiveError("invalid_request", f"不支持的恢复模式: {mode}")

        batch_dir = self._batch_dir(batch_id)
        actual_batch = batch_dir.name
        files = self._scan_files(batch_dir, actual_batch)
        # 旧版文件只在需要回退时读取；这里只刷新磁盘上出现的新文件，
        # 不会把已校验的 gzip v2 重置回“未校验”。
        self._sync_batch_files(batch_dir, actual_batch)
        assets = archive_state.list_assets(batch_id=actual_batch)

        run_id = "RUN-" + uuid.uuid4().hex[:16]
        started_at = datetime.now(timezone.utc).isoformat()
        manifest = archive_state.load_manifest(actual_batch)
        run_record = {
            "run_id": run_id,
            "requester": requester,
            "mode": mode,
            "requested_partitions": requested,
            "started_at": started_at,
            "completed_at": None,
            "status": "running",
            "results": {},
        }
        manifest["restore_runs"].append(run_record)

        receipts: list[dict[str, Any]] = []
        try:
            for partition in requested:
                partition_files = [f for f in files if f["partition"] == partition]
                partition_assets = [a for a in assets if a.get("partition") == partition]
                receipt = self._restore_partition(
                    actual_batch, partition, mode, requester,
                    partition_files, partition_assets, run_id,
                )
                receipt = archive_state.add_receipt(receipt)
                receipts.append(receipt)
                self._update_manifest(manifest, actual_batch, partition, receipt, run_id)
                run_record = manifest["restore_runs"][-1]
                run_record["results"][partition] = {
                    "receipt_id": receipt["receipt_id"],
                    "status": receipt["status"],
                    "source_file": receipt.get("source_file"),
                    "source_format": receipt.get("source_format"),
                    "restored_count": receipt.get("restored_count", 0),
                    "skipped_count": receipt.get("skipped_count", 0),
                    "reason": receipt.get("reason"),
                }
                archive_state.save_manifest(actual_batch, manifest)
        finally:
            run_record = manifest["restore_runs"][-1]
            run_record["completed_at"] = datetime.now(timezone.utc).isoformat()
            statuses = [r.get("status") for r in receipts]
            if any(s == "failed" for s in statuses):
                run_record["status"] = "partial" if any(s != "failed" for s in statuses) else "failed"
            else:
                run_record["status"] = "completed"
            manifest["last_restore_run"] = run_record
            path = archive_state.save_manifest(actual_batch, manifest)

        return {
            "ok": all(r.get("status") != "failed" for r in receipts),
            "batch_id": actual_batch,
            "run_id": run_id,
            "mode": mode,
            "receipts": receipts,
            "partitions": {r["partition"]: r for r in receipts},
            "manifest_path": str(path),
            "manifest": manifest,
        }

    def _restore_partition(
        self,
        batch_id: str,
        partition: str,
        mode: str,
        requester: str,
        files: list[dict[str, Any]],
        assets: list[dict[str, Any]],
        run_id: str,
    ) -> dict[str, Any]:
        rejected: list[dict[str, Any]] = []
        gzip_assets = [a for a in assets if a.get("format") == GZIP_FORMAT]
        legacy_files = [f for f in files if f["format"] in (LEGACY_ZLIB_FORMAT, "json")]

        # 1) 选择已校验且文件未被改动的 gzip v2。
        verified_asset: dict[str, Any] | None = None
        verified_payload: dict[str, Any] | None = None
        for asset in sorted(gzip_assets, key=lambda a: int(a.get("version", 0)), reverse=True):
            path = self._asset_path(asset)
            if not path.exists():
                rejected.append(_candidate(asset, "file_missing", "gzip 文件不存在"))
                continue
            try:
                current_sha = digest_label(file_sha256(path))
            except OSError as exc:
                rejected.append(_candidate(asset, "file_unreadable", str(exc)))
                continue
            if asset.get("status") != GZIP_USABLE_STATUS:
                reason = (
                    "not_verified" if asset.get("status") in ("unverified", "discovered")
                    else "verification_failed" if asset.get("status") == "verification_failed"
                    else "not_verified"
                )
                rejected.append(_candidate(asset, reason, "gzip v2 资产尚未通过校验，不可用于恢复"))
                continue
            if current_sha != asset.get("compressed_sha256"):
                rejected.append(_candidate(asset, "integrity_mismatch", "文件字节与已校验摘要不一致"))
                continue
            try:
                info = read_gzip_v2(path)
            except ArchiveError as exc:
                rejected.append(_candidate(asset, exc.reason, exc.message))
                continue
            verification = verify_gzip_payload(info["payload"])
            if not verification["ok"]:
                rejected.append(_candidate(
                    asset, "verification_failed", "; ".join(verification["errors"]),
                ))
                continue
            verified_asset = asset
            verified_payload = info["payload"]
            break

        # 2) 旧版文件只在需要回退时读取；损坏的旧文件记录为拒绝原因。
        legacy_candidates: list[tuple[dict[str, Any], dict[str, Any]]] = []
        for item in legacy_files:
            try:
                info = read_legacy_archive(item["path"])
            except ArchiveError as exc:
                rejected.append({
                    "file": item["file"], "format": item["format"],
                    "partition": partition, "reason": exc.reason,
                    "detail": exc.message, "usable": False,
                })
                continue
            legacy_candidates.append((item, info))

        chosen_source = ""
        if mode == "legacy":
            if not legacy_candidates:
                return self._failed_receipt(
                    batch_id, partition, mode, requester, run_id,
                    "no_readable_legacy", "旧版恢复环境没有可读的旧压缩文件", rejected,
                )
            chosen_source = "legacy"
        elif mode == "gzip":
            if verified_asset is None or verified_payload is None:
                return self._failed_receipt(
                    batch_id, partition, mode, requester, run_id,
                    "no_verified_gzip", "没有通过校验的 gzip v2 资产可用于恢复", rejected,
                )
            chosen_source = "gzip"
        else:
            if verified_asset is not None and verified_payload is not None:
                chosen_source = "gzip"
            elif legacy_candidates:
                chosen_source = "legacy"
            else:
                reason = "legacy_corrupt" if any(
                    r.get("reason") in ("legacy_corrupt", "legacy_unreadable") for r in rejected
                ) else "no_usable_archive"
                message = (
                    "该分区尚未完成迁移且旧版压缩文件已损坏，不能冒充可用数据"
                    if reason == "legacy_corrupt"
                    else "该分区没有可用归档数据"
                )
                return self._failed_receipt(
                    batch_id, partition, mode, requester, run_id,
                    reason, message, rejected,
                )

        # 3) 组装事件并写入数据湖。
        if chosen_source == "gzip" and verified_asset is not None and verified_payload is not None:
            events = [e for e in verified_payload.get("events", []) if isinstance(e, dict)]
            archive_date = normalize_archive_date(verified_payload.get("archive_date"))
            source_file = str(verified_asset.get("file") or "")
            source_files = [source_file] if source_file else []
            source_format = GZIP_FORMAT
            source_verified = True
            asset_version = int(verified_asset.get("version", 0))
            asset_id = str(verified_asset.get("asset_id") or "")
            digest = str(verified_asset.get("partition_digest") or verified_payload.get("partition_digest") or "")
            fallback = False
        else:
            events, source_files, dates = _merge_legacy(legacy_candidates)
            archive_date = sorted(dates)[0] if dates else self._batch_date(batch_id)
            source_file = source_files[0] if len(source_files) == 1 else (
                "+".join(source_files) if source_files else ""
            )
            source_format = "zlib" if any(
                info["source_format"] == LEGACY_ZLIB_FORMAT for _, info in legacy_candidates
            ) else "json"
            source_verified = False
            legacy_assets = [
                a for a in assets
                if a.get("format") != GZIP_FORMAT and a.get("file") in source_files
            ]
            asset_version = max(
                (int(a.get("version", 0)) for a in legacy_assets), default=0,
            )
            # 只保留最近一次读到的存量版本，避免同一份旧文件被重复计版本。
            entry = next(
                (a for a in legacy_assets if a.get("file") == source_file), None,
            )
            asset_id = str(entry.get("asset_id") or "") if entry else ""
            digest = compute_partition_digest(partition, archive_date, events)
            fallback = True

        receipt_id = "RCPT-" + uuid.uuid4().hex[:16]
        from app.services.data_lake_sink import DataLakeSinkService
        sink = DataLakeSinkService()
        written = sink.write_restored_events(
            events,
            partition=partition,
            archive_date=archive_date,
            receipt_id=receipt_id,
            batch_id=batch_id,
            source_file=source_file,
            source_format=source_format,
            source_verified=source_verified,
            run_id=run_id,
        )

        return self._success_receipt(
            receipt_id=receipt_id,
            batch_id=batch_id,
            partition=partition,
            mode=mode,
            requester=requester,
            run_id=run_id,
            source_file=source_file,
            source_files=source_files,
            source_format=source_format,
            source_verified=source_verified,
            asset=verified_asset,
            asset_version=asset_version,
            asset_id=asset_id,
            digest=digest,
            archive_date=archive_date,
            events=events,
            written=written,
            rejected=rejected,
            fallback=fallback,
        )

    def _success_receipt(
        self, *, receipt_id: str, batch_id: str, partition: str, mode: str, requester: str,
        run_id: str, source_file: str, source_files: list[str], source_format: str,
        source_verified: bool, asset: dict[str, Any] | None, asset_version: int,
        asset_id: str, digest: str, archive_date: str, events: list[dict[str, Any]],
        written: dict[str, Any], rejected: list[dict[str, Any]], fallback: bool,
    ) -> dict[str, Any]:
        restored_ids = list(written.get("written_event_ids", []))
        skipped_ids = list(written.get("skipped_event_ids", []))
        lake_rows = int(written.get("written_rows", len(restored_ids)))
        status = "restored" if restored_ids else ("deduplicated" if skipped_ids else "restored")

        return {
            "receipt_id": receipt_id,
            "batch_id": batch_id,
            "run_id": run_id,
            "partition": partition,
            "requested_partition": partition,
            "mode": mode,
            "requester": requester,
            "status": status,
            "source_file": source_file,
            "source_files": source_files,
            "source_format": source_format,
            "source_verified": source_verified,
            "source_version": asset_version,
            "asset_id": asset_id,
            "asset_status": (asset or {}).get("status", "legacy" if fallback else ""),
            "digest": digest,
            "archive_date": archive_date,
            "record_count": len(events),
            "restored_count": len(restored_ids),
            "skipped_count": len(skipped_ids),
            "duplicate_count": len(skipped_ids),
            "restored_event_ids": restored_ids,
            "skipped_event_ids": skipped_ids,
            "duplicate_refs": written.get("duplicate_refs", {}),
            "lake_partition": written.get("lake_partition", ""),
            "lake_dates": written.get("lake_dates", []),
            "lake_files": written.get("lake_files", []),
            "lake_rows": lake_rows,
            "fallback_legacy": fallback,
            "upgrade_path": "legacy" if fallback else "gzip-v2",
            "rejected_candidates": rejected,
            "reason": "ok",
            "created_at": datetime.now(timezone.utc).isoformat(),
        }

    def _failed_receipt(
        self, batch_id: str, partition: str, mode: str, requester: str, run_id: str,
        reason: str, message: str, rejected: list[dict[str, Any]],
    ) -> dict[str, Any]:
        return {
            "receipt_id": "RCPT-" + uuid.uuid4().hex[:16],
            "batch_id": batch_id,
            "run_id": run_id,
            "partition": partition,
            "requested_partition": partition,
            "mode": mode,
            "requester": requester,
            "status": "failed",
            "reason": reason,
            "message": message,
            "source_file": None,
            "source_files": [],
            "source_format": None,
            "source_verified": False,
            "source_version": 0,
            "asset_id": "",
            "asset_status": "",
            "digest": "",
            "archive_date": "",
            "record_count": 0,
            "restored_count": 0,
            "skipped_count": 0,
            "restored_event_ids": [],
            "skipped_event_ids": [],
            "lake_partition": "",
            "lake_dates": [],
            "lake_files": [],
            "fallback_legacy": False,
            "upgrade_path": "",
            "rejected_candidates": rejected,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }

    def _update_manifest(
        self, manifest: dict[str, Any], batch_id: str, partition: str,
        receipt: dict[str, Any], run_id: str,
    ) -> None:
        partitions = manifest.setdefault("partitions", {})
        entry = partitions.get(partition) or {
            "partition": partition,
            "receipts": [],
            "restored_event_ids": [],
            "restored_dates": [],
        }
        receipt_ids = entry.setdefault("receipts", [])
        if receipt["receipt_id"] not in receipt_ids:
            receipt_ids.append(receipt["receipt_id"])

        entry["last_receipt_id"] = receipt["receipt_id"]
        entry["last_attempt"] = {
            "receipt_id": receipt["receipt_id"],
            "run_id": run_id,
            "status": receipt["status"],
            "reason": receipt.get("reason"),
            "source_file": receipt.get("source_file"),
            "source_format": receipt.get("source_format"),
            "at": receipt.get("created_at"),
        }

        if receipt["status"] != "failed":
            event_ids = list(entry.get("restored_event_ids", []))
            seen = set(event_ids)
            for event_id in receipt.get("restored_event_ids", []):
                if event_id not in seen:
                    seen.add(event_id)
                    event_ids.append(event_id)
            entry["restored_event_ids"] = event_ids
            entry["event_ids"] = event_ids
            entry["restored_count"] = len(event_ids)
            entry["source_file"] = receipt.get("source_file")
            entry["source_files"] = receipt.get("source_files", [])
            entry["source_format"] = receipt.get("source_format")
            entry["source_verified"] = receipt.get("source_verified", False)
            entry["upgrade_path"] = receipt.get("upgrade_path")
            entry["archive_date"] = receipt.get("archive_date")
            entry["lake_partition"] = receipt.get("lake_partition")
            entry["lake_dates"] = receipt.get("lake_dates", [])
            entry["lake_files"] = list(dict.fromkeys(
                [*entry.get("lake_files", []), *receipt.get("lake_files", [])]
            ))
            entry["status"] = "restored"
            entry["last_success"] = {
                "receipt_id": receipt["receipt_id"],
                "run_id": run_id,
                "source_file": receipt.get("source_file"),
                "source_format": receipt.get("source_format"),
                "at": receipt.get("created_at"),
            }
        else:
            entry["status"] = "failed"
            entry["last_failure"] = {
                "receipt_id": receipt["receipt_id"],
                "run_id": run_id,
                "reason": receipt.get("reason"),
                "message": receipt.get("message"),
                "at": receipt.get("created_at"),
            }
            # 失败时保留上一次成功的来源信息，供部署中核对。
            entry.setdefault("restored_event_ids", [])
            entry.setdefault("event_ids", entry.get("restored_event_ids", []))
            entry.setdefault("restored_count", len(entry.get("restored_event_ids", [])))

        entry["updated_at"] = datetime.now(timezone.utc).isoformat()
        entry["batch_id"] = batch_id
        partitions[partition] = entry

    # ──────────────────────────────────────────────
    # 回查
    # ──────────────────────────────────────────────
    def inspect_archive(self, batch_id: str, partition: str) -> dict[str, Any]:
        """检查分区最新 gzip v2 的持久化元数据（日期、实际分区、事件身份）。"""
        batch_dir = self._batch_dir(batch_id)
        partition = canonical_partition(partition)
        self._sync_batch_files(batch_dir, batch_dir.name)
        candidates = [
            a for a in archive_state.list_assets(batch_id=batch_dir.name, partition=partition)
            if a.get("format") == GZIP_FORMAT
        ]
        if not candidates:
            return {
                "ok": False, "message": "该分区尚无 gzip v2 归档",
                "partition": partition, "batch_id": batch_dir.name,
            }
        asset = max(candidates, key=lambda a: int(a.get("version", 0)))
        path = self._asset_path(asset)
        result: dict[str, Any] = {
            "ok": path.exists(),
            "batch_id": batch_dir.name,
            "partition": partition,
            "asset_id": asset.get("asset_id"),
            "version": asset.get("version"),
            "asset_status": asset.get("status"),
            "file": asset.get("file"),
            "path": str(path),
            "record_count": asset.get("record_count", 0),
            "event_ids": asset.get("event_ids", []),
            "partition_digest": asset.get("partition_digest"),
            "compressed_sha256": asset.get("compressed_sha256"),
            "archive_date": asset.get("archive_date"),
        }
        if not path.exists():
            result["message"] = "归档文件不存在"
            return result

        try:
            with open(path, "rb") as f:
                header = f.read(64)
            header_name = _gzip_header_name(header)
            info = read_gzip_v2(path)
            payload = info["payload"]
            verification = verify_gzip_payload(payload)
            result.update({
                "ok": verification["ok"],
                "gzip_header_name": header_name,
                "payload_partition": payload.get("partition"),
                "payload_archive_date": normalize_archive_date(payload.get("archive_date")),
                "event_ids": verification["event_ids"],
                "record_count": verification["record_count"],
                "stored_digest": verification["stored_digest"],
                "computed_digest": verification["computed_digest"],
                "verification_errors": verification["errors"],
                "source_files": payload.get("source_files", []),
            })
        except ArchiveError as exc:
            result.update({"ok": False, "message": exc.message, "reason": exc.reason})
        return result

    def get_manifest(self, batch_id: str) -> dict[str, Any]:
        batch_dir = self._batch_dir(batch_id)
        manifest = archive_state.load_manifest(batch_dir.name)
        manifest["batch_id"] = batch_dir.name
        manifest["manifest_path"] = str(archive_state.manifest_path(batch_dir.name))
        return manifest

    def get_receipt(self, receipt_id: str) -> dict[str, Any] | None:
        return archive_state.get_receipt(receipt_id)

    def list_receipts(self, batch_id: str | None = None, partition: str | None = None,
                      limit: int = 100) -> list[dict[str, Any]]:
        return archive_state.list_receipts(batch_id=batch_id, partition=partition, limit=limit)

    def receipt_rows(self, receipt_id: str) -> dict[str, Any]:
        """回查某个回执实际恢复的来源行（持久化在数据湖中）。"""
        receipt = archive_state.get_receipt(receipt_id)
        if receipt is None:
            return {"ok": False, "message": f"回执 {receipt_id} 不存在", "rows": []}
        rows: list[dict[str, Any]] = []
        for lake_file in receipt.get("lake_files", []):
            path = Path(str(lake_file))
            if not path.exists():
                continue
            try:
                import json
                with open(path, "r", encoding="utf-8") as f:
                    data = json.load(f)
            except (OSError, ValueError):
                continue
            if isinstance(data, dict):
                data = [data]
            if not isinstance(data, list):
                continue
            for row in data:
                if isinstance(row, dict) and row.get("restore_receipt_id") == receipt_id:
                    rows.append(row)
        return {"ok": True, "receipt": receipt, "rows": rows, "total": len(rows)}

    def legacy_receipt_view(self, receipt_id: str) -> dict[str, Any]:
        """旧版恢复环境可用的回执视图：返回原档案文本和实际文件。"""
        import json
        result = self.receipt_rows(receipt_id)
        if not result["ok"]:
            return {"ok": False, "message": result["message"]}
        receipt = result["receipt"]
        payloads = [row.get("payload", row) for row in result["rows"]]
        return {
            "ok": True,
            "receipt_id": receipt_id,
            "archive_file": receipt.get("source_file"),
            "archive_batch": receipt.get("batch_id"),
            "partition": receipt.get("partition"),
            "data": json.dumps(payloads, ensure_ascii=False),
            "rows": result["rows"],
            "source_format": receipt.get("source_format"),
        }

    def source_row_receipt(self, partition: str, event_id: str) -> dict[str, Any]:
        """从已恢复的来源行反查原回执（上线/回滚后都可查）。"""
        entry = archive_state.ledger_get(canonical_partition(partition), event_id)
        if entry is None:
            return {"ok": False, "message": "未找到该来源记录的恢复索引", "event_id": event_id}
        receipt = archive_state.get_receipt(str(entry.get("receipt_id") or ""))
        return {
            "ok": receipt is not None,
            "event_id": event_id,
            "partition": canonical_partition(partition),
            "ledger": entry,
            "receipt": receipt,
        }

    # ──────────────────────────────────────────────
    # 数据湖核对
    # ──────────────────────────────────────────────
    def query_lake(
        self, partition: str, date: str | None = None, receipt_id: str | None = None,
        limit: int = 200,
    ) -> dict[str, Any]:
        """查询数据湖中原分区下已恢复的记录。"""
        from app.services.data_lake_sink import DataLakeSinkService

        partition = canonical_partition(partition)
        date_partition = date_to_lake_partition(date) if date else None
        lake = DataLakeSinkService()
        records: list[dict[str, Any]] = []
        partitions: list[str] = []
        if date_partition:
            partitions = [date_partition]
        else:
            base = lake._lake_base / "events" / partition
            if base.exists():
                for day in sorted(base.glob("*/*/*")):
                    if day.is_dir():
                        partitions.append(day.relative_to(base).as_posix())

        for day in partitions:
            result = lake.query_lake_data(partition, day, limit=limit)
            records.extend(result.get("records", []))
        if receipt_id:
            records = [r for r in records if r.get("restore_receipt_id") == receipt_id]
        records = records[:limit]
        return {
            "ok": True,
            "partition": partition,
            "lake_partition": f"events/{partition}",
            "dates": partitions,
            "records": records,
            "total": len(records),
            "event_ids": [str(r.get("event_id")) for r in records],
            "receipt_ids": sorted({str(r.get("restore_receipt_id")) for r in records if r.get("restore_receipt_id")}),
        }

    def domain_snapshot(self) -> dict[str, Any]:
        from app.persistence import json_store
        rows = json_store.get_all(self.MODULE)
        by_status = {status: 0 for status in self.STATUS_ORDER}
        for row in rows:
            status = row.get("status", "")
            if status in by_status:
                by_status[status] += 1
        return {
            "module": self.MODULE,
            "entity": self.ENTITY,
            "artifact": "事件归档清理数据集",
            "count": len(rows),
            "by_status": by_status,
            "abnormal": sum(1 for r in rows if r.get("abnormal")),
            "operation": self.OPERATION_TERM,
        }


# ──────────────────────────────────────────────────
# 辅助函数
# ──────────────────────────────────────────────────
def _candidate(asset: dict[str, Any], reason: str, detail: str) -> dict[str, Any]:
    return {
        "file": asset.get("file"),
        "format": asset.get("format"),
        "version": asset.get("version"),
        "asset_id": asset.get("asset_id"),
        "status": asset.get("status"),
        "reason": reason,
        "detail": detail,
        "usable": False,
    }


def _is_latest(asset: dict[str, Any]) -> bool:
    siblings = [
        a for a in archive_state.list_assets(
            batch_id=asset.get("batch_id"), partition=asset.get("partition"),
        )
        if a.get("format") == asset.get("format")
    ]
    latest = max((int(a.get("version", 0)) for a in siblings), default=0)
    return int(asset.get("version", 0)) >= latest


def _merge_legacy(
    candidates: list[tuple[dict[str, Any], dict[str, Any]]],
) -> tuple[list[dict[str, Any]], list[str], list[str]]:
    events: list[dict[str, Any]] = []
    seen: set[str] = set()
    source_files: list[str] = []
    dates: list[str] = []
    for item, info in candidates:
        source_files.append(item["file"])
        dates.append(item["archive_date"])
        for record in info["records"]:
            identity = event_identity(record)
            if identity in seen:
                continue
            seen.add(identity)
            events.append(record)
    return events, source_files, dates


def _gzip_header_name(header: bytes) -> str:
    """读取 gzip 头里的原始文件名（FNAME 字段），用于核对归档身份。"""
    if len(header) < 10 or header[:2] != b"\x1f\x8b":
        return ""
    flags = header[3]
    offset = 10
    if flags & 0x04:  # FEXTRA
        if len(header) < offset + 2:
            return ""
        extra_len = int.from_bytes(header[offset:offset + 2], "little")
        offset += 2 + extra_len
    if flags & 0x08:  # FNAME
        end = header.find(b"\x00", offset)
        if end < 0:
            return ""
        return header[offset:end].decode("latin-1")
    return ""
