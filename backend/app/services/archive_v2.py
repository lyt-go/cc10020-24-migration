"""gzip v2 归档编解码：分区摘要、gzip 封装与旧版 zlib 读取。

v2 归档约定：

* 每个分区一个 ``.gzip`` 文件，gzip 头里保留归档日期与原分区名；
* 解压后的 JSON 载荷保留 ``archive_date``、``partition`` 以及每条事件身份；
* 载荷里的 ``partition_digest`` 是对 ``partition + archive_date + events``
  做规范化 JSON 后的 SHA-256，迁移、校验和恢复都复用同一套算法；
* 旧版 ``.zlib`` 文件只通过 :func:`read_legacy_archive` 读取，读取结果里带上
  压缩前后摘要，便于判断旧文件是否在迁移后被破坏。

这里只依赖标准库，调用方负责决定“哪个候选文件可用”，本模块不做业务选择。
"""
from __future__ import annotations

import gzip
import hashlib
import io
import json
import os
import re
import zlib
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

GZIP_FORMAT = "gzip-v2"
GZIP_FORMAT_VERSION = 2
LEGACY_ZLIB_FORMAT = "zlib"
LEGACY_JSON_FORMAT = "json"

ARCHIVE_SUFFIXES = (".gzip", ".gz", ".zlib", ".json")

_DATE_RE = re.compile(r"(20\d{2})[-_/](\d{1,2})[-_/](\d{1,2})")
_COMPACT_DATE_RE = re.compile(r"(20\d{2})(\d{2})(\d{2})")
_UNSAFE_NAME_RE = re.compile(r"[^0-9A-Za-z._-]+")


class ArchiveError(Exception):
    """归档读取/校验失败；``reason`` 供回执直接复用为机器可读原因。"""

    def __init__(self, reason: str, message: str, detail: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.reason = reason
        self.message = message
        self.detail = detail or {}


# ──────────────────────────────────────────────────────────
# 规范化、摘要与身份
# ──────────────────────────────────────────────────────────
def canonical_json_bytes(value: Any) -> bytes:
    """生成用于摘要和落盘的规范化 JSON 字节串。"""
    return json.dumps(
        value, sort_keys=True, ensure_ascii=False, separators=(",", ":"), default=str,
    ).encode("utf-8")


def sha256_hex(data: bytes | str) -> str:
    if isinstance(data, str):
        data = data.encode("utf-8")
    return hashlib.sha256(data).hexdigest()


def digest_label(hex_digest: str) -> str:
    return f"sha256:{hex_digest}"


def file_sha256(path: str | os.PathLike[str]) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def compute_partition_digest(partition: str, archive_date: str, events: Iterable[Any]) -> str:
    """分区摘要：只覆盖分区、日期与事件内容，不受压缩字节影响。"""
    material = {
        "partition": partition,
        "archive_date": archive_date,
        "events": list(events),
    }
    return digest_label(sha256_hex(canonical_json_bytes(material)))


def normalize_records(data: Any) -> list[dict[str, Any]]:
    """把归档载荷整理成事件字典列表。"""
    if data is None:
        return []
    if isinstance(data, dict):
        if isinstance(data.get("events"), list):
            return [r for r in data["events"] if isinstance(r, dict)]
        if isinstance(data.get("records"), list):
            return [r for r in data["records"] if isinstance(r, dict)]
        return [data]
    if isinstance(data, list):
        records: list[dict[str, Any]] = []
        for item in data:
            if isinstance(item, dict):
                records.append(item)
            else:
                records.append({"value": item})
        return records
    return [{"value": data}]


def event_identity(record: dict[str, Any]) -> str:
    """事件身份：优先使用事件自带 ID，否则退化为内容摘要。

    迁移、恢复和湖内去重都使用这个函数，保证新旧恢复路径对同一条
    源记录得到同一个身份。
    """
    for key in ("event_id", "eventId", "id", "record_id", "source_record_id"):
        value = record.get(key)
        if value is not None and str(value).strip() != "":
            return str(value)

    payload = record.get("payload")
    if isinstance(payload, dict):
        for key in ("event_id", "eventId", "id", "record_id"):
            value = payload.get(key)
            if value is not None and str(value).strip() != "":
                return f"payload:{value}"

    return "anon:" + sha256_hex(canonical_json_bytes(record))[:24]


def unique_event_ids(records: Iterable[dict[str, Any]]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for record in records:
        identity = event_identity(record)
        if identity not in seen:
            seen.add(identity)
            result.append(identity)
    return result


# ──────────────────────────────────────────────────────────
# 文件名/路径元数据
# ──────────────────────────────────────────────────────────
def parse_date(text: str) -> str | None:
    """从文件名、分区目录或批次名中提取 ``YYYY-MM-DD``。"""
    for regex in (_DATE_RE, _COMPACT_DATE_RE):
        for match in regex.finditer(text):
            try:
                candidate = datetime(
                    int(match.group(1)), int(match.group(2)), int(match.group(3)),
                )
            except ValueError:
                continue
            return candidate.date().isoformat()
    return None


def normalize_archive_date(value: Any, fallback: str | None = None) -> str:
    if isinstance(value, str) and value.strip():
        parsed = parse_date(value.strip())
        if parsed:
            return parsed
    return fallback or datetime.now(timezone.utc).date().isoformat()


def date_to_lake_partition(archive_date: str) -> str:
    """``2026-11-20`` → ``2026/11/20``（数据湖分区格式）。"""
    parsed = parse_date(archive_date)
    if not parsed:
        parsed = datetime.now(timezone.utc).date().isoformat()
    return parsed.replace("-", "/")


def lake_date_from_timestamp(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    return parse_date(value)


def safe_name(value: str, default: str = "partition") -> str:
    cleaned = _UNSAFE_NAME_RE.sub("_", str(value)).strip("._-")
    return cleaned or default


_PARTITION_ALIASES = {
    "orders": "order",
    "order": "order",
    "订单": "order",
    "订单分区": "order",
    "订单数据": "order",
    "inventory": "inventory",
    "inventories": "inventory",
    "stock": "inventory",
    "库存": "inventory",
    "库存分区": "inventory",
    "库存数据": "inventory",
}


def canonical_partition(value: str) -> str:
    text = str(value).strip()
    if not text:
        return "default"
    alias = _PARTITION_ALIASES.get(text.casefold())
    if alias:
        return alias
    alias = _PARTITION_ALIASES.get(text)
    return alias or text


def partition_from_path(relative_path: str, fallback_date: str | None = None) -> tuple[str, str | None]:
    """从归档文件名推断实际分区和日期。

    支持这些常见形态：

    * ``order.zlib`` → ``order``
    * ``order_2026_11_20.json.zlib`` → ``order`` + 日期
    * ``order_created/2026/11/20/120000_ab.json.zlib`` → ``order_created`` + 日期
    * ``arch_20261120/inventory.zlib`` → ``inventory`` (日期取批次名)
    """
    rel = str(relative_path).replace("\\", "/")
    path = Path(rel)
    name = path.name
    lower = name.casefold()
    for suffix in ARCHIVE_SUFFIXES:
        if lower.endswith(suffix):
            name = name[: -len(suffix)]
            lower = name.casefold()
    if lower.endswith(".json"):
        name = name[:-5]

    # 先看完整相对路径里的日期，能覆盖按日期目录分区的旧归档。
    path_date = parse_date(rel) or fallback_date
    date = parse_date(name) or path_date

    prefix = name
    match = _DATE_RE.search(prefix) or _COMPACT_DATE_RE.search(prefix)
    if match:
        prefix = prefix[: match.start()]
    prefix = re.sub(r"[_\-.]+$", "", prefix)
    prefix = re.sub(r"^\d{6,8}_?", "", prefix)  # 去掉文件名前缀里的时间戳

    # 文件名只是时间戳/序号时，向上找最近一个有意义的分区目录。
    if _TIME_PREFIX_RE.match(name):
        return partition_from_parents(path, date)

    if prefix:
        return canonical_partition(prefix), date

    return partition_from_parents(path, date)


_TIME_PREFIX_RE = re.compile(r"^\d{6,8}(?:_|$)")


def partition_from_parents(path: Path, date: str | None) -> tuple[str, str | None]:
    for parent in reversed(path.parent.parts):
        if parent.casefold() in {"archive", "archives", "data_store", "events", "batch", "batches"}:
            continue
        if parse_date(parent):
            continue
        if parent.startswith("arch_"):
            continue
        if _TIME_PREFIX_RE.match(parent):
            continue
        # 按日期分区的目录层级（2026/11/20）不参与分区名判定。
        if parent.isdigit():
            continue
        if parent:
            return canonical_partition(parent), date
    return "default", date


def is_state_file(name: str) -> bool:
    lower = name.casefold()
    if lower.startswith("_") or lower.startswith("."):
        return True
    return any(token in lower for token in ("manifest", "receipt", "registry", "ledger"))


def archive_suffix_format(name: str) -> str | None:
    lower = name.casefold()
    if lower.endswith(".zlib"):
        return LEGACY_ZLIB_FORMAT
    if lower.endswith(".gzip") or lower.endswith(".gz"):
        return "gzip"
    if lower.endswith(".json"):
        return LEGACY_JSON_FORMAT
    return None


# ──────────────────────────────────────────────────────────
# gzip v2 读写
# ──────────────────────────────────────────────────────────
def build_gzip_payload(
    *,
    batch_id: str,
    partition: str,
    archive_date: str,
    events: list[dict[str, Any]],
    source_files: list[dict[str, Any]] | None = None,
    created_at: str | None = None,
) -> dict[str, Any]:
    partition = canonical_partition(partition)
    archive_date = normalize_archive_date(archive_date)
    source_files = source_files or []
    payload = {
        "format": GZIP_FORMAT,
        "format_version": GZIP_FORMAT_VERSION,
        "archive_batch": batch_id,
        "archive_date": archive_date,
        "partition": partition,
        "partition_digest": compute_partition_digest(partition, archive_date, events),
        "digest_algorithm": "sha256",
        "record_count": len(events),
        "event_ids": unique_event_ids(events),
        "events": events,
        "source_files": source_files,
        "source_format": LEGACY_ZLIB_FORMAT,
        "created_at": created_at or datetime.now(timezone.utc).isoformat(),
    }
    return payload


def _gzip_mtime(archive_date: str) -> int:
    parsed = parse_date(archive_date)
    if not parsed:
        return 0
    dt = datetime.fromisoformat(parsed).replace(tzinfo=timezone.utc)
    return int(dt.timestamp())


def write_gzip_v2(path: str | os.PathLike[str], payload: dict[str, Any]) -> dict[str, Any]:
    """原子写入 gzip v2 文件，返回文件摘要信息。"""
    partition = str(payload.get("partition") or "partition")
    header_name = safe_name(partition) + ".json"
    raw = canonical_json_bytes(payload)
    buffer = io.BytesIO()
    with gzip.GzipFile(
        fileobj=buffer,
        mode="wb",
        filename=header_name,
        mtime=_gzip_mtime(str(payload.get("archive_date", ""))),
    ) as gz:
        gz.write(raw)

    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_name(target.name + ".tmp")
    tmp.write_bytes(buffer.getvalue())
    os.replace(tmp, target)
    return {
        "file": target.name,
        "path": str(target.resolve()),
        "size_bytes": target.stat().st_size,
        "compressed_sha256": file_sha256(target),
        "payload_sha256": sha256_hex(raw),
        "digest": payload.get("partition_digest"),
    }


def read_gzip_v2(path: str | os.PathLike[str]) -> dict[str, Any]:
    """读取 gzip v2，返回载荷和字节级摘要。"""
    target = Path(path)
    try:
        compressed = target.read_bytes()
    except OSError as exc:
        raise ArchiveError("archive_unreadable", f"无法读取归档文件: {exc}") from exc
    try:
        raw = gzip.decompress(compressed)
    except (OSError, EOFError, gzip.BadGzipFile) as exc:
        raise ArchiveError("archive_corrupt", f"gzip 解压失败: {exc}") from exc
    try:
        payload = json.loads(raw.decode("utf-8-sig"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ArchiveError("archive_corrupt", f"归档 JSON 解析失败: {exc}") from exc
    if not isinstance(payload, dict):
        raise ArchiveError("invalid_archive", "gzip 载荷不是 JSON 对象")
    return {
        "payload": payload,
        "compressed_sha256": sha256_hex(compressed),
        "payload_sha256": sha256_hex(raw),
    }


def verify_gzip_payload(payload: dict[str, Any]) -> dict[str, Any]:
    """校验载荷结构、事件身份和分区摘要。"""
    errors: list[str] = []
    if payload.get("format") != GZIP_FORMAT:
        errors.append(f"format != {GZIP_FORMAT}")
    if payload.get("format_version") != GZIP_FORMAT_VERSION:
        errors.append(f"format_version != {GZIP_FORMAT_VERSION}")
    partition = payload.get("partition")
    if not isinstance(partition, str) or not partition.strip():
        errors.append("缺少 partition")
        partition = "default"
    archive_date = normalize_archive_date(payload.get("archive_date"))
    events = payload.get("events")
    if not isinstance(events, list):
        errors.append("events 不是列表")
        events = []

    computed = compute_partition_digest(partition, archive_date, events)
    stored = payload.get("partition_digest")
    if stored != computed:
        errors.append("partition_digest 不匹配")

    event_ids = unique_event_ids([e for e in events if isinstance(e, dict)])
    stored_ids = payload.get("event_ids")
    if isinstance(stored_ids, list) and list(stored_ids) != event_ids:
        errors.append("event_ids 与 events 不一致")
    if isinstance(payload.get("record_count"), int) and payload["record_count"] != len(events):
        errors.append("record_count 与 events 不一致")

    return {
        "ok": not errors,
        "errors": errors,
        "computed_digest": computed,
        "stored_digest": stored,
        "event_ids": event_ids,
        "record_count": len(events),
    }


# ──────────────────────────────────────────────────────────
# 旧版 zlib / 其他旧格式读取
# ──────────────────────────────────────────────────────────
def read_legacy_archive(path: str | os.PathLike[str]) -> dict[str, Any]:
    """读取旧版归档（zlib 或未压缩 JSON），保留摘要用于损坏判断。"""
    target = Path(path)
    try:
        raw_bytes = target.read_bytes()
    except OSError as exc:
        raise ArchiveError("legacy_unreadable", f"无法读取旧归档文件: {exc}") from exc

    compressed_sha = sha256_hex(raw_bytes)
    lower = target.name.casefold()
    if lower.endswith(".zlib"):
        try:
            data = zlib.decompress(raw_bytes)
        except zlib.error as exc:
            raise ArchiveError(
                "legacy_corrupt", f"旧版 zlib 解压失败: {exc}",
                {"compressed_sha256": digest_label(compressed_sha), "size_bytes": len(raw_bytes)},
            ) from exc
        source_format = LEGACY_ZLIB_FORMAT
    elif lower.endswith(".gz") or lower.endswith(".gzip"):
        try:
            data = gzip.decompress(raw_bytes)
        except (OSError, EOFError, gzip.BadGzipFile) as exc:
            raise ArchiveError(
                "legacy_corrupt", f"旧版 gzip 解压失败: {exc}",
                {"compressed_sha256": digest_label(compressed_sha), "size_bytes": len(raw_bytes)},
            ) from exc
        source_format = "gzip"
    else:
        data = raw_bytes
        source_format = LEGACY_JSON_FORMAT

    try:
        text = data.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise ArchiveError("legacy_corrupt", f"旧归档不是 UTF-8 文本: {exc}") from exc
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError as exc:
        raise ArchiveError(
            "legacy_corrupt", f"旧归档 JSON 解析失败: {exc}",
            {"compressed_sha256": digest_label(compressed_sha), "size_bytes": len(raw_bytes)},
        ) from exc

    records = normalize_records(parsed)
    return {
        "records": records,
        "source_format": source_format,
        "compressed_sha256": digest_label(compressed_sha),
        "payload_sha256": digest_label(sha256_hex(data)),
        "size_bytes": len(raw_bytes),
        "event_ids": unique_event_ids(records),
        "record_count": len(records),
    }


def read_gzip_payload_or_error(path: str | os.PathLike[str]) -> dict[str, Any]:
    """读取并返回 gzip v2 候选的完整信息，失败时抛出 ArchiveError。"""
    info = read_gzip_v2(path)
    payload = info["payload"]
    verification = verify_gzip_payload(payload)
    return {**info, "verification": verification}
