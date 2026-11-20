"""归档格式迁移与分区恢复测试。

覆盖升级部署的真实序列：

* 旧版 zlib → 带分区摘要的 gzip v2，迁移重试不制造重复版本；
* 只有通过校验的 gzip v2 才能被恢复，损坏/未校验文件不能冒充可用数据；
* 同一批次两个分区，订单已迁移且旧文件损坏，库存尚未迁移；
* 恢复回执标明实际使用的压缩文件，恢复数据落湖并按事件身份去重；
* 恢复清单可按批次回查已恢复的源记录；
* 旧版恢复环境回滚后，来源行仍能反查原回执。

测试通过 ``APP_DATA_STORE_DIR`` 把归档、数据湖和 JSON 仓库隔离到临时目录。
"""
from __future__ import annotations

import gzip
import json
import os
import tempfile
import unittest
import zlib
from pathlib import Path

from app.paths import archive_root, lake_root
from app.services import archive_state
from app.services.archive_migration import ArchiveMigrationService
from app.services.data_lake_sink import DataLakeSinkService

BATCH = "arch_20261120"
BATCH_DATE = "2026-11-20"


def _write_legacy(path: Path, records: list[dict], corrupt: bool = False) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    if corrupt:
        path.write_bytes(b"this-is-not-zlib")
    else:
        path.write_bytes(zlib.compress(json.dumps(records, ensure_ascii=False).encode()))
    return path


class ArchiveMigrationTestBase(unittest.TestCase):
    """每个测试使用独立数据目录，并在结束后恢复环境变量。"""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self._previous = os.environ.get("APP_DATA_STORE_DIR")
        os.environ["APP_DATA_STORE_DIR"] = self._tmp.name
        self.batch_dir = Path(archive_root()) / BATCH
        self.batch_dir.mkdir(parents=True, exist_ok=True)
        self.service = ArchiveMigrationService()
        self.sink = DataLakeSinkService()

    def tearDown(self) -> None:
        if self._previous is None:
            os.environ.pop("APP_DATA_STORE_DIR", None)
        else:
            os.environ["APP_DATA_STORE_DIR"] = self._previous
        self._tmp.cleanup()

    def seed_two_partitions(self) -> None:
        _write_legacy(
            self.batch_dir / "orders_2026_11_20.json.zlib",
            [
                {"event_id": "ORD-1001", "order_id": "SO-1", "amount": 120},
                {"event_id": "ORD-1002", "order_id": "SO-2", "amount": 340},
            ],
        )
        _write_legacy(
            self.batch_dir / "inventory_2026_11_20.json.zlib",
            [
                {"event_id": "INV-2001", "sku": "SKU-9", "qty": 12},
                {"event_id": "INV-2002", "sku": "SKU-7", "qty": 4},
            ],
        )

    def migrate_and_break_orders(self) -> dict:
        self.seed_two_partitions()
        result = self.service.migrate_batch(BATCH, ["order"], requester="deploy")
        self.assertEqual(result["results"][0]["status"], "migrated")
        # 订单旧压缩文件在迁移后被损坏。
        (self.batch_dir / "orders_2026_11_20.json.zlib").write_bytes(b"corrupted-after-migration")
        return result


class TestArchiveMigration(ArchiveMigrationTestBase):
    def test_migration_creates_gzip_v2_with_partition_digest(self):
        self.seed_two_partitions()
        result = self.service.migrate_batch(BATCH, ["order"])

        self.assertTrue(result["ok"])
        migrated = result["results"][0]
        self.assertEqual(migrated["status"], "migrated")
        asset = migrated["asset"]
        self.assertEqual(asset["format"], "gzip-v2")
        self.assertEqual(asset["format_version"], 2)
        self.assertEqual(asset["status"], "verified")
        self.assertTrue(asset["partition_digest"].startswith("sha256:"))
        self.assertEqual(asset["event_ids"], ["ORD-1001", "ORD-1002"])

        gzip_path = Path(asset["path"])
        self.assertTrue(gzip_path.name.endswith(".gzip"))
        with gzip.open(gzip_path, "rb") as f:
            payload = json.load(f)
        self.assertEqual(payload["partition"], "order")
        self.assertEqual(payload["archive_date"], BATCH_DATE)
        self.assertEqual(payload["event_ids"], ["ORD-1001", "ORD-1002"])
        self.assertEqual(payload["partition_digest"], asset["partition_digest"])

    def test_gzip_preserves_date_partition_and_event_identity(self):
        self.seed_two_partitions()
        self.service.migrate_batch(BATCH, ["order"])

        inspected = self.service.inspect_archive(BATCH, "订单分区")
        self.assertTrue(inspected["ok"])
        self.assertEqual(inspected["partition"], "order")
        self.assertEqual(inspected["archive_date"], BATCH_DATE)
        self.assertEqual(inspected["payload_archive_date"], BATCH_DATE)
        self.assertEqual(inspected["gzip_header_name"], "order.json")
        self.assertEqual(inspected["event_ids"], ["ORD-1001", "ORD-1002"])
        self.assertEqual(inspected["computed_digest"], inspected["stored_digest"])

    def test_migration_retry_does_not_duplicate_versions(self):
        self.seed_two_partitions()
        first = self.service.migrate_batch(BATCH, ["order"])
        second = self.service.migrate_batch(BATCH, ["order"])

        self.assertEqual(first["results"][0]["status"], "migrated")
        self.assertEqual(second["results"][0]["status"], "reused")
        assets = archive_state.list_assets(batch_id=BATCH, partition="order")
        gzip_assets = [a for a in assets if a["format"] == "gzip-v2"]
        self.assertEqual(len(gzip_assets), 1)
        self.assertEqual(gzip_assets[0]["version"], 1)

    def test_migration_retry_after_legacy_corruption_reuses_verified_asset(self):
        self.migrate_and_break_orders()
        retry = self.service.migrate_batch(BATCH, ["order"])

        self.assertEqual(retry["results"][0]["status"], "reused")
        self.assertEqual(retry["results"][0]["reason"], "already_verified_legacy_unavailable")
        self.assertEqual(retry["results"][0]["asset"]["status"], "verified")

    def test_unverified_gzip_is_not_reused_and_new_version_is_written(self):
        self.seed_two_partitions()
        self.service.migrate_batch(BATCH, ["order"], verify=False)
        unverified = [
            a for a in archive_state.list_assets(batch_id=BATCH, partition="order")
            if a["format"] == "gzip-v2"
        ]
        self.assertEqual(len(unverified), 1)
        self.assertEqual(unverified[0]["status"], "unverified")

        retry = self.service.migrate_batch(BATCH, ["order"], verify=True)
        self.assertEqual(retry["results"][0]["status"], "migrated")
        self.assertEqual(retry["results"][0]["asset"]["version"], 2)
        self.assertEqual(retry["results"][0]["asset"]["status"], "verified")

    def test_verify_batch_detects_tampered_gzip(self):
        self.seed_two_partitions()
        self.service.migrate_batch(BATCH, ["order"])
        asset = self.service._find_verified_asset(BATCH, "order")
        Path(asset["path"]).write_bytes(b"tampered")

        result = self.service.verify_batch(BATCH, ["order"])
        self.assertFalse(result["ok"])
        self.assertIn(result["results"][0]["status"], ("corrupt", "verification_failed"))
        self.assertTrue(result["results"][0]["errors"])


class TestPartitionedRestore(ArchiveMigrationTestBase):
    def test_two_partition_deployment_scenario(self):
        """订单已迁完且旧文件损坏，库存未迁移：两个分区都按实际文件恢复。"""
        self.migrate_and_break_orders()

        result = self.service.restore(
            BATCH, ["订单分区", "库存分区"], requester="deploy-bot",
        )

        self.assertTrue(result["ok"])
        orders = result["partitions"]["order"]
        inventory = result["partitions"]["inventory"]

        self.assertEqual(orders["status"], "restored")
        self.assertEqual(orders["source_file"], "order_20261120.v1.gzip")
        self.assertEqual(orders["source_format"], "gzip-v2")
        self.assertTrue(orders["source_verified"])
        self.assertEqual(orders["restored_count"], 2)

        self.assertEqual(inventory["status"], "restored")
        self.assertEqual(inventory["source_file"], "inventory_2026_11_20.json.zlib")
        self.assertEqual(inventory["source_format"], "zlib")
        self.assertFalse(inventory["source_verified"])
        self.assertEqual(inventory["restored_count"], 2)

        # 回执要标出被拒绝的候选文件，损坏的旧文件和未校验新版都不能冒充数据。
        order_rejected = {(c["file"], c["reason"]) for c in orders["rejected_candidates"]}
        self.assertIn(("orders_2026_11_20.json.zlib", "legacy_corrupt"), order_rejected)

    def test_restore_writes_to_original_lake_partition(self):
        self.migrate_and_break_orders()
        result = self.service.restore(BATCH, ["order", "inventory"])

        for receipt in result["receipts"]:
            self.assertEqual(
                receipt["lake_partition"],
                f"events/{receipt['partition']}/2026/11/20",
            )

        lake_orders = self.service.query_lake("order")
        lake_inventory = self.service.query_lake("inventory")
        self.assertEqual(lake_orders["event_ids"], ["ORD-1001", "ORD-1002"])
        self.assertEqual(lake_inventory["event_ids"], ["INV-2001", "INV-2002"])

        rows = lake_orders["records"]
        self.assertEqual({row["archive_partition"] for row in rows}, {"order"})
        self.assertEqual(
            {row["restore_receipt_id"] for row in rows},
            {result["partitions"]["order"]["receipt_id"]},
        )

    def test_restore_is_idempotent_across_deploy_and_rollback(self):
        self.migrate_and_break_orders()
        first = self.service.restore(BATCH, ["order", "inventory"], requester="deploy")
        second = self.service.restore(BATCH, ["order", "inventory"], requester="deploy-retry")

        self.assertEqual(first["partitions"]["order"]["restored_count"], 2)
        self.assertEqual(second["partitions"]["order"]["restored_count"], 0)
        self.assertEqual(second["partitions"]["order"]["skipped_count"], 2)
        self.assertEqual(second["partitions"]["order"]["status"], "deduplicated")

        lake_orders = self.service.query_lake("order")
        self.assertEqual(lake_orders["total"], 2)
        self.assertEqual(
            lake_orders["receipt_ids"], [first["partitions"]["order"]["receipt_id"]],
        )

    def test_unverified_gzip_and_corrupt_legacy_cannot_restore(self):
        _write_legacy(
            self.batch_dir / "stock.json.zlib", [{"event_id": "INV-3001"}],
        )
        self.service.migrate_batch(BATCH, ["inventory"], verify=False)
        (self.batch_dir / "stock.json.zlib").write_bytes(b"broken")

        result = self.service.restore(BATCH, ["inventory"])

        receipt = result["partitions"]["inventory"]
        self.assertEqual(receipt["status"], "failed")
        self.assertEqual(receipt["reason"], "legacy_corrupt")
        self.assertIsNone(receipt["source_file"])
        self.assertEqual(receipt["restored_count"], 0)
        reasons = {(c["file"], c["reason"]) for c in receipt["rejected_candidates"]}
        self.assertIn(("inventory_20261120.v1.gzip", "not_verified"), reasons)
        self.assertEqual(self.service.query_lake("inventory")["total"], 0)

    def test_tampered_verified_gzip_is_rejected(self):
        self.seed_two_partitions()
        self.service.migrate_batch(BATCH, ["order"])
        asset = self.service._find_verified_asset(BATCH, "order")
        Path(asset["path"]).write_bytes(b"tampered-after-verification")

        result = self.service.restore(BATCH, ["order"], mode="gzip")
        receipt = result["partitions"]["order"]
        self.assertEqual(receipt["status"], "failed")
        self.assertEqual(receipt["reason"], "no_verified_gzip")
        reasons = {(c["file"], c["reason"]) for c in receipt["rejected_candidates"]}
        self.assertIn(("order_20261120.v1.gzip", "integrity_mismatch"), reasons)
        self.assertEqual(self.service.query_lake("order")["total"], 0)


class TestRestoreManifestAndReceipts(ArchiveMigrationTestBase):
    def test_manifest_records_restored_source_rows_per_partition(self):
        self.migrate_and_break_orders()
        result = self.service.restore(BATCH, ["order", "inventory"], requester="deploy")
        manifest = self.service.get_manifest(BATCH)

        orders = manifest["partitions"]["order"]
        self.assertEqual(orders["status"], "restored")
        self.assertEqual(orders["source_file"], "order_20261120.v1.gzip")
        self.assertEqual(orders["event_ids"], ["ORD-1001", "ORD-1002"])
        self.assertEqual(orders["restored_count"], 2)
        self.assertEqual(orders["last_receipt_id"], result["partitions"]["order"]["receipt_id"])

        inventory = manifest["partitions"]["inventory"]
        self.assertEqual(inventory["source_file"], "inventory_2026_11_20.json.zlib")
        self.assertEqual(inventory["event_ids"], ["INV-2001", "INV-2002"])

        latest_run = manifest["last_restore_run"]
        self.assertEqual(latest_run["status"], "completed")
        self.assertEqual(set(latest_run["requested_partitions"]), {"order", "inventory"})
        self.assertTrue(Path(manifest["manifest_path"]).exists())

    def test_failed_partition_keeps_previous_success_in_manifest(self):
        self.seed_two_partitions()
        self.service.migrate_batch(BATCH, ["order"])
        self.service.restore(BATCH, ["order"])
        (self.batch_dir / "order_20261120.v1.gzip").write_bytes(b"tampered")

        failed = self.service.restore(BATCH, ["order"], mode="gzip")
        self.assertEqual(failed["partitions"]["order"]["status"], "failed")

        entry = self.service.get_manifest(BATCH)["partitions"]["order"]
        self.assertEqual(entry["status"], "failed")
        self.assertEqual(entry["last_failure"]["reason"], "no_verified_gzip")
        # 上一次成功的来源信息仍保留，方便部署中核对。
        self.assertEqual(entry["source_file"], "order_20261120.v1.gzip")
        self.assertEqual(entry["event_ids"], ["ORD-1001", "ORD-1002"])

    def test_receipt_rows_and_legacy_view(self):
        self.migrate_and_break_orders()
        result = self.service.restore(BATCH, ["order", "inventory"])
        receipt = result["partitions"]["order"]

        rows = self.service.receipt_rows(receipt["receipt_id"])
        self.assertTrue(rows["ok"])
        self.assertEqual(len(rows["rows"]), 2)
        self.assertTrue(all(
            row["restore_receipt_id"] == receipt["receipt_id"] for row in rows["rows"]
        ))

        legacy = self.service.legacy_receipt_view(receipt["receipt_id"])
        self.assertTrue(legacy["ok"])
        self.assertEqual(legacy["archive_file"], "order_20261120.v1.gzip")
        self.assertIn("ORD-1001", legacy["data"])

    def test_source_row_resolves_original_receipt_after_rollback(self):
        self.migrate_and_break_orders()
        deploy = self.service.restore(BATCH, ["order", "inventory"], requester="deploy")
        order_receipt = deploy["partitions"]["order"]["receipt_id"]
        inventory_receipt = deploy["partitions"]["inventory"]["receipt_id"]

        # 回滚到旧版恢复环境：订单旧文件已损坏，回执必须明确失败而不是伪造数据；
        # 库存旧文件仍可读，但重复恢复会被去重。
        rollback = self.service.restore(
            BATCH, ["order", "inventory"], mode="legacy", requester="old-env",
        )
        self.assertEqual(rollback["partitions"]["order"]["status"], "failed")
        self.assertEqual(rollback["partitions"]["order"]["reason"], "no_readable_legacy")
        self.assertEqual(rollback["partitions"]["inventory"]["skipped_count"], 2)

        # 已恢复的来源行仍能查到原回执。
        order_row = self.service.source_row_receipt("order", "ORD-1001")
        self.assertTrue(order_row["ok"])
        self.assertEqual(order_row["receipt"]["receipt_id"], order_receipt)
        self.assertEqual(order_row["receipt"]["source_file"], "order_20261120.v1.gzip")

        inventory_row = self.service.source_row_receipt("inventory", "INV-2001")
        self.assertTrue(inventory_row["ok"])
        self.assertEqual(inventory_row["receipt"]["receipt_id"], inventory_receipt)
        self.assertEqual(
            inventory_row["receipt"]["source_file"], "inventory_2026_11_20.json.zlib",
        )

        # 回滚不会多灌同一个源记录。
        self.assertEqual(self.service.query_lake("order")["total"], 2)
        self.assertEqual(self.service.query_lake("inventory")["total"], 2)


class TestArchiveApi(unittest.TestCase):
    """直接调用归档路由函数，验证恢复入口的返回结构。"""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self._previous = os.environ.get("APP_DATA_STORE_DIR")
        os.environ["APP_DATA_STORE_DIR"] = self._tmp.name
        batch_dir = Path(archive_root()) / BATCH
        batch_dir.mkdir(parents=True, exist_ok=True)
        _write_legacy(
            batch_dir / "orders_2026_11_20.json.zlib",
            [{"event_id": "ORD-1"}, {"event_id": "ORD-2"}],
        )
        _write_legacy(
            batch_dir / "inventory_2026_11_20.json.zlib",
            [{"event_id": "INV-1"}],
        )
        ArchiveMigrationService().migrate_batch(BATCH, ["order"])
        (batch_dir / "orders_2026_11_20.json.zlib").write_bytes(b"corrupted")

    def tearDown(self) -> None:
        if self._previous is None:
            os.environ.pop("APP_DATA_STORE_DIR", None)
        else:
            os.environ["APP_DATA_STORE_DIR"] = self._previous
        self._tmp.cleanup()

    def test_archive_routes_registered_before_generic_resource(self):
        from app.main import app
        from app.routers import ROUTERS, archive
        self.assertIs(ROUTERS[0], archive)
        self.assertIsNotNone(app)

    def test_restore_endpoint_returns_per_partition_receipts(self):
        from app.routers.archive import RestoreRequest, restore_partitions

        body = restore_partitions(RestoreRequest(
            batch_id=BATCH,
            partitions=["order", "inventory"],
            requester="deploy",
        ))
        self.assertTrue(body["ok"])
        self.assertEqual(body["partitions"]["order"]["source_file"], "order_20261120.v1.gzip")
        self.assertEqual(
            body["partitions"]["inventory"]["source_file"],
            "inventory_2026_11_20.json.zlib",
        )

        from app.routers.archive import get_manifest, query_lake, get_source_row_receipt
        lake = query_lake("order")
        self.assertEqual(lake["total"], 2)
        self.assertEqual(lake["event_ids"], ["ORD-1", "ORD-2"])

        manifest = get_manifest(BATCH)["manifest"]
        self.assertEqual(manifest["partitions"]["order"]["restored_count"], 2)
        self.assertEqual(manifest["partitions"]["inventory"]["restored_count"], 1)

        row = get_source_row_receipt("order", "ORD-1")
        self.assertTrue(row["ok"])
        self.assertEqual(
            row["receipt"]["receipt_id"], body["partitions"]["order"]["receipt_id"],
        )


if __name__ == "__main__":
    unittest.main()
