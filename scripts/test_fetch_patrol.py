import json
import sys
import tempfile
import unittest
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parent))

from fetch_patrol import _parse_patrol_params, load_cached_autopatrol_groups
from compute_patrol import (
    collect_parquet_files,
    compute_patrol_metrics,
    load_revision_subset_by_ids,
    merge_patrol_outputs,
    patrol_parts_dir,
    resolve_revision_store,
    write_defaults_patrol_json,
)


class FetchPatrolTests(unittest.TestCase):
    def _write_revision_partition(self, root: Path, year_month: str, revisions: dict):
        path = root / "warehouse" / "frwiki" / f"year={year_month[:4]}" / f"year_month={year_month}" / "part-00000.parquet"
        path.parent.mkdir(parents=True, exist_ok=True)
        schema = pa.schema([
            ("revision_id", pa.int64()),
            ("event_timestamp", pa.string()),
            ("event_user_id", pa.int64()),
            ("event_user_text", pa.string()),
            ("page_namespace", pa.int32()),
            ("event_user_is_bot_by", pa.string()),
            ("event_user_is_anonymous", pa.bool_()),
            ("event_user_is_temporary", pa.bool_()),
        ])
        pq.write_table(pa.table(revisions, schema=schema), path, compression="zstd")

    def test_parse_legacy_patrol_params(self):
        cur, prev, is_auto = _parse_patrol_params("6556036\n6556016\n0")
        self.assertEqual(cur, 6556036)
        self.assertEqual(prev, 6556016)
        self.assertFalse(is_auto)

    def test_parse_php_serialized_patrol_params(self):
        cur, prev, is_auto = _parse_patrol_params(
            'a:3:{s:8:"4::curid";s:8:"29704253";s:9:"5::previd";s:1:"0";s:7:"6::auto";i:1;}'
        )
        self.assertEqual(cur, 29704253)
        self.assertEqual(prev, 0)
        self.assertTrue(is_auto)

    def test_parse_empty_php_serialized_patrol_params(self):
        cur, prev, is_auto = _parse_patrol_params("a:0:{}")
        self.assertEqual(cur, 0)
        self.assertEqual(prev, 0)
        self.assertFalse(is_auto)

    def test_load_cached_autopatrol_groups(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            meta_path = Path(tmpdir) / "autopatrol_groups.json"
            meta_path.write_text(json.dumps({"wiki": "nlwiki", "autopatrol_groups": ["autopatrolled", "sysop"]}))
            self.assertEqual(load_cached_autopatrol_groups(meta_path), ["autopatrolled", "sysop"])

    def test_merge_patrol_outputs_refreshes_root_metric(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            schema = pa.schema([
                ("year_month", pa.utf8()),
                ("wiki", pa.utf8()),
                ("page_namespace", pa.int32()),
                ("user_type", pa.utf8()),
                ("total_patrols", pa.int64()),
                ("unique_patrollers", pa.int32()),
                ("patrol_new_pages", pa.int64()),
                ("patrol_diffs", pa.int64()),
                ("median_latency_hours", pa.float64()),
                ("p90_latency_hours", pa.float64()),
                ("patrolled_revisions", pa.int64()),
                ("autopatrolled_revisions", pa.int64()),
                ("total_revisions", pa.int64()),
                ("patrol_coverage_pct", pa.float64()),
                ("adjusted_coverage_pct", pa.float64()),
                ("top1_pct", pa.float64()),
                ("min_patrollers_50pct", pa.int32()),
            ])
            for wiki, period in [("nlwiki", "2026-01"), ("svwiki", "2026-02")]:
                wiki_dir = output_dir / wiki
                wiki_dir.mkdir(parents=True, exist_ok=True)
                table = pa.table({
                    "year_month": [period],
                    "wiki": [wiki],
                    "page_namespace": [0],
                    "user_type": ["registered"],
                    "total_patrols": [1],
                    "unique_patrollers": [1],
                    "patrol_new_pages": [0],
                    "patrol_diffs": [1],
                    "median_latency_hours": [1.5],
                    "p90_latency_hours": [2.5],
                    "patrolled_revisions": [1],
                    "autopatrolled_revisions": [0],
                    "total_revisions": [1],
                    "patrol_coverage_pct": [100.0],
                    "adjusted_coverage_pct": [100.0],
                    "top1_pct": [100.0],
                    "min_patrollers_50pct": [1],
                }, schema=schema)
                pq.write_table(table, wiki_dir / "patrol.parquet", compression="zstd")

            merged_path = merge_patrol_outputs(output_dir)
            self.assertEqual(merged_path, output_dir / "patrol.parquet")

            merged = pq.read_table(merged_path)
            self.assertEqual(merged.num_rows, 2)
            self.assertEqual(sorted(merged.column("wiki").to_pylist()), ["nlwiki", "svwiki"])

    def test_write_defaults_patrol_json_uses_merged_patrol_data(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            merged_path = output_dir / "patrol.parquet"
            schema = pa.schema([
                ("year_month", pa.utf8()),
                ("wiki", pa.utf8()),
                ("page_namespace", pa.int32()),
                ("user_type", pa.utf8()),
                ("total_patrols", pa.int64()),
                ("unique_patrollers", pa.int32()),
                ("patrol_new_pages", pa.int64()),
                ("patrol_diffs", pa.int64()),
                ("median_latency_hours", pa.float64()),
                ("p90_latency_hours", pa.float64()),
                ("patrolled_revisions", pa.int64()),
                ("autopatrolled_revisions", pa.int64()),
                ("total_revisions", pa.int64()),
                ("patrol_coverage_pct", pa.float64()),
                ("adjusted_coverage_pct", pa.float64()),
                ("top1_pct", pa.float64()),
                ("min_patrollers_50pct", pa.int32()),
            ])
            table = pa.table({
                "year_month": ["2012-02", "2026-01", "2026-03"],
                "wiki": ["nlwiki", "nlwiki", "nlwiki"],
                "page_namespace": [0, 0, 0],
                "user_type": ["registered", "registered", "registered"],
                "total_patrols": [1, 2, 3],
                "unique_patrollers": [1, 2, 3],
                "patrol_new_pages": [0, 0, 0],
                "patrol_diffs": [1, 2, 3],
                "median_latency_hours": [1.0, 2.5, 99.0],
                "p90_latency_hours": [2.0, 3.5, 120.0],
                "patrolled_revisions": [1, 2, 3],
                "autopatrolled_revisions": [0, 4, 5],
                "total_revisions": [1, 10, 11],
                "patrol_coverage_pct": [100.0, 20.0, 30.0],
                "adjusted_coverage_pct": [100.0, 60.0, 70.0],
                "top1_pct": [100.0, 50.0, 40.0],
                "min_patrollers_50pct": [1, 2, 3],
            }, schema=schema)
            pq.write_table(table, merged_path, compression="zstd")

            defaults_path = write_defaults_patrol_json(output_dir, merged_path)
            defaults = json.loads(defaults_path.read_text())
            patrol = defaults["patrol"]

            self.assertEqual(defaults_path, output_dir / "defaults_patrol.json")
            self.assertEqual(defaults["defaultWiki"], "nlwiki")
            self.assertEqual(defaults["maxMonth"], "2026-03")
            self.assertEqual([row["period"] for row in patrol], ["2012", "2026"])
            self.assertEqual(patrol[-1]["median_latency_hours"], 50.75)
            self.assertEqual(patrol[-1]["patrolled_revisions"], 5.0)

    def test_collect_parquet_files_prefers_partitioned_layout(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            parquet_dir = Path(tmpdir)
            (parquet_dir / "legacy.parquet").write_bytes(b"legacy")
            part_file = parquet_dir / "year=2026" / "year_month=2026-02" / "part-00000.parquet"
            part_file.parent.mkdir(parents=True, exist_ok=True)
            part_file.write_bytes(b"partitioned")

            files = collect_parquet_files(parquet_dir)

            self.assertEqual(files, [str(part_file)])

    def test_resolve_revision_store_prefers_warehouse(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            data_dir = Path(tmpdir)
            parquet_file = data_dir / "parquet" / "frwiki" / "legacy.parquet"
            parquet_file.parent.mkdir(parents=True, exist_ok=True)
            parquet_file.write_bytes(b"legacy")
            warehouse_file = data_dir / "warehouse" / "frwiki" / "year=2026" / "year_month=2026-02" / "part-00000.parquet"
            warehouse_file.parent.mkdir(parents=True, exist_ok=True)
            warehouse_file.write_bytes(b"warehouse")

            label, path = resolve_revision_store(data_dir, "frwiki")

            self.assertEqual(label, "warehouse")
            self.assertEqual(path, data_dir / "warehouse" / "frwiki")

    def test_load_revision_subset_by_ids_handles_string_view_partitions(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            parquet_dir = Path(tmpdir) / "warehouse" / "frwiki" / "year=2007" / "year_month=2007-01"
            parquet_dir.mkdir(parents=True, exist_ok=True)
            schema = pa.schema([
                ("revision_id", pa.int64()),
                ("event_timestamp", pa.string_view()),
                ("event_user_id", pa.int64()),
                ("event_user_text", pa.string_view()),
                ("page_namespace", pa.int32()),
                ("event_user_is_bot_by", pa.string_view()),
                ("event_user_is_anonymous", pa.bool_()),
                ("event_user_is_temporary", pa.bool_()),
            ])
            table = pa.table({
                "revision_id": [100, 101, 202],
                "event_timestamp": ["2007-01-01 00:00:00", "2007-01-02 00:00:00", "2007-02-01 00:00:00"],
                "event_user_id": [1, 2, 3],
                "event_user_text": ["A", "B", "C"],
                "page_namespace": [0, 0, 0],
                "event_user_is_bot_by": [None, None, None],
                "event_user_is_anonymous": [False, False, False],
                "event_user_is_temporary": [False, False, False],
            }, schema=schema)
            pq.write_table(table, parquet_dir / "part-00000.parquet", compression="zstd")

            subset = load_revision_subset_by_ids(Path(tmpdir) / "warehouse" / "frwiki", {101, 999})

            self.assertIsNotNone(subset)
            self.assertEqual(subset.num_rows, 1)
            self.assertEqual(subset.column("revision_id").to_pylist(), [101])

    def test_compute_patrol_metrics_resumes_from_existing_month_parts(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            data_dir = root / "data"
            output_dir = root / "output"
            patrol_dir = data_dir / "patrol" / "frwiki"
            patrol_dir.mkdir(parents=True, exist_ok=True)

            patrol_schema = pa.schema([
                ("timestamp", pa.string()),
                ("current_revision_id", pa.int64()),
                ("prev_revision_id", pa.int64()),
                ("user", pa.string()),
            ])
            patrol_table = pa.table({
                "timestamp": ["2026-01-05 12:00:00", "2026-02-07 12:00:00"],
                "current_revision_id": [101, 101],
                "prev_revision_id": [100, 100],
                "user": ["PatrollerA", "PatrollerB"],
            }, schema=patrol_schema)
            pq.write_table(patrol_table, patrol_dir / "patrol.parquet", compression="zstd")

            rights_schema = pa.schema([
                ("timestamp", pa.string()),
                ("target_user", pa.string()),
                ("old_groups", pa.string()),
                ("new_groups", pa.string()),
            ])
            pq.write_table(pa.table({
                "timestamp": [],
                "target_user": [],
                "old_groups": [],
                "new_groups": [],
            }, schema=rights_schema), patrol_dir / "rights.parquet", compression="zstd")
            (patrol_dir / "autopatrol_groups.json").write_text(json.dumps({"autopatrol_groups": []}))

            self._write_revision_partition(data_dir, "2026-01", {
                "revision_id": [101, 102],
                "event_timestamp": ["2026-01-05 10:00:00", "2026-01-06 11:00:00"],
                "event_user_id": [1, 2],
                "event_user_text": ["EditorA", "EditorB"],
                "page_namespace": [0, 0],
                "event_user_is_bot_by": [None, None],
                "event_user_is_anonymous": [False, False],
                "event_user_is_temporary": [False, False],
            })
            self._write_revision_partition(data_dir, "2026-02", {
                "revision_id": [202],
                "event_timestamp": ["2026-02-07 09:00:00"],
                "event_user_id": [3],
                "event_user_text": ["EditorC"],
                "page_namespace": [0],
                "event_user_is_bot_by": [None],
                "event_user_is_anonymous": [False],
                "event_user_is_temporary": [False],
            })

            ok = compute_patrol_metrics("frwiki", data_dir, output_dir, limit_months=1)
            self.assertTrue(ok)
            part_dir = patrol_parts_dir(output_dir, "frwiki")
            self.assertEqual(sorted(path.stem for path in part_dir.glob("*.parquet")), ["2026-01"])

            ok = compute_patrol_metrics("frwiki", data_dir, output_dir)
            self.assertTrue(ok)
            self.assertEqual(sorted(path.stem for path in part_dir.glob("*.parquet")), ["2026-01", "2026-02"])

            final_table = pq.read_table(output_dir / "frwiki" / "patrol.parquet")
            self.assertEqual(final_table.num_rows, 2)
            self.assertEqual(final_table.column("year_month").to_pylist(), ["2026-01", "2026-02"])
            self.assertEqual(final_table.column("patrolled_revisions").to_pylist(), [1, 0])
            self.assertEqual(final_table.column("median_latency_hours").to_pylist(), [2.0, 794.0])


if __name__ == "__main__":
    unittest.main()
