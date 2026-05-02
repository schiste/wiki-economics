#!/usr/bin/env python3
"""
Compute patrol metrics by joining patrol log data with revision data.

Reads:
  - data/patrol/<wiki>/patrol.parquet         (from fetch_patrol.py)
  - data/patrol/<wiki>/rights.parquet         (from fetch_patrol.py)
  - data/patrol/<wiki>/autopatrol_groups.json (from fetch_patrol.py)
  - data/warehouse/<wiki>/.../*.parquet       (preferred upgraded revision store)
  - data/parquet/<wiki>/*.parquet             (legacy revision store fallback)

Writes:
  - output/<wiki>/_patrol_parts/<YYYY-MM>.parquet  (resumable monthly patrol shards)
  - output/<wiki>/patrol.parquet                   (merged monthly patrol metrics)

Metrics per month:
  - total_patrols: number of patrol events
  - unique_patrollers: distinct patrol users
  - patrol_new_pages: patrols of new pages (previd=0)
  - patrol_diffs: patrols of existing page diffs
  - median_latency_hours: median time between edit and patrol
  - p90_latency_hours: 90th percentile latency
  - patrolled_revisions: revisions that were manually patrolled
  - autopatrolled_revisions: revisions by users with autopatrol right
  - total_revisions: total revisions in that month
  - patrol_coverage_pct: manually patrolled / total * 100
  - adjusted_coverage_pct: (patrolled + autopatrolled) / total * 100
  - top1_pct: share of patrols by the top patroller
  - min_patrollers_50pct: fragility — fewest patrollers for 50% of work
"""

import argparse
import json
import os
import sys
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

REVISION_SCHEMA = pa.schema([
    ("revision_id", pa.int64()),
    ("event_timestamp", pa.string()),
    ("event_user_id", pa.int64()),
    ("event_user_text", pa.string()),
    ("page_namespace", pa.int32()),
    ("event_user_is_bot_by", pa.string()),
    ("event_user_is_anonymous", pa.bool_()),
    ("event_user_is_temporary", pa.bool_()),
])
PATROL_SCHEMA = pa.schema([
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
REVISION_COLUMNS = [
    "revision_id", "event_timestamp", "event_user_id", "event_user_text",
    "page_namespace", "event_user_is_bot_by", "event_user_is_anonymous", "event_user_is_temporary",
]


def parse_ts(s):
    if not s:
        return None
    try:
        s = s.replace("T", " ").rstrip("Z").split(".")[0]
        return datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
    except (ValueError, TypeError):
        return None


def year_month_from_utf8(array):
    """Extract YYYY-MM from string-ish Arrow arrays."""
    return pc.utf8_slice_codeunits(pc.cast(array, pa.string()), 0, 7)


def collect_parquet_files(parquet_dir):
    """Collect parquet files, preferring the partitioned layout when both layouts exist."""
    flat_files = []
    partitioned_files = []

    for root, dirs, filenames in os.walk(parquet_dir):
        # Skip marker directories
        dirs[:] = [d for d in dirs if not d.startswith("_")]
        for f in filenames:
            if f.endswith(".parquet"):
                path = Path(root) / f
                rel_parts = path.relative_to(parquet_dir).parts[:-1]
                if any(part.startswith("year=") or part.startswith("year_month=") for part in rel_parts):
                    partitioned_files.append(str(path))
                else:
                    flat_files.append(str(path))

    files = partitioned_files or flat_files
    files.sort()
    return files


def load_parquet_files(parquet_dir, columns):
    """Load specific columns from all parquet files under a directory (recursive)."""
    files = collect_parquet_files(parquet_dir)
    if not files:
        return None
    # Use ParquetFile.read() to avoid dataset-style partition inference on a single file path.
    tables = [pq.ParquetFile(f).read(columns=columns) for f in files]
    return pa.concat_tables(tables)


def resolve_revision_store(data_dir, wiki):
    """Return the richest available revision store for patrol computations."""
    warehouse_dir = data_dir / "warehouse" / wiki
    parquet_dir = data_dir / "parquet" / wiki

    for label, path in (("warehouse", warehouse_dir), ("parquet", parquet_dir)):
        if path.exists() and collect_parquet_files(path):
            return label, path
    return None, None


def merge_patrol_outputs(output_dir):
    """Refresh the dashboard-facing combined patrol dataset from per-wiki outputs."""
    metric_files = sorted(output_dir.glob("*/patrol.parquet"))
    if not metric_files:
        return None
    tables = [pq.read_table(path) for path in metric_files]
    combined = pa.concat_tables(tables)
    merged_path = output_dir / "patrol.parquet"
    pq.write_table(combined, merged_path, compression="zstd")
    print(f"  Merged {len(metric_files):,} wiki patrol outputs into {merged_path}")
    return merged_path


def patrol_parts_dir(output_dir, wiki):
    return output_dir / wiki / "_patrol_parts"


def patrol_part_path(output_dir, wiki, year_month):
    return patrol_parts_dir(output_dir, wiki) / f"{year_month}.parquet"


def list_patrol_part_files(output_dir, wiki):
    parts_dir = patrol_parts_dir(output_dir, wiki)
    if not parts_dir.exists():
        return []
    files = sorted(parts_dir.glob("*.parquet"))
    return files


def existing_patrol_months(output_dir, wiki):
    return {path.stem for path in list_patrol_part_files(output_dir, wiki)}


def write_patrol_part(output_dir, wiki, year_month, table):
    parts_dir = patrol_parts_dir(output_dir, wiki)
    parts_dir.mkdir(parents=True, exist_ok=True)
    part_path = patrol_part_path(output_dir, wiki, year_month)
    pq.write_table(table, part_path, compression="zstd")
    return part_path


def merge_wiki_patrol_parts(output_dir, wiki):
    part_files = list_patrol_part_files(output_dir, wiki)
    if not part_files:
        return None
    tables = [pq.read_table(path) for path in part_files]
    combined = pa.concat_tables(tables)
    out_dir = output_dir / wiki
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "patrol.parquet"
    pq.write_table(combined, out_path, compression="zstd")
    print(f"  Wrote merged patrol metric with {combined.num_rows:,} rows to {out_path}")
    return out_path


def bootstrap_patrol_parts_from_final(output_dir, wiki):
    parts_dir = patrol_parts_dir(output_dir, wiki)
    if parts_dir.exists() and list(parts_dir.glob("*.parquet")):
        return
    final_path = output_dir / wiki / "patrol.parquet"
    if not final_path.exists():
        return

    table = pq.read_table(final_path)
    if table.num_rows == 0:
        parts_dir.mkdir(parents=True, exist_ok=True)
        return

    print(f"Bootstrapping patrol month shards from existing {final_path}")
    months = sorted({value for value in table.column("year_month").to_pylist() if value})
    for year_month in months:
        mask = pc.equal(table.column("year_month"), pa.scalar(year_month))
        month_table = table.filter(mask)
        write_patrol_part(output_dir, wiki, year_month, month_table)


def write_defaults_patrol_json(output_dir, merged_path=None):
    """Write the baked patrol dashboard defaults as a real JSON artifact."""
    if merged_path is None:
        merged_path = output_dir / "patrol.parquet"
    if not merged_path.exists():
        return None

    table = pq.read_table(merged_path)
    rows = [
        dict(zip(table.column_names, values))
        for values in zip(*(table.column(name).to_pylist() for name in table.column_names))
    ]

    wikis = sorted({row["wiki"] for row in rows})
    default_wiki = wikis[0] if wikis else None
    max_month = max(
        (row["year_month"] for row in rows if row["year_month"]),
        default=None,
    )
    ns_by_wiki = [
        {"wiki": wiki, "page_namespace": ns}
        for wiki in wikis
        for ns in sorted({row["page_namespace"] for row in rows if row["wiki"] == wiki})
    ]
    range_by_wiki = []
    for wiki in wikis:
        months = sorted(
            {
                row["year_month"]
                for row in rows
                if row["wiki"] == wiki and row["year_month"]
            }
        )
        if months:
            range_by_wiki.append({"wiki": wiki, "mn": months[0], "mx": months[-1]})

    yearly = defaultdict(
        lambda: {
            "total_patrols": 0.0,
            "unique_patrollers": 0.0,
            "patrol_new_pages": 0.0,
            "patrol_diffs": 0.0,
            "median_latency_sum": 0.0,
            "median_latency_count": 0,
            "p90_latency_sum": 0.0,
            "p90_latency_count": 0,
            "patrolled_revisions": 0.0,
            "autopatrolled_revisions": 0.0,
            "total_revisions": 0.0,
            "patrol_coverage_sum": 0.0,
            "patrol_coverage_count": 0,
            "adjusted_coverage_sum": 0.0,
            "adjusted_coverage_count": 0,
            "top1_sum": 0.0,
            "top1_count": 0,
            "min_patrollers_50pct": 0.0,
        }
    )

    for row in rows:
        if row["wiki"] != default_wiki:
            continue
        if row["page_namespace"] != 0:
            continue
        if row["user_type"] != "registered":
            continue

        period = row["year_month"][:4]
        entry = yearly[period]
        entry["total_patrols"] += float(row["total_patrols"] or 0)
        entry["unique_patrollers"] += float(row["unique_patrollers"] or 0)
        entry["patrol_new_pages"] += float(row["patrol_new_pages"] or 0)
        entry["patrol_diffs"] += float(row["patrol_diffs"] or 0)
        entry["patrolled_revisions"] += float(row["patrolled_revisions"] or 0)
        entry["autopatrolled_revisions"] += float(row["autopatrolled_revisions"] or 0)
        entry["total_revisions"] += float(row["total_revisions"] or 0)
        entry["min_patrollers_50pct"] += float(row["min_patrollers_50pct"] or 0)

        if row["median_latency_hours"] is not None:
            entry["median_latency_sum"] += float(row["median_latency_hours"])
            entry["median_latency_count"] += 1
        if row["p90_latency_hours"] is not None:
            entry["p90_latency_sum"] += float(row["p90_latency_hours"])
            entry["p90_latency_count"] += 1
        if row["patrol_coverage_pct"] is not None:
            entry["patrol_coverage_sum"] += float(row["patrol_coverage_pct"])
            entry["patrol_coverage_count"] += 1
        if row["adjusted_coverage_pct"] is not None:
            entry["adjusted_coverage_sum"] += float(row["adjusted_coverage_pct"])
            entry["adjusted_coverage_count"] += 1
        if row["top1_pct"] is not None:
            entry["top1_sum"] += float(row["top1_pct"])
            entry["top1_count"] += 1

    patrol = []
    for period in sorted(yearly):
        entry = yearly[period]
        patrol.append({
            "period": period,
            "total_patrols": entry["total_patrols"],
            "unique_patrollers": entry["unique_patrollers"],
            "patrol_new_pages": entry["patrol_new_pages"],
            "patrol_diffs": entry["patrol_diffs"],
            "median_latency_hours": (
                entry["median_latency_sum"] / entry["median_latency_count"]
                if entry["median_latency_count"] > 0
                else None
            ),
            "p90_latency_hours": (
                entry["p90_latency_sum"] / entry["p90_latency_count"]
                if entry["p90_latency_count"] > 0
                else None
            ),
            "patrolled_revisions": entry["patrolled_revisions"],
            "autopatrolled_revisions": entry["autopatrolled_revisions"],
            "total_revisions": entry["total_revisions"],
            "patrol_coverage_pct": (
                entry["patrol_coverage_sum"] / entry["patrol_coverage_count"]
                if entry["patrol_coverage_count"] > 0
                else None
            ),
            "adjusted_coverage_pct": (
                entry["adjusted_coverage_sum"] / entry["adjusted_coverage_count"]
                if entry["adjusted_coverage_count"] > 0
                else None
            ),
            "top1_pct": (
                entry["top1_sum"] / entry["top1_count"]
                if entry["top1_count"] > 0
                else None
            ),
            "min_patrollers_50pct": entry["min_patrollers_50pct"],
        })

    defaults = {
        "defaultWiki": default_wiki,
        "maxMonth": max_month,
        "wikis": [{"wiki": wiki} for wiki in wikis],
        "nsByWiki": ns_by_wiki,
        "rangeByWiki": range_by_wiki,
        "patrol": patrol,
    }

    defaults_path = output_dir / "defaults_patrol.json"
    defaults_path.write_text(json.dumps(defaults, separators=(",", ":")))
    print(f"  Wrote baked patrol defaults to {defaults_path}")
    return defaults_path


def collect_month_partition_files(parquet_dir, year_month):
    files = []
    for root, dirs, filenames in os.walk(parquet_dir):
        dirs[:] = [d for d in dirs if not d.startswith("_")]
        root_path = Path(root)
        rel_parts = root_path.relative_to(parquet_dir).parts if root_path != parquet_dir else ()
        if f"year_month={year_month}" not in rel_parts:
            continue
        for filename in filenames:
            if filename.endswith(".parquet"):
                files.append(str(root_path / filename))
    files.sort()
    return files


def read_parquet_tables(files, columns):
    if not files:
        return None
    tables = [pq.ParquetFile(path).read(columns=columns) for path in files]
    return pa.concat_tables(tables)


def load_revision_month_data(parquet_dir, year_month):
    partition_files = collect_month_partition_files(parquet_dir, year_month)
    if partition_files:
        return read_parquet_tables(partition_files, REVISION_COLUMNS)

    table = load_parquet_files(parquet_dir, REVISION_COLUMNS)
    if table is None:
        return None
    mask = pc.equal(year_month_from_utf8(table.column("event_timestamp")), pa.scalar(year_month))
    return table.filter(mask)


def load_revision_subset_by_ids(parquet_dir, revision_ids):
    filtered_ids = sorted({rid for rid in revision_ids if rid is not None})
    if not filtered_ids:
        return None

    files = collect_parquet_files(parquet_dir)
    if not files:
        return None

    tables = []
    chunk_size = 50_000
    for offset in range(0, len(filtered_ids), chunk_size):
        chunk = filtered_ids[offset: offset + chunk_size]
        chunk_min = chunk[0]
        chunk_max = chunk[-1]
        chunk_ids = set(chunk)
        for path in files:
            parquet_file = pq.ParquetFile(path)
            revision_column_index = parquet_file.schema_arrow.get_field_index("revision_id")
            if revision_column_index < 0:
                continue

            matching_row_groups = []
            for row_group_index in range(parquet_file.metadata.num_row_groups):
                column_meta = parquet_file.metadata.row_group(row_group_index).column(revision_column_index)
                stats = column_meta.statistics
                if stats is not None and stats.has_min_max:
                    if stats.max < chunk_min or stats.min > chunk_max:
                        continue
                matching_row_groups.append(row_group_index)

            if not matching_row_groups:
                continue

            table = parquet_file.read_row_groups(matching_row_groups, columns=REVISION_COLUMNS)
            if table.num_rows == 0:
                continue

            selected_rows = {field.name: [] for field in REVISION_SCHEMA}
            revision_ids_column = table.column("revision_id")
            matched_rows = 0
            for row_index in range(table.num_rows):
                revision_id = revision_ids_column[row_index].as_py()
                if revision_id not in chunk_ids:
                    continue
                matched_rows += 1
                for field_name in selected_rows:
                    selected_rows[field_name].append(table.column(field_name)[row_index].as_py())

            if matched_rows == 0:
                continue

            tables.append(pa.table(selected_rows, schema=REVISION_SCHEMA))
    if not tables:
        return None
    return pa.concat_tables(tables)


def empty_patrol_table():
    return pa.table({field.name: [] for field in PATROL_SCHEMA}, schema=PATROL_SCHEMA)


def empty_revision_table():
    return pa.table({field.name: [] for field in REVISION_SCHEMA}, schema=REVISION_SCHEMA)


def build_patrol_month_table(wiki, year_month, patrol_month, ap_intervals, revision_store_dir):
    revisions = load_revision_month_data(revision_store_dir, year_month)
    if revisions is None:
        revisions = empty_revision_table()

    rev_id_arr = revisions.column("revision_id")
    rev_ts_arr = revisions.column("event_timestamp")
    rev_ns_arr = revisions.column("page_namespace")
    rev_bot_arr = revisions.column("event_user_is_bot_by")
    rev_anon_arr = revisions.column("event_user_is_anonymous")
    rev_temp_arr = revisions.column("event_user_is_temporary")
    rev_uid_arr = revisions.column("event_user_id")
    rev_utext_arr = revisions.column("event_user_text")

    month_revision_ids = {rid for rid in rev_id_arr.to_pylist() if rid is not None}
    patrolled_rev_ids = {rid for rid in patrol_month.column("current_revision_id").to_pylist() if rid is not None}
    missing_rev_ids = patrolled_rev_ids - month_revision_ids
    revision_subset = load_revision_subset_by_ids(revision_store_dir, missing_rev_ids)

    rev_ts_map = {}
    rev_ns_map = {}
    rev_ut_map = {}

    def index_revision_table(table):
        if table is None:
            return
        id_arr = table.column("revision_id")
        ts_arr = table.column("event_timestamp")
        ns_arr = table.column("page_namespace")
        bot_arr = table.column("event_user_is_bot_by")
        anon_arr = table.column("event_user_is_anonymous")
        temp_arr = table.column("event_user_is_temporary")
        for i in range(table.num_rows):
            rid = id_arr[i].as_py()
            if rid is None:
                continue
            ts = ts_arr[i].as_py()
            if ts is not None:
                rev_ts_map[rid] = ts
            ns = ns_arr[i].as_py()
            if ns is not None:
                rev_ns_map[rid] = ns
            rev_ut_map[rid] = classify_user_type(
                bot_arr[i].as_py(), anon_arr[i].as_py(), temp_arr[i].as_py()
            )

    index_revision_table(revisions)
    index_revision_table(revision_subset)
    print(
        f"    Indexed {len(rev_ts_map):,} revision timestamps for {year_month}"
        + (f" ({len(missing_rev_ids):,} external lookup IDs)" if missing_rev_ids else "")
    )

    username_to_id = {}
    for i in range(revisions.num_rows):
        uid = rev_uid_arr[i].as_py()
        utext = rev_utext_arr[i].as_py()
        if uid and utext and utext not in username_to_id:
            username_to_id[utext] = uid

    id_to_username = {uid: username for username, uid in username_to_id.items()}
    ap_intervals_by_id = {
        uid: intervals
        for username, intervals in ap_intervals.items()
        if (uid := username_to_id.get(username))
    }

    latencies_by_key = {}
    patrol_ym_arr = patrol_month.column("year_month")
    patrol_rev_arr = patrol_month.column("current_revision_id")
    patrol_ts_arr = patrol_month.column("timestamp")

    matched = 0
    for i in range(patrol_month.num_rows):
        ym = patrol_ym_arr[i].as_py()
        rev_id = patrol_rev_arr[i].as_py()
        patrol_time = parse_ts(patrol_ts_arr[i].as_py())

        if rev_id and rev_id in rev_ts_map and patrol_time:
            rev_time = parse_ts(rev_ts_map[rev_id])
            if rev_time and patrol_time > rev_time:
                latency_hours = (patrol_time - rev_time).total_seconds() / 3600
                if latency_hours < 8760:
                    ns = rev_ns_map.get(rev_id, 0)
                    ut = rev_ut_map.get(rev_id, "registered")
                    latencies_by_key.setdefault((ym, ns, ut), []).append(latency_hours)
                    matched += 1

    patrol_users = patrol_month.column("user")
    patrol_prev = patrol_month.column("prev_revision_id")
    patrol_by_key = {}
    for i in range(patrol_month.num_rows):
        ym = patrol_ym_arr[i].as_py()
        rev_id = patrol_rev_arr[i].as_py()
        ns = rev_ns_map.get(rev_id, 0)
        ut = rev_ut_map.get(rev_id, "registered")
        key = (ym, ns, ut)
        entry = patrol_by_key.setdefault(key, {"users": set(), "count": 0, "new_page": 0, "diffs": 0, "user_counts": {}})
        entry["count"] += 1
        user = patrol_users[i].as_py()
        if user:
            entry["users"].add(user)
            entry["user_counts"][user] = entry["user_counts"].get(user, 0) + 1
        prev = patrol_prev[i].as_py()
        if prev == 0:
            entry["new_page"] += 1
        else:
            entry["diffs"] += 1

    rev_by_key = {}
    patrolled_by_key = {}
    autopatrolled_by_key = {}

    rev_ym = year_month_from_utf8(revisions.column("event_timestamp"))
    for i in range(revisions.num_rows):
        ym = rev_ym[i].as_py()
        ns = rev_ns_arr[i].as_py()
        if not ym or ns is None:
            continue
        ut = classify_user_type(
            rev_bot_arr[i].as_py(), rev_anon_arr[i].as_py(), rev_temp_arr[i].as_py()
        )
        key = (ym, ns, ut)
        rev_by_key[key] = rev_by_key.get(key, 0) + 1

        rid = rev_id_arr[i].as_py()
        if rid in patrolled_rev_ids:
            patrolled_by_key[key] = patrolled_by_key.get(key, 0) + 1

        uid = rev_uid_arr[i].as_py()
        if uid and uid in ap_intervals_by_id and rid not in patrolled_rev_ids:
            ts = rev_ts_arr[i].as_py()
            username = id_to_username.get(uid, "")
            if username and user_has_autopatrol_at(ap_intervals, username, ts or ""):
                autopatrolled_by_key[key] = autopatrolled_by_key.get(key, 0) + 1

    all_keys = sorted(set(list(patrol_by_key.keys()) + list(rev_by_key.keys())))
    rows = {field.name: [] for field in PATROL_SCHEMA}

    for ym, ns, ut in all_keys:
        p = patrol_by_key.get((ym, ns, ut), {"users": set(), "count": 0, "new_page": 0, "diffs": 0, "user_counts": {}})
        lats = sorted(latencies_by_key.get((ym, ns, ut), []))
        total_rev = rev_by_key.get((ym, ns, ut), 0)
        patrolled_rev = patrolled_by_key.get((ym, ns, ut), 0)
        autopatrolled_rev = autopatrolled_by_key.get((ym, ns, ut), 0)
        median_lat = lats[len(lats) // 2] if lats else None
        p90_lat = lats[int(len(lats) * 0.9)] if lats else None

        uc = p["user_counts"]
        top1 = max(uc.values()) / p["count"] * 100 if uc and p["count"] > 0 else 0
        sorted_uc = sorted(uc.values(), reverse=True)
        cumulative = 0
        min50 = 0
        for count in sorted_uc:
            cumulative += count
            min50 += 1
            if cumulative >= p["count"] * 0.5:
                break

        coverage = round(patrolled_rev / total_rev * 100, 1) if total_rev > 0 else 0
        adjusted = round((patrolled_rev + autopatrolled_rev) / total_rev * 100, 1) if total_rev > 0 else 0

        rows["year_month"].append(ym)
        rows["wiki"].append(wiki)
        rows["page_namespace"].append(ns)
        rows["user_type"].append(ut)
        rows["total_patrols"].append(p["count"])
        rows["unique_patrollers"].append(len(p["users"]))
        rows["patrol_new_pages"].append(p["new_page"])
        rows["patrol_diffs"].append(p["diffs"])
        rows["median_latency_hours"].append(round(median_lat, 2) if median_lat is not None else None)
        rows["p90_latency_hours"].append(round(p90_lat, 2) if p90_lat is not None else None)
        rows["patrolled_revisions"].append(patrolled_rev)
        rows["autopatrolled_revisions"].append(autopatrolled_rev)
        rows["total_revisions"].append(total_rev)
        rows["patrol_coverage_pct"].append(coverage)
        rows["adjusted_coverage_pct"].append(adjusted)
        rows["top1_pct"].append(round(top1, 1))
        rows["min_patrollers_50pct"].append(min50)

    return pa.table(rows, schema=PATROL_SCHEMA), matched


def build_autopatrol_intervals(rights_path, autopatrol_groups):
    """
    Build per-username intervals of autopatrol membership.

    Returns: dict of username → sorted list of (start_ts, end_ts) tuples
    where end_ts is None if the user still holds the right.
    """
    if not rights_path.exists() or not autopatrol_groups:
        return {}

    autopatrol_set = set(autopatrol_groups)
    rights = pq.read_table(rights_path)
    print(f"  {rights.num_rows:,} rights change events")

    # Collect rights events per user, sorted by time
    user_events = {}  # username → [(timestamp, had_autopatrol_before, has_autopatrol_after)]
    ts_arr = rights.column("timestamp")
    user_arr = rights.column("target_user")
    old_arr = rights.column("old_groups")
    new_arr = rights.column("new_groups")

    for i in range(rights.num_rows):
        username = user_arr[i].as_py()
        if not username:
            continue
        old_groups = set(g.strip() for g in (old_arr[i].as_py() or "").split(",") if g.strip())
        new_groups = set(g.strip() for g in (new_arr[i].as_py() or "").split(",") if g.strip())
        had_ap = bool(old_groups & autopatrol_set)
        has_ap = bool(new_groups & autopatrol_set)
        ts = ts_arr[i].as_py()
        user_events.setdefault(username, []).append((ts, had_ap, has_ap))

    # Build intervals per user
    intervals = {}
    for username, events in user_events.items():
        events.sort(key=lambda x: x[0] or "")
        user_intervals = []
        current_start = None

        for ts, had_before, has_after in events:
            if has_after and current_start is None:
                # Gained autopatrol
                current_start = ts
            elif not has_after and current_start is not None:
                # Lost autopatrol
                user_intervals.append((current_start, ts))
                current_start = None

        if current_start is not None:
            # Still holds autopatrol
            user_intervals.append((current_start, None))

        if user_intervals:
            intervals[username] = user_intervals

    print(f"  {len(intervals):,} users with autopatrol membership intervals")
    return intervals


def user_has_autopatrol_at(intervals, username, timestamp):
    """Check if username had autopatrol right at the given timestamp."""
    user_intervals = intervals.get(username)
    if not user_intervals:
        return False
    for start, end in user_intervals:
        if start and timestamp < start:
            continue
        if end and timestamp >= end:
            continue
        return True
    return False


def classify_user_type(is_bot_by, is_anonymous, is_temporary):
    """Classify user type matching Rust pipeline logic: bot > anonymous > temporary > registered."""
    if is_bot_by is not None and is_bot_by != "" and is_bot_by != "false":
        return "bot"
    if is_anonymous is True or is_anonymous == "true":
        return "anonymous"
    if is_temporary is True or is_temporary == "true":
        return "temporary"
    return "registered"


def compute_patrol_metrics(wiki, data_dir, output_dir, rebuild=False, limit_months=None):
    patrol_path = data_dir / "patrol" / wiki / "patrol.parquet"
    rights_path = data_dir / "patrol" / wiki / "rights.parquet"
    meta_path = data_dir / "patrol" / wiki / "autopatrol_groups.json"

    if not patrol_path.exists():
        print(f"No patrol data for {wiki}. Run fetch_patrol.py first.")
        return False
    revision_store_label, revision_store_dir = resolve_revision_store(data_dir, wiki)
    if revision_store_dir is None:
        print(f"No parquet data for {wiki}. Run ingest first.")
        return False

    # Load autopatrol configuration
    autopatrol_groups = []
    if meta_path.exists():
        with open(meta_path) as f:
            meta = json.load(f)
            autopatrol_groups = meta.get("autopatrol_groups", [])
        print(f"Autopatrol groups: {autopatrol_groups}")
    else:
        print(f"Warning: {meta_path} not found, skipping autopatrol estimation")

    if rebuild:
        parts_dir = patrol_parts_dir(output_dir, wiki)
        if parts_dir.exists():
            print(f"Clearing existing patrol month shards in {parts_dir}")
            for file in parts_dir.glob("*.parquet"):
                file.unlink()
    else:
        bootstrap_patrol_parts_from_final(output_dir, wiki)

    print(f"Loading patrol data...")
    patrol = pq.read_table(patrol_path)
    print(f"  {patrol.num_rows:,} patrol events")

    # Build autopatrol membership intervals from rights log
    print(f"Building autopatrol membership timeline...")
    ap_intervals = build_autopatrol_intervals(rights_path, autopatrol_groups)

    print(f"Loading revision data...")
    print(f"  using {revision_store_label} store: {revision_store_dir}")

    # Extract year-month from patrol timestamps
    patrol_ts = patrol.column("timestamp")
    patrol_ym = year_month_from_utf8(patrol_ts)
    patrol = patrol.append_column("year_month", patrol_ym)
    all_months = sorted({value for value in patrol.column("year_month").to_pylist() if value})
    completed_months = set() if rebuild else existing_patrol_months(output_dir, wiki)
    pending_months = [month for month in all_months if month not in completed_months]
    if limit_months is not None:
        pending_months = pending_months[:limit_months]

    if completed_months and not rebuild:
        print(f"Found {len(completed_months):,} completed patrol month shards")
    if pending_months:
        print(f"Computing patrol metrics for {len(pending_months):,} month(s): {pending_months[0]} → {pending_months[-1]}")
    else:
        print("No patrol months require recomputation; merging existing month shards")

    for year_month in pending_months:
        print(f"  Processing {year_month}...")
        month_mask = pc.equal(patrol.column("year_month"), pa.scalar(year_month))
        patrol_month = patrol.filter(month_mask)
        month_table, matched = build_patrol_month_table(
            wiki,
            year_month,
            patrol_month,
            ap_intervals,
            revision_store_dir,
        )
        part_path = write_patrol_part(output_dir, wiki, year_month, month_table)
        print(
            f"    Wrote {month_table.num_rows:,} grouped rows to {part_path}"
            f" ({matched:,} patrol events matched to revisions)"
        )

    out_path = merge_wiki_patrol_parts(output_dir, wiki)
    if out_path is None:
        print(f"No patrol month shards available for {wiki}")
        return False
    merged_path = merge_patrol_outputs(output_dir)
    write_defaults_patrol_json(output_dir, merged_path)
    return True


def main():
    parser = argparse.ArgumentParser(description="Compute patrol metrics")
    parser.add_argument("wiki", help="Wiki database name")
    parser.add_argument("--data-dir", default="data")
    parser.add_argument("--output-dir", default="output")
    parser.add_argument(
        "--rebuild",
        action="store_true",
        help="Recompute all patrol months from scratch instead of resuming from existing month shards.",
    )
    args = parser.parse_args()

    t0 = time.time()
    ok = compute_patrol_metrics(
        args.wiki,
        Path(args.data_dir),
        Path(args.output_dir),
        rebuild=args.rebuild,
    )
    if ok:
        print(f"\nDone in {time.time() - t0:.1f}s")
    else:
        sys.exit(1)


if __name__ == "__main__":
    main()
