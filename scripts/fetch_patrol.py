#!/usr/bin/env python3
"""
Fetch and parse Wikipedia patrol log data from XML dumps.

Downloads the pages-logging XML dump from dumps.wikimedia.org,
streams through it extracting patrol events and user rights changes,
queries the siteinfo API for autopatrol group permissions,
and writes Parquet files.

Usage:
    python3 scripts/fetch_patrol.py nlwiki
    python3 scripts/fetch_patrol.py nlwiki --no-download   # parse only (if already downloaded)
    python3 scripts/fetch_patrol.py nlwiki --sample 100000 # first 100k patrol events only
"""

import argparse
import gzip
import json
import os
import re
import sys
import time
import urllib.request
from pathlib import Path
from xml.etree.ElementTree import iterparse

import pyarrow as pa
import pyarrow.parquet as pq

DUMP_BASE = "https://dumps.wikimedia.org"
NS = "http://www.mediawiki.org/xml/export-0.11/"
USER_AGENT = "wiki-econ/0.1 (Wikipedia economic analysis research tool)"

PATROL_SCHEMA = pa.schema([
    ("log_id", pa.int64()),
    ("timestamp", pa.utf8()),
    ("user", pa.utf8()),
    ("user_id", pa.int64()),
    ("page_title", pa.utf8()),
    ("current_revision_id", pa.int64()),
    ("prev_revision_id", pa.int64()),
    ("is_auto", pa.bool_()),
])

RIGHTS_SCHEMA = pa.schema([
    ("timestamp", pa.utf8()),
    ("target_user", pa.utf8()),
    ("old_groups", pa.utf8()),
    ("new_groups", pa.utf8()),
])

BATCH_SIZE = 50_000  # rows per Arrow batch before flushing

# PHP metadata keys to exclude when extracting group names
_PHP_META_KEYS = {"oldmetadata", "newmetadata", "expiry", "4::oldgroups", "5::newgroups", "7::duration"}


def tag(local):
    """Namespace-qualified tag name."""
    return f"{{{NS}}}{local}"


def download_logging_dump(wiki, dest_path):
    """Download the complete logging dump. Supports resume."""
    url = f"{DUMP_BASE}/{wiki}/latest/{wiki}-latest-pages-logging.xml.gz"
    dest_path.parent.mkdir(parents=True, exist_ok=True)

    existing_size = dest_path.stat().st_size if dest_path.exists() else 0

    req = urllib.request.Request(url)
    req.add_header("User-Agent", USER_AGENT)
    if existing_size > 0:
        req.add_header("Range", f"bytes={existing_size}-")

    print(f"Downloading {url}")
    if existing_size > 0:
        print(f"  Resuming from {existing_size / 1e6:.1f} MB")

    try:
        resp = urllib.request.urlopen(req, timeout=3600)
    except urllib.error.HTTPError as e:
        if e.code == 416:  # Range not satisfiable = file complete
            print(f"  Already complete ({existing_size / 1e6:.1f} MB)")
            return
        raise

    total = resp.headers.get("Content-Length")
    if total:
        total = int(total) + existing_size
        print(f"  Total size: {total / 1e6:.1f} MB")

    mode = "ab" if existing_size > 0 and resp.status == 206 else "wb"
    downloaded = existing_size if mode == "ab" else 0

    with open(dest_path, mode) as f:
        while True:
            chunk = resp.read(256 * 1024)
            if not chunk:
                break
            f.write(chunk)
            downloaded += len(chunk)
            if total:
                pct = downloaded / total * 100
                print(f"\r  {downloaded / 1e6:.1f} / {total / 1e6:.1f} MB ({pct:.1f}%)", end="", flush=True)
            else:
                print(f"\r  {downloaded / 1e6:.1f} MB", end="", flush=True)

    print(f"\n  Done: {dest_path} ({downloaded / 1e6:.1f} MB)")


def _safe_int(s):
    s = s.strip()
    try:
        return int(s)
    except (ValueError, TypeError):
        return 0


def _extract_php_field(params, field):
    """Extract a string or integer field from PHP-serialized log params."""
    str_match = re.search(rf'"{re.escape(field)}";s:\d+:"([^"]*)"', params)
    if str_match:
        return str_match.group(1)
    int_match = re.search(rf'"{re.escape(field)}";i:(\d+)', params)
    if int_match:
        return int_match.group(1)
    return ""


def _parse_patrol_params(params):
    """
    Parse patrol log params from both legacy newline and PHP-serialized formats.

    Legacy format:
        <cur_rev>\n<prev_rev>\n<is_auto>

    Modern format (from 2012-03 onward on nlwiki):
        a:3:{s:8:"4::curid";s:8:"29704253";s:9:"5::previd";s:1:"0";s:7:"6::auto";i:0;}
    """
    if not params:
        return 0, 0, False

    params = params.strip()
    if not params:
        return 0, 0, False

    if params.startswith("a:"):
        cur_rev = _safe_int(_extract_php_field(params, "4::curid"))
        prev_rev = _safe_int(_extract_php_field(params, "5::previd"))
        is_auto = _extract_php_field(params, "6::auto") == "1"
        return cur_rev, prev_rev, is_auto

    params_lines = params.split("\n")
    cur_rev = _safe_int(params_lines[0]) if len(params_lines) > 0 else 0
    prev_rev = _safe_int(params_lines[1]) if len(params_lines) > 1 else 0
    is_auto = params_lines[2].strip() == "1" if len(params_lines) > 2 else False
    return cur_rev, prev_rev, is_auto


def _extract_groups_from_php(s):
    """Extract group names from PHP serialized array format."""
    vals = set(re.findall(r's:\d+:"([^"]+)"', s))
    return sorted(vals - _PHP_META_KEYS)


def _extract_groups(params, which="new"):
    """Parse group lists from rights params (both PHP and newline formats)."""
    if "a:" in params:
        # PHP serialized — contains both old and new groups
        # For 'a:2:{...}' format: first array is old, second is new
        # For 'a:4:{...}' format: keyed by "4::oldgroups" and "5::newgroups"
        groups = _extract_groups_from_php(params)
        # Can't reliably split old vs new from PHP, return all found
        # The caller uses old_groups and new_groups from the full param string
        return groups
    # Newline format: "oldgroups\nnewgroups" (comma-separated)
    lines = params.strip().split("\n")
    idx = 1 if which == "new" else 0
    if idx < len(lines):
        return sorted(g.strip() for g in lines[idx].split(",") if g.strip())
    return []


def _parse_rights_params(params):
    """Parse rights params into (old_groups, new_groups) comma-separated strings."""
    if not params or not params.strip():
        return "", ""
    if "a:" in params:
        # PHP serialized — extract keyed old/new groups
        old_match = re.search(r'"4::oldgroups";(a:\d+:\{[^}]*\})', params)
        new_match = re.search(r'"5::newgroups";(a:\d+:\{[^}]*\})', params)
        old_groups = _extract_groups_from_php(old_match.group(1)) if old_match else []
        new_groups = _extract_groups_from_php(new_match.group(1)) if new_match else []
        # Filter out timestamps (pure digit strings of length 14)
        old_groups = [g for g in old_groups if not (g.isdigit() and len(g) == 14)]
        new_groups = [g for g in new_groups if not (g.isdigit() and len(g) == 14)]
        return ",".join(old_groups), ",".join(new_groups)
    # Newline format: "oldgroups\nnewgroups"
    lines = params.strip().split("\n")
    old_str = lines[0].strip() if len(lines) > 0 else ""
    new_str = lines[1].strip() if len(lines) > 1 else ""
    return old_str, new_str


def parse_logging_events(xml_path, sample_limit=None):
    """
    Stream-parse the gzipped XML, yielding tagged event dicts.
    Extracts both patrol and rights events in a single pass.
    Uses iterparse + element clearing to keep memory constant.
    """
    patrol_count = 0
    rights_count = 0
    skipped = 0

    with gzip.open(xml_path, "rb") as f:
        context = iterparse(f, events=("end",))
        for event, elem in context:
            if elem.tag != tag("logitem"):
                continue

            log_type = elem.findtext(tag("type"))

            if log_type == "patrol":
                log_id = elem.findtext(tag("id"))
                timestamp = elem.findtext(tag("timestamp"))
                contrib = elem.find(tag("contributor"))
                username = contrib.findtext(tag("username")) if contrib is not None else None
                user_id = contrib.findtext(tag("id")) if contrib is not None else None
                page_title = elem.findtext(tag("logtitle"))
                params_text = elem.findtext(tag("params")) or ""

                cur_rev, prev_rev, is_auto = _parse_patrol_params(params_text)

                yield ("patrol", {
                    "log_id": int(log_id) if log_id else 0,
                    "timestamp": timestamp.replace("Z", "") if timestamp else None,
                    "user": username,
                    "user_id": int(user_id) if user_id else None,
                    "page_title": page_title,
                    "current_revision_id": cur_rev,
                    "prev_revision_id": prev_rev,
                    "is_auto": is_auto,
                })
                patrol_count += 1

                if (patrol_count % 100_000 == 0):
                    print(f"\r  Patrol: {patrol_count:,}  Rights: {rights_count:,}  (skipped {skipped:,})", end="", flush=True)

            elif log_type == "rights":
                timestamp = elem.findtext(tag("timestamp"))
                target_user = elem.findtext(tag("logtitle")) or ""
                # Strip "User:" prefix if present
                if target_user.startswith("User:") or target_user.startswith("Gebruiker:"):
                    target_user = target_user.split(":", 1)[1]
                params_text = elem.findtext(tag("params")) or ""
                old_groups, new_groups = _parse_rights_params(params_text)

                yield ("rights", {
                    "timestamp": timestamp.replace("Z", "") if timestamp else None,
                    "target_user": target_user,
                    "old_groups": old_groups,
                    "new_groups": new_groups,
                })
                rights_count += 1

            else:
                skipped += 1

            elem.clear()

            if sample_limit and patrol_count >= sample_limit:
                break

    print(f"\r  Patrol: {patrol_count:,}  Rights: {rights_count:,}  (skipped {skipped:,})")
    return patrol_count, rights_count


class BatchWriter:
    """Batched Parquet writer for a given schema."""

    def __init__(self, out_path, schema):
        self.out_path = out_path
        self.schema = schema
        self.writer = None
        self.batch_rows = {col: [] for col in schema.names}
        self.total = 0
        out_path.parent.mkdir(parents=True, exist_ok=True)

    def add(self, row):
        for col in self.schema.names:
            self.batch_rows[col].append(row[col])
        self.total += 1
        if self.total % BATCH_SIZE == 0:
            self._flush()

    def _flush(self):
        if not self.batch_rows[self.schema.names[0]]:
            return
        table = pa.table(self.batch_rows, schema=self.schema)
        if self.writer is None:
            self.writer = pq.ParquetWriter(str(self.out_path), self.schema, compression="zstd")
        self.writer.write_table(table)
        self.batch_rows = {col: [] for col in self.schema.names}

    def close(self):
        self._flush()
        if self.writer:
            self.writer.close()
        print(f"  Wrote {self.total:,} rows to {self.out_path}")
        return self.total


def wiki_to_api_domain(wiki):
    """Convert wiki db name to API domain: nlwiki → nl.wikipedia.org"""
    # Handle common suffixes
    if wiki.endswith("wiki") and wiki != "wiki":
        lang = wiki[:-4]
        return f"{lang}.wikipedia.org"
    return None


def fetch_autopatrol_groups(wiki):
    """Query the siteinfo API to find which groups have the autopatrol right."""
    domain = wiki_to_api_domain(wiki)
    if not domain:
        print(f"  Cannot determine API domain for {wiki}, skipping API query")
        return []

    url = f"https://{domain}/w/api.php?action=query&meta=siteinfo&siprop=usergroups&format=json"
    req = urllib.request.Request(url)
    req.add_header("User-Agent", USER_AGENT)

    print(f"  Querying {url}")
    try:
        resp = urllib.request.urlopen(req, timeout=30)
        data = json.loads(resp.read())
        groups = []
        for g in data.get("query", {}).get("usergroups", []):
            if "autopatrol" in g.get("rights", []):
                groups.append(g["name"])
        return groups
    except Exception as e:
        print(f"  Warning: API query failed: {e}")
        return []


def load_cached_autopatrol_groups(meta_path):
    """Load cached autopatrol groups if the live siteinfo query is unavailable."""
    if not meta_path.exists():
        return []
    try:
        with open(meta_path) as f:
            data = json.load(f)
        groups = data.get("autopatrol_groups", [])
        return groups if isinstance(groups, list) else []
    except Exception as e:
        print(f"  Warning: failed to read cached autopatrol groups from {meta_path}: {e}")
        return []


def main():
    parser = argparse.ArgumentParser(description="Fetch and parse Wikipedia patrol log data")
    parser.add_argument("wiki", help="Wiki database name (e.g. nlwiki)")
    parser.add_argument("--no-download", action="store_true", help="Skip download, parse existing file")
    parser.add_argument("--sample", type=int, default=None, help="Limit to first N patrol events")
    parser.add_argument("--data-dir", default="data", help="Base data directory")
    args = parser.parse_args()

    data_dir = Path(args.data_dir)
    base_dir = data_dir / "patrol" / args.wiki
    xml_path = base_dir / f"{args.wiki}-latest-pages-logging.xml.gz"
    patrol_path = base_dir / "patrol.parquet"
    rights_path = base_dir / "rights.parquet"
    meta_path = base_dir / "autopatrol_groups.json"

    t0 = time.time()

    # Step 1: Download
    if not args.no_download:
        print(f"\n=== Downloading patrol log dump for {args.wiki} ===\n")
        download_logging_dump(args.wiki, xml_path)

    if not xml_path.exists():
        print(f"Error: {xml_path} not found. Run without --no-download first.")
        sys.exit(1)

    # Step 2: Query API for autopatrol groups
    print(f"\n=== Querying siteinfo API for autopatrol groups ===\n")
    autopatrol_groups = fetch_autopatrol_groups(args.wiki)
    if not autopatrol_groups:
        cached_groups = load_cached_autopatrol_groups(meta_path)
        if cached_groups:
            autopatrol_groups = cached_groups
            print(f"  Reusing cached autopatrol groups from {meta_path}")
    print(f"  Groups with autopatrol right: {autopatrol_groups or '(none found)'}")
    base_dir.mkdir(parents=True, exist_ok=True)
    with open(meta_path, "w") as f:
        json.dump({"wiki": args.wiki, "autopatrol_groups": autopatrol_groups}, f, indent=2)
    print(f"  Saved to {meta_path}")

    # Step 3: Parse patrol + rights events in a single pass
    print(f"\n=== Parsing logging events from {xml_path.name} ({xml_path.stat().st_size / 1e6:.1f} MB) ===\n")
    patrol_writer = BatchWriter(patrol_path, PATROL_SCHEMA)
    rights_writer = BatchWriter(rights_path, RIGHTS_SCHEMA)

    for event_type, event in parse_logging_events(xml_path, sample_limit=args.sample):
        if event_type == "patrol":
            patrol_writer.add(event)
        elif event_type == "rights":
            rights_writer.add(event)

    patrol_total = patrol_writer.close()
    rights_total = rights_writer.close()

    elapsed = time.time() - t0
    print(f"\n=== Done in {elapsed:.1f}s — {patrol_total:,} patrol + {rights_total:,} rights events ===")


if __name__ == "__main__":
    main()
