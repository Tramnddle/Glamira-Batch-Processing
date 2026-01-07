from __future__ import annotations

import argparse
import re
import time
import random
from collections import defaultdict
from typing import Dict, List, Set, Tuple, Optional

import gcsfs
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc


# --- Pattern rules for "suspicious" columns (tune as you like) ---
SUSPICIOUS_PREFIXES = ("is_", "has_", "key_", "cat_", "flag_")
SUSPICIOUS_CONTAINS = ("_id", "id_", "uuid", "guid", "code", "type", "search")
SUSPICIOUS_SUFFIXES = ("_id", "_code", "_type", "_key")


RETRYABLE_SUBSTRINGS = [
    "oauth2.googleapis.com",
    "NameResolutionError",
    "getaddrinfo failed",
    "TransportError",
    "ConnectionError",
    "Max retries exceeded",
    "Read timed out",
]


def is_retryable_error(e: Exception) -> bool:
    s = str(e)
    return any(x in s for x in RETRYABLE_SUBSTRINGS)


def with_retries(fn, *, attempts=8, base_sleep=2.0, max_sleep=60.0):
    for a in range(1, attempts + 1):
        try:
            return fn()
        except Exception as e:
            if not is_retryable_error(e) or a == attempts:
                raise
            sleep = min(max_sleep, base_sleep * (2 ** (a - 1)))
            sleep = sleep * (0.7 + random.random() * 0.6)
            print(f"Retryable error ({a}/{attempts}): {e}\nSleeping {sleep:.1f}s then retry...")
            time.sleep(sleep)


def ensure_trailing_slash(s: str) -> str:
    s = s.strip()
    s = s.lstrip("/")
    return s if s.endswith("/") else s + "/"


def list_parquet_paths(fs: gcsfs.GCSFileSystem, bucket: str, prefix: str) -> List[str]:
    prefix = ensure_trailing_slash(prefix)
    return sorted(fs.glob(f"{bucket}/{prefix}**/*.parquet"))


def read_schema(fs: gcsfs.GCSFileSystem, path: str) -> pa.Schema:
    def _read():
        with fs.open(path, "rb") as f:
            return pq.read_schema(f)
    return with_retries(_read)


def arrow_type_key(dt: pa.DataType) -> str:
    return str(dt)


def detect_drifty_columns(schemas: List[pa.Schema], exclude_cols: Set[str]) -> Dict[str, Set[str]]:
    observed: Dict[str, Set[str]] = defaultdict(set)
    for sch in schemas:
        for field in sch:
            if field.name in exclude_cols:
                continue
            observed[field.name].add(arrow_type_key(field.type))
    return {k: v for k, v in observed.items() if len(v) > 1}


def looks_suspicious(col: str) -> bool:
    c = col.lower()
    if c.startswith(SUSPICIOUS_PREFIXES):
        return True
    if c.endswith(SUSPICIOUS_SUFFIXES):
        return True
    if any(x in c for x in SUSPICIOUS_CONTAINS):
        return True
    return False


def cast_selected_to_string(table: pa.Table, cols: Set[str]) -> pa.Table:
    if not cols:
        return table
    for col in cols:
        if col not in table.column_names:
            continue
        idx = table.schema.get_field_index(col)
        table = table.set_column(idx, col, pc.cast(table[col], pa.string(), safe=False))
    return table


def main() -> None:
    ap = argparse.ArgumentParser(
        description="From an already-normalized GCS Parquet prefix, cast suspicious/drifty columns to STRING into a new prefix."
    )
    ap.add_argument("--bucket", default="unigap_l3_batch")
    ap.add_argument("--src_prefix", required=True, help="Normalized prefix in GCS")
    ap.add_argument("--dst_prefix", required=True, help="Output prefix for string-fixed Parquet")
    ap.add_argument("--sample_size", type=int, default=300, help="Number of files to sample for drift detection")
    ap.add_argument("--resume", action="store_true", help="Skip files that already exist in destination")
    ap.add_argument("--log_every", type=int, default=200)
    ap.add_argument("--force_string_cols", default="", help="Comma-separated columns to always cast to STRING")
    ap.add_argument("--exclude_cols", default="", help="Comma-separated columns to never cast")
    ap.add_argument("--include_all_suspicious", action="store_true",
                    help="If set, cast all columns matching suspicious name patterns (even if no drift detected)")
    args = ap.parse_args()

    fs = gcsfs.GCSFileSystem()

    src_prefix = ensure_trailing_slash(args.src_prefix)
    dst_prefix = ensure_trailing_slash(args.dst_prefix)

    def parse_csv_set(s: str) -> Set[str]:
        return {x.strip() for x in s.split(",") if x.strip()} if s else set()

    force_cols = parse_csv_set(args.force_string_cols)
    exclude_cols = parse_csv_set(args.exclude_cols)

    files = list_parquet_paths(fs, args.bucket, src_prefix)
    if not files:
        raise SystemExit(f"No parquet found under gs://{args.bucket}/{src_prefix}")
    print(f"Found {len(files)} parquet files under gs://{args.bucket}/{src_prefix}")

    # ---- Phase 1: sample schemas and detect drift + suspicious columns ----
    sample_n = min(args.sample_size, len(files))
    sample_files = files[:sample_n]
    print(f"Sampling {sample_n} files for schema drift...")

    schemas: List[pa.Schema] = []
    for p in sample_files:
        try:
            schemas.append(read_schema(fs, p))
        except Exception as e:
            print(f"WARNING: failed to read schema for {p}: {e}")

    drifty = detect_drifty_columns(schemas, exclude_cols=exclude_cols)

    # suspicious set: based on names found in sample schemas
    suspicious: Set[str] = set()
    for sch in schemas:
        for field in sch:
            name = field.name
            if name in exclude_cols:
                continue
            if looks_suspicious(name):
                suspicious.add(name)

    # Decide columns to cast:
    # - always cast columns with drift
    # - plus (optional) suspicious columns
    # - plus forced columns
    cast_cols = set(drifty.keys()) | force_cols
    if args.include_all_suspicious:
        cast_cols |= suspicious
    cast_cols -= exclude_cols

    print("\n=== Plan ===")
    print(f"Drifty columns detected: {len(drifty)}")
    if drifty:
        for col, types in list(drifty.items())[:20]:
            print(f" - {col}: {sorted(types)}")
        if len(drifty) > 20:
            print(f" ... and {len(drifty)-20} more")

    print(f"Suspicious columns (name-based) found in sample: {len(suspicious)}")
    print(f"Force-cast cols: {sorted(force_cols) if force_cols else '[]'}")
    print(f"Exclude cols: {sorted(exclude_cols) if exclude_cols else '[]'}")
    print(f"\nTOTAL columns to cast to STRING: {len(cast_cols)}")
    if cast_cols:
        print("First 30:", sorted(list(cast_cols))[:30])

    # ---- Phase 2: rewrite files ----
    written = 0
    skipped = 0
    failed: List[Tuple[str, str]] = []

    for i, src_path in enumerate(files, start=1):
        rel = src_path[len(f"{args.bucket}/"):]          # drop bucket/
        rel_after_src = rel[len(src_prefix):]            # drop src_prefix
        dst_path = f"{args.bucket}/{dst_prefix}{rel_after_src}"

        try:
            if args.resume and fs.exists(dst_path):
                skipped += 1
                continue

            def process_one():
                with fs.open(src_path, "rb") as f:
                    table = pq.read_table(f)
                table2 = cast_selected_to_string(table, cast_cols)
                with fs.open(dst_path, "wb") as out:
                    pq.write_table(table2, out, compression="snappy")

            with_retries(process_one)
            written += 1

            if i % args.log_every == 0 or i == len(files):
                print(f"[{i}/{len(files)}] written={written} skipped={skipped} last={dst_path}")

        except Exception as e:
            failed.append((src_path, str(e)))
            print(f"[{i}/{len(files)}] FAILED {src_path}: {e}")

    print("\n=== Done ===")
    print(f"Total  : {len(files)}")
    print(f"Written: {written}")
    print(f"Skipped: {skipped}")
    print(f"Failed : {len(failed)}")
    if failed:
        print("First 10 failures:")
        for p, msg in failed[:10]:
            print(" -", p, "->", msg)


if __name__ == "__main__":
    main()
