#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
import json
import os
import re
import time
from typing import Dict, Any, Optional, Tuple

import requests


# -----------------------
# Config
# -----------------------
INPUT_CSV = "unique_product_id_current_url.csv"

# Start directly with domain rotation (NO initial .com check)
DOMAIN_PREFIXES = [
    "https://www.glamira.fr/catalog/product/view/id/",     # FR
    "https://www.glamira.de/catalog/product/view/id/",     # DE
    "https://www.glamira.co.uk/catalog/product/view/id/",  # UK
    "https://www.glamira.com/catalog/product/view/id/",    # US / .com
    "https://www.glamira.it/catalog/product/view/id/",     # IT
]

OUTPUT_CSV = "product_id_crawled_data.csv"
MISSING_CSV = "product_id_missing_urls.csv"

REQUEST_TIMEOUT = 20  # seconds
MAX_RETRIES = 3
RETRY_BACKOFF_SECONDS = 2.0

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
}

FIELDS = [
    "product_id",
    "product_name",
    "sku",
    "attribute_set_id",
    "attribute_set",
    "type_id",
    "min_price",
    "max_price",
    "gold_weight",
    "none_metal_weight",
    "fixed_silver_weight",
    "material_design",
    "collection",
    "collection_id",
    "product_type",
    "product_type_value",
    "category",
    "category_name",
    "store_code",
    "gender",
]

RE_REACT_DATA = re.compile(
    r"var\s+react_data\s*=\s*(\{.*?\})\s*;?",
    re.DOTALL | re.IGNORECASE,
)


# -----------------------
# Helpers
# -----------------------
def ensure_csv_with_header(path: str, fieldnames: list) -> None:
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()


def append_row(path: str, fieldnames: list, row: Dict[str, Any]) -> None:
    """Append a single row and flush immediately (crash-safe)."""
    with open(path, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writerow(row)
        f.flush()
        os.fsync(f.fileno())


def load_product_ids(input_csv: str) -> list:
    if not os.path.exists(input_csv):
        raise FileNotFoundError(f"Input file not found: {input_csv}")

    pids = []
    with open(input_csv, "r", newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        if not r.fieldnames:
            raise ValueError(f"{input_csv} has no header row.")

        pid_col = None
        for c in r.fieldnames:
            if c.strip().lower() in {"product_id", "id", "productid"}:
                pid_col = c
                break
        if not pid_col:
            raise ValueError(
                f"Couldn't find a product id column in {input_csv}. "
                f"Expected one of: product_id, id, productid. Found: {r.fieldnames}"
            )

        for row in r:
            pid = (row.get(pid_col) or "").strip()
            if pid:
                pids.append(pid)

    return pids


def fetch_html(session: requests.Session, url: str) -> Tuple[Optional[str], Optional[int], Optional[str]]:
    last_err = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
            status = resp.status_code
            if status == 200 and resp.text:
                return resp.text, status, None
            last_err = f"HTTP {status}"
        except requests.RequestException as e:
            last_err = str(e)

        if attempt < MAX_RETRIES:
            time.sleep(RETRY_BACKOFF_SECONDS * attempt)

    return None, None, last_err


def extract_react_data(html: str) -> Optional[Dict[str, Any]]:
    m = RE_REACT_DATA.search(html)
    if not m:
        return None

    raw_obj = m.group(1).strip()

    # Strict JSON first
    try:
        return json.loads(raw_obj)
    except json.JSONDecodeError:
        pass

    # Best-effort normalization (if page uses JS-ish object)
    try:
        normalized = re.sub(r"\bundefined\b", "null", raw_obj)
        if normalized.count("'") > normalized.count('"'):
            normalized = normalized.replace("'", '"')
        return json.loads(normalized)
    except Exception:
        return None


def pick_fields(react_data: Dict[str, Any], fallback_pid: str) -> Dict[str, Any]:
    out = {k: "" for k in FIELDS}
    out["product_id"] = react_data.get("product_id", fallback_pid) or fallback_pid

    for k in FIELDS:
        if k == "product_id":
            continue
        val = react_data.get(k, "")
        if isinstance(val, (dict, list)):
            val = json.dumps(val, ensure_ascii=False)
        out[k] = val

    return out


def crawl_with_domain_rotation(session: requests.Session, product_id: str) -> Tuple[Optional[Dict[str, Any]], Optional[str], Optional[str]]:
    """
    Try FR -> DE -> UK -> US -> IT
    Returns: (react_data, used_url, fail_reason)
    """
    last_reason = None
    last_url = None

    for prefix in DOMAIN_PREFIXES:
        url = f"{prefix}{product_id}"
        last_url = url

        html, status, err = fetch_html(session, url)
        if not html:
            last_reason = err or "fetch_failed"
            continue

        react_data = extract_react_data(html)
        if react_data:
            return react_data, url, None

        last_reason = f"react_data_not_found_or_unparseable (status={status})"

    return None, last_url, last_reason or "all_domains_failed"


# -----------------------
# Main
# -----------------------
def main():
    ensure_csv_with_header(OUTPUT_CSV, FIELDS)
    ensure_csv_with_header(MISSING_CSV, ["product_id", "url", "reason"])

    product_ids = load_product_ids(INPUT_CSV)
    print(f"Loaded {len(product_ids)} product_ids from {INPUT_CSV}")

    session = requests.Session()

    for idx, pid in enumerate(product_ids, start=1):
        print(f"[{idx}/{len(product_ids)}] Crawling {pid} (domain rotation)")

        react_data, used_url, fail_reason = crawl_with_domain_rotation(session, pid)

        if react_data:
            row = pick_fields(react_data, fallback_pid=pid)
            append_row(OUTPUT_CSV, FIELDS, row)
        else:
            append_row(MISSING_CSV, ["product_id", "url", "reason"], {
                "product_id": pid,
                "url": used_url or "",
                "reason": fail_reason or "failed",
            })

    print("\nDone.")
    print(f"✅ Output saved (incrementally): {OUTPUT_CSV}")
    print(f"⚠️ Missing/failed saved (incrementally): {MISSING_CSV}")


if __name__ == "__main__":
    main()
