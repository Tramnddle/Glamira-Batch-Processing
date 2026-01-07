import os
import csv
from typing import Optional, Dict, List, Tuple
from collections import defaultdict

from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError, OperationFailure

# -------------------------------------------------
# Config
# -------------------------------------------------
# ✅ Hardcoded local MongoDB URI (no auth)
MONGO_URI = "mongodb://127.0.0.1:27017"

DB_NAME = os.environ.get("MONGO_DB", "countly")
SUMMARY_COLLECTION = "summary"

EVENT_TYPES_CURRENT_URL = {
    "view_product_detail",
    "select_product_option",
    "select_product_option_quality",
    "add_to_cart_action",
    "product_detail_recommendation_visible",
    "product_detail_recommendation_noticed",
}
EVENT_TYPE_REFERRER = "product_view_all_recommend_clicked"

UNIQUE_PID_URL_CSV = "unique_product_id_current_url.csv"


# -------------------------------------------------
# Helper functions
# -------------------------------------------------
def normalize_url(u: Optional[str]) -> Optional[str]:
    if not isinstance(u, str):
        return None
    u = u.strip()

    if u.startswith("view-source:"):
        u = u[len("view-source:") :]

    if u.startswith("ttps://"):
        u = "h" + u

    if not u.startswith(("http://", "https://")):
        return None
    return u


def extract_pid(doc: dict) -> Optional[str]:
    pid = doc.get("product_id") or doc.get("viewing_product_id")
    if pid is None:
        return None
    return str(pid)


def domain_priority(url: str) -> int:
    # Lower is better
    if ".com/" in url:
        return 3
    score = 1
    if "/product" in url or "/catalog/product" in url:
        score = 0
    return score


def get_mongo_client() -> MongoClient:
    """
    Connect to local MongoDB on 127.0.0.1:27017 (no auth).
    """
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=30000)
        client.admin.command("ping")
        return client
    except ServerSelectionTimeoutError as e:
        raise RuntimeError(f"Could not connect to MongoDB at {MONGO_URI}: {e}")
    except OperationFailure as e:
        raise RuntimeError(f"MongoDB operation failed: {e}")


def choose_best_url(url_stats: Dict[str, Tuple[int, int]]) -> Optional[str]:
    if not url_stats:
        return None

    def key(u: str):
        is_current, cnt = url_stats[u]
        return (-is_current, domain_priority(u), -cnt, len(u))

    return min(url_stats.keys(), key=key)


# -------------------------------------------------
# Main extraction
# -------------------------------------------------
def main():
    client = get_mongo_client()
    db = client[DB_NAME]

    if SUMMARY_COLLECTION not in db.list_collection_names():
        raise RuntimeError(f"Collection '{SUMMARY_COLLECTION}' not found in DB '{DB_NAME}'")

    col = db[SUMMARY_COLLECTION]

    target_types = list(EVENT_TYPES_CURRENT_URL) + [EVENT_TYPE_REFERRER]

    cursor = col.find(
        {
            "collection": {"$in": target_types},
            "$or": [
                {"product_id": {"$exists": True}},
                {"viewing_product_id": {"$exists": True}},
            ],
        },
        {
            "_id": 0,
            "collection": 1,
            "product_id": 1,
            "viewing_product_id": 1,
            "current_url": 1,
            "referrer_url": 1,
        },
    )

    # pid_url_stats[pid][url] = (is_current_url_source, count)
    pid_url_stats: Dict[str, Dict[str, Tuple[int, int]]] = defaultdict(dict)

    total_seen = 0
    print("Scanning Mongo 'summary' collection...")

    for doc in cursor:
        event_type = doc.get("collection")
        pid = extract_pid(doc)
        if not pid:
            continue

        is_current_source = 0
        if event_type in EVENT_TYPES_CURRENT_URL:
            raw_url = doc.get("current_url")
            is_current_source = 1
        elif event_type == EVENT_TYPE_REFERRER:
            raw_url = doc.get("referrer_url")
        else:
            continue

        url = normalize_url(raw_url)
        if not url:
            continue

        total_seen += 1

        if url in pid_url_stats[pid]:
            prev_is_current, prev_count = pid_url_stats[pid][url]
            pid_url_stats[pid][url] = (max(prev_is_current, is_current_source), prev_count + 1)
        else:
            pid_url_stats[pid][url] = (is_current_source, 1)

    rows: List[dict] = []
    missing_pids = 0

    for pid, stats in pid_url_stats.items():
        best = choose_best_url(stats)
        if not best:
            missing_pids += 1
            continue
        rows.append({"product_id": pid, "current_url": best})

    with open(UNIQUE_PID_URL_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["product_id", "current_url"])
        writer.writeheader()
        writer.writerows(rows)

    print(f"Total URL observations scanned: {total_seen}")
    print(f"Unique product_ids found: {len(pid_url_stats)}")
    print(f"Unique product_id + best current_url rows written: {len(rows)}")
    if missing_pids:
        print(f"Product_ids with no usable URL: {missing_pids}")
    print(f"Saved: {UNIQUE_PID_URL_CSV}")


if __name__ == "__main__":
    main()
