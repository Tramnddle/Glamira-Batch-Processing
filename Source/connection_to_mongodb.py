#!/usr/bin/env python3
"""
IP Location Processing

1. Connect to MongoDB
2. Stream unique IPs from main collection (avoid 16MB distinct cap)
3. Use IP2Location BIN database to get location data
4. Store results in a new collection OR export to CSV

Requirements:
    pip install IP2Location pymongo

You also need an IP2Location BIN file (LITE or commercial).
"""

import os
import csv
from datetime import datetime
from typing import List, Dict, Any

from pymongo import MongoClient, UpdateOne
from pymongo.errors import ConnectionFailure, OperationFailure, ServerSelectionTimeoutError

import IP2Location  # PyPI package name


# ---- Mongo config ----
MONGO_URI = os.environ.get("MONGO_URI")

MONGO_HOST = os.environ.get("MONGO_HOST", "127.0.0.1")
MONGO_PORT = int(os.environ.get("MONGO_PORT", "27017"))

# Optional auth (only used if both user + pass are provided)
MONGO_USER = os.environ.get("MONGO_USER")
MONGO_PASS = os.environ.get("MONGO_PASS")
MONGO_AUTH_DB = os.environ.get("MONGO_AUTH_DB", "admin")

DB_NAME = os.environ.get("MONGO_DB", "countly")
SUMMARY_COLLECTION = "summary"

# Target collection to store IP -> location
TARGET_COLLECTION = os.getenv("MONGO_TARGET_COLLECTION", "ip_locations")

# ---- IP2Location config ----
IP2LOCATION_BIN = os.getenv("IP2LOCATION_BIN", "./IP-COUNTRY-REGION-CITY.BIN")
IP2LOCATION_MODE = os.getenv("IP2LOCATION_MODE", "FILE_IO")  # or "SHARED_MEMORY"


def build_mongo_uri() -> str:
    """
    Priority:
      1) MONGO_URI (if provided)
      2) Build from host/port and optional user/pass
    """
    if MONGO_URI:
        return MONGO_URI

    # No auth by default for local Mongo
    if not MONGO_USER or not MONGO_PASS:
        return f"mongodb://{MONGO_HOST}:{MONGO_PORT}/"

    # If auth is enabled and you created users, use credentials
    user = quote_plus(MONGO_USER)
    pwd = quote_plus(MONGO_PASS)
    return f"mongodb://{user}:{pwd}@{MONGO_HOST}:{MONGO_PORT}/?authSource={MONGO_AUTH_DB}"


def iter_unique_ips(col):
    """
    Stream unique IPs using aggregation cursor to avoid the 16MB distinct cap.
    Assumes 'ip' is a top-level field.
    """
    pipeline = [
        {"$match": {"ip": {"$type": "string", "$ne": ""}}},
        {"$group": {"_id": "$ip"}},
        {"$project": {"_id": 0, "ip": "$_id"}}
    ]

    cursor = col.aggregate(pipeline, allowDiskUse=True)

    for doc in cursor:
        ip = doc.get("ip")
        if isinstance(ip, str) and ip.strip():
            yield ip.strip()


def load_ip2location_db():
    """
    Load IP2Location DB from BIN file.
    """
    if IP2LOCATION_MODE.upper() == "SHARED_MEMORY":
        return IP2Location.IP2Location(IP2LOCATION_BIN, "SHARED_MEMORY")
    return IP2Location.IP2Location(IP2LOCATION_BIN)


def lookup_ip(db, ip: str) -> Dict[str, Any]:
    """
    Perform IP2Location lookup.
    Returned fields depend on the BIN product level.
    """
    rec = db.get_all(ip)

    return {
        "ip": ip,
        "country_code": getattr(rec, "country_short", None),
        "country_name": getattr(rec, "country_long", None),
        "region": getattr(rec, "region", None),
        "city": getattr(rec, "city", None),
        "latitude": getattr(rec, "latitude", None),
        "longitude": getattr(rec, "longitude", None),
        "isp": getattr(rec, "isp", None),
        "domain": getattr(rec, "domain", None),
        "timezone": getattr(rec, "timezone", None),
        "zip_code": getattr(rec, "zipcode", None),
        "usage_type": getattr(rec, "usage_type", None),
        "address_type": getattr(rec, "address_type", None),
        "asn": getattr(rec, "asn", None),
        "as_name": getattr(rec, "as", None),
        "looked_up_at": datetime.utcnow(),
        "source": "ip2location_bin",
    }


def store_to_mongo(target_col, records: List[Dict[str, Any]]):
    """
    Upsert IP location results into target collection by ip.
    """
    if not records:
        return

    ops = [
        UpdateOne({"ip": r["ip"]}, {"$set": r}, upsert=True)
        for r in records
    ]
    target_col.bulk_write(ops, ordered=False)


def append_to_csv(csv_path: str, records: List[Dict[str, Any]]):
    """
    Append records to CSV (streaming-friendly).
    """
    if not records:
        return

    file_exists = os.path.exists(csv_path)
    fieldnames = list(records[0].keys())

    with open(csv_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        writer.writerows(records)


def process_ip_locations(
    store_mode: str = "mongo",
    csv_path: str = "ip_locations.csv",
    batch_size: int = 500
) -> int:
    """
    Main orchestration.
    Returns number of processed unique IPs.
    """
    uri = build_uri(HOST, PORT, USERNAME, PASSWORD)
    print(f"Connecting to MongoDB at {HOST}:{PORT} (user={USERNAME})")

    try:
        client = MongoClient(uri, serverSelectionTimeoutMS=8000)
        client.admin.command("ping")
        print("✅ Connected successfully.")

        db = client[DB_NAME]
        source_col = db[SOURCE_COLLECTION]
        target_col = db[TARGET_COLLECTION]

        # Load IP2Location DB first
        ip_db = load_ip2location_db()
        print("✅ IP2Location database loaded.")

        # Stream unique IPs
        ip_iter = iter_unique_ips(source_col)
        print("✅ Streaming unique IPs via aggregation (no distinct()).")

        buffer: List[Dict[str, Any]] = []
        processed = 0

        for ip in ip_iter:
            processed += 1
            try:
                rec = lookup_ip(ip_db, ip)
                buffer.append(rec)
            except Exception as e:
                print(f"⚠️ Lookup failed for {ip}: {e}")

            if len(buffer) >= batch_size:
                if store_mode == "mongo":
                    store_to_mongo(target_col, buffer)
                elif store_mode == "csv":
                    append_to_csv(csv_path, buffer)

                buffer.clear()

                if processed % (batch_size * 2) == 0:
                    print(f"Processed ~{processed} unique IPs...")

        # Flush remaining
        if buffer:
            if store_mode == "mongo":
                store_to_mongo(target_col, buffer)
            elif store_mode == "csv":
                append_to_csv(csv_path, buffer)

        if store_mode == "mongo":
            print(f"✅ Upserted results into collection '{TARGET_COLLECTION}'")
        else:
            print(f"✅ CSV written/appended: {csv_path}")

        print(f"✅ Done. Total unique IPs processed: {processed}")
        return processed

    except ServerSelectionTimeoutError as e:
        print("❌ Could not reach the server (timeout).")
        print("If your VM only allows localhost access, you may need an SSH tunnel.")
        print(str(e))
        return 0

    except OperationFailure as e:
        print("❌ Authentication/authorization failed.")
        print(str(e))
        return 0

    except ConnectionFailure as e:
        print("❌ Connection failed.")
        print(str(e))
        return 0


if __name__ == "__main__":
    # Choose one:
    # process_ip_locations(store_mode="mongo")
    process_ip_locations(store_mode="csv", csv_path="ip_locations.csv")
