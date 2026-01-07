#!/usr/bin/env python3
"""
Very simple test:
- Connect to MongoDB (localhost)
- Read 1 record from countly.summary
- Upload it as JSON directly to a GCS bucket (no local file)

Requirements:
  pip install pymongo google-cloud-storage

Run:
  setx GCS_BUCKET "your-bucket-name"   (Windows PowerShell)
  python test_extract.py
"""

import json
import os
from datetime import datetime, timezone

from google.cloud import storage
from pymongo import MongoClient
from bson import ObjectId


# ---- Minimal config (override via env vars) ----
MONGO_HOST = os.getenv("MONGO_HOST", "127.0.0.1")
MONGO_PORT = int(os.getenv("MONGO_PORT", "27017"))
MONGO_USERNAME = os.getenv("MONGO_USERNAME", "tram_nguyen")
MONGO_PASSWORD = os.getenv("MONGO_PASSWORD", "admin1234")
MONGO_AUTH_SOURCE = os.getenv("MONGO_AUTH_SOURCE", "admin")

MONGO_DB = os.getenv("MONGO_DB", "countly")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "summary")

GCS_BUCKET = os.getenv("GCS_BUCKET", "")
GCS_PREFIX = os.getenv("GCS_PREFIX", "mongo_test_one_record")


def to_jsonable(x):
    if isinstance(x, ObjectId):
        return str(x)
    if isinstance(x, datetime):
        return x.astimezone(timezone.utc).isoformat()
    return x


def main():
    if not GCS_BUCKET:
        raise SystemExit(
            "ERROR: Set GCS_BUCKET env var, e.g.\n"
            '  PowerShell: setx GCS_BUCKET "your-bucket-name"\n'
            "  (then reopen terminal)\n"
        )

    mongo_uri = (
        f"mongodb://{MONGO_USERNAME}:{MONGO_PASSWORD}"
        f"@{MONGO_HOST}:{MONGO_PORT}/?authSource={MONGO_AUTH_SOURCE}&directConnection=true"
    )

    # 1) Connect + fetch 1 doc
    client = MongoClient(mongo_uri, serverSelectionTimeoutMS=10_000, connectTimeoutMS=10_000)
    client.admin.command("ping")

    col = client[MONGO_DB][MONGO_COLLECTION]
    doc = col.find_one()
    if not doc:
        raise SystemExit(f"No documents found in {MONGO_DB}.{MONGO_COLLECTION}")

    # 2) Convert to JSON-friendly dict
    json_doc = {k: to_jsonable(v) for k, v in doc.items()}

    # 3) Serialize JSON (string in memory)
    json_text = json.dumps(json_doc, ensure_ascii=False, indent=2)

    # 4) Upload to GCS directly (no local file)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    filename = f"one_record_{MONGO_DB}_{MONGO_COLLECTION}_{ts}.json"
    blob_path = f"{GCS_PREFIX.rstrip('/')}/{filename}"

    gcs = storage.Client()
    bucket = gcs.bucket(GCS_BUCKET)
    blob = bucket.blob(blob_path)
    blob.upload_from_string(json_text, content_type="application/json")

    print(f"OK ✅ Uploaded 1 record to: gs://{GCS_BUCKET}/{blob_path}")

    client.close()


if __name__ == "__main__":
    main()

