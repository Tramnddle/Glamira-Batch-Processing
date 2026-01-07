#!/usr/bin/env python3
"""
MongoDB -> Parquet (chunked) -> Google Cloud Storage with resume.

Key features:
- Streams Mongo in batches (safe for 41M+ docs)
- Normalizes "messy" Mongo fields so Parquet won't crash
- Uploads with resumable chunked uploads + long timeouts + retries
- Checkpointing in GCS so you can resume WITHOUT re-reading already exported records
- Optional "skip if already exists" for uploaded parts

Requirements:
  pip install pymongo pandas pyarrow google-cloud-storage

Env vars:
  # Mongo
  MONGO_HOST=127.0.0.1
  MONGO_PORT=27017
  MONGO_USERNAME=...
  MONGO_PASSWORD=...
  MONGO_AUTH_SOURCE=admin
  MONGO_DB=countly
  MONGO_COLLECTION=summary
  SORT_FIELD=_id

  # Export
  BATCH_SIZE=5000
  MAX_DOCS=0               # 0 = unlimited
  LOCAL_TMP_DIR=...        # optional
  LOG_LEVEL=INFO

  # GCS
  GCS_BUCKET=your-bucket
  GCS_PREFIX=exports/mongo_parquet

  # Upload tuning
  GCS_CHUNK_MB=16          # resumable chunk size
  GCS_CONNECT_TIMEOUT=30
  GCS_READ_TIMEOUT=600
  GCS_RETRY_DEADLINE=3600  # seconds per file

Run:
  python export_to_gcs.py
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
import tempfile
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from bson import ObjectId
from google.api_core.retry import Retry
from google.api_core import exceptions as gapi_exceptions
from google.cloud import storage
from pymongo import MongoClient


# ----------------------------
# Config
# ----------------------------
MONGO_HOST = os.getenv("MONGO_HOST", "127.0.0.1")
MONGO_PORT = int(os.getenv("MONGO_PORT", "27017"))
MONGO_USERNAME = os.getenv("MONGO_USERNAME", "tram_nguyen")
MONGO_PASSWORD = os.getenv("MONGO_PASSWORD", "admin1234")
MONGO_AUTH_SOURCE = os.getenv("MONGO_AUTH_SOURCE", "admin")

MONGO_DB = os.getenv("MONGO_DB", "countly")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "summary")
SORT_FIELD = os.getenv("SORT_FIELD", "_id")

BATCH_SIZE = int(os.getenv("BATCH_SIZE", "5000"))
MAX_DOCS = int(os.getenv("MAX_DOCS", "0"))  # 0 = unlimited

LOCAL_TMP_DIR = os.getenv("LOCAL_TMP_DIR") or os.path.join(tempfile.gettempdir(), "mongo_export")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

GCS_BUCKET = os.getenv("GCS_BUCKET", "")
GCS_PREFIX = os.getenv("GCS_PREFIX", "exports/mongo_parquet")

GCS_CHUNK_MB = int(os.getenv("GCS_CHUNK_MB", "16"))
GCS_CONNECT_TIMEOUT = int(os.getenv("GCS_CONNECT_TIMEOUT", "30"))
GCS_READ_TIMEOUT = int(os.getenv("GCS_READ_TIMEOUT", "600"))
GCS_RETRY_DEADLINE = int(os.getenv("GCS_RETRY_DEADLINE", "3600"))


# ----------------------------
# Logging
# ----------------------------
def setup_logging() -> None:
    os.makedirs(LOCAL_TMP_DIR, exist_ok=True)
    log_path = os.path.join(LOCAL_TMP_DIR, "export_to_gcs.log")
    logging.basicConfig(
        level=getattr(logging, LOG_LEVEL, logging.INFO),
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[logging.StreamHandler(sys.stdout), logging.FileHandler(log_path, mode="a", encoding="utf-8")],
    )
    logging.info("Log file: %s", log_path)


def now_utc_compact() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


# ----------------------------
# Connections
# ----------------------------
def build_mongo_uri() -> str:
    return (
        f"mongodb://{MONGO_USERNAME}:{MONGO_PASSWORD}"
        f"@{MONGO_HOST}:{MONGO_PORT}/?authSource={MONGO_AUTH_SOURCE}"
    )


def connect_mongo() -> MongoClient:
    logging.info("Connecting MongoDB: %s:%s (authSource=%s)", MONGO_HOST, MONGO_PORT, MONGO_AUTH_SOURCE)
    client = MongoClient(
        build_mongo_uri(),
        serverSelectionTimeoutMS=10_000,
        connectTimeoutMS=10_000,
        socketTimeoutMS=60_000,
        retryWrites=True,
    )
    client.admin.command("ping")
    logging.info("Mongo ping OK.")
    return client


def connect_gcs() -> storage.Client:
    logging.info("Connecting GCS client (service account / ADC).")
    return storage.Client()


# ----------------------------
# Parquet normalization (robust)
# ----------------------------
def _jsonify_scalar(x: Any) -> Optional[str]:
    """Convert messy Mongo values into stable strings for Parquet."""
    if x is None:
        return None

    try:
        if pd.isna(x):
            return None
    except Exception:
        pass

    if isinstance(x, ObjectId):
        return str(x)

    if isinstance(x, datetime):
        return x.astimezone(timezone.utc).isoformat()

    if isinstance(x, (bytes, bytearray)):
        try:
            return bytes(x).decode("utf-8")
        except Exception:
            return bytes(x).hex()

    if isinstance(x, (dict, list, tuple, set)):
        return json.dumps(x, ensure_ascii=False, default=str)

    if isinstance(x, str):
        return x

    if isinstance(x, (int, float, bool)):
        # stringify to avoid bool-vs-str fights like utm_source
        return str(x)

    return json.dumps(x, ensure_ascii=False, default=str)


def normalize_df_for_parquet(df: pd.DataFrame) -> pd.DataFrame:
    """
    Strong rule for Mongo exports:
    - stringify ALL object columns
    This prevents Arrow inference errors across 41M docs (bool vs str, bytes vs float, list vs scalar, etc.).
    """
    for c in df.columns:
        if df[c].dtype == "object":
            df[c] = df[c].map(_jsonify_scalar)
    return df


def write_parquet_chunk(rows: List[Dict[str, Any]], out_path: str) -> int:
    df = pd.DataFrame(rows)
    df = normalize_df_for_parquet(df)
    df.to_parquet(out_path, index=False)
    return len(df)


# ----------------------------
# GCS helpers (resume + robust upload)
# ----------------------------
def blob_exists(gcs_client: storage.Client, bucket: str, blob_path: str) -> bool:
    return gcs_client.bucket(bucket).blob(blob_path).exists(gcs_client)

def _retry_predicate(exc: Exception) -> bool:
    return isinstance(
        exc,
        (
            gapi_exceptions.TooManyRequests,
            gapi_exceptions.ServiceUnavailable,
            gapi_exceptions.InternalServerError,
            gapi_exceptions.BadGateway,
            gapi_exceptions.GatewayTimeout,
            gapi_exceptions.DeadlineExceeded,
            gapi_exceptions.RequestTimeout,
        ),
    )

retry = Retry(
    predicate=_retry_predicate,
    initial=1.0,
    maximum=60.0,
    multiplier=2.0,
    deadline=float(GCS_RETRY_DEADLINE),
)

def upload_file(gcs_client: storage.Client, bucket_name: str, blob_path: str, local_path: str) -> None:
    bucket = gcs_client.bucket(bucket_name)
    blob = bucket.blob(blob_path)

    # resumable upload tuning
    blob.chunk_size = GCS_CHUNK_MB * 1024 * 1024

    timeout = (GCS_CONNECT_TIMEOUT, GCS_READ_TIMEOUT)

    blob.upload_from_filename(local_path, timeout=timeout, retry=retry)


# ----------------------------
# Checkpointing
# ----------------------------
def encode_sort_value(v: Any) -> Dict[str, Any]:
    if v is None:
        return {"type": "none", "value": None}
    if isinstance(v, ObjectId):
        return {"type": "objectid", "value": str(v)}
    if isinstance(v, datetime):
        return {"type": "datetime", "value": v.astimezone(timezone.utc).isoformat()}
    if isinstance(v, (int, float, bool, str)):
        return {"type": type(v).__name__, "value": v}
    # fallback
    return {"type": "str", "value": str(v)}


def decode_sort_value(payload: Dict[str, Any]) -> Any:
    t = payload.get("type")
    val = payload.get("value")
    if t == "none":
        return None
    if t == "objectid":
        return ObjectId(val)
    if t == "datetime":
        # keep as ISO string unless you need datetime queries
        return val
    # int/float/bool/str
    return val


def checkpoint_blob_path(run_prefix: str) -> str:
    return f"{run_prefix}/_checkpoint.json"


def load_checkpoint(gcs_client: storage.Client, bucket: str, run_prefix: str) -> Dict[str, Any]:
    """
    Returns checkpoint dict:
      {
        "chunk_idx": int,
        "exported_docs": int,
        "last_sort_value": {type,value}
      }
    """
    path = checkpoint_blob_path(run_prefix)
    blob = gcs_client.bucket(bucket).blob(path)
    if not blob.exists(gcs_client):
        return {"chunk_idx": 0, "exported_docs": 0, "last_sort_value": encode_sort_value(None)}

    data = blob.download_as_text()
    return json.loads(data)


def save_checkpoint(gcs_client: storage.Client, bucket: str, run_prefix: str, ckpt: Dict[str, Any]) -> None:
    path = checkpoint_blob_path(run_prefix)
    blob = gcs_client.bucket(bucket).blob(path)
    blob.upload_from_string(json.dumps(ckpt, ensure_ascii=False, indent=2), content_type="application/json")


# ----------------------------
# Main export
# ----------------------------
def export_to_gcs() -> None:
    setup_logging()
    if not GCS_BUCKET:
        raise SystemExit("ERROR: Set env var GCS_BUCKET.")

    run_id = os.getenv("RUN_ID", "").strip() or now_utc_compact()
    run_prefix = f"{GCS_PREFIX.rstrip('/')}/run_id={run_id}/db={MONGO_DB}/collection={MONGO_COLLECTION}"

    logging.info("RUN_ID=%s", run_id)
    logging.info("Target: gs://%s/%s", GCS_BUCKET, run_prefix)
    logging.info("BATCH_SIZE=%d | MAX_DOCS=%d | SORT_FIELD=%s", BATCH_SIZE, MAX_DOCS, SORT_FIELD)

    os.makedirs(LOCAL_TMP_DIR, exist_ok=True)

    gcs = connect_gcs()
    ckpt = load_checkpoint(gcs, GCS_BUCKET, run_prefix)
    chunk_idx = int(ckpt.get("chunk_idx", 0))
    exported_docs = int(ckpt.get("exported_docs", 0))
    last_sort_value = decode_sort_value(ckpt.get("last_sort_value", encode_sort_value(None)))

    logging.info("Resume checkpoint: chunk_idx=%d exported_docs=%d last_sort_value=%s",
                 chunk_idx, exported_docs, str(last_sort_value))

    start_ts = time.time()
    mongo = connect_mongo()
    try:
        col = mongo[MONGO_DB][MONGO_COLLECTION]

        while True:
            if MAX_DOCS and exported_docs >= MAX_DOCS:
                logging.warning("Reached MAX_DOCS=%d. Stopping.", MAX_DOCS)
                break

            query = {SORT_FIELD: {"$gt": last_sort_value}} if last_sort_value is not None else {}
            cursor = col.find(query).sort(SORT_FIELD, 1).limit(BATCH_SIZE)

            batch = list(cursor)
            if not batch:
                logging.info("No more documents. Finished reading.")
                break

            chunk_idx += 1
            part_name = f"part-{chunk_idx:06d}.parquet"
            blob_path = f"{run_prefix}/{part_name}"
            local_path = os.path.join(LOCAL_TMP_DIR, part_name)

            # If this part already exists (e.g. crash after upload but before checkpoint), skip safely
            if blob_exists(gcs, GCS_BUCKET, blob_path):
                logging.info("Skip existing GCS part: %s", blob_path)
                last_sort_value = batch[-1].get(SORT_FIELD)
                exported_docs += len(batch)
                ckpt = {
                    "chunk_idx": chunk_idx,
                    "exported_docs": exported_docs,
                    "last_sort_value": encode_sort_value(last_sort_value),
                    "updated_utc": datetime.now(timezone.utc).isoformat(),
                }
                save_checkpoint(gcs, GCS_BUCKET, run_prefix, ckpt)
                continue

            # Write parquet
            rows_written = write_parquet_chunk(batch, local_path)

            # Upload parquet
            logging.info("Uploading chunk=%d rows=%d -> %s", chunk_idx, rows_written, blob_path)
            upload_file(gcs, GCS_BUCKET, blob_path, local_path)

            # Delete local after successful upload
            try:
                os.remove(local_path)
            except OSError:
                pass

            # Advance checkpoint
            last_sort_value = batch[-1].get(SORT_FIELD)
            exported_docs += len(batch)

            ckpt = {
                "chunk_idx": chunk_idx,
                "exported_docs": exported_docs,
                "last_sort_value": encode_sort_value(last_sort_value),
                "updated_utc": datetime.now(timezone.utc).isoformat(),
            }
            save_checkpoint(gcs, GCS_BUCKET, run_prefix, ckpt)

            elapsed = time.time() - start_ts
            if chunk_idx % 10 == 0:
                logging.info("Progress: chunks=%d exported_docs=%d elapsed=%.1fs", chunk_idx, exported_docs, elapsed)

        # final manifest
        manifest = {
            "run_id": run_id,
            "db": MONGO_DB,
            "collection": MONGO_COLLECTION,
            "chunks": chunk_idx,
            "exported_docs": exported_docs,
            "batch_size": BATCH_SIZE,
            "max_docs": MAX_DOCS,
            "sort_field": SORT_FIELD,
            "started_utc": datetime.fromtimestamp(start_ts, tz=timezone.utc).isoformat(),
            "ended_utc": datetime.now(timezone.utc).isoformat(),
        }
        manifest_blob = f"{run_prefix}/manifest.json"
        gcs.bucket(GCS_BUCKET).blob(manifest_blob).upload_from_string(
            json.dumps(manifest, ensure_ascii=False, indent=2),
            content_type="application/json",
        )
        logging.info("DONE ✅ chunks=%d docs=%d manifest=%s", chunk_idx, exported_docs, manifest_blob)

    finally:
        mongo.close()
        logging.info("MongoDB connection closed.")


if __name__ == "__main__":
    export_to_gcs()
