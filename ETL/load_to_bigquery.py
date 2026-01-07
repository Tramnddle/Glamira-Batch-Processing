"""
Load Parquet from GCS into BigQuery, with support for loading NORMALIZED data.

New features:
- --use_normalized: switch base_prefix to exports/mongo_parquet_normalized/
- --prefix: optionally load only a subpath under the run_id (e.g. db=countly/collection=summary/)
"""

from __future__ import annotations

import argparse
from typing import List, Optional

from google.cloud import storage
from google.cloud import bigquery


def list_parquet_uris(bucket_name: str, prefix: str) -> List[str]:
    storage_client = storage.Client()
    uris: List[str] = []
    for blob in storage_client.list_blobs(bucket_name, prefix=prefix):
        if blob.name.lower().endswith(".parquet"):
            uris.append(f"gs://{bucket_name}/{blob.name}")
    return uris


def chunked(items: List[str], size: int) -> List[List[str]]:
    return [items[i : i + size] for i in range(0, len(items), size)]


def ensure_dataset(bq: bigquery.Client, dataset_id: str, location: str) -> None:
    ds = bigquery.Dataset(f"{bq.project}.{dataset_id}")
    ds.location = location
    bq.create_dataset(ds, exists_ok=True)


def load_parquet_batches(
    project_id: str,
    dataset_id: str,
    table_id: str,
    gcs_uris: List[str],
    location: str = "EU",
    write_disposition: str = "WRITE_TRUNCATE",
    create_disposition: str = "CREATE_IF_NEEDED",
    partition_field: Optional[str] = None,
    clustering_fields: Optional[List[str]] = None,
    batch_size: int = 9500,
) -> None:
    bq = bigquery.Client(project=project_id)
    ensure_dataset(bq, dataset_id, location)

    table_fqn = f"{project_id}.{dataset_id}.{table_id}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=write_disposition,
        create_disposition=create_disposition,
        autodetect=True,
    )

    if partition_field:
        job_config.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field=partition_field,
        )

    if clustering_fields:
        job_config.clustering_fields = clustering_fields

    batches = chunked(gcs_uris, batch_size)
    print(f"Loading {len(gcs_uris)} parquet files into {table_fqn} in {len(batches)} batch(es).")

    # If truncating, do it only on the first batch, then append subsequent batches
    first_write = write_disposition

    for i, uris_batch in enumerate(batches, start=1):
        if i > 1 and first_write == "WRITE_TRUNCATE":
            job_config.write_disposition = "WRITE_APPEND"
        else:
            job_config.write_disposition = first_write

        print(f"Batch {i}/{len(batches)}: {len(uris_batch)} files...")
        job = bq.load_table_from_uri(
            uris_batch,
            destination=table_fqn,
            job_config=job_config,
            location=location,
        )
        print(f"  Job started: {job.job_id}")
        job.result()
        print(f"  Job finished: {job.job_id}")

    tbl = bq.get_table(table_fqn)
    print(f"Done. {table_fqn}: {tbl.num_rows} rows, {len(tbl.schema)} columns.")


def build_run_prefix(
    base_prefix: str,
    run_id: str,
    extra_prefix: Optional[str] = None,
) -> str:
    """
    Returns something like:
      exports/mongo_parquet_normalized/run_id=20251230T165024Z/
    or with extra prefix:
      exports/mongo_parquet_normalized/run_id=.../db=.../collection=.../
    """
    base = base_prefix.rstrip("/")
    run_part = f"{base}/run_id={run_id}/"
    if not extra_prefix:
        return run_part
    extra = extra_prefix.strip("/")
    return f"{run_part}{extra}/"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", required=True)
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--table", required=True)

    parser.add_argument("--bucket", default="unigap_l3_batch")
    parser.add_argument("--run_id", required=True, help="e.g. 20251230T165024Z")

    # original base prefix (non-normalized)
    parser.add_argument("--base_prefix", default="exports/mongo_parquet/")
    # when set, override base_prefix to normalized location
    parser.add_argument(
        "--use_normalized",
        action="store_true",
        help="Load from exports/mongo_parquet_normalized/ instead of exports/mongo_parquet/",
    )

    # optionally load only a subfolder under run_id (e.g. db=countly/collection=summary)
    parser.add_argument(
        "--prefix",
        default=None,
        help="Optional sub-prefix under the run_id, e.g. 'db=countly/collection=summary'",
    )

    parser.add_argument("--location", default="EU")
    parser.add_argument(
        "--write",
        default="WRITE_TRUNCATE",
        choices=["WRITE_APPEND", "WRITE_TRUNCATE", "WRITE_EMPTY"],
        help="WRITE_TRUNCATE is recommended for reloading the same run into the same table.",
    )
    parser.add_argument("--partition_field", default=None)
    parser.add_argument("--cluster", default=None, help="comma-separated clustering fields")
    parser.add_argument("--batch_size", type=int, default=9500)

    args = parser.parse_args()

    cluster_fields = [c.strip() for c in args.cluster.split(",")] if args.cluster else None

    base_prefix = "exports/mongo_parquet_normalized/" if args.use_normalized else args.base_prefix
    run_prefix = build_run_prefix(base_prefix=base_prefix, run_id=args.run_id, extra_prefix=args.prefix)

    print(f"Listing parquet under: gs://{args.bucket}/{run_prefix}")
    uris = list_parquet_uris(args.bucket, run_prefix)

    if not uris:
        raise SystemExit(f"No parquet files found under gs://{args.bucket}/{run_prefix}")

    print(f"Found {len(uris)} parquet files. Example: {uris[0]}")

    load_parquet_batches(
        project_id=args.project,
        dataset_id=args.dataset,
        table_id=args.table,
        gcs_uris=uris,
        location=args.location,
        write_disposition=args.write,
        partition_field=args.partition_field,
        clustering_fields=cluster_fields,
        batch_size=args.batch_size,
    )


if __name__ == "__main__":
    main()
