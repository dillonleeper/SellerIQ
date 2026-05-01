"""
SellerIQ — Reload Sales & Traffic from S3
==========================================
Read every GET_SALES_AND_TRAFFIC_REPORT raw file from S3 and load
the parsed rows into stg_amz_sales_traffic_daily.

DOES NOT CALL SP-API. Pure file replay using data we already have.

Why this exists
---------------
After a destructive operation on prod's Postgres, the warehouse can
be rebuilt from S3 raw files because raw files are immutable per the
ingestion rules. This script handles the sales-traffic portion.

Idempotent: the unique constraint on (start_date, marketplace, child_asin)
in staging means re-running this is safe — duplicates are skipped.

Usage
-----
    python reload_sales_from_s3.py

What you'll see
---------------
- Lists files in S3
- Downloads each one
- Parses to rows
- Bulk inserts to staging
- Reports per-file counts and totals at the end

Estimated runtime: ~5-15 minutes for ~1000 weekly files.
"""

from __future__ import annotations

import gzip
import io
import json
import sys
from datetime import datetime, UTC

import boto3
import psycopg2
from psycopg2.extras import execute_values

import config


REPORT_TYPE = "GET_SALES_AND_TRAFFIC_REPORT"
STAGING_TABLE = "stg_amz_sales_traffic_daily"
S3_PREFIX = f"amazon/{REPORT_TYPE}/"


def get_s3_client():
    return boto3.client(
        "s3",
        aws_access_key_id=config.AWS_ACCESS_KEY_ID,
        aws_secret_access_key=config.AWS_SECRET_ACCESS_KEY,
        region_name=config.AWS_REGION,
    )


def get_postgres_connection():
    return psycopg2.connect(
        host=config.POSTGRES_HOST,
        port=config.POSTGRES_PORT,
        dbname=config.POSTGRES_DB,
        user=config.POSTGRES_USER,
        password=config.POSTGRES_PASSWORD,
    )


def list_all_keys(s3, bucket: str, prefix: str) -> list[str]:
    """List every S3 object key under a prefix. Returns list of keys."""
    keys = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            # We want the .json.gz files (originals). Skip the .json copies.
            if key.endswith(".json.gz"):
                keys.append(key)
    return sorted(keys)


def download_and_parse(s3, bucket: str, key: str) -> tuple[list[dict], dict]:
    """
    Download one S3 object, decompress, parse, and return (rows, metadata).

    metadata contains marketplace, start_date, end_date inferred from the
    file path. The actual data may have its own start/end dates which we
    cross-check.
    """
    response = s3.get_object(Bucket=bucket, Key=key)
    raw_bytes = response["Body"].read()
    parsed_bytes = gzip.decompress(raw_bytes)
    report_json = json.loads(parsed_bytes.decode("utf-8"))

    # Extract marketplace and date range from the file path:
    # amazon/GET_SALES_AND_TRAFFIC_REPORT/{MARKETPLACE}/{YYYY}/{MM}/{DD}/file.json.gz
    parts = key.split("/")
    # parts[2] = marketplace, parts[3] = year, parts[4] = month, parts[5] = day
    if len(parts) < 7:
        return [], {}
    marketplace = parts[2]

    # Date range comes from the filename which looks like:
    # amazon_sales_traffic_US_2026-04-19_2026-04-25.json.gz
    filename = parts[-1]
    try:
        # Strip prefix and suffix to get the date portion
        date_portion = filename.replace("amazon_sales_traffic_", "").replace(".json.gz", "")
        # Now: US_2026-04-19_2026-04-25
        # Split off the marketplace prefix (US_ or CA_)
        _, start_date_str, end_date_str = date_portion.split("_", 2)
        start_date = datetime.strptime(start_date_str, "%Y-%m-%d").date()
        end_date = datetime.strptime(end_date_str, "%Y-%m-%d").date()
    except (ValueError, IndexError) as exc:
        print(f"  WARN: could not parse dates from {filename}: {exc}")
        return [], {}

    # Use the same parsing logic as ingest_sales_traffic.py
    rows = []
    for row in report_json.get("salesAndTrafficByAsin", []):
        sales = row.get("salesByAsin", {})
        traffic = row.get("trafficByAsin", {})
        rows.append({
            "marketplace": marketplace,
            "start_date": start_date,
            "end_date": end_date,
            "child_asin": row.get("childAsin"),
            "parent_asin": row.get("parentAsin"),
            "sku": row.get("sku"),
            "sessions": traffic.get("sessions"),
            "page_views": traffic.get("pageViews"),
            "buy_box_percentage": traffic.get("buyBoxPercentage"),
            "units_ordered": sales.get("unitsOrdered"),
            "ordered_product_sales_amount": (sales.get("orderedProductSales") or {}).get("amount"),
            "ordered_product_sales_currency": (sales.get("orderedProductSales") or {}).get("currencyCode"),
            "unit_session_percentage": traffic.get("unitSessionPercentage"),
        })

    metadata = {
        "marketplace": marketplace,
        "start_date": start_date,
        "end_date": end_date,
        "s3_key": key,
        "row_count": len(rows),
    }
    return rows, metadata


def insert_rows(conn, rows: list[dict], s3_key: str) -> int:
    """
    Insert rows into stg_amz_sales_traffic_daily.

    Uses ON CONFLICT DO NOTHING on the natural key
    (start_date, marketplace, child_asin). The original report_id
    isn't available in the raw filename so we synthesize one from s3_key.

    Returns: number of rows inserted (excludes duplicates skipped).
    """
    if not rows:
        return 0

    # Synthesize a "report_id" from the S3 key so the row has traceability
    # back to its origin file. Use the filename portion only.
    synthetic_report_id = "s3:" + s3_key.split("/")[-1].replace(".json.gz", "")

    sql = f"""
        INSERT INTO {STAGING_TABLE} (
            report_id, s3_key,
            marketplace, start_date, end_date,
            child_asin, parent_asin, sku,
            sessions, page_views, buy_box_percentage,
            units_ordered, ordered_product_sales_amount,
            ordered_product_sales_currency, unit_session_percentage
        )
        VALUES %s
        ON CONFLICT (start_date, marketplace, child_asin) DO NOTHING
    """
    values = [
        (
            synthetic_report_id, s3_key,
            r["marketplace"], r["start_date"], r["end_date"],
            r["child_asin"], r["parent_asin"], r["sku"],
            r["sessions"], r["page_views"], r["buy_box_percentage"],
            r["units_ordered"], r["ordered_product_sales_amount"],
            r["ordered_product_sales_currency"], r["unit_session_percentage"],
        )
        for r in rows
    ]
    with conn.cursor() as cur:
        execute_values(cur, sql, values)
        inserted = cur.rowcount
    return inserted


def main():
    print("=" * 70)
    print("SellerIQ — Sales & Traffic Reload from S3")
    print("=" * 70)
    print(f"Bucket:        {config.S3_BUCKET}")
    print(f"Prefix:        {S3_PREFIX}")
    print(f"Postgres host: {config.POSTGRES_HOST}")
    print(f"Database:      {config.POSTGRES_DB}")
    print()

    # Safety prompt — give the user a chance to stop if something looks off
    print(f"This will load ALL sales-traffic raw files from S3 into")
    print(f"  {config.POSTGRES_HOST} → {STAGING_TABLE}")
    print()
    response = input("Type 'reload' to proceed: ").strip()
    if response != "reload":
        print("Aborted.")
        return 1

    print()
    print("Listing S3 files...")
    s3 = get_s3_client()
    keys = list_all_keys(s3, config.S3_BUCKET, S3_PREFIX)
    print(f"Found {len(keys)} sales-traffic files in S3.")
    print()

    if not keys:
        print("No files to load. Exiting.")
        return 1

    conn = get_postgres_connection()
    total_rows_inserted = 0
    total_rows_parsed = 0
    files_succeeded = 0
    files_failed = 0
    failures: list[tuple[str, str]] = []

    try:
        for i, key in enumerate(keys, 1):
            try:
                rows, meta = download_and_parse(s3, config.S3_BUCKET, key)
                if not rows:
                    files_failed += 1
                    failures.append((key, "no rows parsed"))
                    print(f"  [{i}/{len(keys)}] {key} — 0 rows (skipped)")
                    continue
                inserted = insert_rows(conn, rows, key)
                conn.commit()
                total_rows_parsed += len(rows)
                total_rows_inserted += inserted
                files_succeeded += 1
                if i % 50 == 0 or i == len(keys):
                    print(f"  [{i}/{len(keys)}] processed — running totals: "
                          f"{files_succeeded} ok, {files_failed} failed, "
                          f"{total_rows_inserted} rows inserted")
            except Exception as exc:
                files_failed += 1
                failures.append((key, str(exc)))
                print(f"  [{i}/{len(keys)}] FAILED {key}: {exc}")
                try:
                    conn.rollback()
                except Exception:
                    pass

    finally:
        conn.close()

    print()
    print("=" * 70)
    print("Reload complete")
    print("=" * 70)
    print(f"  Files processed: {len(keys)}")
    print(f"  Files succeeded: {files_succeeded}")
    print(f"  Files failed:    {files_failed}")
    print(f"  Rows parsed:     {total_rows_parsed}")
    print(f"  Rows inserted:   {total_rows_inserted}")
    print(f"  Duplicates skipped: {total_rows_parsed - total_rows_inserted}")
    print()

    if failures:
        print(f"Failures ({len(failures)}):")
        for key, err in failures[:20]:
            print(f"  {key}: {err}")
        if len(failures) > 20:
            print(f"  ... and {len(failures) - 20} more")

    return 0 if files_failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
