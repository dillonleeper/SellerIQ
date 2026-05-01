import csv
import gzip
import io
import json
import logging
import time
from datetime import datetime, UTC
from pathlib import Path

import boto3
import psycopg2
from psycopg2.extras import execute_values
from sp_api.api import Reports
from sp_api.base import Marketplaces, ReportType

import config
from sp_api_utils import with_sp_api_retry, sleep_with_log

log = logging.getLogger(__name__)

SOURCE_SYSTEM = "amazon_sp_api"
REPORT_TYPE = "GET_MERCHANT_LISTINGS_ALL_DATA"
STAGING_TABLE = "stg_amz_listings"
DIM_TABLE = "dim_product"
MAP_TABLE = "int_product_identity_map"


def utc_now() -> datetime:
    return datetime.now(UTC)


def get_sp_api_credentials():
    return {
        "refresh_token": config.AMAZON_REFRESH_TOKEN,
        "lwa_app_id": config.AMAZON_CLIENT_ID,
        "lwa_client_secret": config.AMAZON_CLIENT_SECRET,
    }


def get_reports_api(marketplace_enum):
    return Reports(
        credentials=get_sp_api_credentials(),
        marketplace=marketplace_enum,
    )


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


@with_sp_api_retry(max_attempts=5, operation_name="listings.create_report")
def request_listings_report(reports_api, marketplace_id: str) -> str:
    """Request the merchant listings report and return report_id."""
    response = reports_api.create_report(
        reportType=ReportType.GET_MERCHANT_LISTINGS_ALL_DATA,
        marketplaceIds=[marketplace_id],
    )
    return response.payload["reportId"]


@with_sp_api_retry(max_attempts=5, operation_name="listings.get_report")
def _get_report_status(reports_api, report_id: str):
    return reports_api.get_report(reportId=report_id)


def wait_for_report(reports_api, report_id: str, marketplace_name: str) -> str:
    """Poll until report is complete and return document_id."""
    max_attempts = config.REPORT_POLL_MAX_ATTEMPTS
    sleep_seconds = config.REPORT_POLL_SLEEP_SECONDS

    print(f"  Waiting for report (max {max_attempts} attempts, {sleep_seconds}s interval)...")
    for attempt in range(1, max_attempts + 1):
        time.sleep(sleep_seconds)
        status_response = _get_report_status(reports_api, report_id)
        status = status_response.payload.get("processingStatus")
        print(f"  Poll {attempt}/{max_attempts} — {status}")

        if status == "DONE":
            return status_response.payload["reportDocumentId"]
        if status in ("FATAL", "CANCELLED"):
            raise RuntimeError(f"Report failed with status: {status}")

    raise TimeoutError(f"Report did not complete after {max_attempts} attempts.")


@with_sp_api_retry(max_attempts=5, operation_name="listings.get_report_document")
def _get_report_document(reports_api, document_id: str):
    return reports_api.get_report_document(reportDocumentId=document_id)


def download_report(reports_api, document_id: str):
    """Download report document and return (parsed_bytes, original_bytes)."""
    doc = _get_report_document(reports_api, document_id)
    url = doc.payload["url"]
    compression = doc.payload.get("compressionAlgorithm")

    import urllib.request
    with urllib.request.urlopen(url) as response:
        original_bytes = response.read()

    if compression == "GZIP":
        parsed_bytes = gzip.decompress(original_bytes)
    else:
        parsed_bytes = original_bytes

    return parsed_bytes, original_bytes


def save_raw_to_s3(original_bytes: bytes, marketplace_name: str, report_id: str) -> str:
    """Save raw report file to S3 and return s3_key."""
    s3_key = (
        f"amazon/{REPORT_TYPE}/"
        f"{marketplace_name}/{utc_now():%Y/%m/%d}/"
        f"listings_{marketplace_name}_{report_id}.tsv"
    )
    s3 = get_s3_client()
    s3.put_object(
        Bucket=config.S3_BUCKET,
        Key=s3_key,
        Body=original_bytes,
        ContentType="text/tab-separated-values",
    )
    return s3_key


def parse_listings_report(report_bytes: bytes, marketplace_name: str) -> list[dict]:
    """
    Parse the tab-separated listings report into rows.

    The GET_MERCHANT_LISTINGS_ALL_DATA report contains columns including:
    - seller-sku
    - asin1
    - item-name
    - status
    - item-condition
    """
    rows = []
    content = report_bytes.decode("utf-8", errors="replace")
    reader = csv.DictReader(io.StringIO(content), delimiter="\t")

    for row in reader:
        sku = (row.get("seller-sku") or "").strip()
        asin = (row.get("asin1") or "").strip()

        if not sku:
            continue

        rows.append({
            "sku": sku,
            "asin": asin if asin else None,
            "marketplace": marketplace_name,
            "item_name": (row.get("item-name") or "").strip() or None,
            "status": (row.get("status") or "").strip() or None,
            "condition_type": (row.get("item-condition") or "").strip() or None,
            "raw_response": json.dumps(dict(row)),
        })

    return rows


def upsert_listings_rows(conn, rows: list[dict], s3_key: str) -> int:
    """Upsert all listing rows into stg_amz_listings."""
    if not rows:
        return 0

    sql = f"""
        INSERT INTO {STAGING_TABLE} (
            sku, asin, marketplace,
            item_name, status, condition_type,
            raw_response, s3_key
        )
        VALUES %s
        ON CONFLICT (sku, marketplace)
        DO UPDATE SET
            asin            = EXCLUDED.asin,
            item_name       = EXCLUDED.item_name,
            status          = EXCLUDED.status,
            condition_type  = EXCLUDED.condition_type,
            raw_response    = EXCLUDED.raw_response,
            s3_key          = EXCLUDED.s3_key,
            updated_at      = NOW()
    """
    values = [
        (
            r["sku"], r["asin"], r["marketplace"],
            r["item_name"], r["status"], r["condition_type"],
            r["raw_response"], s3_key,
        )
        for r in rows
    ]
    with conn.cursor() as cur:
        execute_values(cur, sql, values)
        inserted = cur.rowcount
    conn.commit()
    return inserted


def backfill_sku_to_dim_product(conn) -> int:
    """
    Update dim_product.sku using the SKU → ASIN mapping
    from stg_amz_listings.
    """
    print("\nBackfilling SKU into dim_product...")
    sql = f"""
        UPDATE {DIM_TABLE} p
        SET
            sku        = l.sku,
            updated_at = NOW()
        FROM {STAGING_TABLE} l
        WHERE p.asin = l.asin
          AND p.marketplace = l.marketplace
          AND l.sku IS NOT NULL
          AND (p.sku IS NULL OR p.sku != l.sku)
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        updated = cur.rowcount
    conn.commit()
    print(f"dim_product: {updated} rows updated with SKU")
    return updated


def backfill_sku_to_identity_map(conn) -> int:
    """
    Update int_product_identity_map.sku from dim_product
    now that dim_product has SKUs populated.
    """
    print("Backfilling SKU into int_product_identity_map...")
    sql = f"""
        UPDATE {MAP_TABLE} m
        SET
            sku        = p.sku,
            updated_at = NOW()
        FROM {DIM_TABLE} p
        WHERE m.product_id = p.product_id
          AND p.sku IS NOT NULL
          AND (m.sku IS NULL OR m.sku != p.sku)
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        updated = cur.rowcount
    conn.commit()
    print(f"int_product_identity_map: {updated} rows updated with SKU")
    return updated


def report_sku_coverage(conn):
    """Print SKU coverage summary for dim_product."""
    sql = f"""
        SELECT
            marketplace,
            COUNT(*) AS total_products,
            COUNT(sku) AS with_sku,
            COUNT(*) - COUNT(sku) AS without_sku
        FROM {DIM_TABLE}
        GROUP BY marketplace
        ORDER BY marketplace
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()

    print("\nSKU coverage in dim_product:")
    print(f"  {'Marketplace':<15} {'Total':<10} {'With SKU':<12} {'Without SKU'}")
    print(f"  {'-'*52}")
    for row in rows:
        pct = round(row[2] / row[1] * 100, 1) if row[1] > 0 else 0
        print(f"  {row[0]:<15} {row[1]:<10} {row[2]:<12} {row[3]}  ({pct}% coverage)")


def process_marketplace(conn, marketplace_name: str, marketplace_enum, marketplace_id: str) -> bool:
    """
    Returns True on success, False on failure.
    Failures are logged but do NOT raise.
    """
    print(f"\n[{marketplace_name}] Requesting listings report...")
    try:
        reports_api = get_reports_api(marketplace_enum)

        report_id = request_listings_report(reports_api, marketplace_id)
        print(f"  Report ID: {report_id}")

        document_id = wait_for_report(reports_api, report_id, marketplace_name)

        parsed_bytes, original_bytes = download_report(reports_api, document_id)
        print(f"  Downloaded {len(original_bytes):,} bytes")

        s3_key = save_raw_to_s3(original_bytes, marketplace_name, report_id)
        print(f"  Saved to S3: {s3_key}")

        rows = parse_listings_report(parsed_bytes, marketplace_name)
        print(f"  Parsed {len(rows)} listing rows")

        inserted = upsert_listings_rows(conn, rows, s3_key)
        print(f"  Upserted {inserted} rows into {STAGING_TABLE}")
        return True

    except Exception as exc:
        try:
            conn.rollback()
        except Exception:
            pass
        log.error(
            "[%s] Listings ingestion FAILED: %s",
            marketplace_name, exc, exc_info=True,
        )
        return False


# Configurable pause between marketplaces.
INTER_MARKETPLACE_SLEEP_SECONDS = getattr(config, "INTER_MARKETPLACE_SLEEP_SECONDS", 60)


def main() -> dict[str, bool]:
    print("=" * 60)
    print("SellerIQ - Phase 2 Listings Ingestion")
    print("=" * 60)

    marketplaces = [
        ("US", Marketplaces.US, config.US_MARKETPLACE_ID),
        ("CA", Marketplaces.CA, config.CA_MARKETPLACE_ID),
    ]

    results: dict[str, bool] = {}

    conn = get_postgres_connection()
    try:
        # Step 1 — fetch listings report for each marketplace (isolated)
        for idx, (marketplace_name, marketplace_enum, marketplace_id) in enumerate(marketplaces):
            if idx > 0:
                sleep_with_log(
                    INTER_MARKETPLACE_SLEEP_SECONDS,
                    f"pacing before {marketplace_name} listings create_report call",
                )
            ok = process_marketplace(conn, marketplace_name, marketplace_enum, marketplace_id)
            results[marketplace_name] = ok

        # Steps 2-4 only run if at least one marketplace succeeded.
        if any(results.values()):
            try:
                backfill_sku_to_dim_product(conn)
                backfill_sku_to_identity_map(conn)
                report_sku_coverage(conn)
            except Exception as exc:
                log.error("Listings post-processing FAILED: %s", exc, exc_info=True)
                try:
                    conn.rollback()
                except Exception:
                    pass
        else:
            log.warning("Skipping SKU backfill — all marketplaces failed.")

    finally:
        conn.close()

    succeeded = [m for m, ok in results.items() if ok]
    failed = [m for m, ok in results.items() if not ok]

    print("\n" + "=" * 60)
    print("Listings ingestion summary:")
    print(f"  Succeeded: {succeeded or '(none)'}")
    print(f"  Failed:    {failed or '(none)'}")
    print("=" * 60)

    return results


if __name__ == "__main__":
    main()