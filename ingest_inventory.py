import csv
import gzip
import hashlib
import io
import json
import logging
import time
from datetime import datetime, date, UTC

import boto3
import psycopg2
from psycopg2.extras import execute_values
from sp_api.api import Reports
from sp_api.base import Marketplaces, ReportType

import config
from sp_api_utils import with_sp_api_retry, sleep_with_log

log = logging.getLogger(__name__)

SOURCE_SYSTEM = "amazon_sp_api"
REPORT_TYPE = "GET_FBA_MYI_ALL_INVENTORY_DATA"
STAGING_TABLE = "stg_amz_inventory_snapshot"
FCT_TABLE = "fct_inventory_snapshot_daily"
DIM_TABLE = "dim_product"
LOG_TABLE = "ingestion_job_log"


def utc_now() -> datetime:
    return datetime.now(UTC)


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


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


def log_job_status(
    conn,
    *,
    marketplace: str,
    report_id: str | None,
    document_id: str | None,
    request_status: str,
    requested_at: datetime | None = None,
    downloaded_at: datetime | None = None,
    loaded_at: datetime | None = None,
    completed_at: datetime | None = None,
    local_file_path: str | None = None,
    s3_key: str | None = None,
    file_checksum: str | None = None,
    row_count: int | None = None,
    error_message: str | None = None,
):
    sql = f"""
        INSERT INTO {LOG_TABLE} (
            source_system, report_type, marketplace,
            report_id, document_id, request_status,
            requested_at, downloaded_at, loaded_at, completed_at,
            local_file_path, s3_key, file_checksum,
            row_count, error_message, updated_at
        )
        VALUES (
            %(source_system)s, %(report_type)s, %(marketplace)s,
            %(report_id)s, %(document_id)s, %(request_status)s,
            %(requested_at)s, %(downloaded_at)s, %(loaded_at)s, %(completed_at)s,
            %(local_file_path)s, %(s3_key)s, %(file_checksum)s,
            %(row_count)s, %(error_message)s, NOW()
        )
        ON CONFLICT (source_system, report_type, marketplace, report_id, document_id)
        DO UPDATE SET
            request_status  = EXCLUDED.request_status,
            requested_at    = COALESCE(EXCLUDED.requested_at,    {LOG_TABLE}.requested_at),
            downloaded_at   = COALESCE(EXCLUDED.downloaded_at,   {LOG_TABLE}.downloaded_at),
            loaded_at       = COALESCE(EXCLUDED.loaded_at,       {LOG_TABLE}.loaded_at),
            completed_at    = COALESCE(EXCLUDED.completed_at,    {LOG_TABLE}.completed_at),
            local_file_path = COALESCE(EXCLUDED.local_file_path, {LOG_TABLE}.local_file_path),
            s3_key          = COALESCE(EXCLUDED.s3_key,          {LOG_TABLE}.s3_key),
            file_checksum   = COALESCE(EXCLUDED.file_checksum,   {LOG_TABLE}.file_checksum),
            row_count       = COALESCE(EXCLUDED.row_count,       {LOG_TABLE}.row_count),
            error_message   = COALESCE(EXCLUDED.error_message,   {LOG_TABLE}.error_message),
            updated_at      = NOW()
    """
    params = {
        "source_system": SOURCE_SYSTEM,
        "report_type": REPORT_TYPE,
        "marketplace": marketplace,
        "report_id": report_id,
        "document_id": document_id,
        "request_status": request_status,
        "requested_at": requested_at,
        "downloaded_at": downloaded_at,
        "loaded_at": loaded_at,
        "completed_at": completed_at,
        "local_file_path": local_file_path,
        "s3_key": s3_key,
        "file_checksum": file_checksum,
        "row_count": row_count,
        "error_message": error_message,
    }
    with conn.cursor() as cur:
        cur.execute(sql, params)


@with_sp_api_retry(max_attempts=5, operation_name="inventory.create_report")
def request_inventory_report(reports_api, marketplace_id: str) -> str:
    response = reports_api.create_report(
        reportType=ReportType.GET_FBA_MYI_ALL_INVENTORY_DATA,
        marketplaceIds=[marketplace_id],
    )
    return response.payload["reportId"]


@with_sp_api_retry(max_attempts=5, operation_name="inventory.get_report")
def _get_report_status(reports_api, report_id: str):
    return reports_api.get_report(reportId=report_id)


def wait_for_report(reports_api, report_id: str, marketplace_name: str) -> str:
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


@with_sp_api_retry(max_attempts=5, operation_name="inventory.get_report_document")
def _get_report_document(reports_api, document_id: str):
    return reports_api.get_report_document(reportDocumentId=document_id)


def download_report(reports_api, document_id: str):
    """Download report and return (parsed_bytes, original_bytes)."""
    doc = _get_report_document(reports_api, document_id)
    url = doc.payload["url"]
    compression = doc.payload.get("compressionAlgorithm")

    import urllib.request
    with urllib.request.urlopen(url) as response:
        original_bytes = response.read()

    parsed_bytes = gzip.decompress(original_bytes) if compression == "GZIP" else original_bytes
    return parsed_bytes, original_bytes


def save_raw_to_s3(original_bytes: bytes, marketplace_name: str,
                   report_id: str, snapshot_date: date) -> str:
    s3_key = (
        f"amazon/{REPORT_TYPE}/"
        f"{marketplace_name}/{snapshot_date:%Y/%m/%d}/"
        f"inventory_{marketplace_name}_{report_id}.tsv"
    )
    s3 = get_s3_client()
    s3.put_object(
        Bucket=config.S3_BUCKET,
        Key=s3_key,
        Body=original_bytes,
        ContentType="text/tab-separated-values",
    )
    return s3_key


def safe_int(value) -> int | None:
    """Convert a value to int, returning None if empty or invalid."""
    if value is None:
        return None
    try:
        stripped = str(value).strip()
        return int(stripped) if stripped else None
    except (ValueError, TypeError):
        return None


def parse_inventory_report(report_bytes: bytes, marketplace_name: str,
                            snapshot_date: date) -> list[dict]:
    """
    Parse the FBA inventory report TSV into rows.

    The GET_FBA_MYI_UNSUPPRESSED_INVENTORY_DATA report contains:
    - sku
    - fnsku
    - asin
    - product-name
    - condition
    - your-price
    - mfn-listing-exists
    - mfn-fulfillable-quantity
    - afn-listing-exists
    - afn-warehouse-quantity
    - afn-fulfillable-quantity
    - afn-unsellable-quantity
    - afn-reserved-quantity
    - afn-total-quantity
    - per-unit-volume
    - afn-inbound-working-quantity
    - afn-inbound-shipped-quantity
    - afn-inbound-receiving-quantity
    - afn-researching-quantity
    - afn-reserved-future-supply
    - afn-future-supply-buyable
    """
    rows = []
    content = report_bytes.decode("utf-8", errors="replace")
    reader = csv.DictReader(io.StringIO(content), delimiter="\t")

    for row in reader:
        sku = (row.get("sku") or "").strip()
        if not sku:
            continue

        fulfillable = safe_int(row.get("afn-fulfillable-quantity"))
        reserved = safe_int(row.get("afn-reserved-quantity"))
        inbound_working = safe_int(row.get("afn-inbound-working-quantity"))
        inbound_shipped = safe_int(row.get("afn-inbound-shipped-quantity"))
        inbound_receiving = safe_int(row.get("afn-inbound-receiving-quantity"))
        unsellable = safe_int(row.get("afn-unsellable-quantity"))
        total = safe_int(row.get("afn-total-quantity"))

        # Calculate total inbound
        total_inbound = None
        inbound_parts = [inbound_working, inbound_shipped, inbound_receiving]
        if any(v is not None for v in inbound_parts):
            total_inbound = sum(v or 0 for v in inbound_parts)

        rows.append({
            "snapshot_date": snapshot_date,
            "marketplace": marketplace_name,
            "sku": sku,
            "asin": (row.get("asin") or "").strip() or None,
            "fnsku": (row.get("fnsku") or "").strip() or None,
            "product_name": (row.get("product-name") or "").strip() or None,
            "condition": (row.get("condition") or "").strip() or None,
            "fulfillable_quantity": fulfillable,
            "reserved_quantity": reserved,
            "inbound_working_quantity": inbound_working,
            "inbound_shipped_quantity": inbound_shipped,
            "inbound_receiving_quantity": inbound_receiving,
            "total_inbound_quantity": total_inbound,
            "unsellable_quantity": unsellable,
            "total_quantity": total,
        })

    return rows


def load_staging_rows(conn, rows: list[dict], report_id: str,
                      document_id: str, s3_key: str, file_checksum: str) -> int:
    if not rows:
        return 0

    sql = f"""
        INSERT INTO {STAGING_TABLE} (
            report_id, report_document_id, s3_key, file_checksum,
            snapshot_date, marketplace,
            sku, asin, fnsku, product_name, condition,
            fulfillable_quantity, reserved_quantity,
            inbound_working_quantity, inbound_shipped_quantity,
            inbound_receiving_quantity, total_inbound_quantity,
            unsellable_quantity, total_quantity
        )
        VALUES %s
        ON CONFLICT (snapshot_date, sku, marketplace) DO NOTHING
    """
    values = [
        (
            report_id, document_id, s3_key, file_checksum,
            r["snapshot_date"], r["marketplace"],
            r["sku"], r["asin"], r["fnsku"], r["product_name"], r["condition"],
            r["fulfillable_quantity"], r["reserved_quantity"],
            r["inbound_working_quantity"], r["inbound_shipped_quantity"],
            r["inbound_receiving_quantity"], r["total_inbound_quantity"],
            r["unsellable_quantity"], r["total_quantity"],
        )
        for r in rows
    ]
    with conn.cursor() as cur:
        execute_values(cur, sql, values)
        inserted = cur.rowcount
    conn.commit()
    return inserted


def build_fct_inventory_snapshot(conn, snapshot_date: date, marketplace: str) -> int:
    """
    Build fct_inventory_snapshot_daily from staging for a given
    snapshot_date and marketplace.

    Joins to dim_product via SKU + marketplace to resolve product_id.
    Calculates available_quantity = fulfillable - reserved.
    """
    sql = f"""
        INSERT INTO {FCT_TABLE} (
            snapshot_date, marketplace,
            product_id, sku, asin, fnsku,
            fulfillable_quantity, reserved_quantity,
            inbound_working_quantity, inbound_shipped_quantity,
            inbound_receiving_quantity, total_inbound_quantity,
            unsellable_quantity, available_quantity
        )
        SELECT
            s.snapshot_date,
            s.marketplace,
            p.product_id,
            s.sku,
            s.asin,
            s.fnsku,
            s.fulfillable_quantity,
            s.reserved_quantity,
            s.inbound_working_quantity,
            s.inbound_shipped_quantity,
            s.inbound_receiving_quantity,
            s.total_inbound_quantity,
            s.unsellable_quantity,
            -- available = fulfillable - reserved
            GREATEST(
                COALESCE(s.fulfillable_quantity, 0)
                - COALESCE(s.reserved_quantity, 0),
                0
            ) AS available_quantity
        FROM {STAGING_TABLE} s
        LEFT JOIN {DIM_TABLE} p
            ON p.sku = s.sku
            AND p.marketplace = s.marketplace
        WHERE s.snapshot_date = %s
          AND s.marketplace = %s
        ON CONFLICT (snapshot_date, sku, marketplace)
        DO UPDATE SET
            product_id                  = EXCLUDED.product_id,
            asin                        = EXCLUDED.asin,
            fnsku                       = EXCLUDED.fnsku,
            fulfillable_quantity        = EXCLUDED.fulfillable_quantity,
            reserved_quantity           = EXCLUDED.reserved_quantity,
            inbound_working_quantity    = EXCLUDED.inbound_working_quantity,
            inbound_shipped_quantity    = EXCLUDED.inbound_shipped_quantity,
            inbound_receiving_quantity  = EXCLUDED.inbound_receiving_quantity,
            total_inbound_quantity      = EXCLUDED.total_inbound_quantity,
            unsellable_quantity         = EXCLUDED.unsellable_quantity,
            available_quantity          = EXCLUDED.available_quantity,
            loaded_at                   = NOW()
    """
    with conn.cursor() as cur:
        cur.execute(sql, (snapshot_date, marketplace))
        upserted = cur.rowcount
    conn.commit()
    return upserted


def print_inventory_summary(conn, snapshot_date: date):
    """Print a summary of today's inventory snapshot."""
    sql = f"""
        SELECT
            marketplace,
            COUNT(*) AS skus,
            SUM(fulfillable_quantity) AS total_fulfillable,
            SUM(available_quantity) AS total_available,
            SUM(reserved_quantity) AS total_reserved,
            SUM(total_inbound_quantity) AS total_inbound,
            COUNT(CASE WHEN available_quantity = 0 THEN 1 END) AS out_of_stock
        FROM {FCT_TABLE}
        WHERE snapshot_date = %s
        GROUP BY marketplace
        ORDER BY marketplace
    """
    with conn.cursor() as cur:
        cur.execute(sql, (snapshot_date,))
        rows = cur.fetchall()

    print(f"\nInventory snapshot summary for {snapshot_date}:")
    print(f"  {'MKT':<6} {'SKUs':<8} {'Fulfillable':<14} {'Available':<12} {'Reserved':<12} {'Inbound':<12} {'OOS'}")
    print(f"  {'-'*72}")
    for row in rows:
        print(f"  {row[0]:<6} {row[1]:<8} {row[2] or 0:<14} {row[3] or 0:<12} {row[4] or 0:<12} {row[5] or 0:<12} {row[6]}")


def process_marketplace(conn, marketplace_name: str, marketplace_enum,
                        marketplace_id: str, snapshot_date: date) -> bool:
    """
    Returns True on success, False on failure.

    Failures are logged but do NOT raise — so a single marketplace failure
    does not block other marketplaces.
    """
    print(f"\n[{marketplace_name}] Requesting inventory snapshot...")
    requested_at = utc_now()
    report_id = None
    document_id = None
    s3_key = None
    file_checksum = None
    downloaded_at = None
    loaded_at = None
    row_count = None

    try:
        reports_api = get_reports_api(marketplace_enum)
        report_id = request_inventory_report(reports_api, marketplace_id)
        print(f"  Report ID: {report_id}")

        log_job_status(
            conn, marketplace=marketplace_name,
            report_id=report_id, document_id=None,
            request_status="requested", requested_at=requested_at,
        )
        conn.commit()

        document_id = wait_for_report(reports_api, report_id, marketplace_name)
        parsed_bytes, original_bytes = download_report(reports_api, document_id)
        downloaded_at = utc_now()
        file_checksum = sha256_hex(original_bytes)

        s3_key = save_raw_to_s3(original_bytes, marketplace_name, report_id, snapshot_date)
        print(f"  Saved to S3: {s3_key}")

        log_job_status(
            conn, marketplace=marketplace_name,
            report_id=report_id, document_id=document_id,
            request_status="downloaded", requested_at=requested_at,
            downloaded_at=downloaded_at, s3_key=s3_key, file_checksum=file_checksum,
        )
        conn.commit()

        rows = parse_inventory_report(parsed_bytes, marketplace_name, snapshot_date)
        print(f"  Parsed {len(rows)} inventory rows")

        row_count = load_staging_rows(
            conn, rows,
            report_id=report_id, document_id=document_id,
            s3_key=s3_key, file_checksum=file_checksum,
        )
        print(f"  Loaded {row_count} rows into {STAGING_TABLE}")

        fct_count = build_fct_inventory_snapshot(conn, snapshot_date, marketplace_name)
        print(f"  Built {fct_count} rows in {FCT_TABLE}")

        loaded_at = utc_now()
        log_job_status(
            conn, marketplace=marketplace_name,
            report_id=report_id, document_id=document_id,
            request_status="completed", requested_at=requested_at,
            downloaded_at=downloaded_at, loaded_at=loaded_at,
            completed_at=loaded_at, s3_key=s3_key,
            file_checksum=file_checksum, row_count=row_count,
        )
        conn.commit()
        return True

    except Exception as exc:
        try:
            conn.rollback()
        except Exception:
            pass

        log.error(
            "[%s] Inventory ingestion FAILED: %s",
            marketplace_name, exc, exc_info=True,
        )
        try:
            log_job_status(
                conn, marketplace=marketplace_name,
                report_id=report_id, document_id=document_id,
                request_status="failed", requested_at=requested_at,
                downloaded_at=downloaded_at, loaded_at=loaded_at,
                completed_at=utc_now(), s3_key=s3_key,
                file_checksum=file_checksum, row_count=row_count,
                error_message=str(exc),
            )
            conn.commit()
        except Exception as log_exc:
            log.error("[%s] Could not write failure to ingestion_job_log: %s",
                      marketplace_name, log_exc)
            try:
                conn.rollback()
            except Exception:
                pass

        return False


# Configurable pause between marketplaces.
INTER_MARKETPLACE_SLEEP_SECONDS = getattr(config, "INTER_MARKETPLACE_SLEEP_SECONDS", 60)


def main() -> dict[str, bool]:
    print("=" * 60)
    print("SellerIQ - Phase 3 Inventory Snapshot Ingestion")
    print("=" * 60)

    snapshot_date = utc_now().date()
    print(f"Snapshot date: {snapshot_date}")

    marketplaces = [
        ("US", Marketplaces.US, config.US_MARKETPLACE_ID),
        ("CA", Marketplaces.CA, config.CA_MARKETPLACE_ID),
    ]

    results: dict[str, bool] = {}

    conn = get_postgres_connection()
    try:
        for idx, (marketplace_name, marketplace_enum, marketplace_id) in enumerate(marketplaces):
            if idx > 0:
                sleep_with_log(
                    INTER_MARKETPLACE_SLEEP_SECONDS,
                    f"pacing before {marketplace_name} inventory create_report call",
                )

            ok = process_marketplace(
                conn, marketplace_name, marketplace_enum,
                marketplace_id, snapshot_date,
            )
            results[marketplace_name] = ok

        print_inventory_summary(conn, snapshot_date)

    finally:
        conn.close()

    succeeded = [m for m, ok in results.items() if ok]
    failed = [m for m, ok in results.items() if not ok]

    print("\n" + "=" * 60)
    print("Inventory snapshot summary:")
    print(f"  Succeeded: {succeeded or '(none)'}")
    print(f"  Failed:    {failed or '(none)'}")
    print("=" * 60)

    return results


if __name__ == "__main__":
    main()
