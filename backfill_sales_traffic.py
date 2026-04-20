"""
SellerIQ - Sales & Traffic Daily Backfill
==========================================
Pulls one report per month per marketplace from BACKFILL_START
up to and including yesterday.

Each report uses dateGranularity: DAY so a single monthly response
contains product rows for each day in that month.

Safe to re-run — months where every day is already loaded are skipped automatically.

Usage:
    python backfill_sales_traffic.py
"""

import gzip
import hashlib
import json
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, UTC
from pathlib import Path

import boto3
import psycopg2
from psycopg2.extras import execute_values
from sp_api.api import Reports
from sp_api.base import Marketplaces, ReportType

import config

REPORT_TYPE = "GET_SALES_AND_TRAFFIC_REPORT"
SOURCE_SYSTEM = "amazon_sp_api"
LOG_TABLE = "ingestion_job_log"
STAGING_TABLE = "stg_amz_sales_traffic_daily"

# ── Change this date to control how far back the backfill goes ──
BACKFILL_START = datetime(2025, 1, 1, tzinfo=UTC)

# Pause between days to avoid hitting API rate limits
SLEEP_BETWEEN_DAYS = 8  # seconds


def utc_now() -> datetime:
    return datetime.now(UTC)


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def get_yesterday() -> datetime:
    return (utc_now() - timedelta(days=1)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )


def get_month_start(day: datetime) -> datetime:
    return day.replace(day=1, hour=0, minute=0, second=0, microsecond=0)


def get_month_end(day: datetime) -> datetime:
    next_month = (day.replace(day=28) + timedelta(days=4)).replace(day=1)
    return next_month - timedelta(days=1)


def get_backfill_months() -> list[tuple[datetime, datetime]]:
    """Return month windows from BACKFILL_START through the month containing yesterday."""
    yesterday = get_yesterday()
    months = []
    current = get_month_start(BACKFILL_START)

    while current <= yesterday:
        month_end = min(get_month_end(current), yesterday)
        months.append((current, month_end))
        current = (current.replace(day=28) + timedelta(days=4)).replace(day=1)

    return months


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


def ensure_log_table_exists(conn):
    sql = f"""
        CREATE TABLE IF NOT EXISTS {LOG_TABLE} (
            id BIGSERIAL PRIMARY KEY,
            source_system TEXT NOT NULL,
            report_type TEXT NOT NULL,
            marketplace TEXT NOT NULL,
            report_id TEXT,
            document_id TEXT,
            request_status TEXT NOT NULL,
            requested_at TIMESTAMPTZ,
            downloaded_at TIMESTAMPTZ,
            loaded_at TIMESTAMPTZ,
            completed_at TIMESTAMPTZ,
            local_file_path TEXT,
            s3_key TEXT,
            file_checksum TEXT,
            row_count INTEGER,
            error_message TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE (source_system, report_type, marketplace, report_id, document_id)
        )
    """
    with conn.cursor() as cur:
        cur.execute(sql)


def ensure_staging_table_exists(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass(%s)", (STAGING_TABLE,))
        if cur.fetchone()[0] is None:
            raise RuntimeError(
                f"Required staging table '{STAGING_TABLE}' does not exist. "
                "Run selleriq_phase1_ddl.sql before running ingestion."
            )


def is_month_already_loaded(
    conn, marketplace: str, start_date: datetime, end_date: datetime
) -> bool:
    """Return True when every day in the month window already has staged rows."""
    sql = f"""
        SELECT COUNT(DISTINCT start_date)
        FROM {STAGING_TABLE}
        WHERE start_date BETWEEN %s AND %s
          AND marketplace = %s
    """

    expected_days = (end_date.date() - start_date.date()).days + 1
    with conn.cursor() as cur:
        cur.execute(sql, (start_date.date(), end_date.date(), marketplace))
        loaded_days = cur.fetchone()[0] or 0
        return loaded_days >= expected_days


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


def request_report(
    marketplace_name,
    marketplace_enum,
    marketplace_id,
    start_date: datetime,
    end_date: datetime,
):
    reports_api = get_reports_api(marketplace_enum)
    response = reports_api.create_report(
        reportType=ReportType.GET_SALES_AND_TRAFFIC_REPORT,
        dataStartTime=start_date.strftime("%Y-%m-%d"),
        dataEndTime=end_date.strftime("%Y-%m-%d"),
        marketplaceIds=[marketplace_id],
        reportOptions={
            "dateGranularity": "DAY",
            "asinGranularity": "CHILD",
        },
    )
    report_id = response.payload["reportId"]
    return reports_api, report_id


def wait_for_report(
    reports_api,
    report_id,
    marketplace_name,
    start_date: datetime,
    end_date: datetime,
):
    max_attempts = config.REPORT_POLL_MAX_ATTEMPTS
    sleep_seconds = config.REPORT_POLL_SLEEP_SECONDS

    for attempt in range(1, max_attempts + 1):
        time.sleep(sleep_seconds)
        status_response = reports_api.get_report(reportId=report_id)
        status = status_response.payload.get("processingStatus")

        if status == "DONE":
            return status_response.payload["reportDocumentId"]
        if status in ("FATAL", "CANCELLED"):
            raise RuntimeError(f"Report failed with status: {status}")

    raise TimeoutError(
        f"Report for {marketplace_name} {start_date.date()} to {end_date.date()} "
        f"did not complete after {max_attempts} attempts."
    )


def download_report_payload(reports_api, document_id):
    doc = reports_api.get_report_document(reportDocumentId=document_id)
    url = doc.payload["url"]
    compression = doc.payload.get("compressionAlgorithm")

    with urllib.request.urlopen(url) as response:
        original_bytes = response.read()

    parsed_bytes = gzip.decompress(original_bytes) if compression == "GZIP" else original_bytes

    return {
        "original_bytes": original_bytes,
        "parsed_bytes": parsed_bytes,
        "compression": compression,
    }


def save_raw_files(payload, marketplace_name, start_date: datetime, end_date: datetime):
    output_dir = Path(config.RAW_OUTPUT_DIR) / "amazon" / REPORT_TYPE / marketplace_name
    output_dir.mkdir(parents=True, exist_ok=True)
    ext = ".json.gz" if payload["compression"] == "GZIP" else ".json"
    original_path = (
        output_dir
        / f"amazon_sales_traffic_{marketplace_name}_{start_date.date()}_{end_date.date()}{ext}"
    )
    original_path.write_bytes(payload["original_bytes"])
    return original_path


def upload_to_s3(local_path: Path, marketplace_name, start_date: datetime):
    s3_key = (
        f"amazon/{REPORT_TYPE}/"
        f"{marketplace_name}/{start_date:%Y/%m/%d}/"
        f"{local_path.name}"
    )
    s3 = get_s3_client()
    s3.upload_file(str(local_path), config.S3_BUCKET, s3_key)
    return s3_key


def parse_rows(report_json, marketplace_name):
    rows = []
    for day_entry in report_json.get("salesAndTrafficByDate", []):
        row_date = day_entry.get("date")
        if row_date is None:
            continue

        day = datetime.fromisoformat(row_date).date()
        for row in day_entry.get("salesAndTrafficByAsin", []):
            sales = row.get("salesByAsin", {})
            traffic = row.get("trafficByAsin", {})
            rows.append({
                "marketplace": marketplace_name,
                "start_date": day,
                "end_date": day,
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
    return rows


def load_staging_rows(conn, rows, report_id, document_id, s3_key, file_checksum):
    if not rows:
        return 0

    sql = f"""
        INSERT INTO {STAGING_TABLE} (
            report_id, report_document_id, s3_key, file_checksum,
            marketplace, start_date, end_date,
            child_asin, parent_asin, sku,
            sessions, page_views, buy_box_percentage,
            units_ordered, ordered_product_sales_amount,
            ordered_product_sales_currency, unit_session_percentage
        )
        VALUES %s
        ON CONFLICT (start_date, marketplace, child_asin) DO UPDATE SET
            units_ordered = EXCLUDED.units_ordered,
            ordered_product_sales_amount = EXCLUDED.ordered_product_sales_amount,
            sessions = EXCLUDED.sessions,
            page_views = EXCLUDED.page_views,
            buy_box_percentage = EXCLUDED.buy_box_percentage,
            unit_session_percentage = EXCLUDED.unit_session_percentage,
            report_id = EXCLUDED.report_id,
            report_document_id = EXCLUDED.report_document_id
    """
    values = [
        (
            report_id, document_id, s3_key, file_checksum,
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


def process_month(
    marketplace_name,
    marketplace_enum,
    marketplace_id,
    start_date: datetime,
    end_date: datetime,
):
    """Process one month window for one marketplace. Skips if already loaded."""

    conn = get_postgres_connection()

    try:
        if is_month_already_loaded(conn, marketplace_name, start_date, end_date):
            print(
                f"  [{marketplace_name}] {start_date.date()} → {end_date.date()} "
                "— already loaded, skipping"
            )
            return False

        requested_at = utc_now()
        report_id = None
        document_id = None
        s3_key = None
        file_checksum = None
        local_file_path = None
        downloaded_at = None
        loaded_at = None
        row_count = None

        try:
            reports_api, report_id = request_report(
                marketplace_name, marketplace_enum, marketplace_id, start_date, end_date
            )
            log_job_status(
                conn, marketplace=marketplace_name,
                report_id=report_id, document_id=None,
                request_status="requested", requested_at=requested_at,
            )
            conn.commit()

            document_id = wait_for_report(
                reports_api, report_id, marketplace_name, start_date, end_date
            )
            payload = download_report_payload(reports_api, document_id)
            downloaded_at = utc_now()
            file_checksum = sha256_hex(payload["original_bytes"])

            local_path = save_raw_files(payload, marketplace_name, start_date, end_date)
            local_file_path = str(local_path)
            s3_key = upload_to_s3(local_path, marketplace_name, start_date)

            log_job_status(
                conn, marketplace=marketplace_name,
                report_id=report_id, document_id=document_id,
                request_status="downloaded", requested_at=requested_at,
                downloaded_at=downloaded_at, local_file_path=local_file_path,
                s3_key=s3_key, file_checksum=file_checksum,
            )
            conn.commit()

            report_json = json.loads(payload["parsed_bytes"].decode("utf-8"))
            rows = parse_rows(report_json, marketplace_name)
            row_count = load_staging_rows(
                conn, rows,
                report_id=report_id, document_id=document_id,
                s3_key=s3_key, file_checksum=file_checksum,
            )
            loaded_at = utc_now()

            log_job_status(
                conn, marketplace=marketplace_name,
                report_id=report_id, document_id=document_id,
                request_status="completed", requested_at=requested_at,
                downloaded_at=downloaded_at, loaded_at=loaded_at,
                completed_at=loaded_at, local_file_path=local_file_path,
                s3_key=s3_key, file_checksum=file_checksum, row_count=row_count,
            )
            conn.commit()

            print(
                f"  [{marketplace_name}] {start_date.date()} → {end_date.date()} "
                f"— inserted {row_count} rows"
            )
            return True

        except Exception as exc:
            log_job_status(
                conn, marketplace=marketplace_name,
                report_id=report_id, document_id=document_id,
                request_status="failed", requested_at=requested_at,
                downloaded_at=downloaded_at, loaded_at=loaded_at,
                completed_at=utc_now(), local_file_path=local_file_path,
                s3_key=s3_key, file_checksum=file_checksum, row_count=row_count,
                error_message=str(exc),
            )
            conn.commit()
            # Log and continue — don't let one failed day stop the entire backfill
            print(
                f"  [{marketplace_name}] {start_date.date()} → {end_date.date()} "
                f"— FAILED: {exc}"
            )
            return True
    finally:
        conn.close()


def main():
    months = get_backfill_months()

    print("=" * 60)
    print("SellerIQ - Sales & Traffic Daily Backfill")
    print("=" * 60)
    print(f"Backfill range: {months[0][0].date()} → {months[-1][1].date()}")
    print(f"Total months:   {len(months)}")
    print(f"Marketplaces:   US, CA")
    print(f"Max requests:   {len(months) * 2}")
    print()

    marketplaces = [
        ("US", Marketplaces.US, config.US_MARKETPLACE_ID),
        ("CA", Marketplaces.CA, config.CA_MARKETPLACE_ID),
    ]

    conn = get_postgres_connection()
    try:
        with conn:
            ensure_log_table_exists(conn)
            ensure_staging_table_exists(conn)

        with ThreadPoolExecutor(max_workers=2) as executor:
            for i, (start_date, end_date) in enumerate(months, 1):
                print(
                    f"Month {i}/{len(months)}: {start_date.strftime('%Y-%m')} "
                    f"({start_date.date()} → {end_date.date()})"
                )
                futures = [
                    executor.submit(
                        process_month,
                        marketplace_name,
                        marketplace_enum,
                        marketplace_id,
                        start_date,
                        end_date,
                    )
                    for marketplace_name, marketplace_enum, marketplace_id in marketplaces
                ]
                results = [future.result() for future in futures]

                # Pause between months to respect API rate limits
                if i < len(months) and any(results):
                    time.sleep(SLEEP_BETWEEN_DAYS)

    finally:
        conn.close()

    print()
    print("=" * 60)
    print("Backfill complete.")
    print("=" * 60)


if __name__ == "__main__":
    main()