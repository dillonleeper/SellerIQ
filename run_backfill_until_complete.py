"""
SellerIQ - Run Sales Traffic Backfill Until Complete
====================================================
Runs backfill_sales_traffic.py repeatedly until both marketplaces are
within 5 missing days of complete coverage from 2025-01-01 through yesterday,
or until 10 attempts have been made.
"""

import subprocess
import sys
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

import psycopg2

import config

BACKFILL_START = datetime(2025, 1, 1, tzinfo=UTC)
MAX_ATTEMPTS = 10
RETRY_SLEEP_SECONDS = 60
MISSING_DAY_TOLERANCE = 5
MARKETPLACES = ("US", "CA")
STAGING_TABLE = "stg_amz_sales_traffic_daily"


def utc_now() -> datetime:
    return datetime.now(UTC)


def get_postgres_connection():
    return psycopg2.connect(
        host=config.POSTGRES_HOST,
        port=config.POSTGRES_PORT,
        dbname=config.POSTGRES_DB,
        user=config.POSTGRES_USER,
        password=config.POSTGRES_PASSWORD,
    )


def get_expected_date_range() -> tuple[datetime.date, datetime.date, int]:
    yesterday = (utc_now() - timedelta(days=1)).date()
    start_date = BACKFILL_START.date()
    expected_days = (yesterday - start_date).days + 1
    return start_date, yesterday, expected_days


def run_backfill_script() -> int:
    script_path = Path(__file__).resolve().with_name("backfill_sales_traffic.py")
    result = subprocess.run(
        [sys.executable, str(script_path)],
        cwd=script_path.parent,
        check=False,
    )
    return result.returncode


def get_loaded_day_counts(start_date, end_date) -> dict[str, int]:
    sql = f"""
        SELECT marketplace, COUNT(DISTINCT start_date) AS loaded_days
        FROM {STAGING_TABLE}
        WHERE start_date BETWEEN %s AND %s
          AND marketplace IN ('US', 'CA')
        GROUP BY marketplace
    """

    counts = {marketplace: 0 for marketplace in MARKETPLACES}
    conn = get_postgres_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (start_date, end_date))
            for marketplace, loaded_days in cur.fetchall():
                counts[marketplace] = loaded_days
    finally:
        conn.close()

    return counts


def print_progress(attempt: int, expected_days: int, loaded_counts: dict[str, int]):
    print()
    print(f"Attempt {attempt}/{MAX_ATTEMPTS} progress:")
    for marketplace in MARKETPLACES:
        loaded_days = loaded_counts[marketplace]
        missing_days = expected_days - loaded_days
        print(
            f"  {marketplace}: loaded {loaded_days}/{expected_days} days "
            f"(missing {missing_days})"
        )


def is_backfill_complete(expected_days: int, loaded_counts: dict[str, int]) -> bool:
    return all(
        (expected_days - loaded_counts[marketplace]) <= MISSING_DAY_TOLERANCE
        for marketplace in MARKETPLACES
    )


def main():
    start_date, end_date, expected_days = get_expected_date_range()

    print("=" * 60)
    print("SellerIQ - Run Backfill Until Complete")
    print("=" * 60)
    print(f"Coverage window: {start_date} -> {end_date}")
    print(f"Expected days:   {expected_days}")
    print()

    for attempt in range(1, MAX_ATTEMPTS + 1):
        print(f"Starting backfill attempt {attempt}/{MAX_ATTEMPTS}...")
        return_code = run_backfill_script()
        print(f"backfill_sales_traffic.py exited with code {return_code}")

        loaded_counts = get_loaded_day_counts(start_date, end_date)
        print_progress(attempt, expected_days, loaded_counts)

        if is_backfill_complete(expected_days, loaded_counts):
            print()
            print("Backfill complete!")
            return

        if attempt < MAX_ATTEMPTS:
            print()
            print(
                f"Coverage is still more than {MISSING_DAY_TOLERANCE} days short "
                f"for at least one marketplace. Waiting {RETRY_SLEEP_SECONDS} seconds before retrying..."
            )
            time.sleep(RETRY_SLEEP_SECONDS)

    print()
    print(f"Stopped after {MAX_ATTEMPTS} attempts without reaching completion threshold.")
    raise SystemExit(1)


if __name__ == "__main__":
    main()