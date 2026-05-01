"""
One-off: run sales & traffic ingestion for CA only.
Skips US entirely. Uses the same retry/logging logic as the weekly pipeline.
"""
import sys
from sp_api.base import Marketplaces

import config
from ingest_sales_traffic import (
    get_postgres_connection,
    get_last_full_sunday_saturday_week,
    process_marketplace,
    ensure_log_table_exists,
    ensure_staging_table_exists,
)


def main() -> int:
    print("=" * 60)
    print("SellerIQ - Sales & Traffic — CA ONLY (one-off)")
    print("=" * 60)

    start_date, end_date = get_last_full_sunday_saturday_week()
    print(f"Date range: {start_date.date()} -> {end_date.date()}")

    conn = get_postgres_connection()
    try:
        with conn:
            ensure_log_table_exists(conn)
            ensure_staging_table_exists(conn)

        ok = process_marketplace(
            conn,
            "CA",
            Marketplaces.CA,
            config.CA_MARKETPLACE_ID,
            start_date,
            end_date,
        )
    finally:
        conn.close()

    print("\n" + "=" * 60)
    print(f"CA result: {'SUCCESS' if ok else 'FAILED'}")
    print("=" * 60)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())