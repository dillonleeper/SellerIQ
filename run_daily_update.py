"""
SellerIQ Daily Update Pipeline
================================
Runs every day:
  1. Ingest sales & traffic report (yesterday's daily data)
  2. Ingest inventory snapshot
  3. Transform staging -> fct_sales_daily

Runs on Mondays only (in addition to the above):
  4. Ingest catalog (refresh dim_product + product identity map)
  5. Ingest listings (refresh SKU mappings)

Schedule this to run every day at 7:00 AM via Windows Task Scheduler.

Logs are written to: logs/daily_update_YYYY-MM-DD.log
"""

import importlib
import logging
import sys
from datetime import UTC, datetime
from pathlib import Path

import psycopg2

import config

# Logging setup - writes to console + log file
LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)
log_filename = LOG_DIR / f"daily_update_{datetime.now(UTC).strftime('%Y-%m-%d')}.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(log_filename, encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)


def get_postgres_connection():
    return psycopg2.connect(
        host=config.POSTGRES_HOST,
        port=config.POSTGRES_PORT,
        dbname=config.POSTGRES_DB,
        user=config.POSTGRES_USER,
        password=config.POSTGRES_PASSWORD,
    )


def step_ingest_sales():
    log.info("=" * 60)
    log.info("STEP 1 - Sales & Traffic Ingestion (yesterday)")
    log.info("=" * 60)
    import ingest_sales_traffic
    importlib.reload(ingest_sales_traffic)
    ingest_sales_traffic.main()
    log.info("Step 1 complete.")


def step_ingest_inventory():
    log.info("=" * 60)
    log.info("STEP 2 - Inventory Snapshot Ingestion")
    log.info("=" * 60)
    import ingest_inventory
    importlib.reload(ingest_inventory)
    ingest_inventory.main()
    log.info("Step 2 complete.")


def step_ingest_catalog():
    log.info("=" * 60)
    log.info("STEP 3 - Catalog Ingestion (dim_product refresh)")
    log.info("=" * 60)
    import ingest_catalog
    importlib.reload(ingest_catalog)
    ingest_catalog.main()
    log.info("Step 3 complete.")


def step_ingest_listings():
    log.info("=" * 60)
    log.info("STEP 4 - Listings Ingestion (SKU refresh)")
    log.info("=" * 60)
    import ingest_listings
    importlib.reload(ingest_listings)
    ingest_listings.main()
    log.info("Step 4 complete.")


TRANSFORM_SQL = """
INSERT INTO fct_sales_daily (
    start_date,
    end_date,
    marketplace,
    child_asin,
    parent_asin,
    sku,
    product_id,
    title,
    brand,
    units_ordered,
    ordered_product_sales_amount,
    ordered_product_sales_currency,
    sessions,
    page_views,
    buy_box_percentage,
    unit_session_percentage,
    loaded_at
)
SELECT
    stg.start_date,
    stg.end_date,
    stg.marketplace,
    stg.child_asin,
    stg.parent_asin,
    dp.sku,
    dp.product_id,
    dp.title,
    dp.brand,
    stg.units_ordered,
    stg.ordered_product_sales_amount,
    stg.ordered_product_sales_currency,
    stg.sessions,
    stg.page_views,
    stg.buy_box_percentage,
    stg.unit_session_percentage,
    NOW()
FROM stg_amz_sales_traffic_daily stg
LEFT JOIN dim_product dp
    ON  dp.asin        = stg.child_asin
    AND dp.marketplace = stg.marketplace
WHERE NOT EXISTS (
    SELECT 1
    FROM fct_sales_daily fct
    WHERE fct.start_date  = stg.start_date
      AND fct.child_asin  = stg.child_asin
      AND fct.marketplace = stg.marketplace
);
"""

BACKFILL_SQL = """
UPDATE fct_sales_daily f
SET
    sku        = dp.sku,
    title      = dp.title,
    brand      = dp.brand,
    product_id = dp.product_id
FROM dim_product dp
WHERE dp.asin        = f.child_asin
  AND dp.marketplace = f.marketplace
  AND f.sku IS NULL
  AND dp.sku IS NOT NULL;
"""

ROW_COUNT_SQL = """
SELECT
    marketplace,
    MIN(start_date)                   AS earliest_date,
    MAX(start_date)                   AS latest_date,
    COUNT(*)                          AS total_rows,
    SUM(units_ordered)                AS total_units,
    SUM(ordered_product_sales_amount) AS total_revenue
FROM fct_sales_daily
GROUP BY marketplace
ORDER BY marketplace;
"""


def step_transform():
    log.info("=" * 60)
    log.info("STEP 5 - Transform: staging -> fct_sales_daily")
    log.info("=" * 60)

    conn = get_postgres_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(TRANSFORM_SQL)
            inserted = cur.rowcount
        conn.commit()
        log.info(f"Inserted {inserted} new rows into fct_sales_daily.")

        with conn.cursor() as cur:
            cur.execute(BACKFILL_SQL)
            backfilled = cur.rowcount
        conn.commit()
        log.info(f"Backfilled SKU/title/brand for {backfilled} rows with NULL SKU.")

        with conn.cursor() as cur:
            cur.execute(ROW_COUNT_SQL)
            rows = cur.fetchall()

        log.info("fct_sales_daily summary:")
        log.info(f"  {'Marketplace':<14} {'Earliest':<14} {'Latest':<14} {'Rows':<10} {'Units':<12} {'Revenue'}")
        log.info(f"  {'-' * 72}")
        for row in rows:
            log.info(f"  {str(row[0]):<14} {str(row[1]):<14} {str(row[2]):<14} {str(row[3]):<10} {str(row[4]):<12} {row[5]}")

    finally:
        conn.close()

    log.info("Step 5 complete.")


def main():
    start = datetime.now(UTC)
    is_monday = start.weekday() == 0

    log.info("=" * 60)
    log.info("SellerIQ Daily Update Pipeline - START")
    log.info(f"Run started at: {start.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    log.info(f"Day: {'Monday - full refresh' if is_monday else 'Weekday - sales + inventory only'}")
    log.info("=" * 60)

    try:
        step_ingest_sales()
        step_ingest_inventory()

        if is_monday:
            log.info("Monday detected - running catalog and listings refresh...")
            step_ingest_catalog()
            step_ingest_listings()
        else:
            log.info("Skipping catalog and listings refresh (Monday only).")

        step_transform()

    except Exception as exc:
        log.error(f"Pipeline failed: {exc}", exc_info=True)
        sys.exit(1)

    end = datetime.now(UTC)
    elapsed = round((end - start).total_seconds() / 60, 1)
    log.info("=" * 60)
    log.info(f"SellerIQ Daily Update Pipeline - COMPLETE ({elapsed} min)")
    log.info("=" * 60)


if __name__ == "__main__":
    main()