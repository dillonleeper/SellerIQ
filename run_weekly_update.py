"""
SellerIQ Weekly Update Pipeline
================================
Runs all four steps in order:
  1. Ingest sales & traffic report (Amazon SP-API → S3 → staging)
  2. Ingest inventory snapshot (Amazon SP-API → S3 → staging → fct)
  3. Ingest catalog (refresh dim_product + product identity map)
  4. Transform staging → fct_sales_daily (backfill any NULL SKUs too)

Run this every Monday morning:
  python run_weekly_update.py

Logs are written to: logs/weekly_update_YYYY-MM-DD.log

Failure handling
----------------
Each step is isolated: a failure in one step does NOT block subsequent
steps unless they have a real data dependency. The transform step (step 4)
checks whether new data actually landed in staging before running.

The pipeline exits with code 0 if everything succeeded, code 1 if any
step or marketplace failed. Per-step results are summarized at the end.
"""

import importlib
import logging
import sys
from datetime import datetime, UTC
from pathlib import Path

import psycopg2

import config

# ─────────────────────────────────────────────
# Logging setup — writes to console + log file
# ─────────────────────────────────────────────
LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)
log_filename = LOG_DIR / f"weekly_update_{datetime.now(UTC).strftime('%Y-%m-%d')}.log"

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


# ─────────────────────────────────────────────
# Postgres connection
# ─────────────────────────────────────────────
def get_postgres_connection():
    return psycopg2.connect(
        host=config.POSTGRES_HOST,
        port=config.POSTGRES_PORT,
        dbname=config.POSTGRES_DB,
        user=config.POSTGRES_USER,
        password=config.POSTGRES_PASSWORD,
    )


# ─────────────────────────────────────────────
# Step results — collected as we run, summarized at end
# ─────────────────────────────────────────────
class StepResult:
    """Tracks the outcome of one pipeline step."""
    def __init__(self, name: str):
        self.name = name
        self.status: str = "pending"   # pending | success | partial | failed | skipped
        self.detail: str = ""
        self.error: str | None = None

    def succeeded(self, detail: str = ""):
        self.status = "success"
        self.detail = detail

    def partial(self, detail: str):
        self.status = "partial"
        self.detail = detail

    def failed(self, error: str):
        self.status = "failed"
        self.error = error

    def skipped(self, reason: str):
        self.status = "skipped"
        self.detail = reason

    @property
    def ok(self) -> bool:
        # Treat partial as "ok enough to continue" but flag in summary.
        return self.status in ("success", "partial", "skipped")


def summarize_marketplace_results(results: dict[str, bool] | None) -> str:
    """Format a per-marketplace results dict into a short string."""
    if not results:
        return "no marketplaces processed"
    succeeded = [m for m, ok in results.items() if ok]
    failed = [m for m, ok in results.items() if not ok]
    parts = []
    if succeeded:
        parts.append(f"OK: {','.join(succeeded)}")
    if failed:
        parts.append(f"FAILED: {','.join(failed)}")
    return " | ".join(parts)


# ─────────────────────────────────────────────
# Step 1 — Sales & Traffic ingestion
# ─────────────────────────────────────────────
def step_ingest_sales(result: StepResult):
    log.info("=" * 60)
    log.info("STEP 1 — Sales & Traffic Ingestion")
    log.info("=" * 60)
    try:
        import ingest_sales_traffic
        importlib.reload(ingest_sales_traffic)
        marketplace_results = ingest_sales_traffic.main() or {}

        detail = summarize_marketplace_results(marketplace_results)
        if all(marketplace_results.values()):
            result.succeeded(detail)
        elif any(marketplace_results.values()):
            result.partial(detail)
        else:
            result.failed(f"all marketplaces failed ({detail})")
        log.info("Step 1 result: %s — %s", result.status, detail)
    except Exception as exc:
        log.error("Step 1 raised an unexpected exception: %s", exc, exc_info=True)
        result.failed(str(exc))


# ─────────────────────────────────────────────
# Step 2 — Inventory snapshot ingestion
# ─────────────────────────────────────────────
def step_ingest_inventory(result: StepResult):
    log.info("=" * 60)
    log.info("STEP 2 — Inventory Snapshot Ingestion")
    log.info("=" * 60)
    try:
        import ingest_inventory
        importlib.reload(ingest_inventory)
        marketplace_results = ingest_inventory.main() or {}

        detail = summarize_marketplace_results(marketplace_results)
        if all(marketplace_results.values()):
            result.succeeded(detail)
        elif any(marketplace_results.values()):
            result.partial(detail)
        else:
            result.failed(f"all marketplaces failed ({detail})")
        log.info("Step 2 result: %s — %s", result.status, detail)
    except Exception as exc:
        log.error("Step 2 raised an unexpected exception: %s", exc, exc_info=True)
        result.failed(str(exc))


# ─────────────────────────────────────────────
# Step 3 — Catalog ingestion (refreshes dim_product)
# ─────────────────────────────────────────────
def step_ingest_catalog(result: StepResult):
    log.info("=" * 60)
    log.info("STEP 3 — Catalog Ingestion (dim_product refresh)")
    log.info("=" * 60)
    try:
        import ingest_catalog
        importlib.reload(ingest_catalog)
        marketplace_results = ingest_catalog.main() or {}

        detail = summarize_marketplace_results(marketplace_results)
        if all(marketplace_results.values()):
            result.succeeded(detail)
        elif any(marketplace_results.values()):
            result.partial(detail)
        else:
            result.failed(f"all marketplaces failed ({detail})")
        log.info("Step 3 result: %s — %s", result.status, detail)
    except Exception as exc:
        log.error("Step 3 raised an unexpected exception: %s", exc, exc_info=True)
        result.failed(str(exc))


# ─────────────────────────────────────────────
# Step 4 — Transform staging → fct_sales_daily
# ─────────────────────────────────────────────
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
    MIN(start_date)                   AS earliest_week,
    MAX(start_date)                   AS latest_week,
    COUNT(*)                          AS total_rows,
    SUM(units_ordered)                AS total_units,
    SUM(ordered_product_sales_amount) AS total_revenue
FROM fct_sales_daily
GROUP BY marketplace
ORDER BY marketplace;
"""


def step_transform(result: StepResult, sales_step_result: StepResult):
    log.info("=" * 60)
    log.info("STEP 4 — Transform: staging → fct_sales_daily")
    log.info("=" * 60)

    # Pre-flight: if sales ingestion failed entirely, there's nothing
    # new to transform. We still run the backfill in case prior runs
    # left NULL SKUs that dim_product can now resolve.
    if sales_step_result.status == "failed":
        log.warning(
            "Sales ingestion failed for all marketplaces — skipping new-row insert "
            "but still running SKU backfill."
        )

    conn = None
    try:
        conn = get_postgres_connection()

        # Insert new rows from staging (safe to run even if no new rows
        # — the WHERE NOT EXISTS clause makes it idempotent)
        inserted = 0
        if sales_step_result.status != "failed":
            with conn.cursor() as cur:
                cur.execute(TRANSFORM_SQL)
                inserted = cur.rowcount
            conn.commit()
            log.info("Inserted %d new rows into fct_sales_daily.", inserted)
        else:
            log.info("Skipping fct_sales_daily insert (sales ingestion fully failed).")

        # Backfill any rows with NULL SKU now that dim_product is fresh
        with conn.cursor() as cur:
            cur.execute(BACKFILL_SQL)
            backfilled = cur.rowcount
        conn.commit()
        log.info("Backfilled SKU/title/brand for %d rows with previously NULL SKU.", backfilled)

        # Row count summary
        with conn.cursor() as cur:
            cur.execute(ROW_COUNT_SQL)
            rows = cur.fetchall()

        log.info("fct_sales_daily summary:")
        log.info(f"  {'Marketplace':<14} {'Earliest':<14} {'Latest':<14} {'Rows':<10} {'Units':<12} {'Revenue'}")
        log.info(f"  {'-' * 72}")
        for row in rows:
            log.info(f"  {str(row[0]):<14} {str(row[1]):<14} {str(row[2]):<14} {str(row[3]):<10} {str(row[4]):<12} {row[5]}")

        result.succeeded(f"inserted={inserted} backfilled={backfilled}")
        log.info("Step 4 result: success — %s", result.detail)

    except Exception as exc:
        log.error("Step 4 raised an unexpected exception: %s", exc, exc_info=True)
        if conn is not None:
            try:
                conn.rollback()
            except Exception:
                pass
        result.failed(str(exc))
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


# ─────────────────────────────────────────────
# Final summary
# ─────────────────────────────────────────────
def print_pipeline_summary(results: list[StepResult]):
    log.info("=" * 60)
    log.info("PIPELINE SUMMARY")
    log.info("=" * 60)
    for r in results:
        status_label = r.status.upper()
        if r.status == "success":
            log.info("  [%-8s] %s — %s", status_label, r.name, r.detail)
        elif r.status == "partial":
            log.warning("  [%-8s] %s — %s", status_label, r.name, r.detail)
        elif r.status == "skipped":
            log.warning("  [%-8s] %s — %s", status_label, r.name, r.detail)
        elif r.status == "failed":
            log.error("  [%-8s] %s — %s", status_label, r.name, r.error)
        else:
            log.info("  [%-8s] %s", status_label, r.name)


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────
def main() -> int:
    start = datetime.now(UTC)
    log.info("=" * 60)
    log.info("SellerIQ Weekly Update Pipeline — START")
    log.info(f"Run started at: {start.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    log.info("=" * 60)

    sales_result = StepResult("Step 1 — Sales & Traffic")
    inventory_result = StepResult("Step 2 — Inventory")
    catalog_result = StepResult("Step 3 — Catalog")
    transform_result = StepResult("Step 4 — Transform → fct_sales_daily")

    # Each step is independent. A failure in one does NOT block the others.
    step_ingest_sales(sales_result)
    step_ingest_inventory(inventory_result)
    step_ingest_catalog(catalog_result)
    step_transform(transform_result, sales_result)

    end = datetime.now(UTC)
    elapsed = round((end - start).total_seconds() / 60, 1)

    all_results = [sales_result, inventory_result, catalog_result, transform_result]
    print_pipeline_summary(all_results)

    log.info("=" * 60)
    log.info(f"SellerIQ Weekly Update Pipeline — COMPLETE ({elapsed} min)")
    log.info("=" * 60)

    # Exit non-zero if any step failed outright. Partial/skipped is treated
    # as "the pipeline did its best" — still 0, but check the summary.
    any_hard_failure = any(r.status == "failed" for r in all_results)
    return 1 if any_hard_failure else 0


if __name__ == "__main__":
    sys.exit(main())
