"""
build_fct_sales_daily.py

Transforms stg_amz_sales_traffic_daily into fct_sales_daily
by joining through int_product_identity_map to resolve
product_id, sku, title, and brand from dim_product.

Run this after:
  - ingest_sales_traffic.py (or backfill_sales_traffic.py)
  - ingest_catalog.py
  - ingest_listings.py

Safe to run repeatedly — uses ON CONFLICT DO UPDATE
so re-running refreshes existing rows with latest data.
"""

import psycopg2
from datetime import datetime, UTC

import config

STAGING_TABLE   = "stg_amz_sales_traffic_daily"
MAP_TABLE       = "int_product_identity_map"
FCT_TABLE       = "fct_sales_daily"


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


def ensure_fct_table_exists(conn):
    """Fail loudly if fct_sales_daily hasn't been created yet."""
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass(%s)", (FCT_TABLE,))
        if cur.fetchone()[0] is None:
            raise RuntimeError(
                f"Required table '{FCT_TABLE}' does not exist. "
                "Run selleriq_fct_sales_daily_ddl.sql before running this script."
            )


def build_fct_sales_daily(conn, marketplace: str | None = None) -> int:
    """
    Upsert rows into fct_sales_daily from stg_amz_sales_traffic_daily,
    joining through int_product_identity_map to resolve product identity.

    If marketplace is provided, only processes that marketplace.
    Otherwise processes all marketplaces.

    Returns the number of rows upserted.
    """
    marketplace_filter = "AND s.marketplace = %(marketplace)s" if marketplace else ""

    sql = f"""
        INSERT INTO {FCT_TABLE} (
            start_date,
            end_date,
            marketplace,
            product_id,
            child_asin,
            parent_asin,
            sku,
            title,
            brand,
            units_ordered,
            ordered_product_sales_amount,
            ordered_product_sales_currency,
            sessions,
            page_views,
            buy_box_percentage,
            unit_session_percentage
        )
        SELECT
            s.start_date,
            s.end_date,
            s.marketplace,
            m.product_id,
            s.child_asin,
            COALESCE(s.parent_asin, m.parent_asin)  AS parent_asin,
            COALESCE(m.sku, s.sku)                  AS sku,
            m.title,
            m.brand,
            s.units_ordered,
            s.ordered_product_sales_amount,
            s.ordered_product_sales_currency,
            s.sessions,
            s.page_views,
            s.buy_box_percentage,
            s.unit_session_percentage
        FROM {STAGING_TABLE} s
        LEFT JOIN {MAP_TABLE} m
            ON  m.child_asin  = s.child_asin
            AND m.marketplace = s.marketplace
        {marketplace_filter}
        ON CONFLICT (start_date, child_asin, marketplace)
        DO UPDATE SET
            end_date                        = EXCLUDED.end_date,
            product_id                      = EXCLUDED.product_id,
            parent_asin                     = EXCLUDED.parent_asin,
            sku                             = EXCLUDED.sku,
            title                           = EXCLUDED.title,
            brand                           = EXCLUDED.brand,
            units_ordered                   = EXCLUDED.units_ordered,
            ordered_product_sales_amount    = EXCLUDED.ordered_product_sales_amount,
            ordered_product_sales_currency  = EXCLUDED.ordered_product_sales_currency,
            sessions                        = EXCLUDED.sessions,
            page_views                      = EXCLUDED.page_views,
            buy_box_percentage              = EXCLUDED.buy_box_percentage,
            unit_session_percentage         = EXCLUDED.unit_session_percentage,
            loaded_at                       = NOW()
    """
    params = {"marketplace": marketplace} if marketplace else {}

    with conn.cursor() as cur:
        cur.execute(sql, params)
        upserted = cur.rowcount
    conn.commit()
    return upserted


def print_summary(conn):
    """Print a quick summary of what's in fct_sales_daily."""
    sql = f"""
        SELECT
            marketplace,
            COUNT(*)                                        AS total_rows,
            COUNT(product_id)                               AS resolved,
            COUNT(*) - COUNT(product_id)                    AS unresolved,
            MIN(start_date)                                 AS earliest_week,
            MAX(start_date)                                 AS latest_week,
            SUM(units_ordered)                              AS total_units,
            ROUND(SUM(ordered_product_sales_amount)::numeric, 2) AS total_revenue
        FROM {FCT_TABLE}
        GROUP BY marketplace
        ORDER BY marketplace
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()

    print("\nfct_sales_daily summary:")
    print(f"  {'MKT':<6} {'Rows':<8} {'Resolved':<10} {'Unresolved':<12} {'Earliest':<14} {'Latest':<14} {'Units':<12} {'Revenue'}")
    print(f"  {'-'*90}")
    for row in rows:
        print(f"  {row[0]:<6} {row[1]:<8} {row[2]:<10} {row[3]:<12} {str(row[4]):<14} {str(row[5]):<14} {row[6] or 0:<12} {row[7] or 0}")


def main():
    print("=" * 60)
    print("SellerIQ - Build fct_sales_daily")
    print("=" * 60)

    conn = get_postgres_connection()
    try:
        ensure_fct_table_exists(conn)

        print("Building fct_sales_daily from staging...")
        upserted = build_fct_sales_daily(conn)
        print(f"Upserted {upserted} rows into {FCT_TABLE}")

        print_summary(conn)

    finally:
        conn.close()

    print("\n" + "=" * 60)
    print("fct_sales_daily build complete.")
    print("=" * 60)


if __name__ == "__main__":
    main()
