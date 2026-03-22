import json
import time
from datetime import datetime, UTC
from pathlib import Path

import boto3
import psycopg2
from psycopg2.extras import execute_values
from sp_api.api.catalog_items.catalog_items_2022_04_01 import CatalogItemsV20220401 as CatalogItems
from sp_api.base import Marketplaces

import config

SOURCE_SYSTEM = "amazon_sp_api"
CATALOG_API_VERSION = "2022-04-01"
STAGING_TABLE = "stg_amz_catalog_items"
DIM_TABLE = "dim_product"
MAP_TABLE = "int_product_identity_map"
SALES_STAGING_TABLE = "stg_amz_sales_traffic_daily"

# Catalog Items API rate limit is 2 requests/second with a burst of 2.
# We pause briefly between calls to stay within limits.
API_CALL_SLEEP_SECONDS = 0.6

# Fields to request from the Catalog Items API
INCLUDED_DATA = ["summaries", "identifiers", "relationships", "attributes"]


def utc_now() -> datetime:
    return datetime.now(UTC)


def get_sp_api_credentials():
    return {
        "refresh_token": config.AMAZON_REFRESH_TOKEN,
        "lwa_app_id": config.AMAZON_CLIENT_ID,
        "lwa_client_secret": config.AMAZON_CLIENT_SECRET,
    }


def get_catalog_api(marketplace_enum):
    return CatalogItems(
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


def get_active_asins(conn, marketplace: str) -> list[str]:
    """
    Return the distinct list of child_asins that have sales activity
    in stg_amz_sales_traffic_daily for the given marketplace.
    """
    sql = f"""
        SELECT DISTINCT child_asin
        FROM {SALES_STAGING_TABLE}
        WHERE marketplace = %s
          AND child_asin IS NOT NULL
        ORDER BY child_asin
    """
    with conn.cursor() as cur:
        cur.execute(sql, (marketplace,))
        return [row[0] for row in cur.fetchall()]


def upload_raw_to_s3(asin: str, marketplace: str, raw_data: dict) -> str:
    """Save raw API response to S3 and return the s3_key."""
    s3_key = (
        f"amazon/catalog/{marketplace}/{asin}.json"
    )
    raw_bytes = json.dumps(raw_data).encode("utf-8")
    s3 = get_s3_client()
    s3.put_object(
        Bucket=config.S3_BUCKET,
        Key=s3_key,
        Body=raw_bytes,
        ContentType="application/json",
    )
    return s3_key


def parse_catalog_item(asin: str, marketplace: str, payload: dict) -> dict:
    """
    Parse the raw Catalog Items API response into a flat row
    for stg_amz_catalog_items.
    """
    summaries = payload.get("summaries", [])
    relationships = payload.get("relationships", [])
    attributes = payload.get("attributes", {})

    # Extract summary fields — take the first summary entry
    summary = summaries[0] if summaries else {}
    title = summary.get("itemName")
    brand = summary.get("brand")
    product_type = summary.get("productType")

    # Extract parent ASIN from relationships
    parent_asin = None
    for rel in relationships:
        if rel.get("type") == "VARIATION_PARENT":
            parent_asin = rel.get("identifiers", {}).get("asin")
            break
        # Some API versions use a different structure
        if rel.get("childAsins") is None and rel.get("parentAsin"):
            parent_asin = rel.get("parentAsin")
            break

    # Extract variation attributes
    color = None
    size = None
    variation_theme = None

    color_attr = attributes.get("color", [])
    if color_attr:
        color = color_attr[0].get("value")

    size_attr = attributes.get("size", [])
    if size_attr:
        size = size_attr[0].get("value")

    variation_theme_attr = attributes.get("variationTheme", [])
    if variation_theme_attr:
        variation_theme = variation_theme_attr[0].get("value")

    return {
        "asin": asin,
        "marketplace": marketplace,
        "parent_asin": parent_asin,
        "title": title,
        "brand": brand,
        "product_type": product_type,
        "color": color,
        "size": size,
        "variation_theme": variation_theme,
        "raw_response": json.dumps(payload),
    }


def upsert_catalog_row(conn, row: dict, s3_key: str):
    """
    Upsert one row into stg_amz_catalog_items.
    On conflict (asin, marketplace) update with latest values.
    """
    sql = f"""
        INSERT INTO {STAGING_TABLE} (
            asin, marketplace, parent_asin,
            title, brand, product_type,
            color, size, variation_theme,
            raw_response, s3_key, updated_at
        )
        VALUES (
            %(asin)s, %(marketplace)s, %(parent_asin)s,
            %(title)s, %(brand)s, %(product_type)s,
            %(color)s, %(size)s, %(variation_theme)s,
            %(raw_response)s, %(s3_key)s, NOW()
        )
        ON CONFLICT (asin, marketplace)
        DO UPDATE SET
            parent_asin     = EXCLUDED.parent_asin,
            title           = EXCLUDED.title,
            brand           = EXCLUDED.brand,
            product_type    = EXCLUDED.product_type,
            color           = EXCLUDED.color,
            size            = EXCLUDED.size,
            variation_theme = EXCLUDED.variation_theme,
            raw_response    = EXCLUDED.raw_response,
            s3_key          = EXCLUDED.s3_key,
            updated_at      = NOW()
    """
    params = {**row, "s3_key": s3_key}
    with conn.cursor() as cur:
        cur.execute(sql, params)


def fetch_and_load_catalog(conn, marketplace_name: str, marketplace_enum, marketplace_id: str):
    """
    Fetch catalog data for all active ASINs in a marketplace
    and load into stg_amz_catalog_items.
    """
    print(f"\n[{marketplace_name}] Fetching active ASINs...")
    asins = get_active_asins(conn, marketplace_name)
    print(f"[{marketplace_name}] Found {len(asins)} active ASINs to fetch")

    catalog_api = get_catalog_api(marketplace_enum)
    success = 0
    failed = 0
    failed_asins = []

    for i, asin in enumerate(asins, 1):
        try:
            response = catalog_api.get_catalog_item(
                asin=asin,
                marketplaceIds=[marketplace_id],
                includedData=INCLUDED_DATA,
                sellerId=config.SELLER_ID,
            )
            payload = response.payload

            # Save raw response to S3
            s3_key = upload_raw_to_s3(asin, marketplace_name, payload)

            # Parse and upsert into staging
            row = parse_catalog_item(asin, marketplace_name, payload)
            upsert_catalog_row(conn, row, s3_key)
            conn.commit()

            success += 1
            if i % 25 == 0 or i == len(asins):
                print(f"  [{marketplace_name}] Progress: {i}/{len(asins)} — {success} ok, {failed} failed")

        except Exception as exc:
            failed += 1
            failed_asins.append(asin)
            print(f"  [{marketplace_name}] FAILED {asin}: {exc}")
            conn.rollback()

        # Respect API rate limits
        time.sleep(API_CALL_SLEEP_SECONDS)

    print(f"[{marketplace_name}] Catalog fetch complete — {success} loaded, {failed} failed")
    if failed_asins:
        print(f"[{marketplace_name}] Failed ASINs: {failed_asins}")

    return success, failed


def build_dim_product(conn):
    """
    Build dim_product from stg_amz_catalog_items.
    Upserts so re-running always reflects latest catalog state.
    """
    print("\nBuilding dim_product...")
    sql = f"""
        INSERT INTO {DIM_TABLE} (
            asin, marketplace, parent_asin,
            title, brand, product_type,
            color, size, variation_theme,
            updated_at
        )
        SELECT
            asin,
            marketplace,
            parent_asin,
            title,
            brand,
            product_type,
            color,
            size,
            variation_theme,
            NOW()
        FROM {STAGING_TABLE}
        ON CONFLICT (asin, marketplace)
        DO UPDATE SET
            parent_asin     = EXCLUDED.parent_asin,
            title           = EXCLUDED.title,
            brand           = EXCLUDED.brand,
            product_type    = EXCLUDED.product_type,
            color           = EXCLUDED.color,
            size            = EXCLUDED.size,
            variation_theme = EXCLUDED.variation_theme,
            updated_at      = NOW()
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        row_count = cur.rowcount
    conn.commit()
    print(f"dim_product: {row_count} rows upserted")
    return row_count


def build_product_identity_map(conn):
    """
    Build int_product_identity_map by joining
    stg_amz_sales_traffic_daily to dim_product on
    child_asin + marketplace.

    Records NULL product_id for any ASINs that could not
    be resolved so gaps are visible.
    """
    print("\nBuilding int_product_identity_map...")
    sql = f"""
        INSERT INTO {MAP_TABLE} (
            child_asin, parent_asin, marketplace,
            product_id, sku, title, brand,
            updated_at
        )
        SELECT DISTINCT ON (s.child_asin, s.marketplace)
            s.child_asin,
            s.parent_asin,
            s.marketplace,
            p.product_id,
            p.sku,
            p.title,
            p.brand,
            NOW()
        FROM {SALES_STAGING_TABLE} s
        LEFT JOIN {DIM_TABLE} p
            ON p.asin = s.child_asin
            AND p.marketplace = s.marketplace
        WHERE s.child_asin IS NOT NULL
        ON CONFLICT (child_asin, marketplace)
        DO UPDATE SET
            parent_asin = EXCLUDED.parent_asin,
            product_id  = EXCLUDED.product_id,
            sku         = EXCLUDED.sku,
            title       = EXCLUDED.title,
            brand       = EXCLUDED.brand,
            updated_at  = NOW()
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        row_count = cur.rowcount
    conn.commit()
    print(f"int_product_identity_map: {row_count} rows upserted")
    return row_count


def report_resolution_summary(conn):
    """Print a summary of how many ASINs resolved to dim_product."""
    sql = f"""
        SELECT
            marketplace,
            COUNT(*) AS total_asins,
            COUNT(product_id) AS resolved,
            COUNT(*) - COUNT(product_id) AS unresolved
        FROM {MAP_TABLE}
        GROUP BY marketplace
        ORDER BY marketplace
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()

    print("\nProduct identity resolution summary:")
    print(f"  {'Marketplace':<15} {'Total':<10} {'Resolved':<12} {'Unresolved'}")
    print(f"  {'-'*50}")
    for row in rows:
        print(f"  {row[0]:<15} {row[1]:<10} {row[2]:<12} {row[3]}")


def main():
    print("=" * 60)
    print("SellerIQ - Phase 2 Catalog Ingestion")
    print("=" * 60)

    marketplaces = [
        ("US", Marketplaces.US, config.US_MARKETPLACE_ID),
        ("CA", Marketplaces.CA, config.CA_MARKETPLACE_ID),
    ]

    conn = get_postgres_connection()
    try:
        # Step 1 — fetch catalog data for all active ASINs
        for marketplace_name, marketplace_enum, marketplace_id in marketplaces:
            fetch_and_load_catalog(conn, marketplace_name, marketplace_enum, marketplace_id)

        # Step 2 — build dim_product from staging
        build_dim_product(conn)

        # Step 3 — build product identity map
        build_product_identity_map(conn)

        # Step 4 — print resolution summary
        report_resolution_summary(conn)

    finally:
        conn.close()

    print("\n" + "=" * 60)
    print("Phase 2 complete.")
    print("=" * 60)


if __name__ == "__main__":
    main()
