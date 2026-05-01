-- ============================================================
-- SellerIQ Phase 3 DDL
-- ============================================================
-- Tables:
--   stg_amz_inventory_snapshot
--   fct_inventory_snapshot_daily
--
-- Run this once against your Postgres database before running
-- the Phase 3 ingestion script.
--
-- Safe to run repeatedly: uses IF NOT EXISTS.
-- ============================================================


-- ------------------------------------------------------------
-- stg_amz_inventory_snapshot
-- ------------------------------------------------------------
-- Grain: one row per (snapshot_date, sku, marketplace)
--
-- Source: Amazon SP-API Reports API
-- Report type: GET_FBA_MYI_UNSUPPRESSED_INVENTORY_DATA
--
-- Rules:
--   - All source columns preserved as-is from the raw report.
--   - No business logic applied here.
--   - Idempotency enforced via unique constraint on
--     (snapshot_date, sku, marketplace).
--   - Inventory is a snapshot fact — each row represents
--     the inventory state at a point in time, not a transaction.
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS stg_amz_inventory_snapshot (

    -- ------------------------------------
    -- Surrogate key
    -- ------------------------------------
    id                              BIGSERIAL PRIMARY KEY,

    -- ------------------------------------
    -- Ingestion metadata
    -- ------------------------------------
    report_id                       TEXT,
    report_document_id              TEXT,
    s3_key                          TEXT,
    file_checksum                   TEXT,
    loaded_at                       TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- ------------------------------------
    -- Snapshot date
    -- ------------------------------------
    -- The date this inventory snapshot was taken.
    snapshot_date                   DATE        NOT NULL,

    -- ------------------------------------
    -- Marketplace / source context
    -- ------------------------------------
    marketplace                     TEXT        NOT NULL,

    -- ------------------------------------
    -- Product identifiers (source values)
    -- ------------------------------------
    sku                             TEXT        NOT NULL,
    asin                            TEXT,
    fnsku                           TEXT,
    product_name                    TEXT,
    condition                       TEXT,

    -- ------------------------------------
    -- Inventory quantities (source values)
    -- ------------------------------------
    -- Units available for sale in FBA
    fulfillable_quantity            INTEGER,

    -- Units reserved (customer orders, transfers, etc.)
    reserved_quantity               INTEGER,

    -- Units inbound to FBA but not yet received
    inbound_working_quantity        INTEGER,
    inbound_shipped_quantity        INTEGER,
    inbound_receiving_quantity      INTEGER,

    -- Total inbound (working + shipped + receiving)
    total_inbound_quantity          INTEGER,

    -- Units in unsellable condition
    unsellable_quantity             INTEGER,

    -- Total quantity across all states
    total_quantity                  INTEGER,

    -- ------------------------------------
    -- Idempotency constraint
    -- ------------------------------------
    CONSTRAINT uq_stg_amz_inventory_snapshot
        UNIQUE (snapshot_date, sku, marketplace)

);

CREATE INDEX IF NOT EXISTS idx_stg_amz_inventory_snapshot_date
    ON stg_amz_inventory_snapshot (snapshot_date, marketplace);

CREATE INDEX IF NOT EXISTS idx_stg_amz_inventory_snapshot_sku
    ON stg_amz_inventory_snapshot (sku, marketplace);

CREATE INDEX IF NOT EXISTS idx_stg_amz_inventory_snapshot_asin
    ON stg_amz_inventory_snapshot (asin, marketplace);


-- ------------------------------------------------------------
-- fct_inventory_snapshot_daily
-- ------------------------------------------------------------
-- Grain: one row per (snapshot_date, product_id, marketplace)
--
-- This is the modeled inventory fact table.
-- Built from stg_amz_inventory_snapshot joined to dim_product.
--
-- Rules:
--   - product_id foreign key to dim_product.
--   - available_quantity = fulfillable_quantity - reserved_quantity
--   - days_of_cover calculated using 30-day average daily units sold
--     from fct_sales_daily (added in downstream SQL model).
--   - Inventory is a snapshot, not a transaction.
--     Do not sum across dates.
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS fct_inventory_snapshot_daily (

    -- ------------------------------------
    -- Surrogate key
    -- ------------------------------------
    id                              BIGSERIAL PRIMARY KEY,

    -- ------------------------------------
    -- Grain
    -- ------------------------------------
    snapshot_date                   DATE        NOT NULL,
    marketplace                     TEXT        NOT NULL,

    -- ------------------------------------
    -- Product identity
    -- ------------------------------------
    product_id                      BIGINT      REFERENCES dim_product(product_id),
    sku                             TEXT,
    asin                            TEXT,
    fnsku                           TEXT,

    -- ------------------------------------
    -- Inventory quantities (modeled)
    -- ------------------------------------
    -- Core quantities sourced from staging
    fulfillable_quantity            INTEGER,
    reserved_quantity               INTEGER,
    inbound_working_quantity        INTEGER,
    inbound_shipped_quantity        INTEGER,
    inbound_receiving_quantity      INTEGER,
    total_inbound_quantity          INTEGER,
    unsellable_quantity             INTEGER,

    -- Modeled available quantity
    -- available = fulfillable - reserved
    available_quantity              INTEGER,

    -- ------------------------------------
    -- Audit
    -- ------------------------------------
    loaded_at                       TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- ------------------------------------
    -- Idempotency constraint
    -- ------------------------------------
    CONSTRAINT uq_fct_inventory_snapshot_daily
        UNIQUE (snapshot_date, sku, marketplace)

);

CREATE INDEX IF NOT EXISTS idx_fct_inventory_snapshot_date
    ON fct_inventory_snapshot_daily (snapshot_date, marketplace);

CREATE INDEX IF NOT EXISTS idx_fct_inventory_snapshot_product
    ON fct_inventory_snapshot_daily (product_id, snapshot_date);

CREATE INDEX IF NOT EXISTS idx_fct_inventory_snapshot_sku
    ON fct_inventory_snapshot_daily (sku, marketplace);
