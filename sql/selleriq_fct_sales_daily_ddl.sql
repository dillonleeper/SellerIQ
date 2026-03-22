-- ============================================================
-- SellerIQ Phase 1 DDL — Append
-- ============================================================
-- Table:
--   fct_sales_daily
--
-- Run this in Supabase SQL Editor to add the fact table.
-- Safe to run repeatedly: IF NOT EXISTS.
-- ============================================================


-- ------------------------------------------------------------
-- fct_sales_daily
-- ------------------------------------------------------------
-- Grain: one row per (start_date, child_asin, marketplace)
--
-- Source: stg_amz_sales_traffic_daily
-- Joined to: int_product_identity_map → dim_product
--
-- Note on date:
-- The Sales and Traffic report is pulled at weekly granularity.
-- start_date represents the first day of the report week.
-- One row = one product's performance for that week.
--
-- Rules:
--   - product_id resolved via int_product_identity_map
--   - If a child_asin cannot be resolved, product_id is NULL
--   - All source metrics preserved from staging
--   - No business logic beyond joining to dim_product
--   - Idempotency enforced via unique constraint on
--     (start_date, child_asin, marketplace)
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS fct_sales_daily (

    -- ------------------------------------
    -- Surrogate key
    -- ------------------------------------
    id                              BIGSERIAL PRIMARY KEY,

    -- ------------------------------------
    -- Grain
    -- ------------------------------------
    -- start_date = first day of the report week
    start_date                      DATE        NOT NULL,
    end_date                        DATE        NOT NULL,
    marketplace                     TEXT        NOT NULL,

    -- ------------------------------------
    -- Product identity
    -- ------------------------------------
    -- Resolved from int_product_identity_map → dim_product
    -- NULL if child_asin could not be resolved
    product_id                      BIGINT      REFERENCES dim_product(product_id),
    child_asin                      TEXT,
    parent_asin                     TEXT,
    sku                             TEXT,
    title                           TEXT,
    brand                           TEXT,

    -- ------------------------------------
    -- Sales metrics
    -- ------------------------------------
    units_ordered                   INTEGER,
    ordered_product_sales_amount    NUMERIC(14, 2),
    ordered_product_sales_currency  TEXT,

    -- ------------------------------------
    -- Traffic metrics
    -- ------------------------------------
    sessions                        INTEGER,
    page_views                      INTEGER,
    buy_box_percentage              NUMERIC(6, 2),
    unit_session_percentage         NUMERIC(6, 2),

    -- ------------------------------------
    -- Audit
    -- ------------------------------------
    loaded_at                       TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- ------------------------------------
    -- Idempotency constraint
    -- ------------------------------------
    CONSTRAINT uq_fct_sales_daily
        UNIQUE (start_date, child_asin, marketplace)

);

-- Index for date range queries
CREATE INDEX IF NOT EXISTS idx_fct_sales_daily_date
    ON fct_sales_daily (start_date, marketplace);

-- Index for product joins
CREATE INDEX IF NOT EXISTS idx_fct_sales_daily_product
    ON fct_sales_daily (product_id, start_date);

-- Index for ASIN-based lookups
CREATE INDEX IF NOT EXISTS idx_fct_sales_daily_asin
    ON fct_sales_daily (child_asin, marketplace);

-- Index for SKU-based lookups
CREATE INDEX IF NOT EXISTS idx_fct_sales_daily_sku
    ON fct_sales_daily (sku, marketplace);
