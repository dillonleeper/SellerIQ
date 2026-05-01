-- ============================================================
-- SellerIQ Phase 2 DDL — Append to selleriq_phase2_ddl.sql
-- ============================================================
-- Table:
--   stg_amz_listings
--
-- Run this in Supabase SQL Editor to add the listings
-- staging table. Safe to run repeatedly: IF NOT EXISTS.
-- ============================================================


-- ------------------------------------------------------------
-- stg_amz_listings
-- ------------------------------------------------------------
-- Grain: one row per (sku, marketplace)
--
-- Source: Amazon Listings Items API
-- Endpoint: getListingsItem
--
-- Purpose:
-- Provides the canonical SKU → ASIN mapping for your seller
-- account. This is the only reliable source of SKU data since
-- the Catalog Items API does not return seller-specific SKUs.
--
-- Rules:
--   - One row per SKU per marketplace.
--   - On re-ingestion, existing rows are updated (upsert).
--   - Raw API response preserved in raw_response for reprocessing.
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS stg_amz_listings (

    -- ------------------------------------
    -- Surrogate key
    -- ------------------------------------
    id                      BIGSERIAL PRIMARY KEY,

    -- ------------------------------------
    -- Ingestion metadata
    -- ------------------------------------
    s3_key                  TEXT,
    loaded_at               TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- ------------------------------------
    -- Core identifiers
    -- ------------------------------------
    sku                     TEXT        NOT NULL,
    asin                    TEXT,
    marketplace             TEXT        NOT NULL,

    -- ------------------------------------
    -- Listing metadata
    -- ------------------------------------
    item_name               TEXT,
    status                  TEXT,
    condition_type          TEXT,

    -- ------------------------------------
    -- Raw API response
    -- ------------------------------------
    raw_response            JSONB,

    -- ------------------------------------
    -- Idempotency constraint
    -- ------------------------------------
    CONSTRAINT uq_stg_amz_listings
        UNIQUE (sku, marketplace)

);

CREATE INDEX IF NOT EXISTS idx_stg_amz_listings_sku
    ON stg_amz_listings (sku, marketplace);

CREATE INDEX IF NOT EXISTS idx_stg_amz_listings_asin
    ON stg_amz_listings (asin, marketplace);