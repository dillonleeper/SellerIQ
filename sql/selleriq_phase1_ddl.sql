-- ============================================================
-- SellerIQ Phase 1 DDL
-- ============================================================
-- Tables:
--   stg_amz_sales_traffic_daily
--   ingestion_job_log
--
-- Run this once against your Postgres database before running
-- the ingestion script.
--
-- Safe to run repeatedly: uses IF NOT EXISTS.
-- ============================================================


-- ------------------------------------------------------------
-- stg_amz_sales_traffic_daily
-- ------------------------------------------------------------
-- Grain: one row per (report_id, marketplace, child_asin)
--
-- Source: Amazon SP-API Reports API
-- Report type: GET_SALES_AND_TRAFFIC_REPORT
-- Granularity: CHILD asin, WEEK date range
--
-- Rules:
--   - All source columns preserved as-is from the raw report.
--   - No business logic applied here.
--   - Idempotency enforced via unique constraint on
--     (report_id, marketplace, child_asin).
--   - Ingestion metadata columns added at load time.
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS stg_amz_sales_traffic_daily (

    -- ------------------------------------
    -- Surrogate / load key
    -- ------------------------------------
    id                              BIGSERIAL PRIMARY KEY,

    -- ------------------------------------
    -- Ingestion metadata
    -- ------------------------------------
    -- The SP-API reportId for this ingestion run.
    -- Used for idempotency and job log linkage.
    report_id                       TEXT        NOT NULL,

    -- The SP-API reportDocumentId for this ingestion run.
    report_document_id              TEXT        NOT NULL,

    -- S3 path where the raw file is stored.
    s3_key                          TEXT,

    -- SHA256 checksum of the raw bytes downloaded.
    file_checksum                   TEXT,

    -- When this row was loaded into Postgres.
    loaded_at                       TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- ------------------------------------
    -- Report date range
    -- ------------------------------------
    -- The week window this report covers.
    -- Amazon Sales & Traffic reports at weekly grain.
    -- start_date = first day of the report week.
    -- end_date   = last day of the report week.
    start_date                      DATE        NOT NULL,
    end_date                        DATE        NOT NULL,

    -- ------------------------------------
    -- Marketplace / source context
    -- ------------------------------------
    -- Short marketplace label: "US", "CA", etc.
    marketplace                     TEXT        NOT NULL,

    -- ------------------------------------
    -- Product identifiers (source values, not yet resolved)
    -- ------------------------------------
    -- child_asin is the primary row identifier in this report.
    child_asin                      TEXT,
    parent_asin                     TEXT,

    -- SKU may or may not be present depending on report configuration.
    sku                             TEXT,

    -- ------------------------------------
    -- Traffic metrics (source values)
    -- ------------------------------------
    sessions                        INTEGER,
    page_views                      INTEGER,
    buy_box_percentage              NUMERIC(6, 2),
    unit_session_percentage         NUMERIC(6, 2),

    -- ------------------------------------
    -- Sales metrics (source values)
    -- ------------------------------------
    units_ordered                   INTEGER,
    ordered_product_sales_amount    NUMERIC(14, 2),
    ordered_product_sales_currency  TEXT,

    -- ------------------------------------
    -- Idempotency constraint
    -- ------------------------------------
    -- Prevents duplicate loads of the same report row.
    -- If the same report_id is loaded twice, rows are skipped.
    CONSTRAINT uq_stg_amz_sales_traffic
        UNIQUE (report_id, marketplace, child_asin)

);

-- Index to support downstream joins and filtering by date and marketplace.
CREATE INDEX IF NOT EXISTS idx_stg_amz_sales_traffic_date
    ON stg_amz_sales_traffic_daily (start_date, marketplace);

-- Index to support product identity resolution joins.
CREATE INDEX IF NOT EXISTS idx_stg_amz_sales_traffic_asin
    ON stg_amz_sales_traffic_daily (child_asin, marketplace);

-- Index to support SKU-based joins where SKU is present.
CREATE INDEX IF NOT EXISTS idx_stg_amz_sales_traffic_sku
    ON stg_amz_sales_traffic_daily (sku, marketplace);


-- ------------------------------------------------------------
-- ingestion_job_log
-- ------------------------------------------------------------
-- Grain: one row per ingestion job run (per marketplace per report).
--
-- This table is the audit trail for the pipeline.
-- A job = one report request + download + load for one marketplace.
--
-- Rules:
--   - Written at job start and updated through each stage.
--   - request_status values: requested | downloaded | completed | failed
--   - error_message populated on failure.
--   - row_count populated on successful load.
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS ingestion_job_log (

    -- ------------------------------------
    -- Surrogate key
    -- ------------------------------------
    id                      BIGSERIAL PRIMARY KEY,

    -- ------------------------------------
    -- Job identity
    -- ------------------------------------
    source_system           TEXT        NOT NULL,
    report_type             TEXT        NOT NULL,
    marketplace             TEXT        NOT NULL,

    -- ------------------------------------
    -- SP-API report tracking
    -- ------------------------------------
    report_id               TEXT,
    document_id             TEXT,

    -- ------------------------------------
    -- S3 / file tracking
    -- ------------------------------------
    local_file_path         TEXT,
    s3_key                  TEXT,
    file_checksum           TEXT,

    -- ------------------------------------
    -- Outcome
    -- ------------------------------------
    request_status          TEXT        NOT NULL,
    row_count               INTEGER,
    error_message           TEXT,

    -- ------------------------------------
    -- Timing
    -- ------------------------------------
    requested_at            TIMESTAMPTZ,
    downloaded_at           TIMESTAMPTZ,
    loaded_at               TIMESTAMPTZ,
    completed_at            TIMESTAMPTZ,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- ------------------------------------
    -- Idempotency constraint
    -- ------------------------------------
    UNIQUE (source_system, report_type, marketplace, report_id, document_id)

);

CREATE INDEX IF NOT EXISTS idx_ingestion_job_log_report_type
    ON ingestion_job_log (report_type, marketplace);

CREATE INDEX IF NOT EXISTS idx_ingestion_job_log_status
    ON ingestion_job_log (request_status, updated_at);
