# SellerIQ Pipeline Plan

This file defines the high-level data pipeline plan for SellerIQ.

The purpose is to clarify how data moves from source systems into raw storage, normalized warehouse tables, and downstream dashboards / AI tools.

---

## Pipeline Goal

SellerIQ is a report-first ecommerce analytics platform.

It ingests marketplace and advertising data, preserves the raw files, parses them into staging tables, transforms them into normalized fact and dimension tables, and serves them to dashboards and AI workflows.

This is a batch analytics system, not a live API query system.

---

## High-Level Flow

Source API / report
→ report request
→ report completion
→ raw file download
→ raw file stored in S3
→ parsed rows loaded into Postgres staging
→ SQL transformations into intermediate tables
→ final fact/dimension tables
→ dashboard / AI / internal app

---

## Source Systems

### Current / planned sources
- Amazon SP-API
- Amazon Advertising API
- Walmart Marketplace API
- Walmart advertising reports / Ads API
- CSV uploads for supplemental data
- internal app-generated data

---

## Raw Layer

### Purpose
The raw layer stores the original source files exactly as received.

### Raw storage location
- Amazon S3

### Why raw storage exists
- reprocessing
- debugging
- auditability
- schema evolution
- protection against parsing mistakes

### Raw layer rules
- raw files are immutable
- raw files should be saved before transformation
- original compressed files should be preserved
- file metadata should be logged

---

## Staging Layer

### Purpose
The staging layer converts raw files into structured rows with light cleaning.

### Storage
- Postgres staging tables

### Examples
- stg_amz_sales_traffic_daily
- stg_amz_order_items
- stg_amz_inventory_snapshot
- stg_amz_catalog_items
- stg_amz_finance_events
- stg_amz_ads_campaign_daily
- stg_wmt_sales
- stg_wmt_inventory

### What staging does
- parse rows
- standardize headers
- cast basic data types
- preserve source columns
- attach ingestion metadata

### What staging does not do
- final joins
- final business logic
- final fee categorization
- dashboard metrics
- product identity resolution across all sources

---

## Intermediate Layer

### Purpose
The intermediate layer handles transformations that are reusable across marts.

### Storage
- Postgres intermediate tables

### Examples
- int_product_identity_map
- int_parent_child_asin_map
- int_fee_type_standardization
- int_order_items_enriched
- int_inventory_latest_snapshot
- int_ads_product_bridge

### What intermediate does
- deduplication
- reusable joins
- product identity mapping
- fee normalization
- marketplace harmonization
- source reconciliation

### What intermediate does not do
- act as final dashboard-facing tables unless necessary
- hide raw source lineage

---

## Mart Layer

### Purpose
The mart layer contains stable fact and dimension tables used by dashboards, AI, and the SellerIQ application.

### Examples

Dimensions:
- dim_product
- dim_marketplace
- dim_date
- dim_account
- dim_currency

Facts:
- fct_sales_daily
- fct_order_item
- fct_inventory_snapshot_daily
- fct_fee_event
- fct_ads_campaign_daily
- fct_ads_keyword_daily

### Rules
- every fact table must have a clearly defined grain
- dimensions should be stable and reusable
- business logic should live here or in intermediate SQL models
- dashboards should consume marts, not staging

---

## Ingestion Job Types

### 1. Report request jobs
These jobs request reports from APIs.

Examples:
- request sales and traffic report
- request inventory report
- request finance report

### 2. Report polling jobs
These jobs check status until reports are complete.

### 3. Download jobs
These jobs fetch the completed report documents.

### 4. Raw archive jobs
These jobs store raw downloaded files in S3.

### 5. Parse/load jobs
These jobs parse raw files and load rows into Postgres staging tables.

### 6. Transform jobs
These jobs run SQL transformations from staging to intermediate to marts.

### 7. Data quality / validation jobs
These jobs validate record counts, duplicates, nulls, and freshness.

---

## Metadata and Logging

The pipeline should maintain logging tables for observability.

Examples:
- log_report_request
- log_file_download
- log_ingestion_job
- log_load_validation

Recommended metadata:
- source_system
- report_type
- marketplace
- report_id
- report_document_id
- request_status
- requested_at
- completed_at
- downloaded_at
- loaded_at
- file_path
- checksum
- row_count

---

## Idempotency Rules

Every job should be safe to rerun.

Methods:
- use report_id uniqueness
- use document_id uniqueness
- use file hash/checksum
- use unique load keys in staging
- deduplicate in intermediate where necessary

The system should prefer append + dedupe over destructive overwrite.

---

## Data Quality Rules

Validation checks should include:
- row count checks
- null checks on required keys
- duplicate key checks
- freshness checks
- marketplace/report completeness checks
- schema drift detection where possible

Failures should be logged clearly.

---

## Current Recommended Infrastructure

### Raw storage
- Amazon S3

### Warehouse
- Postgres (hosted via Supabase or equivalent)

### Ingestion language
- Python

### Transformation language
- SQL

### App layer
- Node / TypeScript (future)

### Visualization
- Looker Studio (temporary)
- custom app UI later

---

## Minimal Viable Pipeline

The first production-worthy SellerIQ pipeline should support:

1. request Amazon Sales and Traffic report
2. save raw file to S3
3. parse into stg_amz_sales_traffic_daily
4. transform into fct_sales_daily
5. join to dim_product
6. serve to dashboard

After that, add:
- inventory
- orders
- fees
- ads
- Walmart

---

## Future Enhancements

Potential future additions:
- orchestration with cron / scheduler / workflow tool
- dbt-style transformations
- retry framework
- alerting
- data lineage dashboard
- unified cross-marketplace sales mart
- AI query layer on curated marts

---

## Role of Claude

Claude should use this pipeline plan when:
- recommending project structure
- deciding where logic belongs
- suggesting ingestion workflows
- defining table dependencies
- separating raw, staging, intermediate, and mart responsibilities

Claude should avoid suggesting shortcuts that collapse these layers prematurely.