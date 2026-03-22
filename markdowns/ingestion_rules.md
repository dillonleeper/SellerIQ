# SellerIQ Ingestion Rules

This file defines the ingestion rules for SellerIQ.

## Purpose

SellerIQ is an ecommerce analytics platform that ingests raw data from:
- Amazon SP-API
- Walmart Marketplace API
- Amazon Ads API (future)
- Walmart Ads API (future)
- CSV uploads (if needed)

The ingestion layer exists to:
1. request raw reports or data exports
2. download and preserve raw files
3. parse those files into structured staging tables
4. support downstream normalization and analytics

The ingestion layer does NOT define business logic or dashboard logic.

---

## Core Principles

### 1. Raw data must always be preserved
Every raw report file should be saved before transformation.

Raw files are the immutable source record.

Reason:
- allows reprocessing
- allows parser fixes
- allows audit/debugging
- prevents data loss if schema changes later

### 2. Reports API is preferred over live endpoint pulling
When a report exists, prefer pulling the report rather than repeatedly calling live endpoints.

Reason:
- better for batch processing
- more stable
- easier to reprocess
- closer to how analytics systems are built

### 3. Ingestion and normalization are separate
Ingestion should only:
- request
- poll
- download
- decompress
- lightly parse
- load into staging

Ingestion should NOT:
- calculate profit
- calculate dashboard metrics
- map final fee logic
- create final product identity logic
- mix business grains

### 4. The SDK is only a transport helper
Libraries such as `python-amazon-sp-api` are used for:
- authentication
- token handling
- report requests
- report polling
- document download

SDK structure must NOT dictate the warehouse schema.

### 5. Raw data should land in S3
All raw report files should be stored in S3 under a clean folder structure.

Suggested path format:

s3://selleriq-raw/amazon/{report_type}/{marketplace}/{yyyy}/{mm}/{dd}/{filename}

Examples:
- amazon/GET_SALES_AND_TRAFFIC_REPORT/US/2026/03/19/report.json.gz
- amazon/GET_AMAZON_FULFILLED_SHIPMENTS_DATA_GENERAL/US/2026/03/19/report.tsv
- walmart/sem-performance/US/2026/03/19/report.csv

### 6. Staging loads go into Postgres
Parsed raw rows should be loaded into Postgres staging tables.

Examples:
- stg_amz_sales_traffic_daily
- stg_amz_orders
- stg_amz_inventory
- stg_wmt_sales
- stg_wmt_ads_sem

### 7. Every ingestion job should be idempotent
Running the same ingestion job twice should not create duplicates.

Use:
- report_id
- document_id
- file hash
- source file name
- unique load keys

to avoid duplicate loads.

### 8. Every loaded dataset must record metadata
Every ingestion load should track:
- source system
- report type
- marketplace
- report_id
- document_id
- requested_at
- downloaded_at
- loaded_at
- file path
- checksum/hash

This metadata should be stored either:
- in a load log table
- or as metadata columns in staging tables

### 9. Do not use Google Sheets as source of truth
Google Sheets may be used for:
- QA
- one-off debugging
- temporary exports

But Sheets must not be the canonical raw layer or normalized warehouse.

### 10. Prefer append + dedupe over overwrite
When possible:
- append raw loads
- deduplicate in staging/intermediate
- avoid destructive overwrites

This protects history and supports reprocessing.

---

## Ingestion Workflow

### Standard Amazon report workflow

1. Create report request
2. Poll report status until complete
3. Retrieve report document
4. Download raw file
5. Save raw file to S3
6. Parse raw file into rows
7. Load rows into Postgres staging table
8. Record ingestion metadata
9. Mark job complete

### Standard Walmart report workflow

1. Request report/export if needed
2. Download raw file
3. Save raw file to S3
4. Parse raw file into rows
5. Load rows into Postgres staging table
6. Record ingestion metadata

---

## File Handling Rules

### Compression
Handle compressed files safely.
Common formats:
- .gz
- .zip
- .json.gz
- .tsv.gz

Always preserve the original downloaded file.

### Parsing
Parsing should:
- standardize headers
- preserve source columns
- cast data types carefully
- avoid dropping fields too early

### Type handling
During staging:
- dates should be converted to proper date/timestamp fields
- numerics should be cast to numeric/decimal where possible
- empty strings should become NULL when appropriate
- raw source text may also be retained for auditability

---

## Scheduling Rules

Suggested schedule:
- sales/traffic: daily
- orders: daily or more frequent if needed
- inventory: daily
- ads: daily
- finance/settlement: daily or weekly depending on report

Do not design dashboards around live API calls.

---

## Error Handling Rules

Ingestion jobs must:
- fail loudly
- log errors
- preserve partial metadata where possible
- never silently skip failed loads

If parsing fails:
- raw file should still remain in S3
- job should log the failure
- reprocessing should be possible later

---

## Naming Conventions

### Raw files
Use source-aware names.

Example:
amazon_sales_traffic_US_2026-03-19_report123.json.gz

### Staging tables
Use prefix:
- stg_amz_
- stg_wmt_
- stg_ads_

### Logs
Use tables like:
- ingestion_job_log
- ingestion_file_log
- report_request_log

---

## What Ingestion Should NOT Do

Ingestion should NOT:
- calculate TACOS
- calculate profit
- decide final fee categories
- create final product identity mapping
- join multiple business grains into one table
- define dashboard scorecards
- hardcode Looker logic

Those belong in the modeling layer.

---

## Role of Claude

When advising on ingestion, Claude should:
- prefer report-based ingestion
- preserve raw files
- keep ingestion separate from business logic
- favor idempotent batch pipelines
- avoid suggesting Google Sheets as a warehouse
- design ingestion to support reprocessing and scale