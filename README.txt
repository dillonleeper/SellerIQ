# SellerIQ

**Ecommerce data pipeline and warehouse**

SellerIQ is an ecommerce analytics project I'm building around problems I've dealt with at work: getting marketplace data out of different systems, organizing it, and using it to answer questions. This repository contains the Python ingestion scripts and PostgreSQL warehouse SQL.

I'm moving reporting work from spreadsheets into a pipeline that saves raw responses in S3 and loads structured data into Postgres. The public default branch, `stable-weekly`, covers Amazon sales and traffic, catalog items, listings, and FBA inventory. Walmart support and the other sources listed below are plans for this branch, not implemented integrations.

The dashboard is in [selleriq-app](https://github.com/dillonleeper/selleriq-app), and the public website is in [selleriq-site](https://github.com/dillonleeper/selleriq-site).

---

## Architecture

```
Amazon SP-API
        â”‚
        â–¼
  Reports / Catalog Requests
        â”‚
        â–¼
  Raw File Download
        â”‚
        â–¼
  S3 Raw Archive
        â”‚
        â–¼
  Postgres Staging (stg_*)
        â”‚
        â–¼
  Intermediate Layer (int_*)
        â”‚
        â–¼
  Fact & Dimension Tables (fct_*, dim_*)
        â”‚
        â–¼
  Dashboard (separate repo)
```

The scripts archive raw report or catalog responses in S3 and load staging tables in Postgres. Sales and inventory ingestion include job-status logging. Conflict handling and restart behavior vary by script; see the notes below before rerunning a load.

---

## Tech Stack

| Layer | Technology |
|---|---|
| Language | Python |
| Warehouse | PostgreSQL (Supabase) |
| Raw Storage | Amazon S3 |
| API SDK | python-amazon-sp-api |
| DB Driver | psycopg2 |
| AWS Client | boto3 |
| Implemented source | Amazon SP-API |

---

## Data Sources

### Implemented on this branch
- **Amazon Sales & Traffic** â€” weekly sales, sessions, page views, buy box %, conversion by ASIN
- **Amazon Catalog Items** â€” product titles, brands, parent/child ASIN relationships
- **Amazon Listings** â€” SKU to ASIN mapping via merchant listings report
- **Amazon FBA Inventory** â€” daily fulfillable, reserved, inbound, and available quantities

### Planned
- Amazon Orders / Order Items
- Amazon Finance & Settlement
- Amazon Advertising (campaign + keyword level)
- Walmart Marketplace (sales, inventory, orders)
- Walmart Advertising

---

## Warehouse Schema

### Phase 1 â€” Sales & Traffic

| Table | Grain | Description |
|---|---|---|
| `stg_amz_sales_traffic_daily` | report_id + child_asin + marketplace | Raw weekly sales and traffic data from SP-API |
| `ingestion_job_log` | one row per job run | Report-ingestion status and metadata |

### Phase 2 â€” Product Identity

| Table | Grain | Description |
|---|---|---|
| `stg_amz_catalog_items` | asin + marketplace | Raw catalog metadata from Catalog Items API |
| `stg_amz_listings` | sku + marketplace | Raw SKU â†’ ASIN mapping from listings report |
| `dim_product` | asin + marketplace | Canonical product dimension with SKU, title, brand |
| `int_product_identity_map` | child_asin + marketplace | Bridge table mapping sales rows to dim_product |

### Phase 3 â€” Inventory

| Table | Grain | Description |
|---|---|---|
| `stg_amz_inventory_snapshot` | snapshot_date + sku + marketplace | Raw FBA inventory snapshot |
| `fct_inventory_snapshot_daily` | snapshot_date + sku + marketplace | Modeled inventory fact joined to dim_product |

---

## Pipeline Scripts

| Script | Purpose |
|---|---|
| `ingest_sales_traffic.py` | Weekly Sales & Traffic ingestion (last complete week) |
| `backfill_sales_traffic.py` | Historical backfill â€” Jan 2025 to present |
| `ingest_catalog.py` | Catalog metadata ingestion + dim_product build |
| `ingest_listings.py` | Listings report ingestion + SKU backfill to dim_product |
| `ingest_inventory.py` | Daily FBA inventory snapshot ingestion |

---

## Key Design Decisions

**Reports for batch ingestion**
Sales and traffic, listings, and FBA inventory use Amazon's Reports API. Catalog metadata uses the Catalog Items API directly. Both paths save raw responses for later inspection or reprocessing.

**Conflict handling and reruns**
Sales staging uses `ON CONFLICT DO NOTHING` on `(report_id, marketplace, child_asin)`. Catalog and listings use upserts, while inventory loads use date/SKU/marketplace keys. A new report ID for the same sales period is not blocked by the report-ID constraint, so these keys do not guarantee period-level deduplication. The backfill script separately checks completed jobs by marketplace and week.

**Keep the raw responses**
The scripts save raw responses in S3 so parsing can be revisited later. This code does not configure S3 Object Lock or bucket versioning; keeping an immutable archive depends on the bucket configuration and write permissions.

**Layered warehouse modeling**
The SQL separates staging, intermediate, fact, and dimension tables. Staging keeps the source data, the product identity map connects ASINs to product IDs, and the inventory fact joins snapshots to products. I use these layers to keep shared definitions in the database where possible.

**Canonical product identity**
Product identity is resolved through a dedicated intermediate table (`int_product_identity_map`) that maps raw ASINs to a stable `product_id` in `dim_product`. SKU, ASIN, parent ASIN, title, and brand are all normalized in one place.

**Ingestion logs**
`ingestion_job_log` records report IDs, document IDs, S3 paths, checksums, row counts, statuses, and errors where the ingestion script supplies them. Logging coverage differs across scripts; it is not a complete monitoring system for every operation.

---

## Setup

### Prerequisites
- Python 3.11+
- PostgreSQL database (I use Supabase)
- Amazon AWS account with S3 bucket
- Amazon SP-API developer app with refresh token

### Install dependencies
```bash
pip install python-amazon-sp-api boto3 psycopg2-binary
```

### Configure credentials
Create a local `config.py` using the settings below. There is no checked-in config template on this branch. `config.py` is ignored by Git; keep credentials out of commits:

```python
# Amazon SP-API
AMAZON_CLIENT_ID      = "your_client_id"
AMAZON_CLIENT_SECRET  = "your_client_secret"
AMAZON_REFRESH_TOKEN  = "your_refresh_token"

# AWS / S3
AWS_ACCESS_KEY_ID     = "your_access_key"
AWS_SECRET_ACCESS_KEY = "your_secret_key"
AWS_REGION            = "us-east-1"
S3_BUCKET             = "your-bucket-name"

# Postgres
POSTGRES_HOST         = "your_host"
POSTGRES_PORT         = 5432
POSTGRES_DB           = "postgres"
POSTGRES_USER         = "postgres"
POSTGRES_PASSWORD     = "your_password"

# Marketplaces
US_MARKETPLACE_ID     = "ATVPDKIKX0DER"
CA_MARKETPLACE_ID     = "A2EUQ1WTGCTBG2"
SELLER_ID             = "your_seller_id"

# Pipeline settings
REPORT_POLL_MAX_ATTEMPTS  = 30
REPORT_POLL_SLEEP_SECONDS = 30
DRY_RUN                   = False
ENVIRONMENT               = "dev"
RAW_OUTPUT_DIR            = "raw_reports"
```

### Run DDL
Review the DDL before applying it to your own Postgres database. From the repository root, the checked-in files can be run in `psql` in this order:

```sql
-- Run in order
\i sql/selleriq_phase1_ddl.sql
\i sql/selleriq_phase2_ddl.sql
\i sql/selleriq_phase3_ddl.sql
```

The scripts require your own SP-API access, S3 bucket, and database. This is not a self-contained demo with sample marketplace data. Some maintenance scripts assume additional database changes: for example, `reload_sales_from_s3.py` uses a period-based conflict key that differs from the checked-in Phase 1 DDL. Check those assumptions before using maintenance or reload scripts.

### Run ingestion
```bash
# Phase 1 â€” current week sales & traffic
python ingest_sales_traffic.py

# Phase 1 â€” historical backfill
python backfill_sales_traffic.py

# Phase 2 â€” catalog and product identity
python ingest_catalog.py
python ingest_listings.py

# Phase 3 â€” daily inventory snapshot
python ingest_inventory.py
```

---

## Backfill

The backfill script starts with the first Sunday on or after January 1, 2025 and ends with the last complete Sundayâ€“Saturday week. It skips weeks recorded as completed in the job log. After an interrupted or failed run, check the job log and loaded rows before restarting.

The original README recorded this example run; these figures are historical, not a current database count:

```
Backfill range: 2025-01-05 â†’ 2026-03-14
Total weeks: 62
Marketplaces: US, CA
Total rows loaded: ~14,600
```

---

## Branch scope and plans

| Phase | Status |
|---|---|
| Phase 0 â€” Infrastructure | Scripts and SQL present |
| Phase 1 â€” Sales & Traffic | Scripts and SQL present |
| Phase 2 â€” Product Identity | Scripts and SQL present |
| Phase 3 â€” Inventory Snapshots | Scripts and SQL present |
| Phase 4 â€” Order Item Detail | Planned |
| Phase 5 â€” Finance & Fees | Planned |
| Phase 6 â€” Amazon Advertising | Planned |
| Phase 7 â€” Walmart Marketplace | Planned |
| Phase 8 â€” Walmart Advertising | Planned |
| Phase 9 â€” Unified Profitability & AI Layer | Planned |

---

## Documentation

The checked-in `sql/` files contain schema definitions and comments about table grains. The `markdowns/` folder referenced in earlier documentation is ignored by Git and is not included in this public repository.

---

## License

The repository is public, but no license for external use is granted.