# SellerIQ

**Ecommerce Data Warehouse & Analytics Pipeline**

SellerIQ is a production-grade data pipeline and dimensional warehouse built to ingest, model, and analyze marketplace data from Amazon SP-API and Walmart Marketplace API.

It replaces manual spreadsheet workflows with a reliable, scalable, and reprocessable data system — raw files preserved in S3, structured data modeled in Postgres, and a clean warehouse layer ready for dashboards and AI-assisted analytics.

---

## Architecture

```
Amazon SP-API / Walmart API
        │
        ▼
  Report Request & Poll
        │
        ▼
  Raw File Download
        │
        ▼
  S3 Raw Archive (immutable)
        │
        ▼
  Postgres Staging (stg_*)
        │
        ▼
  Intermediate Layer (int_*)
        │
        ▼
  Fact & Dimension Tables (fct_*, dim_*)
        │
        ▼
  Dashboards / AI Query Layer
```

Every raw file is preserved in S3 before transformation. Every ingestion job is logged with status, timing, file path, checksum, and row count. Every pipeline is idempotent — safe to rerun without creating duplicates.

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
| Sources | Amazon SP-API, Walmart Marketplace API |

---

## Data Sources

### Currently Ingested
- **Amazon Sales & Traffic** — weekly sales, sessions, page views, buy box %, conversion by ASIN
- **Amazon Catalog Items** — product titles, brands, parent/child ASIN relationships
- **Amazon Listings** — SKU to ASIN mapping via merchant listings report
- **Amazon FBA Inventory** — daily fulfillable, reserved, inbound, and available quantities

### Planned
- Amazon Orders / Order Items
- Amazon Finance & Settlement
- Amazon Advertising (campaign + keyword level)
- Walmart Marketplace (sales, inventory, orders)
- Walmart Advertising

---

## Warehouse Schema

### Phase 1 — Sales & Traffic

| Table | Grain | Description |
|---|---|---|
| `stg_amz_sales_traffic_daily` | report_id + child_asin + marketplace | Raw weekly sales and traffic data from SP-API |
| `ingestion_job_log` | one row per job run | Full audit log for every pipeline execution |

### Phase 2 — Product Identity

| Table | Grain | Description |
|---|---|---|
| `stg_amz_catalog_items` | asin + marketplace | Raw catalog metadata from Catalog Items API |
| `stg_amz_listings` | sku + marketplace | Raw SKU → ASIN mapping from listings report |
| `dim_product` | asin + marketplace | Canonical product dimension with SKU, title, brand |
| `int_product_identity_map` | child_asin + marketplace | Bridge table mapping sales rows to dim_product |

### Phase 3 — Inventory

| Table | Grain | Description |
|---|---|---|
| `stg_amz_inventory_snapshot` | snapshot_date + sku + marketplace | Raw FBA inventory snapshot |
| `fct_inventory_snapshot_daily` | snapshot_date + sku + marketplace | Modeled inventory fact joined to dim_product |

---

## Pipeline Scripts

| Script | Purpose |
|---|---|
| `ingest_sales_traffic.py` | Weekly Sales & Traffic ingestion (current week) |
| `backfill_sales_traffic.py` | Historical backfill — Jan 2025 to present |
| `ingest_catalog.py` | Catalog metadata ingestion + dim_product build |
| `ingest_listings.py` | Listings report ingestion + SKU backfill to dim_product |
| `ingest_inventory.py` | Daily FBA inventory snapshot ingestion |

---

## Key Design Decisions

**Report-based ingestion over live API polling**
All data is pulled via Amazon's Reports API rather than live endpoint calls. This is more stable, better for batch processing, and produces reprocessable raw files.

**Idempotent loads**
Every staging table uses `ON CONFLICT DO NOTHING` with natural key constraints. Running the same pipeline twice produces no duplicates.

**Raw files are immutable**
Every report is saved to S3 before any transformation. If a parser breaks or a schema changes, raw data can be reprocessed from the original source file.

**Layered warehouse modeling**
Data flows through four distinct layers — staging preserves source columns, intermediate handles joins and deduplication, facts define business grains, dimensions provide stable join keys. Business logic never lives in dashboards.

**Canonical product identity**
Product identity is resolved through a dedicated intermediate table (`int_product_identity_map`) that maps raw ASINs to a stable `product_id` in `dim_product`. SKU, ASIN, parent ASIN, title, and brand are all normalized in one place.

**Full pipeline observability**
Every ingestion job writes to `ingestion_job_log` regardless of success or failure — recording report ID, document ID, S3 path, file checksum, row count, status, and error message.

---

## Setup

### Prerequisites
- Python 3.11+
- PostgreSQL database (Supabase recommended)
- Amazon AWS account with S3 bucket
- Amazon SP-API developer app with refresh token

### Install dependencies
```bash
pip install python-amazon-sp-api boto3 psycopg2-binary
```

### Configure credentials
Copy `config.py.example` to `config.py` and fill in your values:

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
```

### Run DDL
Create the warehouse tables in your Postgres database:

```sql
-- Run in order
\i sql/phase1_ddl.sql
\i sql/phase2_ddl.sql
\i sql/phase2_ddl_append.sql
\i sql/phase3_ddl.sql
```

### Run ingestion
```bash
# Phase 1 — current week sales & traffic
python ingest_sales_traffic.py

# Phase 1 — historical backfill
python backfill_sales_traffic.py

# Phase 2 — catalog and product identity
python ingest_catalog.py
python ingest_listings.py

# Phase 3 — daily inventory snapshot
python ingest_inventory.py
```

---

## Backfill

The backfill script processes all Sunday–Saturday weeks from January 2025 to present. It is safe to stop and restart at any point — already-loaded weeks are detected and skipped automatically.

```
Backfill range: 2025-01-05 → 2026-03-14
Total weeks: 62
Marketplaces: US, CA
Total rows loaded: ~14,600
```

---

## Project Status

| Phase | Status |
|---|---|
| Phase 0 — Infrastructure | ✅ Complete |
| Phase 1 — Sales & Traffic | ✅ Complete |
| Phase 2 — Product Identity | ✅ Complete |
| Phase 3 — Inventory Snapshots | ✅ Complete |
| Phase 4 — Order Item Detail | 🔄 Planned |
| Phase 5 — Finance & Fees | 🔄 Planned |
| Phase 6 — Amazon Advertising | 🔄 Planned |
| Phase 7 — Walmart Marketplace | 🔄 Planned |
| Phase 8 — Walmart Advertising | 🔄 Planned |
| Phase 9 — Unified Profitability & AI Layer | 🔄 Planned |

---

## Documentation

Architecture decisions, schema definitions, metric definitions, ingestion rules, and build order are documented in the `markdowns/` folder.

---

## License

Private project. Not licensed for external use.