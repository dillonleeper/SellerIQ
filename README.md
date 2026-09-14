# SellerIQ

SellerIQ is the next version of an idea I first explored in my [DIY Amazon Sales & Traffic Pipeline](https://github.com/dillonleeper/amazon-sp-api-sales-pipeline): pull marketplace data automatically so I do not have to live in Seller Central reports and spreadsheets.

The earlier project sends a small set of weekly metrics directly to Google Sheets. SellerIQ handles the data differently. It saves the original API responses in S3, transforms them with Python, and loads structured tables into PostgreSQL. A separate dashboard can then query those tables without using a spreadsheet as the database.

This repository contains the Python ingestion scripts and warehouse SQL. The public `stable-weekly` branch currently covers Amazon sales and traffic, catalog items, listings, and FBA inventory. Walmart and the other sources listed below are plans, not implemented integrations on this branch.

The dashboard is in [selleriq-app](https://github.com/dillonleeper/selleriq-app), and the public website is in [selleriq-site](https://github.com/dillonleeper/selleriq-site).

## How this differs from the Google Sheets pipeline

Both projects start with Amazon SP-API, but they are meant for different levels of complexity.

| | DIY Amazon pipeline | SellerIQ |
| --- | --- | --- |
| Destination | Google Sheets | S3 and PostgreSQL |
| Raw source files | Downloaded and processed for the sheet | Saved in S3 before database loading |
| Data structure | Spreadsheet tabs and a `Sales_Fact` sheet | Staging, intermediate, fact, and dimension tables |
| Sources in the current code | Weekly Sales & Traffic | Sales & Traffic, Catalog Items, Listings, and FBA Inventory |
| Product identity | Existing ASIN and SKU rows in the sheet | Product dimension and identity mapping in Postgres |
| Analysis | Sheets, pivot tables, or Looker Studio | SQL and a separate Next.js dashboard |
| Best fit | A small seller who wants a simple, low-cost reporting setup | A larger learning project with more history, sources, and modeling |

The important change is not simply replacing Google Sheets with Postgres. SellerIQ separates the pipeline into stages:

1. Keep a raw copy so the original response is still available.
2. Load source-shaped data into staging tables.
3. Resolve relationships such as SKU, child ASIN, parent ASIN, and marketplace.
4. Build fact and dimension tables at defined grains.
5. Let dashboards query modeled data instead of repeating business logic in spreadsheet formulas.

That separation makes it easier to add sources, rerun transformations, preserve history, and use the same definitions across several dashboard views. It also adds more infrastructure and maintenance. For a seller who only needs a weekly sheet, the earlier project may be the more sensible tool.

One technical clarification: S3 stores the raw files; S3 itself does not transform them. The Python scripts perform the parsing and transformation before loading structured data into Postgres.

## Architecture

```text
Amazon SP-API
      |
      v
Reports and Catalog responses
      |
      v
Python ingestion scripts
      |
      +----> Save raw response in S3
      |
      v
Postgres staging tables (stg_*)
      |
      v
Intermediate tables (int_*)
      |
      v
Fact and dimension tables (fct_*, dim_*)
      |
      v
SellerIQ dashboard (separate repository)
```

The scripts archive raw report or catalog responses in S3 and load staging tables in Postgres. Sales and inventory ingestion include job-status logging. Conflict handling and restart behavior vary by script, so review the rerun notes below before repeating a load.

## Tech stack

| Layer | Technology |
| --- | --- |
| Language | Python |
| Warehouse | PostgreSQL (I use Supabase) |
| Raw storage | Amazon S3 |
| API SDK | `python-amazon-sp-api` |
| Database driver | `psycopg2` |
| AWS client | `boto3` |
| Implemented source | Amazon SP-API |

## Data sources

### Implemented on this branch

- **Amazon Sales & Traffic** - weekly sales, sessions, page views, Buy Box percentage, and conversion by ASIN
- **Amazon Catalog Items** - titles, brands, identifiers, and parent/child relationships
- **Amazon Listings** - seller SKU to ASIN mappings
- **Amazon FBA Inventory** - daily fulfillable, reserved, inbound, and available quantities

### Planned

- Amazon Orders and Order Items
- Amazon Finance and settlements
- Amazon Advertising
- Walmart Marketplace
- Walmart Advertising

## Warehouse schema

### Phase 1: Sales and traffic

| Table | Grain | Purpose |
| --- | --- | --- |
| `stg_amz_sales_traffic_daily` | report ID + child ASIN + marketplace | Source-shaped weekly Sales & Traffic rows |
| `ingestion_job_log` | one row per job run | Report-ingestion status and metadata |

### Phase 2: Product identity

| Table | Grain | Purpose |
| --- | --- | --- |
| `stg_amz_catalog_items` | ASIN + marketplace | Catalog metadata from the Catalog Items API |
| `stg_amz_listings` | SKU + marketplace | Seller SKU to ASIN mapping from the listings report |
| `dim_product` | ASIN + marketplace | Product dimension with SKU, title, and brand |
| `int_product_identity_map` | child ASIN + marketplace | Bridge from sales rows to `dim_product` |

### Phase 3: Inventory

| Table | Grain | Purpose |
| --- | --- | --- |
| `stg_amz_inventory_snapshot` | snapshot date + SKU + marketplace | Source-shaped FBA inventory snapshot |
| `fct_inventory_snapshot_daily` | snapshot date + SKU + marketplace | Inventory fact joined to the product dimension |

## Pipeline scripts

| Script | Purpose |
| --- | --- |
| `ingest_sales_traffic.py` | Ingest the last complete week of Sales & Traffic data |
| `backfill_sales_traffic.py` | Backfill weekly Sales & Traffic data from January 2025 forward |
| `ingest_catalog.py` | Ingest catalog metadata and build `dim_product` |
| `ingest_listings.py` | Ingest listings and add seller SKUs to the product model |
| `ingest_inventory.py` | Ingest a daily FBA inventory snapshot |
| `run_weekly_update.py` | Run the weekly ingestion steps together |
| `reload_sales_from_s3.py` | Reparse archived sales files and reload them into Postgres |

## Design notes

**Reports and direct API calls**

Sales and traffic, listings, and FBA inventory use Amazon's Reports API. Catalog metadata uses the Catalog Items API directly. Both paths save raw responses for later inspection or reprocessing.

**Conflict handling and reruns**

Sales staging uses `ON CONFLICT DO NOTHING` on `(report_id, marketplace, child_asin)`. Catalog and listings use upserts, while inventory loads use date, SKU, and marketplace keys. A new report ID for the same sales period is not blocked by the report-ID constraint, so these keys do not guarantee period-level deduplication. The backfill script separately checks completed jobs by marketplace and week.

**Raw-file retention**

The scripts save raw responses in S3 so parsing can be revisited later. This code does not configure S3 Object Lock or bucket versioning. Whether the raw archive is truly immutable depends on the bucket configuration and write permissions.

**Layered modeling**

The SQL separates staging, intermediate, fact, and dimension tables. Staging keeps source-shaped data, the product identity map connects ASINs to product IDs, and the inventory fact joins snapshots to products. I use these layers to keep shared definitions in the database where possible.

**Ingestion logs**

`ingestion_job_log` records report IDs, document IDs, S3 paths, checksums, row counts, statuses, and errors where the ingestion script supplies them. Logging coverage differs across scripts; this is not a complete monitoring system for every operation.

## Setup

### Prerequisites

- Python 3.11 or later
- A PostgreSQL database
- An AWS account with an S3 bucket
- An Amazon SP-API developer application with a refresh token

### Install dependencies

```bash
pip install python-amazon-sp-api boto3 psycopg2-binary
```

### Configure credentials

Create a local `config.py` using the settings below. There is no checked-in configuration template on this branch. `config.py` is ignored by Git; keep credentials out of commits.

```python
# Amazon SP-API
AMAZON_CLIENT_ID = "your_client_id"
AMAZON_CLIENT_SECRET = "your_client_secret"
AMAZON_REFRESH_TOKEN = "your_refresh_token"

# AWS and S3
AWS_ACCESS_KEY_ID = "your_access_key"
AWS_SECRET_ACCESS_KEY = "your_secret_key"
AWS_REGION = "us-east-1"
S3_BUCKET = "your-bucket-name"

# Postgres
POSTGRES_HOST = "your_host"
POSTGRES_PORT = 5432
POSTGRES_DB = "postgres"
POSTGRES_USER = "postgres"
POSTGRES_PASSWORD = "your_password"

# Marketplaces
US_MARKETPLACE_ID = "ATVPDKIKX0DER"
CA_MARKETPLACE_ID = "A2EUQ1WTGCTBG2"
SELLER_ID = "your_seller_id"

# Pipeline settings
REPORT_POLL_MAX_ATTEMPTS = 30
REPORT_POLL_SLEEP_SECONDS = 30
DRY_RUN = False
ENVIRONMENT = "dev"
RAW_OUTPUT_DIR = "raw_reports"
```

### Create the tables

Review the DDL before applying it to your database. From the repository root, the checked-in files can be run in `psql` in this order:

```sql
\i sql/selleriq_phase1_ddl.sql
\i sql/selleriq_phase2_ddl.sql
\i sql/selleriq_phase3_ddl.sql
```

The scripts require your own SP-API access, S3 bucket, and database. This is not a self-contained demo with sample marketplace data.

Some maintenance scripts assume database changes beyond the checked-in DDL. For example, `reload_sales_from_s3.py` uses a period-based conflict key that differs from the checked-in Phase 1 definition. Check those assumptions before using a maintenance or reload script.

### Run ingestion

```bash
# Last complete week of sales and traffic
python ingest_sales_traffic.py

# Historical sales and traffic
python backfill_sales_traffic.py

# Product identity
python ingest_catalog.py
python ingest_listings.py

# Daily inventory snapshot
python ingest_inventory.py
```

## Backfill behavior

The backfill starts with the first Sunday on or after January 1, 2025 and ends with the last complete Sunday-Saturday week. It skips weeks recorded as completed in the job log. After an interrupted or failed run, check the job log and loaded rows before restarting.

The original README recorded this historical example. These figures are not a current database count:

```text
Backfill range: 2025-01-05 to 2026-03-14
Total weeks: 62
Marketplaces: US, CA
Total rows loaded: approximately 14,600
```

## Current branch scope

| Phase | Status |
| --- | --- |
| Infrastructure | Requires your own S3 bucket and PostgreSQL database |
| Sales & Traffic | Scripts and SQL present |
| Product identity | Scripts and SQL present |
| Inventory snapshots | Scripts and SQL present |
| Order item detail | Planned |
| Finance and fees | Planned |
| Amazon Advertising | Planned |
| Walmart Marketplace | Planned |
| Walmart Advertising | Planned |
| Unified profitability layer | Planned |

## Documentation

The checked-in `sql/` files contain schema definitions and comments about table grains. The `markdowns/` folder referenced in earlier documentation is ignored by Git and is not included in this public repository.

## License

The repository is public, but no license for external use is granted.

