# SellerIQ Build Order

This file defines the recommended implementation order for SellerIQ.

The purpose is to keep development focused, avoid architecture drift, and build the warehouse in the right dependency order.

The priority is to build foundational datasets first, then add richer analytics.

---

## Guiding Principle

Build in dependency order.

Do not start with the most exciting dataset.
Start with the dataset that unlocks the most downstream value with the least modeling chaos.

For SellerIQ, that means:
1. core sales visibility
2. product identity
3. inventory visibility
4. order detail
5. fee/profit logic
6. advertising
7. cross-marketplace unification

---

## Phase 0 — Foundation Setup

### Goal
Set up the core infrastructure and rules before adding multiple datasets.

### Deliverables
- S3 raw storage structure
- Postgres database setup
- naming conventions defined
- ingestion rules defined
- schema planning docs defined
- logging table design
- environment config structure
- one working ingestion script template

### Success criteria
- raw files can be saved
- Postgres can receive staging loads
- one report can move through the full pipeline end to end

---

## Phase 1 — Amazon Sales and Traffic

### Why first
This is the highest-value report for immediate dashboard use and one of the cleanest ways to establish the daily sales grain.

### Deliverables
- request and download GET_SALES_AND_TRAFFIC_REPORT
- store raw files in S3
- parse into stg_amz_sales_traffic_daily
- create fct_sales_daily first-pass model
- basic dashboard metrics working

### Tables
- stg_amz_sales_traffic_daily
- fct_sales_daily
- dim_marketplace
- dim_date

### Success criteria
- daily sales, units, sessions, and conversion can be queried reliably
- dashboard scorecards can use modeled tables instead of Sheets logic

---

## Phase 2 — Product Identity Layer

### Why second
Everything else depends on joining SKU, ASIN, parent ASIN, and listing metadata correctly.

### Deliverables
- ingest catalog/listing metadata
- create canonical dim_product
- create int_product_identity_map
- map sales facts to dim_product

### Tables
- stg_amz_catalog_items
- stg_amz_listings
- int_product_identity_map
- dim_product

### Success criteria
- all core sales facts can join to a canonical product table
- SKU / ASIN joins are stable
- titles are no longer used as keys

---

## Phase 3 — Inventory Snapshots

### Why third
Inventory is operationally critical and relatively clean once the product identity layer exists.

### Deliverables
- ingest inventory snapshots
- create stg_amz_inventory_snapshot
- create fct_inventory_snapshot_daily
- support days-of-cover and stockout monitoring

### Tables
- stg_amz_inventory_snapshot
- fct_inventory_snapshot_daily

### Success criteria
- dashboard can show on-hand, inbound, reserved, and available quantities
- product joins are stable
- reorder logic has a reliable source

---

## Phase 4 — Order Item Detail

### Why fourth
Order-level data is important, but it introduces a different grain and should come after the daily sales grain is already stable.

### Deliverables
- ingest orders / order items
- create stg_amz_order_items
- create fct_order_item
- preserve line-level grain
- support refund/order analysis

### Tables
- stg_amz_order_items
- fct_order_item

### Success criteria
- order-level questions can be answered without polluting daily facts
- order item grain is clearly separated from daily sales grain

---

## Phase 5 — Finance / Fees / Settlements

### Why fifth
This unlocks true profitability, but finance data is messier and should be added only after core sales and product mapping are stable.

### Deliverables
- ingest finance events and settlement lines
- create stg_amz_finance_events
- create stg_amz_settlement_lines
- create int_fee_type_standardization
- create fct_fee_event

### Tables
- stg_amz_finance_events
- stg_amz_settlement_lines
- int_fee_type_standardization
- fct_fee_event

### Success criteria
- fee and charge data is queryable
- raw fee types are preserved
- standardized fee categories exist for downstream profit logic

---

## Phase 6 — Amazon Advertising

### Why sixth
Advertising is extremely valuable, but it adds separate auth, separate grains, and more complex attribution logic.

### Deliverables
- ingest campaign-level ad performance
- ingest keyword-level ad performance
- create separate campaign and keyword facts
- connect spend to marketplace/product context carefully

### Tables
- stg_amz_ads_campaign_daily
- stg_amz_ads_keyword_daily
- fct_ads_campaign_daily
- fct_ads_keyword_daily

### Success criteria
- campaign spend and keyword performance are queryable
- ad grains remain separate
- TACOS/ROAS can be modeled using marts, not dashboard hacks

---

## Phase 7 — Walmart Marketplace

### Why seventh
By this point the modeling framework is mature enough to add a second marketplace cleanly.

### Deliverables
- ingest Walmart sales
- ingest Walmart inventory
- ingest Walmart orders
- create Walmart staging models
- optionally build unified marketplace marts

### Tables
- stg_wmt_sales
- stg_wmt_inventory
- stg_wmt_orders
- source-specific intermediate models

### Success criteria
- Walmart data lands cleanly in the same layered architecture
- source-specific staging is preserved
- Amazon and Walmart are not prematurely forced into one raw model

---

## Phase 8 — Walmart Advertising

### Why eighth
Walmart advertising access is more dependent on credentials and partner/API access, so it should come after the core marketplace pipeline is proven.

### Deliverables
- ingest Walmart SEM report or Ads API data
- create stg_wmt_ads_sem_daily
- create fct_wmt_ads_daily or source-appropriate ad fact table

### Success criteria
- Walmart ad reporting is queryable
- source grain is documented clearly
- ad metrics can be compared across channels later

---

## Phase 9 — Unified Profitability and AI Layer

### Why last
This depends on having stable core facts across sales, inventory, fees, and ads.

### Deliverables
- cross-mart profitability views
- contribution margin models
- executive dashboard views
- AI assistant query layer over curated marts
- trusted semantic layer for app use

### Success criteria
- SellerIQ can answer natural language business questions from curated tables
- profitability metrics are consistent and explainable
- dashboards no longer rely on spreadsheet logic

---

## Suggested Build Priority Summary

1. Infrastructure / raw layer / logging
2. Amazon Sales and Traffic
3. Product identity / dim_product
4. Inventory snapshots
5. Order item detail
6. Finance / fees
7. Amazon ads
8. Walmart marketplace
9. Walmart ads
10. Unified profit / AI layer

---

## What to Avoid

Do not:
- start with ads before core sales data is stable
- build unified marketplace tables before source-specific staging exists
- skip dim_product
- calculate key metrics only in dashboards
- let raw files live only in Google Sheets
- mix order grain and daily grain

---

## Definition of “Done” for Each Phase

A phase is done only when:
1. raw file is preserved
2. staging table exists
3. intermediate logic exists where needed
4. final mart exists
5. row counts are validated
6. key joins are validated
7. dashboard or query consumer can use the output

---

## Role of Claude

Claude should use this build order to:
- prioritize recommendations
- keep the implementation sequence logical
- avoid suggesting later-phase work before prerequisites exist
- reinforce dependency-aware development