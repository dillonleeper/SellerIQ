# SellerIQ Report Mapping

This file maps source reports and APIs to SellerIQ staging tables and downstream modeled tables.

The purpose is to make clear:
- what report to pull
- what business purpose it serves
- where it should land first
- what normalized tables it should feed

This is a planning document, not a strict final schema.

---

## Amazon SP-API Report and Dataset Mapping

### 1. Sales and Traffic Report

Source:
- Amazon SP-API Reports API
- report type: GET_SALES_AND_TRAFFIC_REPORT

Purpose:
- daily sales analytics
- sessions
- page views
- ordered revenue
- ordered units
- buy box %
- conversion metrics

Raw landing:
- S3/raw/amazon/GET_SALES_AND_TRAFFIC_REPORT/

Staging table:
- stg_amz_sales_traffic_daily

Likely grain:
- date + child_asin + marketplace

Feeds:
- fct_sales_daily
- dim_product enrichment
- dashboard sales scorecards
- conversion analysis
- traffic trend analysis

Important fields:
- date
- parent_asin
- child_asin
- sku (if present)
- sessions
- page_views
- buy_box_percentage
- units_ordered
- ordered_product_sales
- unit_session_percentage

Notes:
- one of the most important foundation datasets
- should be one of the first ingestion pipelines built

---

### 2. Orders / Order Items

Source:
- Amazon Orders API
- or applicable order reports when used

Purpose:
- line-level order history
- true item-level sales events
- cancellations
- refunds
- order detail analysis

Raw landing:
- S3/raw/amazon/orders/

Staging table:
- stg_amz_order_items

Likely grain:
- one row per order line item

Feeds:
- fct_order_item
- refund analysis
- customer/order logic
- profit attribution logic

Important fields:
- amazon_order_id
- order_item_id
- purchase_date
- sku
- asin
- quantity_ordered
- item_price
- item_tax
- shipping_price
- order_status
- fulfillment_channel

Notes:
- do not merge directly into daily sales fact
- keep order-line grain separate from daily report grain

---

### 3. Inventory / FBA Inventory

Source:
- Amazon inventory reports or FBA inventory data

Purpose:
- inventory on hand
- inbound inventory
- reserved inventory
- stockout monitoring
- reorder logic

Raw landing:
- S3/raw/amazon/inventory/

Staging table:
- stg_amz_inventory_snapshot

Likely grain:
- date + sku + marketplace

Feeds:
- fct_inventory_snapshot_daily
- inventory dashboard
- reorder calculations
- stockout risk analysis

Important fields:
- snapshot_date
- sku
- asin
- fnsku
- fulfillable_quantity
- reserved_quantity
- inbound_quantity
- available_quantity

Notes:
- inventory should be modeled as snapshots, not transactions

---

### 4. Catalog / Listings

Source:
- Amazon Catalog Items API
- Listings API
- Product metadata exports

Purpose:
- canonical product identity
- title normalization
- parent-child mapping
- SKU to ASIN relationship

Raw landing:
- S3/raw/amazon/catalog/

Staging table:
- stg_amz_catalog_items
- stg_amz_listings

Likely grain:
- one row per listing entity / product entity

Feeds:
- dim_product
- int_product_identity_map
- product metadata enrichment

Important fields:
- sku
- asin
- parent_asin
- title
- brand
- color
- size
- variation_theme
- marketplace

Notes:
- this is critical for joining all other datasets
- dim_product should become the canonical identity layer

---

### 5. Finance / Settlement / Fees

Source:
- Amazon Finances API
- settlement reports
- fee reports

Purpose:
- profitability
- fee categorization
- reimbursements
- refunds
- net margin analysis

Raw landing:
- S3/raw/amazon/finance/

Staging table:
- stg_amz_finance_events
- stg_amz_settlement_lines

Likely grain:
- one row per financial event / charge / fee line

Feeds:
- fct_fee_event
- profit model
- contribution margin calculations
- accounting exports

Important fields:
- event_date
- order_id
- sku
- asin
- fee_type
- fee_amount
- currency
- settlement_id
- transaction_type

Notes:
- do not oversimplify fee categories too early
- keep raw fee types available for mapping later

---

## Amazon Advertising Mapping (Future)

### 6. Ads Campaign Performance

Source:
- Amazon Advertising API
- campaign-level report

Purpose:
- spend tracking
- ROAS
- TACOS support
- campaign trend analysis

Raw landing:
- S3/raw/amazon_ads/campaigns/

Staging table:
- stg_amz_ads_campaign_daily

Likely grain:
- date + campaign_id

Feeds:
- fct_ads_campaign_daily
- advertising dashboards
- spend vs sales analysis

Important fields:
- date
- campaign_id
- campaign_name
- clicks
- impressions
- spend
- attributed_sales
- orders

Notes:
- keep campaign grain separate from keyword/search term grain

---

### 7. Ads Keyword Performance

Source:
- Amazon Advertising API
- keyword-level report

Purpose:
- keyword optimization
- bid analysis
- search performance

Raw landing:
- S3/raw/amazon_ads/keywords/

Staging table:
- stg_amz_ads_keyword_daily

Likely grain:
- date + campaign_id + ad_group_id + keyword_id

Feeds:
- fct_ads_keyword_daily
- optimization workflows
- keyword performance dashboards

Important fields:
- date
- campaign_id
- ad_group_id
- keyword_id
- keyword_text
- match_type
- clicks
- impressions
- spend
- attributed_sales

Notes:
- do not combine with campaign-level tables

---

## Walmart Mapping

### 8. Walmart Sales / Orders / Inventory

Source:
- Walmart Marketplace API
- Walmart report exports

Purpose:
- Walmart sales analytics
- inventory and order reporting
- unified marketplace view

Raw landing:
- S3/raw/walmart/

Staging table:
- stg_wmt_sales
- stg_wmt_orders
- stg_wmt_inventory

Feeds:
- marketplace-specific facts
- unified sales layer
- multi-channel dashboards

Notes:
- maintain source-specific staging before attempting unified marketplace facts

---

### 9. Walmart SEM / Ad Performance

Source:
- Walmart SEM performance report
- Walmart Connect Ads API if approved later

Purpose:
- ad spend
- impressions
- clicks
- ROAS
- ad trend analysis

Raw landing:
- S3/raw/walmart_ads/

Staging table:
- stg_wmt_ads_sem_daily

Likely grain:
- date + campaign / item / report row grain depending on source

Feeds:
- fct_wmt_ads_daily
- marketplace ad dashboards
- cross-channel ad analysis

Notes:
- exact final modeling depends on whether the source is report-based or full Ads API

---

## Canonical Modeled Tables

These are the core normalized tables SellerIQ should eventually support.

### Dimensions
- dim_product
- dim_marketplace
- dim_account
- dim_date
- dim_currency

### Facts
- fct_sales_daily
- fct_order_item
- fct_inventory_snapshot_daily
- fct_fee_event
- fct_ads_campaign_daily
- fct_ads_keyword_daily

### Intermediate / Bridge Tables
- int_product_identity_map
- int_parent_child_asin_map
- int_fee_type_map
- int_currency_rates
- int_marketplace_product_map

---

## Priority Order for Buildout

### Phase 1
Build first:
1. Sales and Traffic
2. Catalog / Product mapping
3. Inventory

### Phase 2
Build next:
4. Orders
5. Finance / Fees

### Phase 3
Build later:
6. Amazon Ads
7. Walmart Ads
8. unified profitability models

---

## Modeling Rules

1. Never mix daily facts with order-line facts
2. Never use title as key
3. Always map to dim_product
4. Keep raw fee types available
5. Keep campaign-level and keyword-level ads data separate
6. Preserve source-specific staging tables before unifying across marketplaces

---

## Role of Claude

When using this file, Claude should:
- recommend reports and tables based on business grain
- avoid collapsing multiple source grains into one model
- favor stable, reusable fact and dimension design
- prefer source-specific staging followed by normalized marts