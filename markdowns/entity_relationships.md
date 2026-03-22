# SellerIQ Entity Relationships

This file defines the core entities in SellerIQ and how they relate to one another.

The purpose is to help Claude understand:
- which tables are dimensions vs facts
- how entities join together
- which identifiers are canonical
- where one-to-many and many-to-many relationships exist
- how to avoid incorrect joins and duplicated metrics

This is a conceptual relationship guide, not a full ERD.

---

## Core Modeling Principle

SellerIQ is built around a warehouse model with:
- dimensions
- fact tables
- intermediate mapping / bridge tables

The most important relationship rule is:

All business facts should map to a stable product identity whenever possible.

That means:
- dim_product is a foundational dimension
- facts should join through canonical product identity
- raw source keys should be preserved, but modeled joins should not depend on unstable text fields like title

---

## Core Entities

### 1. Product

Canonical table:
- dim_product

Purpose:
Represents the stable product identity used across sales, inventory, orders, fees, and advertising.

Canonical key:
- product_id

Important identifiers:
- sku
- asin
- parent_asin
- fnsku
- marketplace
- brand
- title

Relationship notes:
- one product may appear in many fact tables
- one parent_asin may relate to many child products
- one sku usually maps to one child listing within a marketplace, but source systems may be messy
- product identity may require intermediate mapping logic before facts can join cleanly

dim_product is one of the most important tables in SellerIQ.

---

### 2. Marketplace

Canonical table:
- dim_marketplace

Purpose:
Represents the selling platform and market context.

Examples:
- Amazon US
- Amazon CA
- Walmart US

Canonical key:
- marketplace_id

Relationship notes:
- one marketplace has many products
- one marketplace has many orders
- one marketplace has many daily sales rows
- one marketplace has many inventory snapshots
- marketplace is often part of the natural grain in source data

A product should usually be interpreted within marketplace context.

---

### 3. Date

Canonical table:
- dim_date

Purpose:
Represents the calendar/date dimension used for daily reporting and time analysis.

Canonical key:
- date or date_id

Relationship notes:
- one date has many daily fact rows
- date is used across sales, inventory, and ads facts
- order facts may also use purchase_date, ship_date, settlement_date, etc.

Date joins should be explicit and use the correct business meaning.

---

### 4. Account

Canonical table:
- dim_account

Purpose:
Represents the seller account, brand account, or connected marketplace account.

Canonical key:
- account_id

Relationship notes:
- one account has many products
- one account has many orders
- one account has many ad campaigns
- one account may span multiple marketplaces depending on platform setup

This is important for multi-account or SaaS scenarios.

---

## Core Fact Relationships

### 5. Daily Sales Fact

Canonical table:
- fct_sales_daily

Purpose:
Represents daily product-level marketplace performance.

Typical grain:
- date + product_id + marketplace_id + account_id

Common metrics:
- ordered_revenue
- ordered_units
- sessions
- page_views
- buy_box_percentage
- unit_session_percentage

Relationships:
- many rows relate to one product
- many rows relate to one marketplace
- many rows relate to one date
- many rows relate to one account

Join path:
- fct_sales_daily.product_id → dim_product.product_id
- fct_sales_daily.marketplace_id → dim_marketplace.marketplace_id
- fct_sales_daily.date → dim_date.date
- fct_sales_daily.account_id → dim_account.account_id

Important rule:
Do not join fct_sales_daily directly to raw catalog text fields when dim_product exists.

---

### 6. Order Item Fact

Canonical table:
- fct_order_item

Purpose:
Represents individual order line items.

Typical grain:
- one row per order item / order line

Common fields:
- order_id
- order_item_id
- product_id
- marketplace_id
- account_id
- purchase_date
- quantity_ordered
- item_price

Relationships:
- many order items relate to one product
- many order items belong to one order_id
- many order items relate to one marketplace
- many order items relate to one account

Important distinction:
fct_order_item is not the same grain as fct_sales_daily.

Do not join or aggregate these together casually without respecting grain differences.

---

### 7. Inventory Snapshot Fact

Canonical table:
- fct_inventory_snapshot_daily

Purpose:
Represents daily inventory state for a product.

Typical grain:
- date + product_id + marketplace_id + account_id

Common metrics:
- on_hand_quantity
- available_quantity
- reserved_quantity
- inbound_quantity

Relationships:
- many inventory snapshots relate to one product
- many inventory snapshots relate to one date
- many inventory snapshots relate to one marketplace
- many inventory snapshots relate to one account

Important rule:
Inventory is a snapshot fact, not a transaction fact.

Do not treat inventory rows like sales events.

---

### 8. Fee Event Fact

Canonical table:
- fct_fee_event

Purpose:
Represents fee, charge, refund, reimbursement, or settlement-related events.

Typical grain:
- one row per fee or finance event

Common fields:
- fee_event_id
- order_id (if applicable)
- product_id (if available)
- settlement_id
- fee_type
- fee_amount
- event_date

Relationships:
- many fee events may relate to one order
- many fee events may relate to one product
- many fee events relate to one marketplace
- many fee events relate to one account

Important rule:
Fee events may not always map cleanly to product_id.
Source lineage must be preserved.

---

### 9. Ads Campaign Fact

Canonical table:
- fct_ads_campaign_daily

Purpose:
Represents daily ad campaign performance.

Typical grain:
- date + campaign_id + account_id + marketplace_id

Common metrics:
- impressions
- clicks
- spend
- attributed_sales
- orders

Relationships:
- many daily campaign rows relate to one campaign
- many daily campaign rows relate to one account
- many daily campaign rows relate to one marketplace
- many daily campaign rows relate to one date

Important rule:
Campaign facts should not be forced to join directly to product facts unless a reliable bridge exists.

---

### 10. Ads Keyword Fact

Canonical table:
- fct_ads_keyword_daily

Purpose:
Represents daily keyword-level ad performance.

Typical grain:
- date + keyword_id + ad_group_id + campaign_id

Relationships:
- many keyword rows belong to one ad group
- many ad groups belong to one campaign
- many keyword rows relate to one date
- many keyword rows relate to one account / marketplace

Important rule:
Do not combine campaign-level and keyword-level metrics in the same fact table.

---

## Supporting Entities

### 11. Campaign

Potential dimension:
- dim_campaign

Purpose:
Represents advertising campaign metadata.

Canonical key:
- campaign_id

Fields may include:
- campaign_name
- campaign_type
- targeting_type
- status
- start_date
- end_date

Relationships:
- one campaign has many campaign fact rows
- one campaign has many ad groups
- one campaign may relate indirectly to products through advertised SKUs or ASINs

---

### 12. Ad Group

Potential dimension:
- dim_ad_group

Purpose:
Represents ad group metadata within campaigns.

Canonical key:
- ad_group_id

Relationships:
- one ad group belongs to one campaign
- one ad group has many keyword facts

---

### 13. Keyword

Potential dimension:
- dim_keyword

Purpose:
Represents keyword metadata.

Canonical key:
- keyword_id

Relationships:
- one keyword may have many daily fact rows
- one keyword belongs to one ad group
- one ad group belongs to one campaign

---

## Bridge and Mapping Relationships

### 14. Product Identity Mapping

Canonical intermediate:
- int_product_identity_map

Purpose:
Maps raw platform identifiers to canonical product_id.

May include:
- sku
- asin
- parent_asin
- fnsku
- marketplace_id
- account_id
- product_id

Why this exists:
Different reports identify products differently.
Some reports use SKU.
Some use ASIN.
Some use parent/child ASIN.
Some include title, which should not be a join key.

This map is often the critical bridge between raw source rows and dim_product.

---

### 15. Parent-Child Product Relationship

Canonical intermediate:
- int_parent_child_asin_map

Purpose:
Represents product variation hierarchy.

Relationship:
- one parent_asin has many child_asins
- one child_asin belongs to one parent_asin within a marketplace context

Important rule:
Parent and child metrics should not be mixed unless aggregation logic is explicit.

---

### 16. Ads-to-Product Bridge

Possible bridge:
- bridge_campaign_product
- bridge_advertised_product
- int_ads_product_bridge

Purpose:
Maps advertising entities to product entities where possible.

Possible relationships:
- one campaign may advertise many products
- one product may appear in many campaigns
- one ad group may relate to many products
- one keyword may indirectly influence many products

This is often many-to-many.

Important rule:
Do not assume campaign_id → product_id is one-to-one.

Use a bridge or source-appropriate attribution logic.

---

## High-Level Relationship Summary

### One-to-many relationships

- one marketplace → many products
- one marketplace → many sales fact rows
- one marketplace → many inventory fact rows
- one account → many products
- one account → many orders
- one date → many daily sales rows
- one date → many inventory rows
- one product → many sales rows
- one product → many inventory rows
- one product → many order items
- one order → many order items
- one campaign → many daily campaign fact rows
- one campaign → many ad groups
- one ad group → many keywords
- one keyword → many daily keyword fact rows

### Many-to-many relationships

- campaigns ↔ products
- ads ↔ products
- parent products ↔ summarized business reporting contexts
- source identifiers ↔ canonical products during messy mapping stages

Many-to-many relationships should use bridges or documented transformation logic.

---

## Join Guidance

### Safe common joins
- fact.product_id → dim_product.product_id
- fact.marketplace_id → dim_marketplace.marketplace_id
- fact.account_id → dim_account.account_id
- daily_fact.date → dim_date.date

### Be careful with these joins
- orders to daily sales
- ads to products
- fees to products
- parent ASIN to child ASIN rollups
- catalog rows to transactional rows using title

### Avoid these joins
- title to title
- campaign_name to product title
- keyword text directly to SKU
- daily facts directly to order-line facts without aggregation planning

---

## Grain Protection Rules

1. Never join two fact tables together at raw grain unless the relationship is clearly defined.
2. Prefer joining facts through shared dimensions.
3. Respect one-row-per-X grain definitions.
4. Use bridge tables when relationships are many-to-many.
5. Aggregate before joining when necessary.

Examples:
- aggregate order items to date + product before comparing with daily sales
- aggregate keyword facts to campaign before comparing with campaign facts
- aggregate child products to parent only with explicit logic

---

## Canonical Join Hierarchy

When possible, SellerIQ should model relationships in this order:

raw source identifiers
→ intermediate product identity map
→ dim_product
→ fact tables
→ dashboard / AI consumption

This keeps joins stable and explainable.

---

## Role of Claude

Claude should use this file to:
- recommend safe joins
- avoid grain mismatches
- identify when bridge tables are needed
- reinforce canonical product identity
- prevent duplicated metrics caused by bad joins

Claude should prefer clear relationship modeling over shortcut joins.