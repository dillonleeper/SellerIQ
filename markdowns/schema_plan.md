# SellerIQ Schema Plan

This file defines the core warehouse tables.

dim_product
-----------
one row per canonical product

fields:
- product_id
- sku
- asin
- parent_asin
- marketplace
- title
- brand


fct_sales_daily
---------------
grain:
date + sku + marketplace

fields:
- date
- sku
- asin
- marketplace
- units
- revenue
- sessions


fct_order_item
--------------
grain:
one row per order line

fields:
- order_id
- order_item_id
- sku
- asin
- quantity
- item_price
- fees


fct_inventory_snapshot
----------------------
grain:
date + sku

fields:
- date
- sku
- asin
- on_hand
- inbound
- reserved


fct_ads_daily
-------------
grain:
date + campaign + sku

fields:
- date
- campaign_id
- sku
- clicks
- spend
- sales