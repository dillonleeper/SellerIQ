# SellerIQ Metric Definitions

This file defines the canonical metric meanings for SellerIQ.

The purpose is to ensure that dashboards, SQL models, AI responses, and internal discussions all use the same definitions for core business metrics.

These definitions should be treated as the default source of truth unless a specific exception is documented.

---

## Core Principles

1. Every metric should have one clear meaning.
2. Metric names should not be reused for multiple definitions.
3. Business logic should live in the warehouse/modeling layer, not only in dashboards.
4. AI responses should use these definitions unless explicitly told otherwise.
5. When source systems differ, SellerIQ should standardize definitions at the modeled layer and preserve source-specific raw fields where needed.

---

## Sales Metrics

### ordered_revenue
Definition:
The gross product sales value associated with customer orders placed during a given time period.

Typical source:
- Amazon Sales and Traffic report
- Walmart sales report

Includes:
- item-level ordered product value

May exclude:
- shipping
- tax
- refunds
- fees
- advertising spend

Notes:
- This is typically the most useful top-line sales metric for marketplace analytics.
- This should not automatically be called "profit" or "net sales."
- If source data is already marketplace-standardized, keep the original meaning explicit.

---

### ordered_units
Definition:
The total number of units ordered during a given time period.

Typical source:
- Sales and Traffic report
- order item data
- Walmart sales exports

Notes:
- This is not the same as shipped units or fulfilled units.
- Use source-specific alternatives if needed, but keep ordered_units as the canonical marketplace sales volume metric unless otherwise noted.

---

### average_selling_price
Definition:
ordered_revenue / ordered_units

Notes:
- Only calculate when ordered_units > 0
- This is an average selling price per ordered unit, not list price or MSRP.

---

### net_sales
Definition:
ordered_revenue minus refunds, cancellations, and other sales reversals that are included in the chosen model.

Notes:
- Net sales should only be shown when the refund/cancellation logic is clearly defined.
- Do not use "net_sales" casually if the system is only using gross ordered revenue.

---

## Traffic and Conversion Metrics

### sessions
Definition:
The number of sessions recorded for a product or listing during a given time period.

Typical source:
- Amazon Sales and Traffic report
- Walmart listing/performance reports where available

Notes:
- Session definitions may vary by platform.
- Preserve source-specific meaning at staging level, but expose a standardized "sessions" metric in modeled daily facts where possible.

---

### page_views
Definition:
The number of page views recorded during a given time period.

Notes:
- This is distinct from sessions.
- One session can generate multiple page views.

---

### unit_session_percentage
Definition:
ordered_units / sessions

Interpretation:
An approximation of unit conversion rate.

Notes:
- This is often called conversion rate in marketplace contexts.
- Should only be calculated when sessions > 0.
- If the source provides its own metric, preserve both raw and modeled versions where useful.

---

### buy_box_percentage
Definition:
The share of page views or time in which the listing held the buy box, as defined by the source platform.

Notes:
- Preserve the source meaning.
- This is typically a percentage metric, not a raw count.

---

## Inventory Metrics

### on_hand_quantity
Definition:
Units physically available in current inventory stock, before considering reservations or future inbound units.

Notes:
- Depending on source system, this may need to be modeled from multiple raw fields.
- Keep source-specific inventory fields available in staging for transparency.

---

### available_quantity
Definition:
Inventory currently sellable and available for fulfillment.

Typical modeled form:
on_hand_quantity - reserved_quantity

Notes:
- This may differ from raw source definitions.
- Use the platform’s most operationally useful "available" meaning, but document source-specific logic in the model.

---

### reserved_quantity
Definition:
Units currently reserved and therefore not available for immediate sale or fulfillment.

Examples:
- customer order allocation
- fulfillment processing
- transfer or warehouse holds

---

### inbound_quantity
Definition:
Units currently inbound to fulfillment or warehouse inventory but not yet available for sale.

---

### days_of_cover
Definition:
available_quantity / average_daily_units_sold

Purpose:
Estimate how many days current available inventory will last at the current modeled sales pace.

Notes:
- The lookback window for average_daily_units_sold must be defined separately.
- This should not use inbound inventory unless explicitly stated.

---

### average_daily_units_sold
Definition:
Total ordered_units across a defined lookback window divided by the number of days in that window.

Notes:
- The lookback window must be explicit, such as 30-day, 60-day, or 90-day ADS.
- SellerIQ should allow flexible lookback windows where possible.

---

### reorder_urgency
Definition:
A business classification that indicates how urgently a product should be reordered based on inventory position and supply lead time.

Suggested logic:
- critical
- reorder_soon
- healthy

This is a derived business metric, not a raw source metric.

Example logic:
- days_of_cover < total_lead_time_days = critical
- days_of_cover within warning buffer = reorder_soon
- otherwise = healthy

Notes:
- Exact thresholds should be defined in model logic, not in dashboards alone.

---

## Order Metrics

### order_count
Definition:
The number of unique customer orders in a period.

Notes:
- Different from ordered_units.
- One order can contain multiple units and multiple SKUs.

---

### average_order_value
Definition:
Total ordered revenue / number of unique orders

Notes:
- Only valid when computed from order-grain or appropriately aggregated source data.
- Should not be derived from SKU-daily facts unless the logic is clearly consistent.

---

## Advertising Metrics

### spend
Definition:
Total advertising spend during a given time period.

Typical source:
- Amazon Ads API
- Walmart SEM report
- Walmart Ads API

Notes:
- Spend should be stored in source currency and optionally standardized currency if needed.

---

### impressions
Definition:
The number of times an ad was shown.

---

### clicks
Definition:
The number of ad clicks recorded during a given time period.

---

### click_through_rate
Definition:
clicks / impressions

Alias:
CTR

Notes:
- Only calculate when impressions > 0.

---

### cost_per_click
Definition:
spend / clicks

Alias:
CPC

Notes:
- Only calculate when clicks > 0.

---

### attributed_sales
Definition:
Sales credited by the advertising platform to ad interactions under that platform’s attribution rules.

Notes:
- Attributed sales are not always the same as total ordered revenue.
- Attribution windows and rules differ by platform and campaign type.
- Preserve source-specific attribution logic where possible.

---

### acos
Definition:
spend / attributed_sales

Alias:
Advertising Cost of Sales

Interpretation:
How much ad spend was required to generate one dollar of attributed sales.

Notes:
- Only calculate when attributed_sales > 0.
- Lower is generally better, depending on margin structure.

---

### roas
Definition:
attributed_sales / spend

Alias:
Return on Ad Spend

Interpretation:
How many dollars of attributed sales were generated per dollar of ad spend.

Notes:
- Only calculate when spend > 0.
- ROAS is the inverse of ACOS.

---

### tacos
Definition:
spend / total_ordered_revenue

Alias:
Total Advertising Cost of Sales

Interpretation:
How much advertising spend is being used relative to total sales, not only attributed sales.

Notes:
- TACOS is useful for understanding advertising as a share of overall business performance.
- Total_ordered_revenue should be clearly defined and sourced consistently.

---

## Fee and Profitability Metrics

### fee_amount
Definition:
The monetary amount associated with a fee, charge, or cost event from the source platform.

Examples:
- referral fee
- FBA fulfillment fee
- storage fee
- advertising fee
- refund administration fee

Notes:
- Raw fee types should be preserved before mapping.

---

### refund_amount
Definition:
The value returned or reversed due to customer refunds, cancellations, or reimbursement events depending on source logic.

Notes:
- Refund handling should remain transparent and traceable to source events.

---

### cogs_per_unit
Definition:
The cost of goods sold assigned to one unit of a product.

Typical source:
- manual upload
- ERP
- internal finance mapping

Notes:
- This is not usually provided by Amazon or Walmart directly.
- COGS should be modeled separately and joined carefully to sales facts.

---

### total_cogs
Definition:
ordered_units * cogs_per_unit
or a more precise inventory-accounting-based allocation if supported.

Notes:
- For more advanced accounting, FIFO or batch-aware allocation may replace the simple method.

---

### gross_profit
Definition:
ordered_revenue - total_cogs

Notes:
- This is before marketplace fees, ad spend, shipping overhead, and other operating costs unless explicitly stated otherwise.

---

### contribution_profit
Definition:
ordered_revenue
- total_cogs
- marketplace fees
- advertising spend
- fulfillment costs
- other directly attributable variable costs

Notes:
- This is one of the most important economic metrics for SellerIQ.
- Exact included cost types must be documented in model logic.

---

### net_profit
Definition:
Profit after all included business costs in the chosen model.

Notes:
- This should not be used casually.
- If overhead, payroll, software, and non-marketplace expenses are excluded, do not label the metric as full net profit without clarification.

---

### margin_percentage
Definition:
profit_metric / ordered_revenue

Examples:
- gross_margin_percentage
- contribution_margin_percentage
- net_margin_percentage

Notes:
- Always specify which profit metric is being used in the numerator.

---

## Time Comparison Metrics

### week_over_week_change
Definition:
(current_period_value - prior_period_value) / prior_period_value

Purpose:
Measure percentage change compared with the immediately preceding comparable week.

---

### month_over_month_change
Definition:
(current_period_value - prior_period_value) / prior_period_value

Purpose:
Measure percentage change compared with the immediately preceding comparable month.

---

### year_over_year_change
Definition:
(current_period_value - same_period_last_year_value) / same_period_last_year_value

Purpose:
Measure annual growth comparison.

---

## Product and Ranking Metrics

### active_sku_count
Definition:
The number of SKUs currently considered active under the chosen business rule.

Notes:
- Active status should be defined in model logic.
- Example: had inventory, sales, or listing activity in the lookback window.

---

### top_selling_sku
Definition:
A SKU ranked highest by the chosen sales metric over a defined period.

Notes:
- The ranking metric must be explicit:
  - ordered_units
  - ordered_revenue
  - contribution_profit
- Do not use this label without defining the rank basis.

---

## Metric Definition Rules

1. Do not use "sales" as a vague catch-all.
   Prefer:
   - ordered_revenue
   - attributed_sales
   - net_sales

2. Do not use "profit" without specifying the type.
   Prefer:
   - gross_profit
   - contribution_profit
   - net_profit

3. Do not mix source-specific metric meanings without documenting the standardization logic.

4. Derived metrics should be calculated in the modeled layer, not only in the dashboard.

5. When denominator = 0, return NULL or a clearly defined fallback, not misleading values.

---

## AI Behavior Guidance

When Claude answers questions using SellerIQ data:
- it should use these canonical metric definitions
- it should avoid vague phrasing when metric meaning matters
- it should clarify when a metric is source-specific, modeled, or inferred
- it should distinguish ordered revenue from attributed sales
- it should distinguish contribution profit from net profit

If a user asks for "sales," Claude should infer the most likely relevant metric from context, but prefer explicit wording when precision matters.

---

## Role of Claude

Claude should use this file when:
- defining metrics
- writing SQL
- designing dashboards
- explaining KPIs
- answering analytical questions
- proposing schema or semantic layer changes

Claude should favor metric clarity, consistency, and business trust.