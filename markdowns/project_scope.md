# SellerIQ Project Scope

This file defines the business scope, product purpose, target user, and high-level vision for SellerIQ.

The goal is to help Claude understand not only the technical architecture, but also the product direction and commercial intent behind the system.

---

## What SellerIQ Is

SellerIQ is an ecommerce analytics platform for marketplace sellers.

It is designed to ingest data from platforms like Amazon and Walmart, normalize that data into a reliable warehouse, and expose it through dashboards, internal tools, and an AI assistant that helps users understand and act on their business data.

SellerIQ is not just a dashboard.
It is not just a reporting script.
It is not just a chatbot.

It is a decision-support system for ecommerce operators.

---

## Core Product Goal

The core goal of SellerIQ is to make marketplace business data:
- easier to trust
- easier to understand
- easier to query
- easier to act on

SellerIQ should help sellers answer questions like:
- What changed in my business this week?
- Which products are growing or declining?
- Which SKUs are at risk of stocking out?
- What is my true performance after fees and ad spend?
- Which campaigns or keywords are wasting money?
- Where is profit leaking?
- What needs attention today?

---

## What Problem SellerIQ Solves

Marketplace seller data is fragmented, messy, and difficult to use.

Common pain points:
- Amazon and Walmart data live in different systems
- reports are inconsistent and hard to join
- product identity is messy across SKU / ASIN / parent ASIN
- dashboards are often hacked together in spreadsheets
- profitability is hard to trust
- operators waste time manually digging through reports
- teams rely on agencies or siloed tools for answers
- data is often available, but not modeled well enough to drive decisions

SellerIQ exists to solve this by creating a clean data foundation first, then layering dashboards and AI on top.

---

## Who SellerIQ Is For

Primary users:
- ecommerce managers
- marketplace operators
- brand managers
- founders of ecommerce brands
- agencies managing marketplace accounts
- analysts supporting ecommerce teams

Ideal user profile:
- sells on Amazon and/or Walmart
- has multiple SKUs or marketplaces
- needs better visibility into sales, inventory, advertising, and profitability
- wants fewer manual spreadsheets
- wants answers faster
- values trustworthy numbers over flashy dashboards

---

## What SellerIQ Is NOT

SellerIQ is not:
- a generic BI tool
- a no-code dashboard template
- a lightweight spreadsheet replacement
- a marketplace listing tool
- a shipping or fulfillment system
- a replacement for Seller Central or Walmart Seller Center UI
- a live operational console driven by real-time API polling

SellerIQ is a modeled analytics platform with a strong data foundation.

---

## Product Pillars

SellerIQ should eventually support these core product pillars:

### 1. Sales Intelligence
Users should be able to understand:
- sales trends
- units ordered
- traffic
- conversion
- top movers
- underperforming SKUs
- week-over-week and month-over-month changes

### 2. Inventory Intelligence
Users should be able to understand:
- current stock position
- inbound inventory
- reserved inventory
- days of cover
- stockout risk
- reorder urgency

### 3. Advertising Intelligence
Users should be able to understand:
- spend
- ROAS
- TACOS
- campaign performance
- keyword performance
- wasted spend
- efficiency trends

### 4. Profitability Intelligence
Users should be able to understand:
- fees
- refunds
- storage costs
- ad impact
- contribution margin
- gross profit and net profit behavior

### 5. AI Query Layer
Users should be able to ask natural language questions like:
- Why are sales down this week?
- Which SKUs are driving growth?
- Which products need to be reordered?
- What changed in ad performance?
- Where is margin getting worse?

The AI layer should sit on top of modeled data, not raw API responses.

---

## Product Philosophy

SellerIQ should prioritize:
- trustworthy numbers
- clean modeling
- explainable metrics
- operational usefulness
- clarity over clutter
- durable architecture over shortcuts

SellerIQ should avoid:
- brittle spreadsheet logic
- dashboard-only calculations
- unclear or conflicting metrics
- overbuilt infrastructure too early
- fancy UI without reliable underlying data

The product should feel calm, premium, analytical, and trustworthy.

---

## Product Maturity Path

SellerIQ will likely mature in phases.

### Phase 1 — Internal Reporting Foundation
- ingest Amazon data
- replace spreadsheet-heavy workflows
- create reliable core metrics
- serve dashboards internally

### Phase 2 — Cross-Channel Analytics
- add Walmart data
- unify sales and inventory views
- improve product identity mapping
- support multi-platform analysis

### Phase 3 — Profitability and Ad Intelligence
- add fee logic
- add advertising data
- build profit-aware views
- support campaign analysis

### Phase 4 — AI Assistant Layer
- expose curated warehouse data to an AI assistant
- support natural language business analysis
- provide answerability across multiple data domains

### Phase 5 — Productized SaaS
- harden infrastructure
- improve UX
- support account onboarding
- package the platform for external users

---

## What “Good” Looks Like

SellerIQ is successful when a user can:
- trust the numbers
- understand what changed
- investigate root causes quickly
- answer business questions without digging through five systems
- take action with confidence

SellerIQ should reduce:
- spreadsheet cleanup
- manual report parsing
- dashboard confusion
- dependence on memory or intuition alone

SellerIQ should increase:
- speed of insight
- confidence in decisions
- operational awareness
- leverage for small ecommerce teams

---

## Scope Boundaries for Now

Current near-term scope:
- Amazon SP-API ingestion
- Walmart marketplace reporting
- raw report storage
- normalized Postgres warehouse
- dashboards built on curated marts
- future AI assistant access to trusted data

Near-term non-goals:
- real-time operational syncing
- marketplace listing management
- pricing automation
- bid automation
- order fulfillment tools
- customer messaging workflows

These may become future opportunities, but they are not the current priority.

---

## Role of Claude

Claude should use this file to understand:
- what SellerIQ is trying to become
- who it is for
- what product decisions matter most
- what problems the platform is meant to solve

When making recommendations, Claude should prefer:
- trustworthy analytics design
- operator-focused workflows
- scalable but practical product decisions
- clarity and usefulness over feature sprawl