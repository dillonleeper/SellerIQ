# SellerIQ Tech Stack

This file defines the current and intended technical stack for SellerIQ.

The purpose is to help Claude make recommendations that fit the actual project direction instead of introducing tools or architectures that conflict with existing decisions.

This document reflects the preferred stack unless there is a strong reason to recommend otherwise.

---

## Core Stack Philosophy

SellerIQ should use a practical, scalable, and understandable stack.

The stack should prioritize:
- reliability
- clarity
- maintainability
- good support for analytics workflows
- easy iteration for a solo or small team
- compatibility with future productization

Avoid stack choices that are overly enterprise-heavy too early unless they solve a real bottleneck.

---

## Current / Preferred Stack

### Ingestion Layer
Preferred language:
- Python

Reason:
- strong ecosystem for APIs, parsing, and data workflows
- already used in existing scripts
- good fit for SP-API wrappers and batch jobs
- good fit for raw file parsing and transformation helpers

Preferred ingestion libraries:
- `python-amazon-sp-api` for Amazon SP-API
- official APIs and SDKs where practical
- separate ads API libraries when needed

Python should be used for:
- report request jobs
- polling jobs
- downloads
- decompression
- parsing raw files
- loading staging tables
- ingestion logging

---

## Raw Storage

Preferred raw storage:
- Amazon S3

Reason:
- cheap
- durable
- scalable
- appropriate for immutable raw file storage
- supports reprocessing and auditability

S3 is the canonical raw layer.

Google Sheets is not the raw layer.

---

## Database / Warehouse

Preferred warehouse:
- PostgreSQL

Preferred hosted option:
- Supabase Postgres

Reason:
- relational model fits the warehouse design
- supports SQL modeling well
- easy to host and manage
- works well for staging, intermediate, and mart layers
- compatible with dashboards, backend services, and future app development

Postgres should be used for:
- staging tables
- intermediate models
- fact tables
- dimension tables
- ingestion logs
- metadata tables

---

## Transformation Layer

Preferred transformation approach:
- SQL-based transformations inside Postgres
- dbt-style layered modeling principles even if dbt is not used immediately

Preferred modeling layers:
- raw
- stg
- int
- fct
- dim

Transformation logic should live in:
- SQL models
- warehouse logic
- repeatable pipeline code

Transformation logic should NOT live primarily in:
- dashboards
- ad hoc spreadsheets
- one-off scripts with hidden business logic

---

## App Layer

Preferred application stack:
- Node.js
- TypeScript

Reason:
- good for building a production app layer
- strong ecosystem for APIs and web apps
- fits well with modern SaaS architecture
- aligns with future internal tools and user-facing product plans

Node/TypeScript should be used for:
- application backend
- internal APIs
- user-facing features
- AI assistant integration layer
- future authentication and account management

---

## Frontend / UI

Current visualization tool:
- Looker Studio (temporary / operational)

Future preferred UI:
- custom frontend

Likely frontend stack:
- React
- TypeScript
- modern component-based UI system

Reason:
- more control
- better product experience
- more flexibility for dashboards + AI + workflows
- better alignment with a polished SaaS experience

Looker Studio is acceptable for early internal analytics, but it is not the long-term product UI.

---

## AI Layer

Preferred AI usage:
- AI should sit on top of curated data, not raw API output

Use cases:
- natural language questions
- root-cause analysis
- summaries
- trend explanations
- anomaly investigation
- decision support

AI should query:
- curated marts
- trusted fact tables
- dimension tables
- pre-modeled business logic

AI should NOT depend on:
- raw source files directly
- dashboard-only calculations
- brittle spreadsheet formulas

---

## Scheduling / Orchestration

Initial approach:
- simple scheduled jobs / cron / lightweight orchestration

Reason:
- enough for current pipeline maturity
- avoids overengineering
- fits early-stage product development

Possible future evolution:
- workflow orchestrator
- more formal job queue
- retries and alerts
- DAG-based pipeline scheduling

Do not assume Airflow/Spark/etc. are required at the current stage unless the system genuinely outgrows simpler scheduling.

---

## Logging / Observability

Preferred approach:
- ingestion metadata and job logs stored in Postgres
- clear error handling in Python jobs
- validation checks on row counts, duplicates, freshness, and schema expectations

Examples:
- log_report_request
- log_file_download
- log_ingestion_job
- log_load_validation

The system should be built to support debugging and reprocessing.

---

## Data Serving Layer

Short-term serving options:
- Looker Studio connected to modeled tables or curated exports
- internal SQL queries
- temporary lightweight views

Long-term serving options:
- custom app UI
- internal APIs backed by Postgres
- AI assistant over curated marts

Dashboards and app features should consume curated warehouse data, not raw staging tables.

---

## What Not to Introduce Prematurely

Avoid recommending the following too early unless there is a real need:
- Spark
- Kafka
- Snowflake
- Redshift
- ClickHouse
- event streaming infrastructure
- microservices split across many repos
- complex workflow orchestration platforms
- live dashboard querying against source APIs

These may become useful later, but they are not the default recommendation for the current stage of SellerIQ.

---

## Current Practical Stack Summary

Preferred end-to-end shape:

Amazon SP-API / Walmart API / Ads APIs
→ Python ingestion
→ raw files in S3
→ Postgres staging/intermediate/marts
→ Looker Studio or custom UI
→ AI assistant over curated warehouse data

This is the current default recommendation.

---

## When Claude Should Suggest Alternatives

Claude may recommend alternatives only when:
- there is a clear scaling bottleneck
- a tool meaningfully reduces complexity
- the current stack cannot support a real product need
- the recommendation still respects the layered warehouse design

Alternatives should be justified, not casually introduced.

---

## Role of Claude

Claude should use this document when:
- recommending architecture
- suggesting tools or frameworks
- proposing new services
- writing implementation plans
- deciding where logic should live

Claude should optimize for:
- coherence with the current stack
- practical implementation
- long-term maintainability
- clean analytics architecture