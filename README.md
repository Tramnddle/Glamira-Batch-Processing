# Glamira Batch Processing – dbt Analytics Project

## Overview
This project implements a scalable batch analytics pipeline for an international e-commerce platform (Glamira), processing 40M+ customer and transaction records using **BigQuery** and **dbt**.

The goal is to transform raw, semi-structured source data into clean, business-ready fact and dimension tables for analytics and BI consumption.

---

## Architecture

![Glamira Batch Analytics Architecture](images/Grand Architecture.svg)

## Key Characteristics

- One-to-one mapping with source systems (e-commerce platform, tracking tools, CRM exports)
- No business logic applied
- High data volume (40M+ records)
- Semi-structured and nested fields preserved (JSON, STRUCT, ARRAY)

### Raw Layer
- Source-aligned ingestion
- Minimal transformation
- Preserves original schemas and nested structures
- Acts as a single source of truth

### Staging Layer
- Data quality checks and normalization
- Deduplication and record selection
- Structural flattening
- Technical (not business) transformations

### Snapshot Layer
- Tracks historical changes using SCD Type 2
- Preserves customer email history over time

### Mart Layer
- Business-ready dimensions and fact tables
- Stable surrogate keys
- Optimized for BI tools and analytics use cases

---

## Key Design Decisions

- **Email address as primary customer identifier**, not volatile user IDs
- **SCD Type 2 snapshots** to preserve historical customer data
- **Surrogate keys** generated via hashing (`FARM_FINGERPRINT`)
- Lossless joins to preserve maximum data granularity
- Clear separation between technical and business logic

---

## Main Models

### Dimensions
- `mart_dim_customer`
- `mart_dim_product`
- `mart_dim_location`

### Facts
- `mart_fact_order`

---

## Technologies Used
- BigQuery
- dbt
- SQL
- Google Cloud Platform (GCS, BigQuery)

---

## Intended Use
- BI dashboards (Looker Studio)
- Customer analytics
- Sales and product performance reporting
- Data science and downstream modeling

| Layer    | SQL File                 | Responsibility                                         |
| -------- | ------------------------ | ------------------------------------------------------ |
| Raw      | `raw.*`                  | Source-aligned ingestion, no transformation            |
| Staging  | `stg_customer.sql`       | Email normalization, user ID validation, deduplication |
| Staging  | `stg_order.sql`          | Order deduplication, timestamp normalization           |
| Staging  | `stg_product.sql`        | Flatten nested product options, clean attributes       |
| Snapshot | `customer_email_scd.sql` | Track historical email changes (SCD Type 2)            |
| Mart     | `mart_dim_customer.sql`  | Latest email selection, surrogate key generation       |
| Mart     | `mart_dim_product.sql`   | Business-ready product attributes                      |
| Mart     | `mart_dim_location.sql`  | Standardized location dimension                        |
| Mart     | `mart_fact_order.sql`    | Transactional fact table with normalized metrics       |
