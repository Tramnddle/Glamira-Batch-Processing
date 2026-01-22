<!-- Auto-generated guidance for AI coding agents. Keep concise and actionable. -->

# Copilot instructions — dbt BigQuery staging models

This repository contains dbt staging models targeting BigQuery. The guidance below highlights project-specific patterns, naming conventions, and safe edit practices so an AI agent is immediately productive.

- **Big picture**: models under `models/staging` are lightweight, deterministic staging views that normalize JSON/raw fields from a BigQuery `raw` dataset (see [models/staging/_sources.yml](models/staging/_sources.yml)). Keep transformations here minimal and deterministic; downstream models (not shown) expect stable keys and column names.

- **Key files to inspect**: [models/staging/stg_product.sql](models/staging/stg_product.sql), [models/staging/stg_order.sql](models/staging/stg_order.sql), [models/staging/stg_customer.sql](models/staging/stg_customer.sql), [models/staging/stg_location.sql](models/staging/stg_location.sql), and [models/staging/schema.yaml](models/staging/schema.yaml).

- **SQL target & functions**: SQL uses BigQuery functions (`to_json_string`, `json_value`, `farm_fingerprint`, `timestamp_seconds`, `SAFE.PARSE_NUMERIC`, `md5`, `to_hex`). When editing SQL, preserve BigQuery-specific functions and casting idioms.

- **Naming & key conventions**:
  - Staging models use `stg_` prefix.
  - Deterministic surrogate keys are produced with `abs(farm_fingerprint(lower(trim(...))))::INT64` (example: `product_key` in [stg_product.sql](models/staging/stg_product.sql)). Prefer preserving this pattern when adding keys.
  - Composite item keys use `to_hex(md5(concat(...)))` (see `item_key` in [stg_order.sql](models/staging/stg_order.sql)).

- **Common parsing patterns to follow**:
  - JSON extraction: wrap raw JSON in `to_json_string(...)` then use `json_value` for specific paths (see [stg_product.sql](models/staging/stg_product.sql)).
  - Nonstandard numerics: price parsing has explicit replacements for localized separators (`'٫'`, `','`, `'.'`) and uses `SAFE.PARSE_NUMERIC` — preserve this logic when handling currency fields (see [stg_order.sql](models/staging/stg_order.sql)).
  - Null/empty normalization for identifiers: code often trims and coalesces string IDs before casting (see `user_id_db` handling in [stg_customer.sql](models/staging/stg_customer.sql)).

- **Materialization policy**: staging models are materialized as views: `{{ config(materialized='view') }}`. Do not change materialization without verifying downstream expectations and team conventions.

- **Schema & tests**: model tests are declared in `models/staging/schema.yaml`. The project uses `dbt_utils` tests (e.g., `unique_combination_of_columns`). When editing tests, prefer relationship tests that reference `ref('stg_*')` and `source('raw', '...')` exactly as used in current YAML.

- **Safe edit checklist for SQL/YAML changes**:
  1. Preserve BigQuery-specific functions and casting.
  2. Keep deterministic key expressions (fingerprint/md5) unless explicitly refactoring keys across the project.
  3. When changing column names, update `schema.yaml` tests and any `ref(...)` usages.
  4. Avoid modifying `_sources.yml` database/schema names unless you have confirmed credentials and intentions.

- **Typical developer commands (verify profiles locally)**:
  - `dbt run --models stg_*` — build staging models
  - `dbt test --models stg_*` — run schema/tests for staging
  - `dbt run --models +stg_*` — include dependents if required

- **Examples of patterns to reuse**:
  - Deterministic key: `CAST(abs(farm_fingerprint(lower(trim(email_address)))) AS int64) AS customer_key` ([stg_customer.sql](models/staging/stg_customer.sql)).
  - JSON extraction + safe casting: `json_value(product_json, '$.product.gold_weight')` then `SAFE_CAST(... AS NUMERIC)` ([stg_product.sql](models/staging/stg_product.sql)).

- **What to avoid**:
  - Introducing non-deterministic keys (random functions) for surrogate keys.
  - Removing the price normalization logic — it handles localized formats critical for reliable numeric parsing.

If a change touches sources, keys, or materializations, note that downstream models and tests depend on existing names and formats. Ask for confirmation before broad refactors.

---
If anything in these instructions is unclear or you'd like additional examples (e.g., a checklist for adding a new staging model), tell me what to expand. I'll iterate on this file.
