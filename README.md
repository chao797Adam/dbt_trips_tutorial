# dbt Trips Case

A layered ride-hailing (trips) data modeling project built with [dbt](https://www.getdbt.com/) on Databricks. Raw data is loaded into the `source` schema via PySpark Structured Streaming, and dbt owns the entire transformation pipeline from there — Bronze → Silver → Gold — including SCD1 deduplication and SCD2 historical tracking via snapshots.

This project intentionally separates concerns differently from the reference tutorial: **PySpark is responsible only for landing raw data**, while **dbt owns all transformation layers (bronze/silver/gold)**. See [Notes & Deviations](#-notes--deviations-from-the-reference-tutorial) below for details and rationale.

---

## 📐 Data Layer Architecture

| Layer | Directory | Schema | Materialization | Description |
|-------|-----------|--------|------------------|-------------|
| Source (raw landing) | PySpark streaming | `source` | Delta table | Raw CSV files loaded via Spark Structured Streaming, untouched/unstandardized |
| Bronze | `models/staging` | `bronze` | `incremental` + `merge` | Standardized, incrementally loaded from `source`, CDC-filtered via `last_updated_timestamp` |
| Silver | `models/intermediate` | `silver` | `incremental` + `merge` | Deduplicated (SCD1), cleaned, and business-transformed |
| Gold | `models/mart` | `gold` | `table` | Analytics-ready fact/dimension model (in progress) |
| Snapshots | `snapshots/` | `snapshots` | `check` strategy | SCD2 historical tracking for select fact/dimension entities |

```
PySpark streaming → source (raw landing)
                          ↓
                    dbt staging (bronze) — incremental + merge, CDC filter
                          ↓
                    dbt intermediate (silver) — dedup (SCD1) + transform
                          ↓
                    dbt mart (gold) — analytics layer
                          ↓
              (in parallel) snapshots — SCD2 history for select entities
```

---

## 🗄️ Data Model

This project models a ride-hailing domain with **two fact tables** and **four dimension tables**:

| Table | Type | Description |
|-------|------|--------------|
| `trips` | **Fact** | One row per ride: pickup/dropoff, distance, fare, status |
| `payments` | **Fact** | One row per payment transaction, linked to a trip |
| `customers` | Dimension | Riders |
| `drivers` | Dimension | Drivers |
| `vehicles` | Dimension | Vehicles used for trips |
| `locations` | Dimension | Pickup/dropoff location attributes |

---

## 📁 Project Structure

```
dbt_trips_case/
├── dataset/                      # Sample source CSV files
├── dataset_incre_load/           # Incremental test data (for CDC/SCD validation)
├── notebooks/                    # PySpark streaming load notebooks (source landing only)
├── dbt_trips_case/
│   ├── models/
│   │   ├── staging/              # Bronze layer
│   │   │   ├── source.yml
│   │   │   ├── stg_customers.sql
│   │   │   ├── stg_drivers.sql
│   │   │   ├── stg_locations.sql
│   │   │   ├── stg_payments.sql
│   │   │   ├── stg_trips.sql
│   │   │   └── stg_vehicles.sql
│   │   ├── intermediate/         # Silver layer
│   │   │   ├── silver_customers.sql
│   │   │   ├── silver_drivers.sql
│   │   │   ├── silver_locations.sql
│   │   │   ├── silver_payments.sql
│   │   │   ├── silver_trips.sql
│   │   │   └── silver_vehicles.sql
│   │   └── mart/                  # Gold layer (in progress)
│   ├── snapshots/                  # SCD2 historical tracking
│   │   ├── trips_snapshot.sql           # fact — no dim_ prefix
│   │   ├── payments_snapshot.sql        # fact — no dim_ prefix
│   │   ├── dim_customers_snapshot.sql
│   │   ├── dim_drivers_snapshot.sql
│   │   ├── dim_vehicles_snapshot.sql
│   │   └── dim_locations_snapshot.sql
│   └── dbt_project.yml
```

---

## 🧱 Models

### Staging (Bronze)

All 6 staging models follow the same pattern: incremental load filtered by `last_updated_timestamp`, with a `coalesce` fallback to handle the empty-table edge case.

```sql
{{
    config(
        materialized='incremental',
        unique_key='customer_id',
        incremental_strategy='merge'
    )
}}

select *, current_timestamp() as ingested_at
from {{ source('trips_source', 'customers') }}

{% if is_incremental() %}
    where last_updated_timestamp > (
        select coalesce(max(last_updated_timestamp), '1900-01-01') from {{ this }}
    )
{% endif %}
```

The `coalesce(..., '1900-01-01')` fallback prevents the filter from silently returning zero rows when the target table is empty (since `NULL` comparisons in SQL always evaluate to false).

### Intermediate (Silver)

All 6 silver models follow a consistent pattern, regardless of whether the underlying table is a fact or a dimension:

```
incremental filter (CDC) → row_number() dedup (SCD1) → business transformation → merge
```

This is a deliberate design decision: **`row_number()` deduplication is a defensive measure to guarantee `merge` never receives duplicate `unique_key` values within a single batch** (a hard requirement of Delta Lake's `MERGE INTO`), not a statement about whether a table is conceptually a fact or dimension. Whether the underlying entity is fact-like (`trips`) or dimension-like (`customers`), the same defensive pattern applies.

Example (`silver_customers.sql`):
```sql
{{
    config(
        materialized='incremental',
        unique_key='customer_id',
        incremental_strategy='merge'
    )
}}

with
    base_customers as (
        select * from {{ ref('stg_customers') }}
        {% if is_incremental() %}
            where last_updated_timestamp > (
                select coalesce(max(last_updated_timestamp), '1900-01-01') from {{ this }}
            )
        {% endif %}
    ),
    deduplicated as (
        select * from (
            select *, row_number() over (
                partition by customer_id order by last_updated_timestamp desc
            ) as rn
            from base_customers
        )
        where rn = 1
    ),
    transformed as (
        select
            *,
            split_part(email, '@', 2) as domain,
            regexp_replace(phone_number, '[^0-9]', '') as phone_number_clean,
            concat(first_name, ' ', last_name) as full_name
        from deduplicated
    )

select
    customer_id, full_name, email, domain,
    phone_number_clean as phone_number,
    city, signup_date, last_updated_timestamp, ingested_at
from transformed
```

### Mart (Gold)

In progress — will include a fact/dimension analytics layer joining `trips`, `payments`, and the four dimension tables.

---

## 📸 Snapshots (SCD2)

Snapshots are kept **independent of the bronze/silver/gold layers** and read from staging (`stg_*`) models — i.e., data close to the original source — rather than from the gold layer. This keeps the historical record clean: each snapshot tracks changes to a single entity, rather than mixing in changes introduced by joins in a downstream wide table.

| Snapshot | Source | Tracks Changes In |
|----------|--------|---------------------|
| `trips_snapshot` | `stg_trips` | `trip_status`, `trip_end_time`, `distance_km`, `fare_amount`, `payment_method` — fields that get filled in / updated as a trip progresses from `ongoing` to `completed` |
| `payments_snapshot` | `stg_payments` | `payment_status`, `payment_method`, `amount` |
| `dim_customers_snapshot` | `stg_customers` | `city`, `phone_number` |
| `dim_drivers_snapshot` | `stg_drivers` | `first_name`,`last_name`, `phone_number` |
| `dim_vehicles_snapshot` | `stg_vehicles` | `vehicle_type`, `year`, `model`, `make` / attributes |
| `dim_locations_snapshot` | `stg_locations` | `city`, `state`, `country` (rarely changes, but tracked defensively in case of data corrections) |

Naming convention: dimension-type entities are prefixed `dim_`; fact-type entities (`trips`, `payments`) are not, consistent with standard Kimball-style naming.

Project-level snapshot defaults (in `dbt_project.yml`):
```yaml
snapshots:
  dbt_trips_case:
    +target_schema: snapshots
    +strategy: check
```

---

## ⚠️ Notes & Deviations from the Reference Tutorial

This project follows a ride-hailing dbt tutorial on Databricks, with several deliberate deviations identified and corrected during implementation:

### 1. ETL boundary: PySpark vs. dbt

In the reference tutorial, **PySpark handles both bronze and silver layers** (including deduplication via `dropDuplicates` and upserts via `DeltaTable.merge()`), with dbt only used for the gold layer.

In this project, **PySpark is restricted to landing raw source data only** (`source` schema). All transformation logic — bronze, silver, and gold — is implemented in dbt. This better reflects the typical Analytics Engineer (AE) vs. Data Engineer (DE) split: DE owns ingestion/infrastructure (streaming, checkpoints), AE owns transformation logic in SQL/dbt, without needing to write Spark/Python class-based pipelines.

The PySpark `upsert()` method (using `DeltaTable.merge().whenMatchedUpdateAll().whenNotMatchedInsertAll()`) is functionally equivalent to dbt's `incremental_strategy='merge'` — both compile down to the same underlying Delta Lake `MERGE INTO` operation. The difference is developer ergonomics, not runtime performance: dbt replaces a multi-line Python class with a single `config()` block.

### 2. Checkpoint location

The reference tutorial's streaming write places `checkpointLocation` under the `bronze` Volume path, despite checkpoints being purely a property of the streaming *read* from `source`, not the *bronze* output layer. In this project, checkpoints live under `/Volumes/pysparkdbt/source/checkpoint/{entity}`, alongside the raw source files they correspond to, keeping the layer boundary (source vs. bronze) unambiguous.

### 3. Snapshots strategy

The reference tutorial used `strategy='timestamp'`. However, this project uses `strategy='check'` for all snapshots instead of relying on a timestamp-based strategy, since it directly compares column values rather than trusting a potentially stale timestamp field.

### 4. `materialized='incremental'` for the silver layer

The reference tutorial materializes silver models as `incremental` (full rebuild on every run). This project uses `incremental` + `merge` for silver models instead, which only reprocesses new/changed rows — a meaningfully different performance profile at scale, even though the difference is not observable at this project's small data volume (~1,000 rows per entity).

### 5. Snapshots materialized into the gold schema, defined via YAML instead of SQL

The reference tutorial defines snapshots declaratively in a `snapshots/SCDs.yml` file (dbt's newer YAML-based snapshot syntax). Each snapshot's `relation` reads from a **silver-schema** source (e.g. `source('source_silver', 'payments')`), but the resulting snapshot table is materialized into the **gold** schema (`config: schema: gold`), using `strategy: timestamp` with `updated_at: last_updated_timestamp`.

This project instead defines snapshots using the traditional `{% snapshot %}` SQL block syntax, both reading from and materializing alongside staging-level data, with the snapshot output kept in its own dedicated `snapshots` schema rather than `gold`. Two separate deviations are bundled here:

- **Format**: SQL-block snapshots (this project) vs. YAML-based snapshots (reference). Both are valid, supported dbt syntaxes — this is a stylistic choice, not a correctness issue. YAML snapshots are dbt's more recent recommended format and reduce boilerplate when many snapshots share the same shape, but the SQL-block format makes the underlying `select` statement and any inline transformation more explicit and easier to read for a small number of snapshots.
- **Output location**: gold schema (reference) vs. a dedicated `snapshots` schema (this project). Materializing SCD2 history directly into the gold layer blurs the boundary between "current-state analytics tables" and "historical change-tracking tables" — a BI tool browsing the gold schema would see snapshot tables mixed in with regular fact/dimension tables. This project keeps snapshots in their own schema, sourced from staging (`stg_*`) models, so that gold remains a clean, current-state-only analytics layer, and snapshot history is clearly demarcated as a separate concern.

### 6. Stylistic: Jinja used for field-list generation

The reference tutorial uses a Jinja `{% for col in cols %}` loop to generate a `select` field list. While syntactically valid, this adds a layer of indirection without reducing actual maintenance burden for a single, non-reused field list — readers must mentally "execute" the loop to know which columns are selected. This project favors explicit `select` statements for single-use field lists, reserving macros/loops for logic that is genuinely reused across multiple models.

---

## 🚀 Getting Started

### 1. Environment

```bash
# Activate the shared dbt environment (adapters pre-installed: dbt-databricks, dbt-snowflake)
F:\git_upload\dbt_master_env\Scripts\Activate.ps1
```

### 2. Configure Profile

Add a profile to `~/.dbt/profiles.yml`:

```yaml
dbt_trips_case:
  target: dev
  outputs:
    dev:
      type: databricks
      catalog: pysparkdbt
      host: <your-databricks-host>
      http_path: <your-warehouse-http-path>
      schema: default
      threads: 4
      token: <your-databricks-token>
```

### 3. Run

```bash
cd dbt_trips_case
dbt debug          # verify connection
dbt run            # run staging + intermediate (+ mart, once built)
dbt snapshot        # capture SCD2 history
dbt test            # run data tests
```

### 4. Source Data Loading

Raw CSV files are loaded into the `pysparkdbt.source` schema via PySpark Structured Streaming (see `notebooks/`), landing into Delta tables that dbt's `source.yml` references directly. This load step is independent of dbt and must be run first.

---

## 📚 References

- [dbt Documentation](https://docs.getdbt.com/docs/introduction)
- [dbt-databricks Adapter Setup](https://docs.getdbt.com/reference/warehouse-setups/databricks-setup)
- [dbt Snapshots](https://docs.getdbt.com/docs/build/snapshots)
