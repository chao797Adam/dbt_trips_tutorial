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
│   │   └── mart/                  # Gold layer 
│   │       ├── gold_trips_summary.sql
│   │       ├── gold_customer_segments.sql
│   │       └── gold_daily_driver_performance.sql
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

All 6 staging models follow the same pattern: incremental filter (CDC) by `last_updated_timestamp`, with a `coalesce` fallback to handle the empty-table edge case, followed by `row_number()` deduplication before the `merge`.

```sql
{{
    config(
        materialized='incremental',
        unique_key='customer_id',
        incremental_strategy='merge'
    )
}}

with
    raw_data as (
        select *, current_timestamp() as ingested_at
        from {{ source('trips_source', 'customers') }}
        {% if is_incremental() %}
            where
                last_updated_timestamp >= (
                    select coalesce(max(last_updated_timestamp), '1900-01-01')
                    from {{ this }}
                )
        {% endif %}
    ),

    deduplicated as (
        select *
        from (
            select
                *,
                row_number() over (
                    partition by customer_id
                    order by last_updated_timestamp desc, ingested_at desc
                ) as rn
            from raw_data
        )
        where rn = 1
    )

select * except (rn)
from deduplicated
```

The `coalesce(..., '1900-01-01')` fallback prevents the filter from silently returning zero rows when the target table is empty (since `NULL` comparisons in SQL always evaluate to false).

The filter uses `>=`, not `>`, so records from the latest already-loaded timestamp are reprocessed on every run — this is safe because `merge` on the unique key is idempotent, and it prevents late-arriving records sharing that exact timestamp from being silently skipped.

`row_number()` deduplication here is required for the same reason it's required in silver (see below): `merge` requires the incoming batch to contain no duplicate `unique_key` values, and the raw `source` table can legitimately contain multiple rows for the same `customer_id` (e.g. successive updates from the streaming write), so this step guarantees the batch handed to `merge` is always clean.

### Intermediate (Silver)

All 6 silver models follow a consistent pattern, regardless of whether the underlying table is a fact or a dimension:

```
incremental filter (CDC) → row_number() dedup (SCD1) → business transformation → merge
```

This is a deliberate design decision: **`row_number()` deduplication is a defensive measure to guarantee `merge` never receives duplicate `unique_key` values within a single batch** (a hard requirement of Delta Lake's `MERGE INTO`), not a statement about whether a table is conceptually a fact or dimension. Whether the underlying entity is fact-like (`trips`) or dimension-like (`customers`), the same defensive pattern applies.

**Why `row_number()` is needed again here, even though staging already deduplicated:**

`stg_customers` is a `merge` target, so at any single point in time, querying that table returns at most one row per `customer_id` — `merge` guarantees this. But silver's incremental filter doesn't read staging at a single point in time; it reads a **time range**:

```sql
where last_updated_timestamp > (select max(last_updated_timestamp) from {{ this }})
```

This range can span **multiple separate staging `merge` runs**. If `customer_id = 101` was updated at 09:00 (staging run #1, merged cleanly) and again at 09:03 (staging run #2, merged cleanly — overwriting the 09:00 version), staging itself never had duplicates at any instant. But if silver's next run reads everything from, say, 08:55 onward, both the 09:00 and the 09:03 versions fall inside that window and both get pulled into the same result set — because they were two different rows in `stg_customers`'s own change history, not two rows that ever coexisted in a single snapshot of the table.

In short: staging guarantees "no duplicates **at any instant**." Silver's incremental read is a **range query across instants**, which can reassemble duplicates that never existed together at any single moment. That's why silver needs its own `row_number()` — it can't inherit staging's guarantee, because the two layers are protecting against different things.

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
            where last_updated_timestamp >= (
                select coalesce(max(last_updated_timestamp), '1900-01-01') from {{ this }}
            )
        {% endif %}
    ),
    deduplicated as (
        select * from (
            select *, row_number() over (
                partition by customer_id order by last_updated_timestamp desc, ingested_at desc
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

The Gold layer provides analytics-ready tables for business consumption. Currently, it includes:

| Model | Description |
| :--- | :--- |
| `gold_trips_summary` | A comprehensive fact-wide table capturing trip details, joined with customer, driver, and vehicle dimensions. |
| `gold_customer_segments` | An analytical model segmenting customers into `Frequent`, `Regular`, and `Casual` tiers based on their lifetime trip frequency and spend. |
| `gold_daily_driver_performance` | Aggregated performance metrics for drivers, calculated on a daily basis (in progress). |

### ⚠️ Known Issues

#### Location Data Granularity Mismatch
* **Defect**: The `locations` dimension table cannot be joined with the `trips` fact table.
* **Root Cause**: A granularity mismatch exists between the datasets:
    * `trips` contains fine-grained, address-level location names (e.g., "North Robert").
    * `locations` contains only high-level administrative city names (e.g., "Lake Davidport").
* **Impact**: Direct joins on `city` names result in total data loss (all-NULL values) for geographic attributes in the `gold_trips_summary` model.
* **Required Resolution**: Implementation of an intermediate Mapping/Bridge table to translate fine-grained locations to administrative city IDs.

---

## 📸 Snapshots (SCD2)

**Snapshots don't retrieve history that silver "lost" — they actively create it.** Silver is a `merge` target, so it only ever holds the *current* state: once `customer_id = 101`'s phone number changes from A to B and silver's merge runs, the row with A is overwritten and gone from silver forever. There is no way to query silver and get A back.

`dbt snapshot` works completely differently. On every run, it takes a **photograph** of silver's current state and compares it to the *previous* photograph already stored in the snapshot table. If something changed, it closes out the old version (stamps `dbt_valid_to`) and inserts a new row for the new version — it never overwrites or deletes. That's why the snapshot table accumulates history (both A and B end up as separate rows there) even though the silver table it reads from has already discarded A.

This also means snapshot history is only as complete as how often it's run: if `customer_id = 101` changes twice between two snapshot runs, only the state at the second run's photograph is captured — the intermediate change is invisible to the snapshot, because it never took a picture of it.

Snapshots read from the **silver layer**, not staging or gold. This is a deliberate choice between three options:

- **Not staging (`stg_*`)**: staging is renamed and timestamped raw data, but not yet deduplicated or cleaned (e.g. `trim()`ed). Snapshots are append-only — once a version is recorded, it's part of the permanent history. If staging data still has formatting noise, `strategy='check'` would misread that noise as a real business change, and that bad version gets baked into history permanently, with no cheap way to undo it later.
- **Not gold**: gold tables are joined and denormalized. A snapshot on gold would attribute changes introduced by a join (e.g. an unrelated dimension's row disappearing) to the tracked entity itself, mixing "this entity changed" with "something upstream in the join changed."
- **Silver** is the layer where deduplication (SCD1) and cleaning are already complete, so a change detected there reflects a genuine change in the underlying entity — not a source-timestamp artifact, and not a join side-effect.

| Snapshot | Source | Tracks Changes In |
|----------|--------|---------------------|
| `trips_snapshot` | `silver_trips` | `trip_status`, `trip_end_time`, `distance_km`, `fare_amount`, `payment_method` — fields that get filled in / updated as a trip progresses from `ongoing` to `completed` |
| `payments_snapshot` | `silver_payments` | `payment_status`, `payment_method`, `amount` |
| `dim_customers_snapshot` | `silver_customers` | `city`, `phone_number`, `full_name` |
| `dim_drivers_snapshot` | `silver_drivers` | `phone_number`, `full_name` |
| `dim_vehicles_snapshot` | `silver_vehicles` | `vehicle_type`, `year`, `model`, `make` |
| `dim_locations_snapshot` | `silver_locations` | `city`, `state`, `country` (rarely changes, but tracked defensively in case of data corrections) |

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

```python
entities = ['customers', 'payments', 'locations', 'trips', 'vehicles', 'drivers']

for entity in entities:
    # CSV streaming sources require an explicit schema (inferSchema isn't
    # supported in streaming mode), so read one batch first just to capture it
    df_batch = spark.read.format('csv') \
        .option('header', True) \
        .option('inferSchema', True) \
        .load(f"/Volumes/pysparkdbt/source/source_data/{entity}/")
    schema_entity = df_batch.schema

    df = spark.readStream.format("csv") \
        .option('header', True) \
        .schema(schema_entity) \
        .load(f"/Volumes/pysparkdbt/source/source_data/{entity}")

    df.writeStream.format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", f"/Volumes/pysparkdbt/source/checkpoint/{entity}") \
        .trigger(once=True) \
        .toTable(f"pysparkdbt.source.{entity}")
```

Note: `outputMode("append")` here means each streaming run only *adds* new rows to the `source` table — it never updates a row already landed. Combined with `trigger(once=True)` (process everything currently available, then stop, rather than running continuously), this is the reason `source.customers` can legitimately contain multiple rows for the same `customer_id` over time, which is exactly the condition that `row_number()` deduplication in staging (see below) is defending against.

**Idempotency, and its blind spot.** Re-running this script is idempotent in the sense that matters for streaming: `checkpointLocation` tracks which files have already been read, so re-running against the same set of files does not re-process or duplicate them — only newly-added files get picked up. This idempotency is file-based, not content-based: if a source file is overwritten in place with corrected data *after* it has already been processed, the checkpoint has no way to detect that and will not re-read it, since it only tracks which file paths/offsets have been consumed, not their content. This is a structural counterpart to the watermark blind spot documented for dbt's incremental models further down — both the ingestion layer and the transformation layer have a boundary condition where a silent correction at the source can fail to propagate downstream.

| | dbt's watermark blind spot (downstream) | PySpark checkpoint's blind spot (ingestion) |
|---|---|---|
| What marks "already processed" | `last_updated_timestamp` (a value on the row) | File path / byte offset (a property of the file itself) |
| What triggers the blind spot | Source data is corrected, but `last_updated_timestamp` isn't bumped | Source file is overwritten in place with corrected data, but the file path/name doesn't change |
| Root cause in common | Both track *whether something was already seen*, not *whether its content has changed* — so a marker that doesn't move means the correction is silently missed |
| Only occurs when | Upstream doesn't reliably update the timestamp on correction | Upstream overwrites files in place rather than emitting a new file per load |

### 🏗️ Architecture Comparison: Reference Tutorial vs. This Project

The reference tutorial and this project make different decisions about **where transformation logic lives** and **which tool owns which layer**. Both are valid architectures; the right choice depends on team structure, tooling standardization goals, and how much you want to centralize transformation logic in one place. This section lays out the trade-off explicitly.

#### Ownership by layer

| Layer | Reference tutorial | This project |
|---|---|---|
| Source → Bronze (raw landing) | PySpark Structured Streaming, `outputMode("append")`, no merge/dedup — writes directly into `bronze` | PySpark Structured Streaming — writes only into a dedicated `source` schema, untouched raw landing |
| Bronze → Silver | **PySpark**, using a Python class with `DeltaTable.merge()` (`whenMatchedUpdateAll().whenNotMatchedInsertAll()`) for upsert + dedup logic | **dbt** — `stg_*` (bronze-equivalent) and `silver_*` models, each using `incremental_strategy='merge'` + `row_number()` |
| Silver → Gold | **dbt** — gold models reading from silver | **dbt** — gold models reading from silver (same) |
| Historical tracking (SCD2) | dbt snapshot, defined in YAML, materialized into the `gold` schema | dbt snapshot, defined in SQL blocks, materialized into a dedicated `snapshots` schema, sourced from silver |

#### The core architectural difference

```
Reference tutorial:
  PySpark (streaming)  →  PySpark (Python class, merge/dedup)  →  dbt (gold only)
  [   source→bronze   ]  [        bronze→silver              ]  [ silver→gold ]

This project:
  PySpark (streaming)  →  dbt (staging: merge/dedup)  →  dbt (silver: merge/dedup)  →  dbt (gold)
  [   source only      ]  [        bronze-equivalent  ]  [        transform          ]  [        ]
```

In the reference tutorial, **two different tools own transformation logic**: PySpark owns bronze→silver (imperative, class-based, Python), and dbt owns silver→gold (declarative, SQL, `config()`-based). In this project, **dbt owns every transformation step**; PySpark's only job is landing raw files into Delta tables.

#### Trade-offs, stated plainly

| Dimension | Reference tutorial (PySpark owns bronze→silver) | This project (dbt owns everything past landing) |
|---|---|---|
| **Bronze table integrity** | `bronze` can legitimately contain duplicate keys (streaming append, no merge) — dedup happens once, in silver | `stg_*` guarantees uniqueness at ingestion via `merge` + `row_number()` — every downstream layer can trust it holds one row per key |
| **Where dedup logic lives** | Once, in a Python class (bronze→silver) | Twice — staging and silver each independently deduplicate, because each layer's incremental read spans a different window and can reassemble duplicates that never coexisted (see [Intermediate (Silver)](#intermediate-silver) for the full reasoning) |
| **Skill set required per layer** | Requires PySpark/Python proficiency for the bronze→silver step — a Data Engineer skillset | Requires only SQL/Jinja for every transformation step — an Analytics Engineer can own the entire pipeline past ingestion |
| **Testing, lineage, docs** | Split across two tools: PySpark logic isn't covered by dbt's `dbt test` / `dbt docs` / lineage graph; only silver→gold is | Unified: `dbt test`, `dbt docs generate`, and the lineage graph cover every transformation step from staging through gold |
| **Tooling to learn/maintain** | Two paradigms: imperative Spark DataFrame API + declarative dbt | One paradigm: declarative SQL/Jinja throughout |
| **Flexibility for complex logic** | PySpark's full programmatic surface (UDFs, complex branching, external API calls) is available at the bronze→silver step | Limited to what SQL/Jinja can express; anything requiring imperative logic would need a different tool (e.g. a Python model in dbt, or pushing it back into the ingestion step) |
| **Change history granularity** | Same limitation applies to both: neither preserves every intermediate value within a single incremental window, only what survives that window's dedup pass |

#### A solutions-architect framing

The reference tutorial reflects a **DE-owns-transformation-early** pattern: Data Engineers write the ingestion *and* the first transformation pass in PySpark, and Analytics Engineers pick up from silver onward in dbt. This can make sense when the bronze→silver step needs Python-specific capabilities (complex parsing, calling external services, non-SQL-expressible business rules) that dbt's SQL/Jinja can't easily express.

This project reflects a **DE-owns-ingestion-only** pattern: Data Engineers are responsible for getting raw data into Delta reliably (streaming, checkpoints, schema handling), and Analytics Engineers own 100% of the transformation logic — staging through gold — in a single tool, with a single testing/documentation/lineage story. This tends to reduce the number of tools and skillsets required to reason about "what does this pipeline actually do end to end," at the cost of losing PySpark's more expressive programmatic capabilities for any transformation that's awkward to write in SQL.

Neither pattern is universally "correct" — the reference tutorial's split is a legitimate choice when transformation logic genuinely needs Python; this project's consolidation is a legitimate choice when the transformation logic is expressible in SQL and the goal is to minimize tooling surface area and keep lineage/testing unified in one system.


### 3. Missing `incremental_strategy='merge'` in the silver layer — drawback

The reference tutorial's silver-layer config specifies `materialized: incremental` without an explicit `incremental_strategy`. On Databricks, dbt's default incremental strategy is `append` — meaning every incremental run **adds new rows on top of existing ones**, rather than overwriting rows that already exist for a given `unique_key`.

This has a concrete drawback: if a row that was already loaded gets updated at the source (e.g. a customer's `last_updated_timestamp` changes because their phone number was corrected), the next incremental run **appends a second row for the same `customer_id`** rather than overwriting the first. Without an additional deduplication step downstream, the silver table accumulates multiple rows per entity over time — defeating the purpose of an "incremental" load that's supposed to reflect current state.

This project explicitly sets `incremental_strategy='merge'` with a matching `unique_key` on every silver model, so that updates to an existing row correctly overwrite the prior version instead of accumulating duplicates. A `row_number()` deduplication step is also included as a defensive measure, since Delta Lake's `MERGE INTO` requires the incoming batch to contain no duplicate `unique_key` values.

This same `row_number()` + `merge` pattern is applied uniformly across staging as well, for the same reason: staging's `merge` target guarantees the *table* has no duplicates at any instant, but that guarantee doesn't automatically extend to what downstream layers read from it. Silver's incremental filter reads a **time range** from staging (`where last_updated_timestamp > last processed value`), and that range can span multiple separate staging `merge` runs. If a row was updated once during staging run #1 and again during staging run #2, staging itself never held duplicates at any single instant — but silver's next run can pull both versions into the same result set, because they were two different rows in staging's change history, not two rows that ever coexisted at once. In short: staging guarantees "no duplicates at any instant"; silver's read is a range query across instants, which can reassemble duplicates that never existed together at any single moment. That's why every layer — staging and silver alike — needs its own independent `row_number()` step; neither can inherit the other's guarantee.

### 4. Snapshot strategy: `check` vs. `timestamp`

The reference tutorial uses `strategy: timestamp` with `updated_at: last_updated_timestamp`. This project uses `strategy: check` instead — but it's worth being precise about why, since `last_updated_timestamp` (this project's only update-tracking field; there is no separate `updated_at` column) is a genuinely reliable, source-maintained field, not a write-once field like `created_at` (an issue identified separately in the companion Airbnb project).

Given a trustworthy `last_updated_timestamp`, `strategy: timestamp` is technically valid here — it is not "wrong" the way relying on `created_at` would be. The tradeoff is:

- **`strategy: timestamp`** (reference): cheaper to evaluate (compares a single timestamp), but implicitly assumes the source system *always* correctly updates `last_updated_timestamp` whenever a tracked field changes. If any code path in the source system updates a row's data without bumping this timestamp, the snapshot would silently miss that change.
- **`strategy: check`** (this project): compares the actual values of `check_cols` directly, with no dependency on any timestamp field being reliably maintained. This is more defensive — a change is only missed if the value genuinely didn't change — at the cost of comparing more columns per run.

This project favors `check` as the more conservative choice given an unverified assumption about upstream timestamp reliability, not because `timestamp` is inherently broken.

### 5. Snapshots materialized into the gold schema, defined via YAML instead of SQL

The reference tutorial defines snapshots declaratively in a `snapshots/SCDs.yml` file (dbt's newer YAML-based snapshot syntax). Each snapshot's `relation` reads from a **silver-schema** source (e.g. `source('source_silver', 'payments')`), but the resulting snapshot table is materialized into the **gold** schema (`config: schema: gold`).

This project instead defines snapshots using the traditional `{% snapshot %}` SQL block syntax, sourced from staging (`stg_*`) models, with the snapshot output kept in its own dedicated `snapshots` schema rather than `gold`. Two separate deviations are bundled here:

- **Format**: SQL-block snapshots (this project) vs. YAML-based snapshots (reference). Both are valid, supported dbt syntaxes — this is a stylistic choice, not a correctness issue. YAML snapshots are dbt's more recent recommended format and reduce boilerplate when many snapshots share the same shape, but the SQL-block format makes the underlying `select` statement and any inline transformation more explicit and easier to read for a small number of snapshots.
- **Output location**: gold schema (reference) vs. a dedicated `snapshots` schema (this project). Materializing SCD2 history directly into the gold layer blurs the boundary between "current-state analytics tables" and "historical change-tracking tables" — a BI tool browsing the gold schema would see snapshot tables mixed in with regular fact/dimension tables. This project keeps snapshots in their own schema, sourced from the **silver** layer rather than gold, so that gold remains a clean, current-state-only analytics layer, and snapshot history is clearly demarcated as a separate concern. Sourcing from silver rather than staging is a further refinement beyond the reference tutorial: silver has already been deduplicated and cleaned, so a snapshot built on it only records genuine entity-level changes, not source-formatting noise.

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
- [Tutorial / Reference Video](https://www.youtube.com/watch?v=cq7Uv7ctGjw&t=1569s)
