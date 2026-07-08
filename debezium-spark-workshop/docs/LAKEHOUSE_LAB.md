# Lakehouse ETL Lab — Medallion Architecture with Debezium + Spark + Delta

> **Self-study lab · Part 2 of the Debezium & Spark workshop**
> Build a Bronze → Silver → Gold pipeline on top of the CDC stream you
> already know from Tasks 1–3 — **one task per entity**.

In Part 1 you *processed* CDC events (dashboards, fraud, alerts) but never
**stored** anything. This lab lands the CDC stream into a **Delta Lake** and
refines it through the three medallion layers into business-ready tables.

You write the ETL logic yourself. Each task file under `spark/jobs/medallion/`
has the plumbing done and the interesting steps marked `# TODO`. A full
reference solution lives under `spark/jobs/solutions/` — use it to check your
work, not to copy from. Shared helpers are in `spark/jobs/medallion/lake_utils.py`.

---

## Design: one task per entity, fact vs dimension

The lab is split **per table (entity)** rather than one big job, because the
two kinds of table are handled differently:

| Entity | Kind | Bronze → Silver sync | Why |
|--------|------|----------------------|-----|
| `orders` | 🔵 **fact** | **Streaming** (foreachBatch + MERGE) | High volume, constantly changing → sync automatically & incrementally |
| `order_items` | 🔵 **fact** | **Streaming** (foreachBatch + MERGE) | Same as above |
| `products` | 🟢 **dimension** | **Batch** (dedup + MERGE) | Small, slowly-changing reference data → batch is simpler and enough |
| `users` | 🟢 **dimension** | **Batch** (dedup + MERGE) + **SCD2** | Reference data + we also keep full history |

> **Key idea:** the *transform* (dedup latest per key → data quality → MERGE)
> is identical for facts and dimensions. The only difference is **how it runs**:
> `spark.readStream` + `foreachBatch` for facts, plain `spark.read` for dims.

```
Kafka (Debezium CDC)                                    ← same stream as Part 1
      │  streaming, append-only (Task 4)
      ▼
🥉 BRONZE   bronze/{orders,order_items,products,users}
      │
      ├─ 🔵 FACT  (streaming)   orders       -> silver/orders        Task 5
      ├─ 🔵 FACT  (streaming)   order_items  -> silver/order_items   Task 6
      ├─ 🟢 DIM   (batch)       products     -> silver/products      Task 7
      ├─ 🟢 DIM   (batch)       users        -> silver/users         Task 8
      └─ 🟢 DIM   (batch, SCD2) users        -> silver/dim_users_scd2 Task 9
      ▼
🥇 GOLD     daily_revenue_by_category + customer_rfm                Task 10
            low_stock_reorder + Spark SQL serving                   Task 11
```

### The three layers in one line each

| Layer | Purpose | Write pattern | Cleaning? |
|-------|---------|---------------|-----------|
| 🥉 **Bronze** | Faithful, immutable landing of the raw stream | Streaming `append` | None — keep everything |
| 🥈 **Silver** | Clean, deduplicated current state (+ history dims) | `MERGE` (streaming for facts, batch for dims) | Dedup, DQ, types, deletes |
| 🥇 **Gold** | Business-level marts for consumption | Batch `overwrite` | Model & aggregate only |

---

## Prerequisites

1. The stack from Part 1 is running:
   ```bash
   bash scripts/setup.sh
   bash scripts/check_status.sh
   ```
2. The Debezium connector is `RUNNING` and the four topics exist.
3. Delta Lake jars are pulled automatically on the first Part 2 run
   (`io.delta:delta-spark_2.12:3.2.0`) — that run is slower while Ivy downloads.

> **Delta is wired in `scripts/run_job.sh`** — it adds the Delta package and the
> required configs (`spark.sql.extensions` + Delta catalog) for every Part 2
> task. You don't configure anything yourself.

The warehouse is written to `spark/warehouse/{bronze,silver,gold}` on the host
(mounted at `/opt/spark-apps/warehouse`), so tables persist between runs.
`bash scripts/teardown.sh --clean` wipes it.

> **Sample data is pre-loaded.** `data/init/01_init.sql` seeds 14 users, 18
> products (several low-stock) and 36 orders / 54 order-items spread over ~40
> days. Debezium emits all of it as an initial **snapshot** (`__op = 'r'`), so
> after Task 4 your bronze — and therefore silver and gold — already contain a
> meaningful dataset, **before** you run `demo_cdc_changes`.
>
> `demo_cdc_changes` then layers *changes* on top (inserts, an update, a delete,
> and the `normal → vip → premium` tier history) — which is what you need to see
> silver MERGE deletes and the **SCD2 history** in Task 9. Snapshot rows all
> share one commit time, so tier history cannot come from the seed alone.

---

## How to run

```bash
bash scripts/run_job.sh <task>            # your version (starter, has TODOs)
bash scripts/run_job.sh <task> solution   # reference solution
```

`<task>` is one of: `bronze` `orders` `items` `products` `users` `scd2`
`gold` `serving` (aliases `task4`…`task11` also work).

### End-to-end run

```bash
# Terminal A — Task 4: bronze ingest (streaming, leave running)
bash scripts/run_job.sh bronze solution

# Terminal B — generate a mix of CDC changes
bash scripts/mysql.sh demo_cdc_changes
# ...watch rows land in Terminal A, then stop it with Ctrl-C

# Terminal B — silver, per entity
bash scripts/run_job.sh orders   solution    # fact, streaming (drains bronze, then stops)
bash scripts/run_job.sh items    solution    # fact, streaming
bash scripts/run_job.sh products solution    # dimension, batch
bash scripts/run_job.sh users    solution    # dimension, batch
bash scripts/run_job.sh scd2     solution    # dimension, SCD Type 2

# Terminal B — gold
bash scripts/run_job.sh gold     solution
bash scripts/run_job.sh serving  solution
```

The fact-silver jobs use trigger `availableNow` — they consume everything
currently in bronze in incremental micro-batches, then **stop**, so you can move
on. Run them again later and the checkpoint makes them process only new data.

---

## Tasks

### Task 4 — 🥉 Bronze Ingest (all entities, streaming)  *(Intermediate)*
**File:** `spark/jobs/medallion/task4_bronze_ingest.py`

Stream all four CDC topics into append-only Delta bronze tables. One job
starts one stream per topic — bronze landing is identical code for every
entity, so it is not split.

- **TODO 1** — add ingestion metadata (`_ingest_timestamp`, `_ingest_date`).
- **TODO 2** — append-only Delta stream partitioned by `_ingest_date`.

**Skills:** streaming sink to Delta, `outputMode("append")`, partitioning,
checkpointing, why bronze keeps *everything* (deletes and duplicates included).

---

### Task 5 — 🥈 Silver `orders` — FACT, **streaming**  *(Intermediate)*
**File:** `spark/jobs/medallion/task5_silver_orders_stream.py`

Sync the orders fact table bronze → silver with Structured Streaming.

- **TODO A** — dedup the micro-batch to the latest row per key (`latest_per_key`).
- **TODO B** — data quality filter + cast `total_amount` to decimal.
- **TODO C** — `merge_upsert()` the batch into `silver/orders`.
- **TODO D** — `readStream` from `bronze/orders` and drive `upsert_to_silver`
  via `foreachBatch`, trigger `availableNow=True`.

**Skills:** streaming CDC sync, `foreachBatch`, incremental processing via
checkpoint, Delta `MERGE` (insert / update / **delete**).

**Expected:** one row per order id; the order deleted by `demo_cdc_changes`
(id=6) is gone; `total_amount` is a decimal.

---

### Task 6 — 🥈 Silver `order_items` — FACT, **streaming**  *(Intermediate)*
**File:** `spark/jobs/medallion/task6_silver_order_items_stream.py`

The same streaming template as Task 5, applied to a second fact table (the
repetition is the point — you should be able to reapply it to any fact table).

- **TODO A–D** — same shape as Task 5, with the order_items key/columns/DQ.

**Skills:** reusing the fact-streaming pattern; noticing what actually changes
between two facts (columns, DQ) vs what stays the same (dedup + MERGE + wiring).

---

### Task 7 — 🥈 Silver `products` — DIMENSION, **batch**  *(Intermediate)*
**File:** `spark/jobs/medallion/task7_silver_products_batch.py`

A dimension does not need streaming. A batch job reads bronze, keeps current
state, and upserts into silver.

- **TODO A** — `latest_per_key`.  **TODO B** — DQ + cast `price`.
  **TODO C** — `merge_upsert` into `silver/products`.

**Skills:** batch vs streaming — same transform, `spark.read` instead of
`spark.readStream` + `foreachBatch`. When batch is the right tool.

---

### Task 8 — 🥈 Silver `users` — DIMENSION, **batch**  *(Intermediate)*
**File:** `spark/jobs/medallion/task8_silver_users_batch.py`

Current-state users dimension (one row per user), batch like products.

- **TODO A–C** — dedup → DQ → `merge_upsert` into `silver/users`.

**Skills:** applying the dimension batch pattern to another entity.

---

### Task 9 — 🥈 SCD Type 2 `dim_users` — DIMENSION, batch  *(Advanced)*
**File:** `spark/jobs/medallion/task9_scd2_users.py`

Where Task 8 keeps only the *current* user, this keeps the *full history*.

- **TODO 1** — collapse duplicate events at the same commit time.
- **TODO 2** — keep only rows where a tracked attribute (`name`/`email`/`tier`)
  actually changed (hash + `lag`).
- **TODO 3** — build `valid_from` / `valid_to` / `is_current` with `lead`, plus a
  `surrogate_key`.

**Skills:** the classic SCD Type 2 pattern; why CDC makes it natural; window
functions `lag`/`lead`; change detection.

**Expected** (after user 1 goes `normal → vip → premium`):
```
surrogate_key  id  tier     valid_from   valid_to             is_current
...            1   normal   <t0>         <t1>                 false
...            1   vip      <t1>         <t2>                 false
...            1   premium  <t2>         9999-12-31 00:00:00  true
```

---

### Task 10 — 🥇 Gold Revenue Mart + Customer RFM  *(Advanced)*
**File:** `spark/jobs/medallion/task10_gold_revenue_mart.py`

Gold is cross-entity, so it is not split per table.

- **TODO 1** — `gold.daily_revenue_by_category` (order_items → orders → products).
- **TODO 2** — `gold.customer_rfm` (orders → users) + a simple segment.

**Skills:** multi-table joins, fact-building from line items, aggregations,
`countDistinct`, RFM metrics, segmentation.

---

### Task 11 — 🥇 Gold Serving  *(Advanced)*
**File:** `spark/jobs/medallion/task11_gold_serving.py`

- **TODO 1** — `gold.low_stock_reorder` (products + order_items).
- **TODO 2** — register gold tables as SQL views and answer three business
  questions with `spark.sql()`.

**Skills:** left joins with `coalesce`, business rules with `greatest`, serving a
lakehouse through Spark SQL views.

---

## Ideas to extend (optional)

- Run the fact-silver jobs **continuously** (`processingTime` trigger) instead of
  `availableNow`, and watch new orders flow to silver live.
- Add a **quarantine** path for rows that fail data quality instead of dropping.
- Make gold **incremental** (recompute only affected `order_date`s).
- Add an `order_status_history` SCD2 (pending → paid → refunded).

---

## Troubleshooting

| Symptom | Cause | Fix |
|---------|-------|-----|
| `DataSource delta not found` / `MERGE` parse error | Delta jars/config missing | Always launch via `scripts/run_job.sh`; wait for the first-run Ivy download. |
| `Path does not exist: .../bronze/orders` | Bronze not created yet | Run Task 4 first, generate changes, let it land, then run the silver jobs. |
| `Permission denied` writing warehouse | Container user can't write the host mount | `chmod -R 777 spark/warehouse spark/checkpoints` (setup.sh does this). |
| Silver stream reads 0 rows / exits immediately | No new bronze data since last run | The checkpoint already consumed it. Generate more changes, or reset: `rm -rf spark/checkpoints/silver/<table>`. |
| Deleted row still in silver | MERGE missing the delete branch | Implement TODO C fully / use `merge_upsert` (handles `__deleted='true'`). |
| SCD2 shows duplicate identical rows | Change-detection (TODO 2) not applied | Keep only rows where the tracked-attribute hash differs from the previous. |
| All rows share the same `order_date` in gold | `_event_time` comes from `__source_ts_ms`, and snapshot rows all share the snapshot commit time | Expected for seed data. `created_at` (an ISO-8601 UTC string from Debezium) *does* keep the real per-order dates — switch `order_date` to `to_date(created_at)` if you want multi-day marts. |

---

## Reference

- [Delta Lake — MERGE / upsert](https://docs.delta.io/latest/delta-update.html#upsert-into-a-table-using-merge)
- [Delta streaming reads & writes](https://docs.delta.io/latest/delta-streaming.html)
- [Structured Streaming — foreachBatch](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#using-foreach-and-foreachbatch)
- [Databricks — Medallion architecture](https://www.databricks.com/glossary/medallion-architecture)
- [SCD Type 2 pattern](https://en.wikipedia.org/wiki/Slowly_changing_dimension#Type_2:_add_new_row)
