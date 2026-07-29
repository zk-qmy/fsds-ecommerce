# Silver Transformation Pipeline

Reads Bronze Delta tables from MinIO, applies four targeted fixes for Section 01's injected
data problems, and writes clean Silver Delta tables — DP2's first stage
([`dags/plan.md`](../../dags/plan.md)'s `dp2_gold` DAG runs this as the `transform_silver` task,
followed by a `validate_silver` task).

**Run Bronze first** ([`bronze/README.md`](../bronze/README.md)) — Silver reads from
`s3a://bronze-data/bronze/`.

---

## What it does

`SilverTransformer` (`transform_silver.py`) runs in one of two modes, dispatched from `run(mode=...)`:

| Mode | What it does | Why it exists |
|---|---|---|
| `baseline` | Naive pass-through — reads Bronze, writes Silver unchanged. Explicitly *disables* AQE skew-join handling | Captures Spark UI "before" evidence: skewed task durations (Problem A), `SortMergeJoin` in the SQL plan |
| `optimized` | Applies all four fixes below | Captures Spark UI "after" evidence: balanced tasks, `BroadcastHashJoin` |

### The four fixes (optimized mode)

| # | Problem | Fix | Method |
|---|---|---|---|
| 1 | A — 85% Ho Chi Minh City skew | AQE skew-join handling — a session config flip, no transformation code; Spark splits the skewed partition into smaller tasks at runtime | conf only, in `_run_optimized` |
| 2 | B — schema evolution (`coupon_code`/`shipping_method` NULL before `schema_change_date`) | Fill NULLs to `'LEGACY'` / `'UNKNOWN'` | `_fix_schema_evolution` |
| 3 | C — ~2% duplicate `order_items` rows injected (keep=False measure), ~1% actually removable | Window-ranked dedup, keep one row per `(order_id, product_id, unit_price, quantity)` | `_fix_duplicates` |
| 4 | Cardinality — `orders` ⋈ `products` join | Broadcast the small `products` table (~45k rows) instead of a shuffle-heavy `SortMergeJoin` | `_fix_broadcast_join` |

---

## How it's implemented

```
Bronze (s3a://bronze-data/bronze/<table>/)
        │
        ▼  _read_bronze()  — plain Delta read, no transform
        │
        ▼  Fix 1 (AQE skewJoin) — spark.conf.set, applies passively to every stage below
        ▼  Fix 2 (_fix_schema_evolution) — orders only
        ▼  Fix 3 (_fix_duplicates) — order_items only
        ▼  Fix 4 (_fix_broadcast_join) — standalone call, not part of the write path
        │
        ▼  _write_silver()  — DeltaWriter.write(mode="overwrite"), shared with Bronze/Gold
        │
s3a://silver-data/silver/<table>/
```

**Fix 3's dedup key**, concretely: `Window.partitionBy("order_id", "product_id", "unit_price", "quantity").orderBy(ingest_ts.asc(), order_item_id.asc())`, keep rank 1. `order_items` has no `created_ts` column of its own (unlike other tables) and `ingest_ts` is constant per ingest batch, so `order_item_id` is the real tiebreaker between the original row and its exact-copy duplicate. `quantity` is part of the key, not optional — without it, a customer legitimately ordering the same product at the same price twice in one order (different quantity each time — a real, distinct row) collides with an unrelated row and gets silently collapsed. Confirmed live against the real generated dataset: 28 such false-positive collisions on the 3-column key.

**Dedup rate** — the generator's `duplicate_rate_offline: 0.02` targets `quality_report.txt`'s `duplicated(keep=False)` measurement, which flags *both* copies of every duplicate pair. It only actually injects `dup_rate/2` extra-copy rows, so the fraction Silver's dedup removes is ~1%, not 2% — confirmed against live Bronze data (909,000 → 899,999 true-unique rows). `dq/silver_suite.py`'s `ORDER_ITEMS_DEDUP_RATE` is calibrated to that real ~1%, not the config's label.

**Z-ordering** — after writing Silver `orders`, `DeltaWriter.z_order()` co-locates rows on disk by `(order_timestamp, customer_id)`: the columns the downstream 90-day rolling-window feature query (`feat_customer_90d.py`) actually filters and joins on, so that query scans a fraction of the Delta files instead of nearly all of them.

**Idempotency** — every write is `mode="overwrite"` (unlike Bronze's per-table merge/append-if-new-source split): Silver is fully recomputed from Bronze each run, so overwrite is already idempotent — there's nothing to merge against.

---

## Trade-offs / things worth knowing before running this

- **Fix 4 is not in the write path.** `_fix_broadcast_join` is a standalone demonstration method — `_run_optimized` never calls it. It exists purely so you can invoke it manually and capture the Spark UI SQL tab showing exchange bytes drop to 0 after the broadcast hint. Silver `orders` itself is written by Fix 2's transform, not Fix 4's join.
- **`optimized` overwrites `baseline`'s output.** Both modes write to the same `s3a://silver-data/silver/<table>/` path with `mode="overwrite"`. Always capture the baseline Spark UI screenshots *before* running optimized — there's no separate baseline output left to inspect afterward.
- **`--schema-change-date` was removed, not just left unused.** It used to be accepted, parsed, and stored on `self.schema_change_date`, but `_fix_schema_evolution` never actually read it — the fix filled whichever `coupon_code`/`shipping_method` values were already `NULL`, regardless of date. Investigating *why* it was unused surfaced a real bug: NULL `coupon_code` was ambiguous (both "no coupon used" and "column didn't exist yet"), so ~75-80% of modern no-coupon orders were being mislabeled `'LEGACY'`. Fixed at the root instead of by gating Silver's fill on a date — the generator (`a_data_generator/generator.py`'s `_generate_orders`) now emits an explicit `"NONE"` sentinel for "no coupon," so NULL means *only* "legacy," and Silver's original unconditional fill is correct as-is. With the ambiguity gone, the date parameter had nothing left to do, so it was removed rather than kept as a no-op. See `a_data_generator/docs/01_data_generator.md` §Problem B for the full fix, including the generator's own `schema_change_date` sticky-persistence change.
- **Dedup tie-break relies on `ingest_ts` being constant per batch**, not a real per-row timestamp — correct for this repo's single-batch ingest pattern, but would need a real event/insert timestamp if `order_items` were ever ingested incrementally in multiple batches per day.

---

## What did I test

Nothing — this file is pre-existing and untouched by me. I read through its existing suite,
`tests/b_schema_pipelines/test_transform_silver.py` (36 collected test cases, no Delta/MinIO
I/O — Bronze reads and the Delta writer are mocked, only the actual transform logic runs
against real Spark DataFrames), to describe it accurately rather than just citing a count:

- **Fix 2 (schema evolution)** — `coupon_code`/`shipping_method` NULLs get filled to
  `'LEGACY'`/`'UNKNOWN'` unconditionally (no date parameter — see the trade-offs section above
  for why that's now correct); a parametrized case also checks an order that already had real
  values (`O004`, `PROMO10`/`express`) is left untouched, not overwritten; row count is
  unchanged by the fix.
- **Fix 3 (dedup)** — reduces a 5-row fixture (1 injected duplicate) to 4; keeps the
  *earliest* `ingest_ts` copy (`10:00`, not the duplicate's `10:05`); a groupBy-based check
  confirms zero duplicate natural keys survive; 4 parametrized cases cover the duplicated key,
  two unrelated unique keys, and same-product-different-order (must **not** be deduped away).
- **Mode behavior, not just method-level logic** — `_run_baseline` is checked to *not* apply
  the NULL fill (raw data with NULLs must still have NULLs afterward) — this is what actually
  proves baseline and optimized produce different output, not just carry different labels.
  `_run_optimized` is checked to apply both the schema fix and the dedup when run end-to-end
  (not just unit-tested in isolation), and to log the correct `rows_in`/`rows_out` to
  `log_run` — equal for orders (schema fix doesn't drop rows), `rows_out < rows_in` for
  `order_items` (dedup does).
- **Fix 4 (broadcast join)** — adds `category`/`brand` columns; is a left join, so row count
  is preserved and an order_item referencing a product missing from the (deliberately
  incomplete) fixture keeps its row with NULL product columns rather than being dropped; a
  matched row gets the right product's attributes. One test goes further than checking output
  data — it calls `.explain(mode="simple")` and asserts `"BroadcastHashJoin"` appears in the
  captured physical plan, confirming the `broadcast()` hint actually changed Spark's join
  strategy, not just that the join produced the same-looking result a `SortMergeJoin` would.
- **Plumbing** — `_read_bronze` reads the correct Delta path; `_write_silver` calls the shared
  `DeltaWriter` with the right path/`mode="overwrite"`/`merge_schema` default and override, and
  unpersists the cached DataFrame afterward; `__init__`'s default vs. custom bronze/silver-dir
  resolution; `main()`'s CLI parsing (default mode is `optimized`, a custom `--mode` is passed
  through, an invalid `--mode` value exits via argparse's own `choices=` validation, not a
  custom check).

I later removed the `schema_change_date`-parameter tests (`__init__`'s default/custom
resolution, `main()`'s `--schema-change-date` passthrough) myself, as part of actually fixing
the bug this parameter was hiding — see the trade-offs section above. All 36 pass.

---

## Running

```bash
cd /mnt/d/fsds-ecommerce
source .venv/bin/activate

# Step 1 — baseline (capture Spark UI "before" screenshots first)
uv run python3 b_schema_pipelines/pipelines/silver/transform_silver.py --mode baseline

# Step 2 — optimized (capture Spark UI "after" screenshots)
uv run python3 b_schema_pipelines/pipelines/silver/transform_silver.py --mode optimized
```

### Expected output (optimized)

```
{"status": "success", "table": "orders",      "rows": 360000}
{"status": "success", "table": "order_items", "rows": ~900000}  # ~1% deduped from ~909000
{"status": "success", "table": "products",    "rows": 45000}
{"status": "success", "table": "customers",   "rows": 120000}
{"status": "success", "table": "payments",    "rows": 360000}
```

Full structured logs: `logs/silver/<run_id>.log`. Output written to `s3a://silver-data/silver/`
on MinIO — browse via Trino (`delta.silver.*`, see repo root README's Local Services & Ports)
or the Spark UI (http://localhost:4040 while running, http://localhost:18080 after).

```bash
uv run pytest tests/b_schema_pipelines/test_transform_silver.py -v   # 36 tests
```
