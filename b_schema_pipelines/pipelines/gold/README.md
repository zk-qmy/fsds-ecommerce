# Gold Layer — `build_gold.py`

Reads Silver Delta tables from MinIO and writes the Kimball star schema (5 dims, 3 facts, 1 OBT) to PostgreSQL under the `gold_ecommerce` schema.

**Run Silver first** — Gold reads from `s3a://silver-data/silver/`.

---

## How it's implemented

`GoldBuilder` builds tables in dependency order (`run()`):

```
dim_date → dim_payment_method → dim_order_status → dim_product
→ dim_customer (SCD2)
→ fact_order → fact_order_item → fact_payment_attempt
→ obt_order_performance
```

**Surrogate keys are deterministic**, not `monotonically_increasing_id()`:

```python
_assign_surrogate_keys(df, key_col, order_col) = row_number() over Window.orderBy(order_col)
```

Same input → same keys every time, so a dimension's key assignment and a fact's lookup of that dimension agree without a round trip.

> **Known issue:** `Window.orderBy(order_col)` has no `partitionBy`, so Spark runs it in a single task — you'll see `WARN WindowExec: No Partition Defined for Window operation!` on every surrogate-key build. It's the price of a globally sequential, deterministic key. **Possible fix:** rank rows within cheap partitions (e.g. a hash bucket of `order_col`), then add each partition's running row-count offset to make the ranks globally sequential — same deterministic keys, no full-table single-task sort.

- **`dim_customer`** is true **SCD2**: bootstrap run assigns fresh keys to everyone; later runs read the persisted table, close (`is_current=False`, `valid_to_ts=now()`) any row whose `segment`/`country`/`marketing_opt_in` changed, and insert a new current row with a fresh key continuing from `max(existing.customer_key)`. Written with `mode="append"` — history is never overwritten.
- **`dim_date`** is generated (no source table) for the last `days_history` (180) days ending today.
- **`dim_payment_method`** / **`dim_order_status`** are static 4-row lookups built from a hardcoded list.
- **`fact_order`** aggregates `order_items` (gross/discount/item_count) onto `orders`, looks up `customer_key` (prefers the persisted `dim_customer`, falls back to a fresh mapping over orders' own `customer_id` on a cold start) and the static `order_status_key`, and derives `order_date_key` directly from `order_timestamp` (no dimension join needed for dates).
- **`fact_order_item`** / **`fact_payment_attempt`** get `order_key`/`product_key` from the same canonical `_order_key_map()` / `_product_key_map()` that `fact_order` and `dim_product` use (cached per run), so all facts agree on the same key for the same order/product even if one Silver table is missing rows the others have.
- **`obt_order_performance`** is a flat, business-key-only join (order + item aggregates + customer + most-recent payment status) — no surrogate keys, by design.

---

## Prerequisites

- Silver Delta tables written (`uv run python b_schema_pipelines/pipelines/silver/transform_silver.py --mode optimized`)
- MinIO running (same as Bronze/Silver)
- PostgreSQL reachable at `jdbc:postgresql://localhost:5432/fsds`, with the `gold_ecommerce` schema created (steps below)
- Credentials/driver come from `b_schema_pipelines/pipelines/pipeline_config.yaml` (`postgres:` section, local-dev `fsds`/`fsds`; JDBC driver `org.postgresql:postgresql:42.7.4` in `packages`)

### Check whether Postgres is up

```bash
docker ps --filter "name=fsds-postgres"            # is the container running?
docker exec fsds-postgres pg_isready -U fsds       # is it accepting connections?
```

If nothing is listening on `5432`, the `postgres` service in `infra/docker-compose.yml` is commented out by default — uncomment it (the service block plus its `postgres_data:` volume under the top-level `volumes:` section) and start it:

```bash
docker compose -f infra/docker-compose.yml up -d postgres
```

### Create the Gold schema (one-time)

`GoldBuilder` only creates *tables* via JDBC — it does not create the schema itself:

```bash
docker exec -it fsds-postgres psql -U fsds -d fsds -c "CREATE SCHEMA IF NOT EXISTS gold_ecommerce;"
```

---

## Running

```bash
cd /mnt/d/fsds-ecommerce
source .venv/bin/activate
uv run python3 b_schema_pipelines/pipelines/gold/build_gold.py
```

## Expected output

Structured log lines (one per table) to stdout, e.g.:

```
[dim_date] phase=end  status=ok  rows_in=180  rows_out=180
[dim_payment_method] phase=end  status=ok  rows_in=4  rows_out=4
[dim_order_status] phase=end  status=ok  rows_in=4  rows_out=4
[dim_product] phase=end  status=ok  rows_in=45000  rows_out=45000
[dim_customer] phase=end  status=ok  rows_in=120000  rows_out=120000   # bootstrap run; smaller delta on later runs
[fact_order] phase=end  status=ok  rows_in=360000  rows_out=360000
[fact_order_item] phase=end  status=ok  rows_in=890000  rows_out=890000
[fact_payment_attempt] phase=end  status=ok  rows_in=360000  rows_out=360000
[obt_order_performance] phase=end  status=ok  rows_in=360000  rows_out=360000
```

Full structured logs are written to `logs/gold/<run_id>.log`. Verify tables landed correctly via `psql`:

```sql
\dt gold_ecommerce.*
SELECT COUNT(*) FROM gold_ecommerce.dim_customer WHERE is_current;
SELECT COUNT(*) FROM gold_ecommerce.fact_order;
```

---

## Tests

```bash
uv run pytest tests/b_schema_pipelines/test_build_gold.py -v
```

JDBC reads/writes are mocked; tests validate the DataFrame shape and SCD2/aggregation logic (31 tests, see the file docstring).
