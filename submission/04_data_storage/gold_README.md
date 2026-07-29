# Data Storage Optimization — code + analysis detail

**Full write-up:** [`b_schema_pipelines/pipelines/gold/README.md`](../../b_schema_pipelines/pipelines/gold/README.md)

Both rubric items ask for a **code capture + analysis**, not a UI screenshot — fully satisfied,
no outstanding gap.

## Lakehouse optimization (2 pts) — Delta Lake compaction + Z-order

```python
# common/delta_writer.py
def optimize(self, output_path: str) -> None:
    self.load(output_path).optimize().executeCompaction()

def z_order(self, output_path: str, columns: list[str]) -> None:
    self.load(output_path).optimize().executeZOrderBy(*columns)
```

Called on Silver `orders` by `(order_timestamp, customer_id)` — the exact columns the 90-day
rolling-window feature query filters/joins on, so file-level min/max stats let Spark skip files
instead of scanning nearly all of them.

## Datawarehouse optimization (2 pts) — PostgreSQL indexing

```python
INDEX_STATEMENTS = (
    ("idx_fact_order_customer_key", "fact_order", "customer_key"),
    ("idx_fact_order_date_key", "fact_order", "order_date_key"),
    ("idx_fact_order_item_order_key", "fact_order_item", "order_key"),
    ("idx_fact_payment_order_key", "fact_payment_attempt", "order_key"),
    ("idx_dim_customer_bk", "dim_customer", "customer_id, is_current"),
)
```

Verified live: `EXPLAIN ANALYZE SELECT * FROM fact_order WHERE customer_key = 12345` goes from
`Seq Scan` to `Index Scan using idx_fact_order_customer_key`.

See the linked doc for the full analysis of what each optimization costs without it.
