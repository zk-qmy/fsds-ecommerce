# Implement Data Generator (offline + streaming) — 32 pts

**Full write-up:** [`a_data_generator/README.md`](../../a_data_generator/README.md)

Generates all synthetic source data (customers, products, orders, order_items, payments,
streaming events) with 6 deliberately injected data-quality problems (A–F). `generator_config.yaml`
and `quality_report.txt` in this folder are the actual generated config snapshot and evidence
report (real captured output, seed 42) — not copies of the write-up.

| Requirement (rubric wording) | Pts | Where satisfied |
|---|---|---|
| Simulate skew, cardinality, schema evolution, dedup rate — "Capture màn hình output" | 2+2+2 | `quality_report.txt` — real captured terminal output |
| Simulate another offline problem (chosen: duplicate rows in `order_items`) | 2 | Problem C — natural key `(order_id, product_id, unit_price, quantity)` |
| Using generator configuration | 2 | `generator_config.yaml` (this folder) — full config reference in the linked doc §5 |
| Store data for Bronze ingest | 2 | `outputs/offline/*.parquet` + `outputs/streaming/events.json`, consumed by `ingest_bronze.py` |
| Simulate burst (Problem D) | 2 | 30× rate, 12:00–12:20 / 20:00–20:20 |
| Simulate late arrivals (Problem E) | 2 | 12% of events, 5–45 min delay |
| Simulate another streaming problem (chosen: duplicate event_ids) | 2 | Problem F — 1.5% duplicate `event_id`s |
| Using generator configuration (streaming) | 2 | same config file covers both paths |
| Document data characteristics + config | — | linked doc §2.2/§3.2 (volumes), §5 (config) |

See the linked doc for the full domain design, table schemas, and per-problem injection
mechanics and captured evidence.
