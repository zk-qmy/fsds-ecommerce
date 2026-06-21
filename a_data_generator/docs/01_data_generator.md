# Section 01 — E-commerce Data Generator

## 1. Domain Overview

This project simulates a medium-scale e-commerce platform operating primarily in Vietnam.
The generator produces two data paths consumed by downstream pipelines:

- **Offline** — five historical/reference Parquet tables covering 180 days of activity.
- **Streaming** — a single newline-delimited JSON file simulating one 24-hour clickstream day.

Both paths feed the Bronze → Silver → Gold pipeline (Section 02), the drift/label layer (Section 03), and the ML training and scoring system (Section 04).

---

## 2. Offline Dataset Design

### 2.1 Table Schemas

| Table | Grain | Key Columns |
|---|---|---|
| `customers` | one per customer | customer_id, signup_ts, country, segment, marketing_opt_in |
| `products` | one per product | product_id, category, brand, base_price, is_active, created_ts |
| `orders` | one per order | order_id, customer_id, order_timestamp, status, shipping_city, shipping_method, coupon_code |
| `order_items` | one per line item | order_item_id, order_id, product_id, quantity, unit_price, discount_amount |
| `payments` | one per payment | payment_id, order_id, payment_timestamp, payment_method, amount, payment_status |

### 2.2 Volume Estimates

| Table | Row count | Notes |
|---|---|---|
| customers | 120,000 | one row per customer |
| products | 45,000 | one row per product |
| orders | ~360,000 | 120,000 × Poisson(λ=3.0) |
| order_items | ~921,000 | ~360,000 × Poisson(λ=2.5) + 2% injected duplicates |
| payments | ~360,000 | one payment per order |

Simulation window: **180 days** ending at runtime. Random seed: **42** (reproducible).

### 2.3 Distribution Choices

**Customer segments** — Pareto-like: 60% bronze, 30% silver, 10% gold.
Reflects typical loyalty tiers where most customers are low-engagement.

**Country** — 90% Vietnam (`VN`), remainder split across US, JP, KR, CN.
Models the platform's home-market concentration.

**Shipping city** — 85% Ho Chi Minh City for Vietnamese customers (Problem A, see §2.4).
Other cities (Hanoi, Da Nang, Can Tho, Bien Hoa) fill remaining weight via Dirichlet sampling.

**Product categories** — 80% electronics, 20% across 7 other categories.
Electronics dominance creates a meaningful category-skew challenge downstream.

**Payment methods** — 40% credit card, 25% bank transfer, 25% e-wallet, 10% COD.
Payment statuses — 85% paid, 10% failed, 5% refunded; ~10% failure rate is realistic for SEA markets.

### 2.4 Injected Data Problems — Offline

#### Problem A — Geographic skew (compulsory)
85% of `orders.shipping_city` = `Ho Chi Minh City`.

**Why:** Skewed joins are a real production problem. Downstream Spark jobs must handle partition skew; Gold is partitioned by city to expose this.

**Downstream handler:** Silver — no fill; Gold — partitioned by `shipping_city`.

#### Problem B — Schema evolution (compulsory)
`orders.coupon_code` and `orders.shipping_method` are NULL for all orders placed before `schema_change_date` (~50% of the 180-day window, targeting 2026-03-24).

**Why:** Schema evolution is unavoidable in production. Old partitions must be handled without breaking new-schema queries.

**Downstream handler:** Silver — NULL → `'LEGACY'` (coupon_code), NULL → `'UNKNOWN'` (shipping_method).

**Implementation note:** `schema_change_date` in config should be set to approximately 50% into the current 180-day window. If running after 2026-09-01, update it to remain at the midpoint (or use a float `0.5` which the loader interprets as a relative fraction of the window).

#### Problem C — Duplicate rows in order_items (optional, chosen)
2% of `order_items` rows are duplicated by natural key `(order_id, product_id, quantity, unit_price)`.

**Why chosen:** Duplicate ingestion is a common operational failure (double-publish from upstream). Dedup logic must be tested explicitly, and the dedup key must include `quantity` to avoid false-positive matches where the same product appears twice at different quantities.

**Downstream handler:** Silver — dedup on `(order_id, product_id, quantity, unit_price)`, keep earliest `created_ts`.

---

## 3. Streaming Dataset Design

### 3.1 Event Schema

Single unified topic; event type is a field, not a separate topic.

| Column | Type | Nullable | Notes |
|---|---|---|---|
| `event_id` | string | no | E + 12-digit zero-padded counter |
| `event_type` | string | no | view, add_to_cart, checkout, purchase, payment_failed |
| `event_timestamp` | ISO-8601 string | no | when the event logically occurred |
| `created_ts` | ISO-8601 string | no | when the row was created; delayed for late arrivals (Problem E) |
| `customer_id` | string | no | FK to customers |
| `session_id` | string | no | S + 8-digit random integer |
| `device_type` | string | no | mobile, desktop, tablet |
| `source` | string | no | app, web |
| `product_id` | string | yes | NULL for non-product events |
| `order_id` | string | yes | NULL except on purchase |
| `quantity` | integer | yes | NULL except on purchase / add_to_cart |
| `price` | float | yes | NULL except on purchase |

**Event type distribution:** 50% view, 20% add_to_cart, 12% checkout, 10% purchase, 8% payment_failed.

### 3.2 Volume Estimate

Base rate: 100 events/min × 1,440 min/day = **~144,000 events/day**.
With two 20-minute burst windows at 3,000 events/min: +2 × 20 × 2,900 ≈ **+116,000** extra events.
With 1.5% duplicate injection: total ~**265,000 events/day**.

### 3.3 Injected Data Problems — Streaming

#### Problem D — Burst traffic (compulsory)
Rate multiplier of **30×** (3,000 events/min) during two 20-minute windows: **12:00–12:20** and **20:00–20:20** (lunch and evening peaks).

**Why:** Burst traffic is a real production pattern. Flink pipelines must configure watermarks and backpressure to avoid OOM and to correctly attribute burst-window events to their time buckets.

**Downstream handler:** Flink — watermarks + backpressure config; Kafka topic partitioned to absorb spikes.

#### Problem E — Late arrivals (compulsory)
12% of events have `created_ts` delayed **5–45 minutes** after `event_timestamp`.

**Why:** Network lag and mobile client buffering cause late emission. Without `AllowedLateness`, late events are silently dropped, corrupting feature aggregations.

**Downstream handler:** Flink — `WatermarkStrategy` with `AllowedLateness`; reprocess affected windows.

#### Problem F — Duplicate event_ids (optional, chosen)
1.5% of events are re-emitted with the same `event_id` but a **1–3 minute shift** in `created_ts`.

**Why chosen:** Kafka at-least-once delivery guarantees mean duplicates are inevitable. Dedup must key on `(event_id, event_timestamp)` — not `event_id` alone — because the same event may legitimately recur at different logical times across different sessions.

**Downstream handler:** Stream dedup keyed on `(event_id, event_timestamp)`.

---

## 4. Feature Engineering Plan

Features are computed in the Gold layer (Section 02) and consumed in training/scoring (Section 04).

### Offline features — `feat_customer_90d` (grain: customer_id, event_timestamp)

| Feature | Computation | Window |
|---|---|---|
| `f_customer_total_orders_90d` | COUNT(order_id) | rolling 90 days |
| `f_customer_avg_order_value_90d` | AVG(order_net_amount) | rolling 90 days |
| `f_customer_distinct_categories_90d` | COUNT(DISTINCT category) | rolling 90 days |
| `f_customer_payment_fail_rate_90d` | SUM(failed) / COUNT(*) | rolling 90 days |

### Streaming features — `feat_stream_60m` (grain: customer_id, event_timestamp)

| Feature | Computation | Window |
|---|---|---|
| `f_stream_views_30m` | COUNT(view events) | 30-minute tumbling |
| `f_stream_add_to_cart_30m` | COUNT(add_to_cart events) | 30-minute tumbling |
| `f_stream_cart_to_purchase_ratio_60m` | purchase / add_to_cart | 60-minute sliding |
| `f_stream_burst_activity_flag` | 1 if event in burst window | per-event lookup |

### Unified feature table — `feat_customer_unified`
Point-in-time join of `feat_customer_90d` and `feat_stream_60m` on `(customer_id, event_timestamp)`.
Features must not use data later than the label timestamp (no leakage).

**Design trade-off:** Separate offline and streaming feature tables (rather than one denormalised store) allows independent refresh schedules — 90d offline features can refresh hourly while 60m streaming features refresh every 5 minutes. The cost is one extra join at training/scoring time.

---

## 5. Generator Configuration Reference

Config file: `a_data_generator/config/generator_config.yaml`

```yaml
n_customers: 120000
n_products: 45000
days_history: 180
random_seed: 42

# Offline problems
skew_ratio_city: 0.85         # Problem A
skew_ratio_category: 0.80
schema_change_date: "2026-03-24"   # Problem B — ~50% into current window
duplicate_rate_offline: 0.02  # Problem C

# Streaming problems
base_events_per_min: 100
burst_multiplier: 30           # Problem D — 30× at burst windows
burst_windows: ["12:00-12:20", "20:00-20:20"]
late_arrival_rate: 0.12        # Problem E — 12% late
late_delay_min_max: [5, 45]
duplicate_rate_stream: 0.015   # Problem F — 1.5% re-emitted

# Volume controls
avg_orders_per_customer: 3.0   # Poisson λ
avg_items_per_order: 2.5       # Poisson λ
marketing_opt_in_rate: 0.70
```

`"auto"` values in distribution dicts are filled by Dirichlet sampling to sum to 1.0, giving stable-but-varied minority-class proportions across re-runs at the same seed.

---

## 6. Deliverables

| Artifact | Path |
|---|---|
| Generator code | `a_data_generator/config/generator.py` |
| Config | `a_data_generator/config/generator_config.yaml` |
| Offline Parquet | `a_data_generator/outputs/offline/*.parquet` |
| Streaming JSON | `a_data_generator/outputs/streaming/events.json` |
| Quality report | `a_data_generator/outputs/quality_report.txt` |
| Tests | `tests/a_data_generator/test_generator.py` |

---

## 7. Run Instructions

```bash
# Install dependencies
uv sync

# Generate all outputs (offline + streaming, ~3 min at full scale)
uv run python a_data_generator/config/generator.py

# Skip stream generation (faster for offline-only dev)
uv run python a_data_generator/config/generator.py --skip-stream

# Run tests (uses a 1 000-customer test config for speed)
uv run pytest tests/a_data_generator/ -v
```

Expected output summary at full scale (120,000 customers):

```
✓ customers      :   120,000 rows → outputs/offline/customers.parquet
✓ products       :    45,000 rows → outputs/offline/products.parquet
✓ orders         :   ~360,000 rows → outputs/offline/orders.parquet
✓ order_items    :   ~921,000 rows → outputs/offline/order_items.parquet
✓ payments       :   ~360,000 rows → outputs/offline/payments.parquet
✓ events         :   ~265,000 events → outputs/streaming/events.json

Quality checks (from quality_report.txt):
  HCMC shipping_city rate ≥ 85%
  Electronics category rate ≥ 80%
  Old-partition coupon_code NULL rate = 100%
  order_items duplicate injection rate ≈ 2%
  Streaming late arrival rate ≈ 12%
  Streaming duplicate event rate ≈ 1.5%
  Burst peak rate ≈ 3,000 events/min
```
