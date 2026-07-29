# Spark Job — Offline Data Problems — 16 pts

**Full write-up:** [`b_schema_pipelines/pipelines/bronze/README.md`](../../../b_schema_pipelines/pipelines/bronze/README.md)
(Bronze/Silver pipeline, idempotency) and
[`b_schema_pipelines/pipelines/silver/README.md`](../../../b_schema_pipelines/pipelines/silver/README.md)
(the 4 fixes in detail). Fix-by-fix before/after: see `02_spark_optimisation_report.md` in this
folder.

| Requirement (rubric wording) | Pts | Status |
|---|---|---|
| Baseline→optimized explanation, Spark UI screenshots, Airflow integration | 2 | 🟡 procedure documented, Airflow integration real (`dp2_gold_dag.py`); **Spark UI screenshots not yet captured** |
| Handle skew (Problem A) with explanation | 3 | ✅ AQE skewJoin — code + explanation done; **screenshot outstanding** |
| Handle high cardinality with explanation | 3 | ✅ broadcast join (products) — code + explanation done; **screenshot outstanding** |
| Handle schema evolution with explanation | 3 | ✅ NULL fill (`coupon_code`/`shipping_method`) — code + explanation done; **screenshot outstanding** |
| Handle other offline problem (chosen: duplicate rows) with explanation | 3 | ✅ window dedup on `(order_id, product_id, unit_price, quantity)` — code + explanation done; **screenshot outstanding** |
| Spark job integrated into Airflow pipeline | 2 | ✅ `dp1_bronze_dag.py`/`dp2_gold_dag.py` call these scripts directly (`BashOperator`) |

All 4 fixes' code and written analysis are done and verified against real data (confirmed live
2026-07-29: Bronze 909,000 → Silver 899,999 `order_items` rows via the corrected dedup key) —
the one remaining gap for full point value is capturing the actual Spark UI before/after
screenshots. See the linked docs for the fix-by-fix code and analysis.
