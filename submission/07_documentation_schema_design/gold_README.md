# Schema Design (DBeaver) — 10 pts

**Full write-up:** [`b_schema_pipelines/pipelines/gold/README.md`](../../b_schema_pipelines/pipelines/gold/README.md)

| Requirement (rubric wording) | Pts | Status |
|---|---|---|
| Visualize tables on all zones — "Capture màn hình trên DBeaver" | 2 | 🟡 Gold zone ✅ (screenshots below); Bronze/Silver (via Trino) queryable but **not yet screenshotted** |
| Dim table with SCD2 (`valid_from_ts`/`valid_to_ts`/`is_current`) | 2 | ✅ visible below — `dim_customer` has all three columns |
| Feature tables (`feat_*`) with `event_timestamp`/`created` columns | 2 | ✅ visible below |
| Relationship between dim & fact tables | 2 | 🟡 code done — `build_gold.py` now declares real `FOREIGN KEY` constraints; screenshots below predate this and show standalone boxes — **re-capture outstanding** |
| Naming convention (`dim_`/`fact_`/`obt_`/`feat_`) | 2 | ✅ visible in both screenshots |

![Gold schema ER diagram — dim/fact tables with SCD2 columns](../../assets/gold-schema.png)

![Database view — feature tables with event_timestamp/created columns](../../assets/database.png)

See the linked doc's "Dim/fact relationships" section for the FK constraint code (`NOT VALID`,
drop-before-rebuild ordering) and full table-by-table design.
