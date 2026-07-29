# Submission — Data Platform (mini-coursework, `coursework/rubrics.md`)

This folder is organized by **rubric section**, not by repo layout, so it can be reviewed
folder-by-folder against the rubric. Each file here is a short summary — status, points, and
the compact proof/checklist — with a link to the canonical write-up in its normal repo
location, rather than a full copy. Only real evidence artifacts (screenshots, generated config
snapshots, quality reports) live directly in this folder.

**`RUBRIC_STATUS.md`** is the master audit — a full item-by-item table against every line in
the rubric, with point values and honest status (done+proof / code-done-proof-missing /
not-started). Read that first for the overall picture; the folders below hold the actual
evidence each row points to.

## Overall status

**~75-80 of 100 pts have working code with real, checkable proof.** What's left:

- **~25-30 pts blocked purely on screenshots not yet captured** — the underlying code,
  written analysis, and (where relevant) live-verified execution all exist; only the literal
  image capture is missing. Sections 03 (Processing Jobs) and 05 (Orchestration) below.
- **12 pts (Data Governance) has zero implementation** — no DataHub integration exists
  anywhere in the codebase. Section 06 below is a status note, not evidence, because there's
  nothing to show yet.

## Folder-by-folder

| Folder | Rubric section | Pts | Status |
|---|---|---|---|
| [`01_docker_docker_compose/`](01_docker_docker_compose/) | Docker & Docker Compose | 3 | ✅ done — real measured image-size reduction (5.7GB → 2.79GB, 51%) |
| [`02_data_generator/`](02_data_generator/) | Implement Data Generator (offline + streaming) | 32 | ✅ done — all 6 injected problems (A–F) documented with real captured output |
| [`03_processing_jobs/spark/`](03_processing_jobs/spark/) | Spark job (offline problems) | 16 | 🟡 code + written analysis done, integrated into Airflow; **Spark UI screenshots not captured** |
| [`03_processing_jobs/flink/`](03_processing_jobs/flink/) | Flink job (streaming problems) | 13 | 🟡 code + written analysis done; window-processing code capture ✅ complete; **Flink UI screenshots not captured** |
| [`04_data_storage/`](04_data_storage/) | Data Storage optimization | 4 | ✅ done — rubric wants code + analysis (not a screenshot) here, fully satisfied |
| [`05_data_pipeline_orchestration/`](05_data_pipeline_orchestration/) | DP1/DP2/DP3 orchestration | 12 | 🟡 all 3 DAGs verified running correctly end-to-end against local docker-compose; **Airflow UI Graph-view screenshot not captured** — `assets/draft-airflow.png` doesn't satisfy this (wrong view, 0 runs) |
| [`06_data_governance/`](06_data_governance/) | DataHub lineage/contracts | 12 | ❌ not started — status note only, no evidence to show |
| [`07_documentation_schema_design/`](07_documentation_schema_design/) | Schema design (DBeaver) | 10 | 🟡 Gold zone fully visualized (2 real screenshots); real `FOREIGN KEY` constraints now declared in `build_gold.py` (code done) — Bronze/Silver zone and a re-captured dim/fact FK-relationship-lines screenshot still outstanding |
| [`08_novel_ideas/`](08_novel_ideas/) | Novel ideas | 10 | ✅ done — 2 real, already-implemented techniques with proof |

## What's still needed to close the remaining gap

All screenshot items follow the same pattern — code is done, the doc explains exactly what to
capture and where:

1. **Spark UI** (`03_processing_jobs/spark/`) — run `bronze_silver_README.md`'s baseline/
   optimized commands, capture the 4 before/after pairs listed in
   `02_spark_optimisation_report.md`'s Screenshot Checklist.
2. **Flink UI** (`03_processing_jobs/flink/README.md`) — same pattern, checklist near the
   bottom of that file. Pass `--web-ui` to the pipeline invocation to reach the UI at
   `http://localhost:8081` while the job runs (verified live — previously needed a manual
   code edit).
3. **Airflow UI** (`05_data_pipeline_orchestration/`) — open any DAG's **Graph view** (not
   the DAGs list) at `http://localhost:8081` — satisfies the literal rubric wording even
   without a live run, per the correction note in `dags_plan.md`.
4. **DBeaver** (`07_documentation_schema_design/`) — connect via Trino (host `localhost`,
   port `8080`) alongside the existing PostgreSQL connection, screenshot Bronze + Silver +
   Gold together. Separately, `build_gold.py` now declares real `FOREIGN KEY` constraints
   (code done, see `gold_README.md`'s "Dim/fact relationships" section) — re-run `build_gold.py`
   and re-open DBeaver's **View Diagram** on `gold_ecommerce` to capture the relationship
   lines; the existing screenshot predates this fix and still shows standalone boxes.

Data Governance (12 pts) is the only section that needs actual new code, not just a
screenshot — see `RUBRIC_STATUS.md` for what building it would involve.
