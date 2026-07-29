# Data Governance — 12 pts

**Full write-up:** [`b_schema_pipelines/dq/README.md`](../../b_schema_pipelines/dq/README.md)

**Honest status: not started.** No `emit_lineage()` call or DataHub integration exists
anywhere in this codebase (confirmed by repo-wide search). This section requires standing up
a DataHub instance (GMS + frontend), adding a lineage-emission call to every Bronze/Silver/
Gold/Feature job, and linking each Great Expectations suite here as a DataHub dataset
assertion — none of which exists yet.

**What does exist**, and covers the "data validation" half in spirit (not the DataHub-linked
"data contract" half, and not lineage at all): a full Great Expectations suite factory per
layer (`bronze_suite.py`/`silver_suite.py`/`gold_suite.py`) — schema, null-PK, uniqueness,
referential integrity, and volume checks — called by `validation_runner.py` from each DAG's
`validate_*` task. Confirmed live 2026-07-29: `dp1_bronze → dp2_gold → dp3_feature` ran
end-to-end against the real docker-compose stack with all `validate_*` tasks passing for real,
not just via unit tests (29 suite-factory tests + DAG graph tests, all passing).

This is real, working data-quality tooling — just not the DataHub-linked governance layer the
rubric specifically asks for. See the linked doc for the full check-by-layer table and "What's
still open."
