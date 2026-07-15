# Reference Review — L9 (Airflow Orchestration) + L10 (Data Validation)

**Scope note on inputs.** The task named four inputs as `[PATH]` placeholders that were
never filled in. They were resolved from context as follows — stated explicitly rather than
assumed silently:

| Requested input | Resolved to |
|---|---|
| Reference implementation folder | `references/k2/L9-orchestration-airflow/pipeline-orchestration-with-airflow/` (primary — matches the IDE's open file and the branch's new `dags/`/`dq/` work). `references/k2/L10-data-validation/data-validation/` pulled in secondarily because it's the only other reference folder covering validation, and the branch's new `validation_runner.py` is a validation-flow deliverable. |
| Current project plan / design docs | `b_schema_pipelines/dags/plan.md`, `b_schema_pipelines/docs/02_schema_piplines.md`, `b_schema_pipelines/dq/README.md` |
| Current repo structure | `CLAUDE.md` repo-structure section + the actual `b_schema_pipelines/` tree on disk |
| Rubric / grading criteria | `coursework/rubrics.md` (the literal scored rubric — confirmed this is what `plan.md` itself cites, not `CLAUDE.md`'s grading checklist, which is a superset for the whole multi-section course) |

If this mapping is wrong, the findings below should be re-scoped accordingly.

---

## 1. Overall assessment

Both reference folders are single-lesson course demos (Full Stack Data Science / K2 coursework), not production-grade reference architectures — L9 is three toy DAGs (`demo.py`, `python.py`, `sklearn.py`) teaching operator *types* (Bash, `@task`, `@task.virtualenv`, `DockerOperator`), and L10 is a Deequ/GX notebook walkthrough against NYC taxi data with no Airflow integration at all. Neither has tests, a config-management pattern, retry policy, or an equivalent of the current project's DP1/DP2/DP3 dependency chain. The current branch's implementation (`plan.md`, the three DAG files, `validation_runner.py`, 236-line test suite) is already substantially more rigorous than either reference on every axis the task asked to prioritize. This reference is useful only as a primer on raw Airflow operator syntax and as a reminder of what "bare minimum" looks like — it should not move the implementation plan.

---

## 2. Impact on the implementation plan

### Must change

**None.** Nothing in either reference folder surfaces a correctness gap, a rubric-conflict, or a best-practice violation in the current plan that isn't already handled. The one genuine gap found during this review (below) originates from the current repo's own CI setup, not from anything the reference suggests changing — flagged under §4 (Risks) instead, since it's not "adopt X from the reference."

### Should change

None driven by the reference. The reference's patterns are all either already superseded by the current plan's own explicit design decisions (see §3) or inapplicable (L10's Deequ/pydeequ path was evaluated and consciously not carried forward — the project already standardized on Great Expectations, which is the right call: one validation framework, not two, and GX's suite-factory pattern in `dq/` is considerably more testable than L10's inline notebook checks).

### Nice to have

| Item | What | Why | Benefit | Affects | Rubric tie |
|---|---|---|---|---|---|
| 1 | Adopt the reference's `@task.virtualenv` / `ExternalPythonOperator` framing more explicitly in `dags/plan.md`'s docstrings as a one-line "why not just `PythonOperator`" note | L9's `python.py` DAG demonstrates `@task.virtualenv` for the exact problem the current plan solves differently (`ExternalPythonOperator` against a pre-built venv, per `plan.md` §4). The current approach is better for this project (no per-task venv rebuild cost, deterministic dependency set) but the reference is a fine one-line citation for *why* two mechanisms exist for the same problem, if a grader ever asks. | Marginal — documentation clarity only | Documentation | Not rubric-scored |
| 2 | None from L10 worth adopting | Deequ's `ColumnProfilerRunner`/`AnalysisRunner` (data_profiling.py) is a nice ad-hoc profiling tool, but the project's `dq/` suites already assert on the *same* signals (completeness, row count, uniqueness) in a way that's unit-testable and CI-checkable, which raw Deequ profiling isn't without a persisted checkpoint. Not worth introducing a second data-quality library for overlapping coverage. | — | — | — |

### Rubric coverage check (for completeness, not new work)

`coursework/rubrics.md`'s six DP1/DP2/DP3 orchestration lines (12 pts total) require only an Airflow UI screenshot per DAG showing stages + order, and the rubric's one binding technical constraint — "all connection and variables should be put inside Airflow" — is already satisfied (`plan.md` §10: `repo_root` Variable, `fsds_postgres`/`fsds_minio` Connections, never hardcoded in DAG files). Neither reference folder changes this assessment; L9 doesn't demonstrate Variables/Connections usage at all (its DAGs have no external service dependencies to configure).

---

## 3. Things to ignore

Explicitly not adopting the following, and why:

- **L9's flat, unrelated-DAGs structure** (`demo`, `python`, `sklearn`, `docker` — no DAG depends on another). The current project's DP1→DP2→DP3 chain via `ExternalTaskSensor` is a materially harder problem the reference never attempts; there's nothing to transfer.
- **`DockerOperator` + a `docker-proxy` (`bobrik/socat`) sidecar for Docker-in-Docker task isolation.** `plan.md` §3/§12 already considered and explicitly rejected `DockerOperator` — no task in the current plan needs per-task container isolation, and the socket-proxy pattern is real added complexity (a new container, a Docker socket mount, a "fix permission denied" workaround) with no corresponding rubric line. Correct call to leave out.
- **`airflow-provider-great-expectations` (the GX Airflow provider), pinned to an old, security-relevant commit SHA** (`pip install git+...@87a42e2...` rather than a released version) in L9's Dockerfile. This provider expects a persisted `great_expectations.yml` FileDataContext + Checkpoint model — the current project's ephemeral, parameter-injected suite factories (`dq/common.py`'s `new_suite`) are a deliberate, already-justified alternative (`plan.md` §12). Installing from an unreleased commit pin is itself a practice to avoid, not copy.
- **L9's plaintext, git-committed `.env`** (`AIRFLOW_UID=1000`, and L10's `.env` with `POSTGRES_PASSWORD=k6` in cleartext) and the CeleryExecutor stack's default `_AIRFLOW_WWW_USER_PASSWORD: airflow`. This is fine for a disposable classroom demo; the current project's `pipeline_config.yaml` + Airflow Connections/Variables approach (never hardcoding credentials in DAG files, per CLAUDE.md's Vault rule for the deployed path) is already stricter and shouldn't regress toward the reference's pattern.
- **CeleryExecutor + Redis + 5 separate Airflow containers** (webserver/scheduler/worker/triggerer/init) in L9's `airflow-docker-compose.yaml`. This is Airflow's own "production-shaped local demo" boilerplate. The current plan's single-container `LocalExecutor` setup (`infra/docker-compose.yml`'s `airflow` service) is the right scope for a local-dev, coursework-grade stack — CLAUDE.md itself designates `infra/docker-compose.yml` as local-dev-only, and Celery/Redis buys nothing here since there's no multi-worker concurrency need.
- **L10's conda-env + local Jupyter notebook workflow** for running Deequ/GX. Not compatible with this project's `uv`-managed, scripted, testable pipeline pattern — adopting it would reintroduce exactly the "not reproducible from a fresh clone" problem CLAUDE.md's grading checklist explicitly guards against ("All code runnable from a fresh clone").
- **Hardcoded absolute host paths**, e.g. `lightgbm/docker.py`'s `TRAINING_DIR = "/home/quandv/Documents/fsds/m2/..."`. The current plan's `{{ var.value.repo_root }}` templating already avoids this class of bug entirely — worth naming as a concrete anti-pattern the current plan has already dodged.

---

## 4. Risks

- **Independent of the reference, but surfaced by comparing the two:** `tests/dags/test_dags.py` is real and reasonably thorough (import-error check, task-graph-matches-plan check, retry-policy check, schedule check — directly answering CLAUDE.md's CI requirement "DAG import check (no circular dependencies)"), but it is gated behind `pytest.importorskip("airflow", ...)`, and the only CI workflow present in the repo (`.github/workflows/ci.yml`) runs `uv run pytest tests/` in the main Python 3.13 venv, where `apache-airflow` is deliberately *not* installed (by design, per `plan.md` §3, to avoid polluting the project's own lockfile). Net effect: **these DAG tests currently never execute in CI** — they silently skip every run, and nothing in the pipeline calls the documented workaround command (`uv run --no-project --python 3.12 --with apache-airflow==2.10.5 ...`). This is a real gap against CLAUDE.md's Track A CI requirement and against `plan.md` §13's own stated test plan, worth fixing (add a second CI job using that exact `--no-project --python 3.12` invocation) regardless of what the reference does or doesn't demonstrate — L9 has no CI at all, so it offers no template to fix this with; it just made the gap visible while reading the test docstring.
- **Two-Python-environment design (`plan.md` §4) is inherently more fragile than L9's single-environment model**, precisely because it solves a harder problem (project code needs 3.13, Airflow needs 3.12). `plan.md` §15 already flags the `ExternalPythonOperator` + GX ephemeral-context path as "a design, not yet a proven spike" — that's the correct level of caution; this review doesn't add new risk here beyond noting the CI gap above compounds it (an environment-wiring bug wouldn't be caught automatically today, only by the manual `airflow dags test` step in `plan.md` §13's integration test row, which likewise has no CI job).
- **`GOLD_TABLE_COLUMNS` in `validation_runner.py` is a manually-synced duplicate of `build_gold.py`'s inline `.select(...)` lists** (already self-documented as a known limitation in `plan.md` §9). Not reference-driven, but worth flagging as a maintenance risk: nothing fails loudly if the two drift except a silently-wrong schema check. No reference example offers a better pattern for this — it's a real, project-specific trade-off already made with eyes open.

---

## 5. Recommendation

**Keep the current implementation plan unchanged.**

Neither reference folder is authoritative or production-representative enough to justify a plan change — they are single-lesson teaching demos with no tests, no config-management discipline, and no multi-DAG dependency pattern to draw from. The current plan (`dags/plan.md`) already exceeds both references on Airflow DAG design (dependency chains, retry policy, Variables/Connections discipline), validation flow (GX suite factories vs. L10's ad-hoc notebook checks), and testing strategy (236 lines of `validation_runner` tests + a dedicated `tests/dags/test_dags.py` vs. zero tests in either reference). The one actionable finding from this review — the CI gap that silently skips the DAG tests — is not something the reference surfaces or fixes; it's worth raising to the user as a separate, small follow-up, not as a reason to revisit this plan.
