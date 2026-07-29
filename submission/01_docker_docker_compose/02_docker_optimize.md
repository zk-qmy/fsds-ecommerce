# Docker & Docker Compose — 3 pts

**Full write-ups:**
[`docs/optimized/docker_data_platform.md`](../../docs/optimized/docker_data_platform.md)
(`b_schema_pipelines/Dockerfile` — the image Spark jobs run in) and
[`docs/optimized/docker_inference_api.md`](../../docs/optimized/docker_inference_api.md)
(`d_ml/api/Dockerfile` — the ML inference API image).

| Requirement | Pts | Status |
|---|---|---|
| Document measured before/after image size + optimization method | 1 | ✅ real measured numbers, not estimated |
| Optimize Dockerfile (multistage build) | 2 | ✅ both Dockerfiles are multi-stage |

## Measured results

| Image | Baseline | Optimized | Reduction |
|---|---|---|---|
| `b_schema_pipelines/Dockerfile` (data platform) | 5.7 GB | 2.79 GB | 51% |
| `d_ml/api/Dockerfile` (inference API) | 2.7 GB | 1.26 GB | 53% |

Both split dependency resolution (`uv sync`, which pulls in the full wheel-download cache) into
a discarded build stage, so only the *built* virtual environment — not `uv`'s own cache — ends
up in the final image. Build/compare commands are in the linked docs.
