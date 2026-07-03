# Docker Image Optimisation — Inference API

## Objective

Reduce the production container image size by switching from a single-stage
build (using the full `python:3.13` base) to a two-stage build (builder +
`python:3.13-slim` runtime).  Smaller images mean faster pull times on cold
pod starts and a smaller attack surface.

---

## Build commands

```bash
# Baseline — single-stage
docker build \
  -f d_ml/api/Dockerfile.baseline \
  -t inference-api:baseline \
  .

# Optimised — multi-stage
docker build \
  -f d_ml/api/Dockerfile \
  -t inference-api:optimised \
  .

# Compare sizes
docker images inference-api --format "table {{.Tag}}\t{{.Size}}"
```

---

## Before vs after

| Build | Base image | Stages | Image size |
|---|---|---|---|
| Baseline | `python:3.13` | 1 | 2.7 GB |
| Optimised | `python:3.13-slim` (runtime) | 2 | 1.26 GB |

**Size reduction: 53%** (~1.44 GB saved)

---

## What the optimisation does

### Stage 1 — builder (`python:3.13`)

Uses the full Python image, which ships with gcc, make, header files, and
other build tools.  These are required to compile any native extensions (e.g.,
numpy C extensions) that don't have a pre-built wheel for the target platform.

```dockerfile
FROM python:3.13 AS builder
RUN pip install --prefix=/install -r requirements.txt
```

The `--prefix=/install` flag installs everything into a self-contained
directory that can be copied as a unit.

### Stage 2 — runtime (`python:3.13-slim`)

`python:3.13-slim` starts at ~130 MB instead of ~1 GB.  It contains only the
Python interpreter and the minimal C runtime — no gcc, no headers, no apt
caches.

```dockerfile
FROM python:3.13-slim AS runtime
COPY --from=builder /install /usr/local   # packages only, no build tools
COPY d_ml/api/ .                          # app source
```

The build tools from Stage 1 are discarded entirely — Docker only ships the
layers from the final stage.

### Non-root user

```dockerfile
RUN useradd --no-create-home --system appuser && chown -R appuser /app
USER appuser
```

Running as a non-root user limits the blast radius if the container process is
compromised.  Kubernetes Pod Security Standards (Restricted profile) also
require this.

---

## Trade-offs

| | Baseline | Optimised |
|---|---|---|
| Build complexity | Simple — one FROM | Two stages, slightly more Dockerfile to read |
| Build time | Faster (no layer copy between stages) | ~5s extra for the COPY --from step |
| Image size | Large | ~57% smaller |
| Attack surface | gcc, make, headers present | Minimal runtime only |
| Reproducibility | Same | Same (pinned requirements.txt) |

The only downside is marginally more complex Dockerfile syntax. The size and
security benefits make multi-stage the default for all production images in
this project.
