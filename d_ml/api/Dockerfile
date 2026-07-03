# ── Stage 1: builder ─────────────────────────────────────────────────────────
# Full Python image has gcc and other build tools needed to compile any
# packages that ship without pre-built wheels on this platform.
# Everything installed here stays OUT of the final image.
FROM python:3.13 AS builder

WORKDIR /build

COPY d_ml/api/requirements.txt .

# Install into an isolated prefix so we can copy just the packages
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt


# ── Stage 2: runtime ─────────────────────────────────────────────────────────
# python:3.13-slim omits gcc, libgomp, header files, and other build-time
# artefacts — saving ~800 MB compared to the full image.
FROM python:3.13-slim AS runtime

WORKDIR /app

# Copy only the installed packages from the builder stage
COPY --from=builder /install /usr/local

# Copy application source
COPY d_ml/api/ .

# Non-root user — principle of least privilege
RUN useradd --no-create-home --system appuser \
    && chown -R appuser /app
USER appuser

EXPOSE 8000
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
