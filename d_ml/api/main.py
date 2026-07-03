"""Inference API — FastAPI application.

Endpoints:
    GET  /health   liveness check (no auth required)
    POST /score    purchase-probability score for a single customer (Bearer auth)
"""

from __future__ import annotations

from fastapi import Depends, FastAPI, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from schemas import ScoreRequest, ScoreResponse
from services import fetch_features, predict

app = FastAPI(title="FSDS Inference API", version="0.1.0")
_bearer = HTTPBearer()


def _verify_token(creds: HTTPAuthorizationCredentials = Depends(_bearer)) -> str:
    """Validate Bearer token against the value injected by Vault Agent."""
    import os

    expected = os.getenv("API_TOKEN", "")
    if not expected or creds.credentials != expected:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")
    return creds.credentials


@app.get("/health", status_code=200)
async def health() -> dict:
    """Liveness probe — Kubernetes readiness/liveness check."""
    return {"status": "ok"}


@app.post("/score", response_model=ScoreResponse)
async def score(
    request: ScoreRequest,
    _token: str = Depends(_verify_token),
) -> ScoreResponse:
    """Return purchase-probability score for the given customer_id."""
    features = await fetch_features(request.customer_id)
    probability, model_version = await predict(features)
    return ScoreResponse(
        customer_id=request.customer_id,
        score=probability,
        model_version=model_version,
    )
