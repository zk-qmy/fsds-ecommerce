"""Inference services — feature fetch + model predict.

Full implementation wired to Feast online store and MLflow registry in Step 16.
Stubs here allow the container to build and /health to respond.
"""

from __future__ import annotations


async def fetch_features(customer_id: str) -> list[float]:
    """Fetch online features for customer_id from Feast online store."""
    # Step 16: replace with feast.FeatureStore().get_online_features(...)
    raise NotImplementedError("Feast feature fetch not yet wired")


async def predict(features: list[float]) -> tuple[float, str]:
    """Run model inference and return (score, model_version)."""
    # Step 16: replace with mlflow.pyfunc.load_model(...).predict(features)
    raise NotImplementedError("MLflow model not yet loaded")
