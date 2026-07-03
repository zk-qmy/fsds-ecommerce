from pydantic import BaseModel


class ScoreRequest(BaseModel):
    customer_id: str


class ScoreResponse(BaseModel):
    customer_id: str
    score: float
    model_version: str
