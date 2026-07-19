from fastapi import APIRouter, Depends
from pydantic import BaseModel, Field
from auth.jwt_handler import require_role
from forecast.service import predict_demand

router = APIRouter(prefix="/api/demand", tags=["Forecast"])

class ForecastRequest(BaseModel):
    zone_id: int = Field(..., ge=1, le=17, description="Zone ID (1-17)")
    datetime: str = Field(..., description="Target ISO datetime, e.g. 2026-06-19T10:30:00")

@router.post("/forecast")
def forecast(req: ForecastRequest, user=Depends(require_role("admin"))):
    return predict_demand(req.zone_id, req.datetime)
