from fastapi import APIRouter, Depends
from pydantic import BaseModel, Field
from auth.jwt_handler import verify_token
from trips.service import publish_trip_request

router = APIRouter(prefix="/api/trips", tags=["Trips"])

class ReserveRequest(BaseModel):
    origin_zone: int = Field(..., ge=1, le=17, description="Pickup zone ID (1-17)")
    destination_zone: int = Field(..., ge=1, le=17, description="Destination zone ID (1-17)")

@router.post("/reserve")
def reserve_trip(req: ReserveRequest, user=Depends(verify_token)):
    return publish_trip_request(req.origin_zone, req.destination_zone, user["sub"])
