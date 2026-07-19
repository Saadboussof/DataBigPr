from fastapi import APIRouter, Depends, Query
from auth.jwt_handler import verify_token
from vehicles.service import get_vehicles_in_zone

router = APIRouter(prefix="/api/vehicles", tags=["Vehicles"])

@router.get("")
def list_vehicles(zone: int = Query(..., ge=1, le=17), user=Depends(verify_token)):
    return get_vehicles_in_zone(zone)
