from fastapi import APIRouter

router = APIRouter()

@router.get("/{vin}")
async def vin_detail(vin: str):
    """Return vehicle spec, active listings, price timeline, last observation."""
    return {"vin": vin, "vehicle": {}, "listings": [], "price_events": []}
