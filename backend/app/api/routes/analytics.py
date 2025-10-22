from fastapi import APIRouter

router = APIRouter()

@router.get("/sold")
async def sold(model: str, period: str = "last_month"):
    return {"model": model, "period": period, "sold_count": 0}

@router.get("/top-features")
async def top_features(model: str, period: str = "last_month"):
    return {"model": model, "period": period, "features": []}
