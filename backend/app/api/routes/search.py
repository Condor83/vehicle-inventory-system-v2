from __future__ import annotations

from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from backend.app.db import models
from backend.app.db.session import get_session

MAX_PAGE_SIZE = 200
DEFAULT_PAGE_SIZE = 50

SORT_OPTIONS = {
    "delta_vs_msrp_asc": lambda: (
        models.Listing.price_delta_msrp.asc().nulls_last(),
        models.Listing.advertised_price.asc().nulls_last(),
    ),
    "delta_vs_msrp_desc": lambda: (
        models.Listing.price_delta_msrp.desc().nulls_last(),
        models.Listing.advertised_price.asc().nulls_last(),
    ),
    "price_asc": lambda: (models.Listing.advertised_price.asc().nulls_last(),),
    "price_desc": lambda: (models.Listing.advertised_price.desc().nulls_last(),),
    "last_seen_desc": lambda: (models.Listing.last_seen_at.desc(),),
}

router = APIRouter()

@router.get("")
async def search(
    model: Optional[str] = None,
    year: Optional[int] = None,
    trim: Optional[str] = None,
    region: Optional[str] = None,
    features: Optional[List[str]] = Query(default=None),
    below_msrp: bool = False,
    status: str = "available",
    page: int = 1,
    size: int = 50,
    sort: str = "delta_vs_msrp_asc",
    db: Session = Depends(get_session),
):
    if page < 1 or size < 1:
        raise HTTPException(status_code=400, detail="page and size must be >= 1")
    size = min(size, MAX_PAGE_SIZE)

    stmt = (
        select(models.Listing, models.Vehicle, models.Dealer)
        .join(models.Vehicle, models.Vehicle.vin == models.Listing.vin)
        .join(models.Dealer, models.Dealer.id == models.Listing.dealer_id)
    )

    if status and status.lower() != "all":
        stmt = stmt.where(models.Listing.status == status.lower())

    if model:
        stmt = stmt.where(models.Vehicle.model.ilike(model))

    if year:
        stmt = stmt.where(models.Vehicle.year == year)

    if trim:
        stmt = stmt.where(models.Vehicle.trim.ilike(f"%{trim}%"))

    if region:
        stmt = stmt.where(models.Dealer.region == region)

    if features:
        for feature in features:
            stmt = stmt.where(models.Vehicle.features.contains([feature]))

    if below_msrp:
        stmt = stmt.where(models.Listing.price_delta_msrp < 0)

    sort_fn = SORT_OPTIONS.get(sort)
    if not sort_fn:
        raise HTTPException(status_code=400, detail=f"Unsupported sort option '{sort}'")

    filtered_stmt = stmt

    total = db.execute(select(func.count()).select_from(filtered_stmt.subquery())).scalar_one()

    order_by = sort_fn()
    stmt = filtered_stmt.order_by(*order_by)
    stmt = stmt.offset((page - 1) * size).limit(size)

    results = db.execute(stmt).all()

    rows = []
    for listing, vehicle, dealer in results:
        advertised_price = float(listing.advertised_price) if listing.advertised_price is not None else None
        msrp_value = float(vehicle.msrp) if vehicle.msrp is not None else None
        price_delta = float(listing.price_delta_msrp) if listing.price_delta_msrp is not None else None

        rows.append(
            {
                "vin": listing.vin,
                "dealer_id": listing.dealer_id,
                "dealer_name": dealer.name,
                "region": dealer.region,
                "model": vehicle.model,
                "year": vehicle.year,
                "trim": vehicle.trim,
                "status": listing.status,
                "advertised_price": advertised_price,
                "msrp": msrp_value,
                "price_delta_msrp": price_delta,
                "vdp_url": listing.vdp_url,
                "first_seen_at": listing.first_seen_at.isoformat() if listing.first_seen_at else None,
                "last_seen_at": listing.last_seen_at.isoformat() if listing.last_seen_at else None,
                "features": vehicle.features,
                "below_msrp": price_delta is not None and price_delta < 0,
            }
        )

    return {
        "page": page,
        "size": size,
        "total": total,
        "rows": rows,
        "applied_filters": {
            "model": model,
            "year": year,
            "trim": trim,
            "region": region,
            "features": features,
            "below_msrp": below_msrp,
            "status": status,
            "sort": sort,
        },
    }
