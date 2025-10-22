from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.orm import Session

from backend.app.db import models
from backend.app.db.session import session_scope

ZERO_JOB_ID = UUID("00000000-0000-0000-0000-000000000000")


def _ensure_utc(dt: Optional[datetime]) -> datetime:
    if dt is None:
        return datetime.now(timezone.utc)
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _as_decimal(value: Any) -> Optional[Decimal]:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except (TypeError, ValueError, ArithmeticError):
        return None


def _merge_vehicle(session: Session, vin: str, vehicle_data: Dict[str, Any]) -> models.Vehicle:
    vehicle = session.get(models.Vehicle, vin)
    if vehicle is None:
        vehicle = models.Vehicle(vin=vin, make=vehicle_data.get("make", ""), model=vehicle_data.get("model", ""))
        session.add(vehicle)
        session.flush()

    mutable_fields = [
        "make",
        "model",
        "year",
        "trim",
        "drivetrain",
        "transmission",
        "exterior_color",
        "interior_color",
        "msrp",
        "invoice_price",
        "features",
    ]
    for field in mutable_fields:
        value = vehicle_data.get(field)
        if field in {"msrp", "invoice_price"}:
            value = _as_decimal(value)
        if value is not None:
            setattr(vehicle, field, value)
    vehicle.updated_at = datetime.now(timezone.utc)
    return vehicle


def upsert_observations_and_listings(rows: List[Dict[str, Any]], source: str) -> Dict[str, int]:
    """Persist observation rows and keep listings/price events in sync.

    Args:
        rows: Parsed scrape outputs enriched with dealer_id, vin, price fields, etc.
        source: Logical source label (inventory_list, vdp, upload).

    Returns:
        Counts of effected entities for observability.
    """
    if not rows:
        return {"observations": 0, "listings_upserted": 0, "price_events": 0}

    observations_created = 0
    listings_upserted = 0
    price_events_created = 0

    with session_scope() as session:
        for row in rows:
            dealer_id = row["dealer_id"]
            vin = row["vin"].upper()
            observed_at = _ensure_utc(row.get("observed_at"))

            vehicle_data = row.get("vehicle") or {}
            vehicle = _merge_vehicle(session, vin, vehicle_data)

            advertised_price = _as_decimal(row.get("advertised_price"))
            msrp = _as_decimal(row.get("msrp"))
            payload = row.get("payload") or {}
            if advertised_price is None and msrp is not None:
                advertised_price = msrp
                payload = {**payload, "assumptions": {"ad_price_equals_msrp": True}}

            job_id = row.get("job_id")
            try:
                job_uuid = UUID(str(job_id)) if job_id else ZERO_JOB_ID
            except (ValueError, TypeError):
                job_uuid = ZERO_JOB_ID

            observation = models.Observation(
                job_id=job_uuid,
                observed_at=observed_at,
                dealer_id=dealer_id,
                vin=vin,
                vdp_url=row.get("vdp_url"),
                advertised_price=advertised_price,
                msrp=msrp,
                payload=payload,
                raw_blob_key=row.get("raw_blob_key"),
                source=row.get("source") or source,
            )
            session.add(observation)
            observations_created += 1

            listing = session.execute(
                select(models.Listing).where(
                    models.Listing.dealer_id == dealer_id,
                    models.Listing.vin == vin,
                )
            ).scalar_one_or_none()

            source_rank = row.get("source_rank")
            source_rank_value = int(source_rank) if source_rank is not None else None
            status = row.get("status") or "available"

            first_seen_at = row.get("first_seen_at")
            first_seen_at = _ensure_utc(first_seen_at) if first_seen_at else observed_at

            last_seen_at = row.get("last_seen_at")
            last_seen_at = _ensure_utc(last_seen_at) if last_seen_at else observed_at

            if listing is None:
                msrp_value = msrp if msrp is not None else vehicle.msrp
                price_delta = (advertised_price - msrp_value) if advertised_price is not None and msrp_value is not None else None

                listing = models.Listing(
                    dealer_id=dealer_id,
                    vin=vin,
                    vdp_url=row.get("vdp_url"),
                    stock_number=row.get("stock_number"),
                    status=status,
                    advertised_price=advertised_price,
                    price_delta_msrp=price_delta,
                    first_seen_at=first_seen_at,
                    last_seen_at=last_seen_at,
                    source_rank=source_rank_value or 100,
                )
                session.add(listing)
                listings_upserted += 1
            else:
                old_price = listing.advertised_price
                old_rank = listing.source_rank

                listing.vdp_url = row.get("vdp_url") or listing.vdp_url
                listing.stock_number = row.get("stock_number") or listing.stock_number
                listing.status = status or listing.status
                if advertised_price is not None:
                    listing.advertised_price = advertised_price

                msrp_value = msrp if msrp is not None else vehicle.msrp
                effective_price = listing.advertised_price
                if effective_price is not None and msrp_value is not None:
                    listing.price_delta_msrp = effective_price - msrp_value

                listing.first_seen_at = min(listing.first_seen_at, first_seen_at) if listing.first_seen_at else first_seen_at
                listing.last_seen_at = max(listing.last_seen_at, last_seen_at) if listing.last_seen_at else last_seen_at
                if source_rank_value is not None:
                    if old_rank is None or source_rank_value < old_rank:
                        listing.source_rank = source_rank_value

                if (
                    advertised_price is not None
                    and old_price is not None
                    and advertised_price != old_price
                ):
                    delta = advertised_price - old_price
                    pct = (delta / old_price * Decimal("100")) if old_price != 0 else None
                    price_event = models.PriceEvent(
                        dealer_id=dealer_id,
                        vin=vin,
                        observed_at=observed_at,
                        old_price=old_price,
                        new_price=advertised_price,
                        delta=delta,
                        pct=pct,
                    )
                    session.add(price_event)
                    price_events_created += 1

                listings_upserted += 1

    return {
        "observations": observations_created,
        "listings_upserted": listings_upserted,
        "price_events": price_events_created,
    }
