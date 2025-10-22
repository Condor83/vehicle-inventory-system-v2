from datetime import datetime, timezone, timedelta
from decimal import Decimal
import uuid

from sqlalchemy import text, select

from backend.app.db.session import session_scope
from backend.app.db import models
from backend.app.services.ingest import upsert_observations_and_listings


def _truncate_tables():
    with session_scope() as session:
        session.execute(
            text(
                "TRUNCATE TABLE observations, price_events, listings, vehicles RESTART IDENTITY CASCADE"
            )
        )
        session.execute(
            text(
                "TRUNCATE TABLE dealers RESTART IDENTITY CASCADE"
            )
        )


def _create_dealer():
    with session_scope() as session:
        dealer = models.Dealer(
            name="Test Dealer",
            backend_type="DEALER_INSPIRE",
            region="Mountain",
            homepage_url="https://dealer.test",
        )
        session.add(dealer)
        session.flush()
        dealer_id = dealer.id
    return dealer_id


def test_ingest_creates_listing_observation_and_price_event():
    _truncate_tables()
    dealer_id = _create_dealer()
    observed_at = datetime(2025, 10, 21, 12, 0, tzinfo=timezone.utc)

    first_rows = [
        {
            "dealer_id": dealer_id,
            "vin": "JTENU5JR4R5299999",
            "advertised_price": 47500,
            "msrp": 51230,
            "vdp_url": "https://dealer.test/vdp/JTENU5JR4R5299999",
            "status": "available",
            "observed_at": observed_at,
            "job_id": str(uuid.uuid4()),
            "source": "inventory_list",
            "source_rank": 50,
            "vehicle": {
                "make": "Toyota",
                "model": "4Runner",
                "year": 2024,
                "trim": "SR5",
            },
        }
    ]

    summary = upsert_observations_and_listings(first_rows, source="inventory_list")
    assert summary == {"observations": 1, "listings_upserted": 1, "price_events": 0}

    with session_scope() as session:
        listing = session.execute(
            select(models.Listing).where(
                models.Listing.dealer_id == dealer_id,
                models.Listing.vin == "JTENU5JR4R5299999",
            )
        ).scalar_one()
        assert listing.advertised_price == Decimal("47500")
        assert listing.price_delta_msrp == Decimal("47500") - Decimal("51230")
        assert listing.first_seen_at == observed_at
        assert listing.last_seen_at == observed_at

    second_observed_at = observed_at + timedelta(days=1)
    second_rows = [
        {
            "dealer_id": dealer_id,
            "vin": "JTENU5JR4R5299999",
            "advertised_price": 46950,
            "msrp": 51230,
            "vdp_url": "https://dealer.test/vdp/JTENU5JR4R5299999",
            "status": "available",
            "observed_at": second_observed_at,
            "job_id": str(uuid.uuid4()),
            "source": "inventory_list",
            "source_rank": 10,
            "vehicle": {},
        }
    ]

    summary = upsert_observations_and_listings(second_rows, source="inventory_list")
    assert summary == {"observations": 1, "listings_upserted": 1, "price_events": 1}

    with session_scope() as session:
        listing = session.execute(
            select(models.Listing).where(
                models.Listing.dealer_id == dealer_id,
                models.Listing.vin == "JTENU5JR4R5299999",
            )
        ).scalar_one()
        assert listing.advertised_price == Decimal("46950")
        assert listing.source_rank == 10  # source rank lowered
        assert listing.first_seen_at == observed_at
        assert listing.last_seen_at == second_observed_at

        price_event = session.execute(select(models.PriceEvent)).scalar_one()
        assert price_event.old_price == Decimal("47500")
        assert price_event.new_price == Decimal("46950")
