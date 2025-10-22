from datetime import datetime, timezone
import uuid

from fastapi.testclient import TestClient
from sqlalchemy import text

from backend.app.api.main import app
from backend.app.db import models
from backend.app.db.session import session_scope
from backend.app.services.ingest import upsert_observations_and_listings


client = TestClient(app)


def _truncate_tables():
    with session_scope() as session:
        session.execute(
            text(
                "TRUNCATE TABLE observations, price_events, listings, vehicles, dealers RESTART IDENTITY CASCADE"
            )
        )


def _seed_inventory():
    with session_scope() as session:
        dealer = models.Dealer(
            name="Finder Toyota",
            backend_type="DEALER_INSPIRE",
            region="Mountain",
            homepage_url="https://finder.toyota",
        )
        session.add(dealer)
        session.flush()
        dealer_id = dealer.id

    now = datetime.now(timezone.utc)
    rows = [
        {
            "dealer_id": dealer_id,
            "vin": "JTENU5JR4R5299999",
            "advertised_price": 47000,
            "msrp": 48000,
            "vdp_url": "https://finder.toyota/vdp/jtenu5jr4r5299999",
            "status": "available",
            "observed_at": now,
            "job_id": str(uuid.uuid4()),
            "source": "inventory_list",
            "source_rank": 20,
            "vehicle": {
                "make": "Toyota",
                "model": "4Runner",
                "year": 2024,
                "trim": "SR5",
                "features": ["Moonroof", "Heated Seats"],
            },
        },
        {
            "dealer_id": dealer_id,
            "vin": "JTEABFAJ9RK001234",
            "advertised_price": 52000,
            "msrp": 50000,
            "vdp_url": "https://finder.toyota/vdp/jteabfaj9rk001234",
            "status": "available",
            "observed_at": now,
            "job_id": str(uuid.uuid4()),
            "source": "inventory_list",
            "source_rank": 30,
            "vehicle": {
                "make": "Toyota",
                "model": "4Runner",
                "year": 2025,
                "trim": "Limited",
                "features": ["Leather", "Moonroof"],
            },
        },
    ]
    upsert_observations_and_listings(rows, source="inventory_list")


def test_search_returns_results_with_filters():
    _truncate_tables()
    _seed_inventory()

    response = client.get("/search", params={"model": "4Runner"})
    assert response.status_code == 200
    data = response.json()
    assert data["total"] == 2
    assert len(data["rows"]) == 2
    assert data["rows"][0]["vin"] == "JTENU5JR4R5299999"
    assert data["rows"][0]["below_msrp"] is True

    response = client.get("/search", params={"below_msrp": True})
    assert response.status_code == 200
    data = response.json()
    assert data["total"] == 1
    assert data["rows"][0]["vin"] == "JTENU5JR4R5299999"

    response = client.get("/search", params={"features": ["Leather"]})
    assert response.status_code == 200
    data = response.json()
    vins = [row["vin"] for row in data["rows"]]
    assert "JTEABFAJ9RK001234" in vins
    assert "JTENU5JR4R5299999" not in vins
