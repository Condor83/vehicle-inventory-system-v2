import io
from decimal import Decimal

import pandas as pd
from sqlalchemy import text, select

from backend.app.db import models
from backend.app.db.session import session_scope
from backend.app.services.upload_ingest import (
    ingest_vehicle_locator_upload,
    SOURCE_RANK_UPLOAD,
)


def _truncate_tables() -> None:
    with session_scope() as session:
        session.execute(
            text(
                "TRUNCATE TABLE observations, price_events, listings, vehicles, uploads RESTART IDENTITY CASCADE"
            )
        )
        session.execute(text("TRUNCATE TABLE dealers RESTART IDENTITY CASCADE"))


def _create_dealer(dealer_id: int, dealer_code: str, name: str) -> None:
    with session_scope() as session:
        dealer = models.Dealer(
            id=dealer_id,
            name=name,
            code=dealer_code,
            backend_type="DEALER_INSPIRE",
            region="",
            homepage_url="https://example.com",
        )
        session.add(dealer)


def _build_vehicle_locator_bytes(rows: list[dict[str, object]]) -> bytes:
    df = pd.DataFrame(rows)
    buffer = io.BytesIO()
    df.to_excel(buffer, index=False)
    return buffer.getvalue()


def test_vehicle_locator_ingest_creates_records_and_persists_payload():
    _truncate_tables()
    _create_dealer(1, "43019", "TONY DIVINO TOYOTA")

    rows = [
        {
            "Model Name": "Land Cruiser",
            "Model": "6167C",
            "Ext.": "02ZC",
            "Int.": "LB20",
            "Yr.": 2025,
            "VIN": "JTEABFAJ5SK049786",
            "Cat.": "G",
            "DIS": 1,
            "Accessories": "FE AA BG",
            "Invoice": "66,149.34",
            "Total SRP": "71,045.00",
            "Presold": False,
            "Distance": 0,
            "Dealer Name": "TONY DIVINO TOYOTA",
            "Dealer Code": 43019,
            "Res.": "N",
            "Cmpgn.": "No campaign",
            "Region": "DVR",
            "District": 4,
            "Dealer Phone": "801.394.5701",
            "Est. Arrival": "09/26/2025",
        },
        {
            "Model Name": "Land Cruiser",
            "Model": "6167C",
            "Ext.": "02ZD",
            "Int.": "LB20",
            "Yr.": 2025,
            "VIN": "JTEABFAJ0SK049596",
            "Cat.": "G",
            "DIS": 1,
            "Accessories": "FE AA BG",
            "Invoice": "67,962.34",
            "Total SRP": "73,290.00",
            "Presold": True,
            "Distance": 0,
            "Dealer Name": "TONY DIVINO TOYOTA",
            "Dealer Code": 43019,
            "Res.": "Y",
            "Cmpgn.": "No campaign",
            "Region": "DVR",
            "District": 4,
            "Dealer Phone": "801.394.5701",
            "Est. Arrival": "09/27/2025",
        },
    ]

    content = _build_vehicle_locator_bytes(rows)
    summary = ingest_vehicle_locator_upload("Vehicle Locator.xlsx", content)

    assert summary["rows_ingested"] == 2
    assert summary["rows_updated"] == 0
    assert summary["status"] == "completed"
    assert summary["row_errors"] == []

    with session_scope() as session:
        first_vehicle = session.get(models.Vehicle, "JTEABFAJ5SK049786")
        assert first_vehicle is not None
        assert first_vehicle.invoice_price == Decimal("66149.34")
        assert first_vehicle.features["vehicle_locator"]["Dealer Code"] == 43019

        second_vehicle = session.get(models.Vehicle, "JTEABFAJ0SK049596")
        assert second_vehicle is not None
        assert second_vehicle.invoice_price == Decimal("67962.34")

        listing = session.execute(
            select(models.Listing).where(models.Listing.vin == "JTEABFAJ5SK049786")
        ).scalar_one()
        assert listing.status == "in_transit"
        assert listing.source_rank == SOURCE_RANK_UPLOAD

        pending_listing = session.execute(
            select(models.Listing).where(models.Listing.vin == "JTEABFAJ0SK049596")
        ).scalar_one()
        assert pending_listing.status == "pending"

        obs_count = session.execute(select(models.Observation)).scalars().all()
        assert len(obs_count) == 2
        observation = obs_count[0]
        assert observation.source == "upload"
        assert observation.payload["vehicle_locator"]["Dealer Phone"] == "801.394.5701"

        upload = session.get(models.Upload, summary["upload_id"])
        assert upload.status == "completed"
        assert upload.rows_ingested == 2
        assert upload.rows_updated == 0
        assert upload.row_errors == []
        assert upload.processed_at is not None

        dealer = session.get(models.Dealer, 1)
        assert dealer.phone == "801.394.5701"
        assert dealer.region == "DVR"
        assert dealer.district_code == "4"


def test_reupload_updates_existing_vins_and_records_errors_for_duplicates():
    _truncate_tables()
    _create_dealer(1, "43019", "TONY DIVINO TOYOTA")

    base_rows = [
        {
            "Model Name": "Land Cruiser",
            "Model": "6167C",
            "Ext.": "02ZC",
            "Int.": "LB20",
            "Yr.": 2025,
            "VIN": "JTEABFAJ5SK049786",
            "Cat.": "G",
            "DIS": 1,
            "Accessories": "FE AA BG",
            "Invoice": "66,149.34",
            "Total SRP": "71,045.00",
            "Presold": False,
            "Distance": 0,
            "Dealer Name": "TONY DIVINO TOYOTA",
            "Dealer Code": 43019,
            "Res.": "N",
            "Cmpgn.": "No campaign",
            "Region": "DVR",
            "District": 4,
            "Dealer Phone": "801.394.5701",
            "Est. Arrival": "09/26/2025",
        }
    ]

    first_content = _build_vehicle_locator_bytes(base_rows)
    first_summary = ingest_vehicle_locator_upload("Vehicle Locator.xlsx", first_content)
    assert first_summary["rows_ingested"] == 1
    assert first_summary["rows_updated"] == 0

    updated_rows = base_rows + [
        {
            "Model Name": "Land Cruiser",
            "Model": "6167C",
            "Ext.": "02ZC",
            "Int.": "LB20",
            "Yr.": 2025,
            "VIN": "JTEABFAJ5SK049786",
            "Cat.": "G",
            "DIS": 1,
            "Accessories": "FE AA BG",
            "Invoice": "66,200.00",
            "Total SRP": "71,045.00",
            "Presold": False,
            "Distance": 0,
            "Dealer Name": "TONY DIVINO TOYOTA",
            "Dealer Code": 43019,
            "Res.": "N",
            "Cmpgn.": "No campaign",
            "Region": "DVR",
            "District": 4,
            "Dealer Phone": "801.394.5701",
            "Est. Arrival": "09/26/2025",
        },
    ]

    second_content = _build_vehicle_locator_bytes(updated_rows)
    second_summary = ingest_vehicle_locator_upload("Vehicle Locator.xlsx", second_content)
    assert second_summary["rows_ingested"] == 1  # duplicate skipped
    assert second_summary["rows_updated"] == 1
    assert len(second_summary["row_errors"]) == 1

    with session_scope() as session:
        vehicle = session.get(models.Vehicle, "JTEABFAJ5SK049786")
        assert vehicle.invoice_price == Decimal("66200.00")
        observations = session.execute(
            select(models.Observation).where(models.Observation.vin == "JTEABFAJ5SK049786")
        ).scalars().all()
        assert len(observations) == 2
        upload_records = session.execute(select(models.Upload)).scalars().all()
        assert len(upload_records) == 2
