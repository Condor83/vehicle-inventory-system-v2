from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from pathlib import Path
from typing import List

import pytest
from sqlalchemy import text

from backend.app.db import models
from backend.app.db.session import session_scope
from backend.app.services import scrape_orchestrator as orchestrator_module
from backend.app.services.firecrawl_client import FirecrawlResult, FirecrawlRetryableError
from backend.app.services.blob_store import LocalBlobStore
from backend.app.services.scrape_orchestrator import ScrapeOrchestrator


def _truncate_tables() -> None:
    with session_scope() as session:
        session.execute(text("TRUNCATE TABLE observations, price_events, listings, vehicles, scrape_tasks, scrape_jobs, dealers RESTART IDENTITY CASCADE"))


def _seed_dealer() -> dict:
    with session_scope() as session:
        dealer = models.Dealer(
            name="Test Dealer",
            region="Mountain",
            backend_type="DEALER_INSPIRE",
            homepage_url="https://dealer.test",
        )
        session.add(dealer)
        session.flush()
        dealer_id = dealer.id
    return {
        "id": dealer_id,
        "name": "Test Dealer",
        "backend_type": "DEALER_INSPIRE",
        "homepage_url": "https://dealer.test",
        "inventory_url_template": "https://dealer.test/inventory/{model_slug}",
        "scraping_config": {"template_scope": "absolute"},
    }


class FakeFirecrawlClient:
    def __init__(self, results: List[FirecrawlResult | Exception]):
        self._results = list(results)

    async def fetch(self, url: str, allow_extract_fallback: bool = False, **kwargs) -> FirecrawlResult:
        if not self._results:
            raise AssertionError("No fake results remaining")
        result = self._results.pop(0)
        if isinstance(result, Exception):
            raise result
        return result


@pytest.mark.asyncio
async def test_scrape_orchestrator_persists_success(tmp_path):
    _truncate_tables()
    dealer = _seed_dealer()
    markdown_path = Path("backend/tests/parsers/fixtures/dealer_inspire/sample_inventory.md")
    content = markdown_path.read_text(encoding="utf-8")
    fake_result = FirecrawlResult(
        url="https://dealer.test/inventory",
        markdown=content,
        html=None,
        raw_html=None,
        metadata={},
        source="scrape",
    )

    orchestrator = ScrapeOrchestrator(
        firecrawl=FakeFirecrawlClient([fake_result]),
        blob_store=LocalBlobStore(tmp_path),
    )

    summary = await orchestrator.run_job([dealer], model="4Runner")
    assert summary["status"] == "success"

    with session_scope() as session:
        listing_count = session.query(models.Listing).count()
        assert listing_count == 3
        observation_count = session.query(models.Observation).count()
        assert observation_count == 3


@pytest.mark.asyncio
async def test_scrape_orchestrator_records_failure_after_retries(tmp_path):
    _truncate_tables()
    dealer = _seed_dealer()
    error = FirecrawlRetryableError("rate limited")
    fake_client = FakeFirecrawlClient([error, error])
    orchestrator = ScrapeOrchestrator(firecrawl=fake_client, blob_store=LocalBlobStore(tmp_path))

    summary = await orchestrator.run_job([dealer], model="4Runner")
    assert summary["fail_count"] == 1
    assert summary["status"] == "failed"

    with session_scope() as session:
        task = session.query(models.ScrapeTask).one()
        assert task.status == "failed"


@pytest.mark.asyncio
async def test_scrape_orchestrator_no_inventory_marks_missing_then_sold(tmp_path, monkeypatch):
    _truncate_tables()
    dealer = _seed_dealer()
    vin = "5TDACRFH9RS123456"
    observed_at = datetime.now(timezone.utc)

    with session_scope() as session:
        vehicle = models.Vehicle(
            vin=vin,
            make="Toyota",
            model="4Runner",
            year=2024,
        )
        session.add(vehicle)
        session.flush()
        listing = models.Listing(
            dealer_id=dealer["id"],
            vin=vin,
            status="available",
            advertised_price=None,
            price_delta_msrp=None,
            first_seen_at=observed_at,
            last_seen_at=observed_at,
            source_rank=50,
        )
        session.add(listing)
        session.commit()

    empty_result = FirecrawlResult(
        url="https://dealer.test/inventory",
        markdown="",
        html="",
        raw_html="<div>No inventory</div>",
        metadata={},
        source="scrape",
    )

    def _empty_parser(content: str) -> list:
        return []

    monkeypatch.setitem(orchestrator_module.PARSER_REGISTRY, "DEALER_INSPIRE", _empty_parser)

    orchestrator = ScrapeOrchestrator(
        firecrawl=FakeFirecrawlClient([empty_result, empty_result]),
        blob_store=LocalBlobStore(tmp_path),
    )

    summary_first = await orchestrator.run_job([dealer], model="4Runner")
    assert summary_first["status"] == "success"

    with session_scope() as session:
        listing = session.query(models.Listing).one()
        assert listing.status == "missing"

    summary_second = await orchestrator.run_job([dealer], model="4Runner")
    assert summary_second["status"] == "success"

    with session_scope() as session:
        listing = session.query(models.Listing).one()
        assert listing.status == "sold"
