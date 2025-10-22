from __future__ import annotations

import asyncio
from pathlib import Path
from typing import List

import pytest
from sqlalchemy import text

from backend.app.db import models
from backend.app.db.session import session_scope
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

    async def fetch(self, url: str, allow_extract_fallback: bool = False) -> FirecrawlResult:
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
