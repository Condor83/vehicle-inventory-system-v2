from __future__ import annotations

import asyncio
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional

from backend.app.core.rate_limit import TokenBucket
from backend.app.db import models
from backend.app.db.session import session_scope
from backend.app.parsers.cdk import parse_inventory as parse_cdk
from backend.app.parsers.dealer_com import parse_inventory as parse_dealer_com
from backend.app.parsers.dealer_inspire import parse_inventory as parse_dealer_inspire
from backend.app.parsers.url_builder import build_inventory_url
from backend.app.services.blob_store import BlobStore, LocalBlobStore
from backend.app.services.firecrawl_client import (
    FirecrawlClient,
    FirecrawlError,
    FirecrawlResult,
    FirecrawlRetryableError,
)
from backend.app.services.ingest import upsert_observations_and_listings

MAX_CONCURRENCY = 5
RPM_LIMIT = 100
SOURCE_RANK_INVENTORY = 50

PARSER_REGISTRY = {
    "DEALER_INSPIRE": parse_dealer_inspire,
    "DEALER_COM": parse_dealer_com,
    "DEALERON": parse_dealer_com,
    "CDK": parse_cdk,
    "CDK_GLOBAL": parse_cdk,
}


class ScrapeOrchestrator:
    def __init__(
        self,
        firecrawl: Optional[FirecrawlClient] = None,
        blob_store: Optional[BlobStore] = None,
        *,
        max_attempts: int = 2,
    ):
        self.firecrawl = firecrawl or FirecrawlClient()
        self.blob_store = blob_store or LocalBlobStore()
        self.bucket = TokenBucket(RPM_LIMIT)
        self.sem = asyncio.Semaphore(MAX_CONCURRENCY)
        self.max_attempts = max(1, max_attempts)

    async def run_job(self, dealers: Iterable[Dict[str, Any]], model: str) -> Dict[str, Any]:
        dealers = list(dealers)
        if not dealers:
            raise ValueError("No dealers provided for scrape job.")

        started_at = datetime.now(timezone.utc)
        job_uuid = uuid.uuid4()

        tasks_meta = self._create_job_and_tasks(job_uuid, dealers, model, started_at)

        results = await asyncio.gather(*(self._process_task(job_uuid, meta, model) for meta in tasks_meta))

        success_count = sum(1 for r in results if r["status"] == "success")
        fail_count = len(results) - success_count
        status = "success" if fail_count == 0 else ("partial" if success_count > 0 else "failed")

        self._finalize_job(job_uuid, success_count, fail_count, status)

        return {
            "job_id": str(job_uuid),
            "status": status,
            "success_count": success_count,
            "fail_count": fail_count,
            "started_at": started_at.isoformat(),
            "completed_at": datetime.now(timezone.utc).isoformat(),
        }

    def _create_job_and_tasks(
        self,
        job_id: uuid.UUID,
        dealers: List[Dict[str, Any]],
        model: str,
        started_at: datetime,
    ) -> List[Dict[str, Any]]:
        tasks_meta: List[Dict[str, Any]] = []
        with session_scope() as session:
            job = models.ScrapeJob(
                id=job_id,
                created_at=started_at,
                started_at=started_at,
                status="running",
                model=model,
                target_count=len(dealers),
                success_count=0,
                fail_count=0,
            )
            session.add(job)
            session.flush()

            for dealer in dealers:
                try:
                    url = build_inventory_url(dealer, model)
                except Exception as exc:
                    # store failed task immediately
                    task = models.ScrapeTask(
                        job_id=job_id,
                        dealer_id=dealer["id"],
                        url="",
                        status="failed",
                        error=str(exc),
                        started_at=started_at,
                        completed_at=started_at,
                    )
                    session.add(task)
                    session.flush()
                    continue

                task = models.ScrapeTask(
                    job_id=job_id,
                    dealer_id=dealer["id"],
                    url=url,
                    status="pending",
                )
                session.add(task)
                session.flush()

                tasks_meta.append(
                    {
                        "task_id": task.id,
                        "dealer": dealer,
                        "url": url,
                    }
                )
        return tasks_meta

    async def _process_task(self, job_id: uuid.UUID, meta: Dict[str, Any], model: str) -> Dict[str, Any]:
        dealer = meta["dealer"]
        url = meta["url"]
        task_id = meta["task_id"]
        backend = (dealer.get("backend_type") or "").upper()
        parser = PARSER_REGISTRY.get(backend)

        observed_at = datetime.now(timezone.utc)
        await self._update_task_status(task_id, status="running", started_at=observed_at)

        if parser is None:
            await self._update_task_status(task_id, status="failed", completed_at=datetime.now(timezone.utc), error=f"No parser for backend {backend}")
            return {"status": "failed"}

        attempt = 0
        last_error: Optional[str] = None
        while attempt < self.max_attempts:
            allow_extract = attempt == self.max_attempts - 1
            try:
                await self.bucket.acquire(1)
                async with self.sem:
                    result = await self.firecrawl.fetch(url, allow_extract_fallback=allow_extract)
            except FirecrawlRetryableError as exc:
                last_error = str(exc)
                attempt += 1
                continue
            except FirecrawlError as exc:
                await self._update_task_status(
                    task_id,
                    status="failed",
                    completed_at=datetime.now(timezone.utc),
                    error=str(exc),
                )
                return {"status": "failed"}

            rows = parser(result.best_content)
            if rows:
                outcome = await self._persist_results(
                    job_id=job_id,
                    dealer=dealer,
                    url=url,
                    rows=rows,
                    model=model,
                    observed_at=observed_at,
                    raw_result=result,
                )
                await self._update_task_status(
                    task_id,
                    status="success",
                    completed_at=datetime.now(timezone.utc),
                )
                return {"status": "success", "observations": outcome["observations"]}

            last_error = "empty_parse"
            attempt += 1

        await self._update_task_status(
            task_id,
            status="failed",
            completed_at=datetime.now(timezone.utc),
            error=last_error or "unknown_error",
        )
        return {"status": "failed"}

    async def _persist_results(
        self,
        *,
        job_id: uuid.UUID,
        dealer: Dict[str, Any],
        url: str,
        rows: List[Dict[str, Any]],
        model: str,
        observed_at: datetime,
        raw_result: FirecrawlResult,
    ) -> Dict[str, int]:
        dealer_id = dealer["id"]
        backend = dealer.get("backend_type")
        raw_content = raw_result.best_content
        suffix = "md" if raw_result.markdown else "html"
        blob_key = ""
        if raw_content:
            blob_key = await self._store_raw_blob(job_id, dealer_id, raw_content, suffix=suffix)

        prepared_rows = []
        for row in rows:
            prepared_rows.append(
                {
                    "dealer_id": dealer_id,
                    "vin": row.get("vin", "").upper(),
                    "advertised_price": row.get("advertised_price"),
                    "msrp": row.get("msrp"),
                    "status": (row.get("status") or "available").lower(),
                    "vdp_url": row.get("vdp_url"),
                    "stock_number": row.get("stock_number"),
                    "observed_at": observed_at,
                    "job_id": str(job_id),
                    "source": "inventory_list",
                    "source_rank": SOURCE_RANK_INVENTORY,
                    "payload": {
                        "firecrawl": {
                            "url": url,
                            "backend": backend,
                            "source": raw_result.source,
                        }
                    },
                    "raw_blob_key": blob_key or None,
                    "vehicle": {
                        "make": row.get("make") or "Toyota",
                        "model": row.get("model") or model,
                        "year": row.get("year"),
                        "trim": row.get("trim"),
                        "features": row.get("features"),
                    },
                }
            )

        outcome = upsert_observations_and_listings(prepared_rows, source="inventory_list")
        return outcome

    async def _store_raw_blob(self, job_id: uuid.UUID, dealer_id: int, content: str, *, suffix: str) -> str:
        blob_key = ""
        if isinstance(self.blob_store, LocalBlobStore):
            key = self.blob_store.build_key(str(job_id), dealer_id, suffix=suffix)
            blob_key = await self.blob_store.put_text(key, content)
        else:
            key = f"{job_id}/{dealer_id}"
            blob_key = await self.blob_store.put_text(f"{key}.{suffix}", content)
        return blob_key

    async def _update_task_status(
        self,
        task_id: int,
        *,
        status: Optional[str] = None,
        started_at: Optional[datetime] = None,
        completed_at: Optional[datetime] = None,
        error: Optional[str] = None,
    ) -> None:
        with session_scope() as session:
            task = session.get(models.ScrapeTask, task_id)
            if not task:
                return
            if status:
                task.status = status
            if started_at:
                task.started_at = started_at
            if completed_at:
                task.completed_at = completed_at
            if error:
                task.error = error

    def _finalize_job(self, job_id: uuid.UUID, success_count: int, fail_count: int, status: str) -> None:
        with session_scope() as session:
            job = session.get(models.ScrapeJob, job_id)
            if not job:
                return
            job.completed_at = datetime.now(timezone.utc)
            job.status = status
            job.success_count = success_count
            job.fail_count = fail_count
