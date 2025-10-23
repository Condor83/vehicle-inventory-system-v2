from __future__ import annotations

import asyncio
import json
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

import httpx
from sqlalchemy import or_, select

from backend.app.core.rate_limit import TokenBucket
from backend.app.db import models
from backend.app.db.session import session_scope
from backend.app.parsers.cdk import (
    parse_inventory as parse_cdk,
    extract_inventory_request as extract_cdk_inventory_request,
    parse_inventory_json as parse_cdk_inventory_json,
)
from backend.app.parsers.dealer_com import parse_inventory as parse_dealer_com
from backend.app.parsers.dealer_inspire import (
    parse_inventory as parse_dealer_inspire,
    extract_algolia_config,
    build_algolia_params,
    parse_algolia_hits,
)
from backend.app.parsers.dealer_alchemy import (
    parse_inventory as parse_dealer_alchemy,
    extract_typesense_config,
    parse_typesense_hits,
)
from backend.app.parsers.dealer_on import (
    parse_inventory as parse_dealer_on,
    DealerOnParseError,
)
from backend.app.parsers.dealer_socket import parse_inventory as parse_dealer_socket
from backend.app.parsers.smartpath import (
    parse_inventory as parse_smartpath,
    SmartPathParseError,
)
from backend.app.parsers.team_velocity import (
    parse_inventory as parse_team_velocity,
    TeamVelocityParseError,
)
from backend.app.parsers.url_builder import MODEL_REGISTRY, build_inventory_url
from backend.app.services.blob_store import BlobStore, LocalBlobStore
from backend.app.services.firecrawl_client import (
    FirecrawlClient,
    FirecrawlError,
    FirecrawlResult,
    FirecrawlRetryableError,
)
from backend.app.services.ingest import upsert_observations_and_listings

MAX_CONCURRENCY = 50
RPM_LIMIT = 500
SOURCE_RANK_INVENTORY = 50

PARSER_REGISTRY = {
    "DEALER_INSPIRE": parse_dealer_inspire,
    "DEALER_COM": parse_dealer_com,
    "DEALERON": parse_dealer_on,
    "CDK": parse_cdk,
    "CDK_GLOBAL": parse_cdk,
    "DEALER_SOCKET": parse_dealer_socket,
    "SMARTPATH": parse_smartpath,
    "TEAM_VELOCITY": parse_team_velocity,
    "DEALER_ALCHEMY": parse_dealer_alchemy,
    "DEALER_VENOM": parse_dealer_alchemy,
    "FOX_DEALER": parse_dealer_alchemy,
}

SMARTPATH_FALLBACK_PARSERS = [
    ("TEAM_VELOCITY", parse_team_velocity),
    ("DEALER_INSPIRE", parse_dealer_inspire),
    ("DEALER_COM", parse_dealer_com),
    ("DEALERON", parse_dealer_on),
    ("DEALER_SOCKET", parse_dealer_socket),
    ("CDK", parse_cdk),
]


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
        effective_backend = backend
        config_raw = dealer.get("scraping_config")
        if isinstance(config_raw, str):
            try:
                config = json.loads(config_raw)
            except json.JSONDecodeError:
                config = {}
        elif isinstance(config_raw, dict):
            config = config_raw
        else:
            config = {}
        firecrawl_cfg = config.get("firecrawl") if isinstance(config, dict) else {}
        proxy = None
        if isinstance(firecrawl_cfg, dict):
            proxy = firecrawl_cfg.get("proxy")

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
                    result = await self.firecrawl.fetch(url, allow_extract_fallback=allow_extract, proxy=proxy)
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

            raw_html = result.raw_html or result.html or result.best_content
            content = raw_html if parser in {parse_dealer_on, parse_smartpath} else result.best_content
            try:
                rows = parser(content)
            except DealerOnParseError as exc:
                content_lower = raw_html.lower() if raw_html else ""
                handled = False
                last_exc: Optional[Exception] = None
                adjusted_html = raw_html
                if adjusted_html and 'rel="canonical"' not in adjusted_html.lower() and url:
                    adjusted_html = f'<link rel="canonical" href="{url}">{adjusted_html}'

                if "smartpath" in content_lower:
                    try:
                        rows = parse_smartpath(adjusted_html)
                        handled = True
                    except SmartPathParseError as smart_exc:
                        last_exc = smart_exc
                if not handled and ("teamvelocityportal" in content_lower or "inventoryapibaseurl" in content_lower):
                    try:
                        rows = parse_team_velocity(adjusted_html)
                        handled = True
                    except TeamVelocityParseError as tv_exc:
                        last_exc = tv_exc
                if not handled:
                    last_error = str(last_exc or exc)
                    break
            except SmartPathParseError as exc:  # pragma: no cover - capture parser-specific errors
                handled = False
                fallback_urls = self._smartpath_fallback_urls(dealer, model)
                for fallback_url in fallback_urls:
                    try:
                        fallback_result = await self.firecrawl.fetch(
                            fallback_url,
                            allow_extract_fallback=allow_extract,
                            proxy=proxy,
                        )
                    except FirecrawlError as fetch_exc:
                        last_error = str(fetch_exc)
                        continue

                    fallback_html = fallback_result.raw_html or fallback_result.html or fallback_result.best_content
                    fallback_backend, fallback_rows = self._try_fallback_parsers(fallback_html)
                    if fallback_rows:
                        rows = fallback_rows
                        result = fallback_result
                        raw_html = fallback_html
                        content = fallback_html
                        effective_backend = fallback_backend or effective_backend
                        handled = True
                        break

                if not handled:
                    last_error = str(exc)
                    break
            except TeamVelocityParseError as exc:  # pragma: no cover
                last_error = str(exc)
                break
            except Exception as exc:  # pragma: no cover - defensive guard for parser failures
                last_error = str(exc)
                break
            if not rows:
                if backend in {"CDK", "CDK_GLOBAL"} and raw_html:
                    try:
                        rows = await self._fetch_cdk_inventory(
                            html=raw_html,
                            page_url=url,
                        )
                    except Exception as exc:  # pragma: no cover - defensive guard
                        last_error = str(exc)
                        break
                elif backend == "DEALER_INSPIRE" and raw_html:
                    try:
                        rows = await self._fetch_dealer_inspire_inventory(
                            html=raw_html,
                            page_url=url,
                            model=model,
                        )
                    except Exception as exc:  # pragma: no cover
                        last_error = str(exc)
                        break
                elif backend in {"DEALER_ALCHEMY", "DEALER_VENOM", "FOX_DEALER"} and raw_html:
                    try:
                        rows = await self._fetch_dealer_alchemy_inventory(
                            html=raw_html,
                            page_url=url,
                            model=model,
                        )
                    except Exception as exc:  # pragma: no cover
                        last_error = str(exc)
                        break

            if rows:
                outcome = await self._persist_results(
                    job_id=job_id,
                    dealer=dealer,
                    url=url,
                    rows=rows,
                    model=model,
                    observed_at=observed_at,
                    raw_result=result,
                    backend_override=effective_backend,
                )
                await self._update_task_status(
                    task_id,
                    status="success",
                    completed_at=datetime.now(timezone.utc),
                )
                return {"status": "success", "observations": outcome["observations"]}

            # No rows after parsing; treat as a successful empty inventory scrape.
            outcome = await self._handle_no_inventory(
                job_id=job_id,
                dealer=dealer,
                url=url,
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

        await self._update_task_status(
            task_id,
            status="failed",
            completed_at=datetime.now(timezone.utc),
            error=last_error or "unknown_error",
        )
        return {"status": "failed"}

    async def _fetch_cdk_inventory(
        self,
        *,
        html: str,
        page_url: str,
    ) -> List[Dict[str, Any]]:
        request = extract_cdk_inventory_request(html)
        if not request:
            return []
        parsed_url = urlparse(page_url)
        base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
        endpoint = urljoin(base_url, request.endpoint)
        headers = {
            "Content-Type": "application/json",
            "Referer": page_url,
            "Origin": base_url,
            "User-Agent": "Mozilla/5.0 (compatible; VehicleInventoryBot/1.0)",
        }
        await self.bucket.acquire(1)
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.post(endpoint, json=request.payload, headers=headers)
            response.raise_for_status()
            data = response.json()
        return parse_cdk_inventory_json(data, base_url=base_url)

    async def _fetch_dealer_inspire_inventory(
        self,
        *,
        html: str,
        page_url: str,
        model: str,
    ) -> List[Dict[str, Any]]:
        config = extract_algolia_config(html)
        if not config:
            return []
        params = build_algolia_params(config, model=model)
        headers = {
            "X-Algolia-Application-Id": config.app_id,
            "X-Algolia-API-Key": config.api_key,
            "Content-Type": "application/json",
        }
        await self.bucket.acquire(1)
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.post(
                f"https://{config.app_id}-dsn.algolia.net/1/indexes/{config.index}/query",
                json={"params": params},
                headers=headers,
            )
            response.raise_for_status()
            data = response.json()
        parsed_url = urlparse(page_url)
        base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
        return parse_algolia_hits(data, base_url=base_url)

    async def _fetch_dealer_alchemy_inventory(
        self,
        *,
        html: str,
        page_url: str,
        model: str,
    ) -> List[Dict[str, Any]]:
        config = extract_typesense_config(html)
        if not config:
            return []

        def _quote(value: str) -> str:
            return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"

        filters: List[str] = []
        if config.condition:
            filters.append(f"condition:={_quote(config.condition)}")
        if model:
            filters.append(f"model:={_quote(model)}")
        filter_by = " && ".join(filters) if filters else None

        payload: Dict[str, Any] = {
            "searches": [
                {
                    "collection": config.index_name,
                    "q": "",
                    "query_by": config.query_by,
                    "per_page": config.hits_per_page,
                }
            ]
        }
        if filter_by:
            payload["searches"][0]["filter_by"] = filter_by

        endpoint = f"{config.protocol}://{config.host}:{config.port}/multi_search?use_cache=true"
        headers = {"X-TYPESENSE-API-KEY": config.api_key}

        await self.bucket.acquire(1)
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.post(endpoint, headers=headers, json=payload)
            response.raise_for_status()
            data = response.json()

        rows = parse_typesense_hits(data, page_url=page_url)
        return rows

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
        backend_override: Optional[str] = None,
    ) -> Dict[str, int]:
        dealer_id = dealer["id"]
        backend = backend_override or dealer.get("backend_type")
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

        observed_vins = {row["vin"].upper() for row in rows if row.get("vin")}
        missing_stats = self._mark_absent_listings(
            dealer_id=dealer_id,
            model=model,
            observed_vins=observed_vins,
            observed_at=observed_at,
        )
        outcome.update(missing_stats)
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

    def _smartpath_fallback_urls(self, dealer: Dict[str, Any], model: str) -> List[str]:
        homepage = (dealer.get("homepage_url") or "").rstrip("/")
        if not homepage:
            return []

        model_entry = MODEL_REGISTRY.get(model) or {}
        slug = model_entry.get("model_slug") or model.lower().replace(" ", "-")
        candidates = [
            f"{homepage}/inventory/new/toyota/{slug}",
            f"{homepage}/inventory/new/{slug}",
            f"{homepage}/inventory/new-toyota-{slug}",
            f"{homepage}/inventory/new-{slug}",
        ]

        unique: List[str] = []
        seen = set()
        for url in candidates:
            if url not in seen:
                unique.append(url)
                seen.add(url)
        return unique

    async def _handle_no_inventory(
        self,
        *,
        job_id: uuid.UUID,
        dealer: Dict[str, Any],
        url: str,
        model: str,
        observed_at: datetime,
        raw_result: FirecrawlResult,
    ) -> Dict[str, int]:
        dealer_id = dealer["id"]
        blob_key = ""
        raw_content = raw_result.best_content or raw_result.raw_html or raw_result.html
        if raw_content:
            suffix = "md" if raw_result.markdown else "html"
            blob_key = await self._store_raw_blob(job_id, dealer_id, raw_content, suffix=suffix)

        missing_stats = self._mark_absent_listings(
            dealer_id=dealer_id,
            model=model,
            observed_vins=set(),
            observed_at=observed_at,
        )
        # no new observations created for empty inventory
        return {"observations": 0, **missing_stats, "raw_blob_key": blob_key or None}

    def _mark_absent_listings(
        self,
        *,
        dealer_id: int,
        model: str,
        observed_vins: set[str],
        observed_at: datetime,
    ) -> Dict[str, int]:
        """Update listings that were not observed in this scrape cycle.

        First miss → status 'missing'; second consecutive miss → status 'sold'.
        Listings already marked sold remain unchanged.
        """
        marked_missing = 0
        marked_sold = 0

        with session_scope() as session:
            stmt = (
                select(models.Listing)
                .join(models.Vehicle, models.Vehicle.vin == models.Listing.vin)
                .where(
                    models.Listing.dealer_id == dealer_id,
                    models.Vehicle.model == model,
                    or_(models.Listing.source_rank.is_(None), models.Listing.source_rank <= SOURCE_RANK_INVENTORY),
                )
            )
            listings = session.execute(stmt).scalars().all()
            for listing in listings:
                vin = (listing.vin or "").upper()
                if vin in observed_vins:
                    continue
                current_status = (listing.status or "").lower()
                if current_status == "sold":
                    continue
                if current_status == "missing":
                    listing.status = "sold"
                    listing.last_seen_at = listing.last_seen_at or observed_at
                    marked_sold += 1
                else:
                    listing.status = "missing"
                    marked_missing += 1
            session.flush()

        return {"marked_missing": marked_missing, "marked_sold": marked_sold}

    def _try_fallback_parsers(self, html: str) -> Tuple[Optional[str], List[Dict[str, Any]]]:
        for backend_key, parser_func in SMARTPATH_FALLBACK_PARSERS:
            try:
                rows = parser_func(html)
            except (DealerOnParseError, SmartPathParseError, TeamVelocityParseError):
                continue
            except Exception:  # pragma: no cover - defensive
                continue
            if rows:
                return backend_key, rows
        return None, []
