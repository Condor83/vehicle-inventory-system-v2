from __future__ import annotations

import asyncio
import random
from dataclasses import dataclass
from typing import Any, Dict, Optional, Protocol

import httpx

from backend.app.core.settings import settings

RETRYABLE_STATUS = {429, 500, 502, 503, 504}


class FirecrawlError(Exception):
    """Base exception for Firecrawl errors."""


class FirecrawlRetryableError(FirecrawlError):
    """Raised when a retryable HTTP status/error is encountered."""


@dataclass
class FirecrawlResult:
    url: str
    markdown: Optional[str]
    html: Optional[str]
    raw_html: Optional[str]
    metadata: Dict[str, Any]
    source: str  # "scrape" or "extract"

    @property
    def best_content(self) -> str:
        if self.markdown:
            return self.markdown
        if self.html:
            return self.html
        if self.raw_html:
            return self.raw_html
        return ""


class AsyncTransport(Protocol):
    async def post(
        self,
        path: str,
        json: Dict[str, Any],
        headers: Dict[str, str],
        timeout: float,
    ) -> httpx.Response: ...

    async def close(self) -> None: ...


class HttpxTransport:
    """httpx-based transport with connection pooling."""

    def __init__(self, base_url: str):
        self._client = httpx.AsyncClient(base_url=base_url, timeout=None)

    async def post(
        self,
        path: str,
        json: Dict[str, Any],
        headers: Dict[str, str],
        timeout: float,
    ) -> httpx.Response:
        return await self._client.post(path, json=json, headers=headers, timeout=timeout)

    async def close(self) -> None:
        await self._client.aclose()


def _camel(value: str) -> str:
    components = value.split("_")
    if not components:
        return value
    return components[0] + "".join(c.title() for c in components[1:])


def _build_scrape_options(formats: Optional[list[str]] = None) -> Dict[str, Any]:
    opts = {
        "onlyMainContent": True,
        "removeBase64Images": True,
        "skipTlsVerification": True,
        "storeInCache": True,
        "blockAds": True,
        "maxAge": 14400000,
    }
    if formats:
        opts["formats"] = formats
    return opts


class FirecrawlClient:
    """Thin async client against Firecrawl scrape/extract endpoints."""

    def __init__(
        self,
        api_key: Optional[str] = None,
        *,
        base_url: str = "https://api.firecrawl.dev",
        timeout: float = 25.0,
        max_attempts: int = 2,
        backoff_base: float = 0.5,
        transport: Optional[AsyncTransport] = None,
    ):
        self.api_key = api_key or settings.firecrawl_api_key
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.max_attempts = max(1, max_attempts)
        self.backoff_base = backoff_base
        self._transport = transport or HttpxTransport(self.base_url)
        self._owns_transport = transport is None
        self._headers = {"Authorization": f"Bearer {self.api_key}"} if self.api_key else {}

    async def aclose(self) -> None:
        if self._owns_transport:
            await self._transport.close()

    async def fetch(
        self,
        url: str,
        *,
        allow_extract_fallback: bool = False,
    ) -> FirecrawlResult:
        document = await self._scrape(url)
        if document.markdown or document.html or not allow_extract_fallback:
            return document
        extract_result = await self._extract(url)
        if extract_result:
            return extract_result
        return document

    async def _scrape(self, url: str) -> FirecrawlResult:
        payload = {
            "url": url,
            **_build_scrape_options(["markdown", "html"]),
        }
        body = await self._post("/v2/scrape", payload)
        if not body.get("success"):
            raise FirecrawlError(body.get("error", "Firecrawl scrape failed"))
        data = body.get("data") or {}
        return FirecrawlResult(
            url=url,
            markdown=data.get("markdown"),
            html=data.get("html"),
            raw_html=data.get("rawHtml") or data.get("raw_html"),
            metadata=self._normalize_metadata(data.get("metadata")),
            source="scrape",
        )

    async def _extract(self, url: str) -> Optional[FirecrawlResult]:
        payload = {
            "urls": [url],
            "scrapeOptions": _build_scrape_options(["markdown", "html"]),
        }
        body = await self._post("/v2/extract", payload)
        status = body.get("status")
        if status and status != "completed":
            error = body.get("error") or f"extract status {status}"
            raise FirecrawlError(error)
        data = body.get("data")
        if not data:
            return None
        candidate = None
        if isinstance(data, dict):
            candidate = data
        elif isinstance(data, list):
            for item in data:
                if isinstance(item, dict):
                    candidate = item
                    break
        if not candidate:
            return None
        documents = candidate.get("documents") if isinstance(candidate, dict) else None
        if isinstance(documents, list) and documents:
            candidate = documents[0]
        markdown = candidate.get("markdown") if isinstance(candidate, dict) else None
        html = candidate.get("html") if isinstance(candidate, dict) else None
        raw_html = candidate.get("rawHtml") if isinstance(candidate, dict) else None
        if not any([markdown, html, raw_html]):
            text_candidate = candidate.get("content") if isinstance(candidate, dict) else None
            markdown = text_candidate or markdown
        return FirecrawlResult(
            url=url,
            markdown=markdown,
            html=html,
            raw_html=raw_html,
            metadata=self._normalize_metadata(candidate.get("metadata") if isinstance(candidate, dict) else {}),
            source="extract",
        )

    async def _post(self, path: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        attempts = 0
        last_error: Optional[Exception] = None
        while attempts < self.max_attempts:
            try:
                response = await self._transport.post(path, json=payload, headers=self._headers, timeout=self.timeout)
            except httpx.RequestError as exc:
                last_error = exc
                await self._maybe_wait(attempts)
                attempts += 1
                continue

            if response.status_code in RETRYABLE_STATUS:
                last_error = FirecrawlRetryableError(f"Firecrawl returned {response.status_code} for {path}")
                await self._maybe_wait(attempts)
                attempts += 1
                continue

            try:
                response.raise_for_status()
            except httpx.HTTPStatusError as exc:
                raise FirecrawlError(str(exc)) from exc

            try:
                return response.json()
            except ValueError as exc:
                raise FirecrawlError("Invalid JSON from Firecrawl") from exc

        if isinstance(last_error, FirecrawlError):
            raise last_error
        if last_error:
            raise FirecrawlError(str(last_error)) from last_error
        raise FirecrawlError("Firecrawl request failed")

    async def _maybe_wait(self, attempt: int) -> None:
        if attempt >= self.max_attempts - 1:
            return
        delay = self.backoff_base * (2 ** attempt)
        jitter = random.uniform(0, 0.3)
        await asyncio.sleep(delay + jitter)

    @staticmethod
    def _normalize_metadata(metadata: Any) -> Dict[str, Any]:
        if isinstance(metadata, dict):
            return {k: v for k, v in metadata.items() if v is not None}
        return {}
