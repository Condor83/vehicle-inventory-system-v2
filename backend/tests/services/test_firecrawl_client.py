import asyncio

import httpx
import pytest

from backend.app.services.firecrawl_client import (
    FirecrawlClient,
    FirecrawlResult,
    FirecrawlRetryableError,
)


class FakeTransport:
    def __init__(self, responses):
        self._responses = list(responses)
        self.calls = []

    async def post(self, path, json, headers, timeout):
        if not self._responses:
            raise AssertionError("No more responses configured")
        response = self._responses.pop(0)
        self.calls.append(path)
        return response

    async def close(self):
        return None


def make_response(status_code: int, body: dict) -> httpx.Response:
    request = httpx.Request("POST", "https://api.firecrawl.dev")
    return httpx.Response(status_code=status_code, json=body, request=request)


@pytest.mark.asyncio
async def test_firecrawl_client_retries_on_retryable_status(monkeypatch):
    responses = [
        make_response(429, {"success": False, "error": "rate"}),
        make_response(200, {"success": True, "data": {"markdown": "content"}}),
    ]
    client = FirecrawlClient(transport=FakeTransport(responses), max_attempts=2)
    result = await client.fetch("https://example.com", allow_extract_fallback=False)
    assert result.markdown == "content"
    await client.aclose()


@pytest.mark.asyncio
async def test_firecrawl_client_extract_fallback_used_when_markdown_missing():
    scrape_body = {"success": True, "data": {"markdown": None, "html": None}}
    extract_body = {
        "status": "completed",
        "data": [{"markdown": "from extract"}],
    }
    responses = [
        make_response(200, scrape_body),
        make_response(200, extract_body),
    ]
    client = FirecrawlClient(transport=FakeTransport(responses), max_attempts=2)
    result = await client.fetch("https://example.com", allow_extract_fallback=True)
    assert result.source == "extract"
    assert result.markdown == "from extract"
    await client.aclose()
