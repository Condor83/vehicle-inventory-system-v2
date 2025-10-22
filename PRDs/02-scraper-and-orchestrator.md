# PRD 02 — Scraper & Orchestrator

## Deliverables
- `services/firecrawl_client.py` adapter (markdown → fallback `/extract`).
- `services/scrape_orchestrator.py` with asyncio semaphore(5) and 100 req/min token bucket.
- Per-CMS parser starting with DealerInspire in `parsers/dealer_inspire.py`.
- Persist observations; upsert vehicles/listings; emit price_events on price change.

## Requirements
- Retry: 429/5xx exponential backoff (jitter), 2 attempts, then mark `scrape_tasks` failed.
- Empty parse: retry once; then fallback to `/extract`.
- Raw blobs saved to object storage (S3/MinIO key attached to observation).

## Acceptance
- Unit tests for rate limiting and retries (fakes).
- Parser fixture tests: ≥95% VIN extraction; 0 false VINs.
- End-to-end dry run over a sample produces correct rows.
