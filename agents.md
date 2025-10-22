# Agents Guide & Project Primer

> **Paste this at the top of any agent session.** It is a compact directive that tells the agent what this repo is, the architecture choices, and the rules of engagement for writing code and docs.

## Mission
Build and maintain a **Vehicle SKU Database & Intelligence** platform for used-car sales managers. We continuously collect Toyota **Land Cruiser / 4Runner / Tacoma / Tundra** listings from ~466 dealer inventory pages, enrich data (VIN/price/specs), and expose:
- A fast VIN Finder with a **"below MSRP"** toggle and feature facets.
- VIN detail pages (price timeline, VDP links).
- Analytics (sold counts, feature popularity, time-to-sell).
- Ops views (scrape jobs, retries, spreadsheet uploads).

## High-Level Architecture (authoritative)
- **Scraping/Ingestion** via Firecrawl (markdown first; fallback `/extract`). Enforced limits: **≤5 concurrent**, **≤100 req/min**.
- **Parser Strategy** per dealer CMS (e.g., DealerInspire/CDK/Dealer.com), unified return shape.
- **Observations** (append-only per scrape) → **Listings** (current state per `(dealer, vin)`) → **Price Events** (derived) → **Analytics**.
- **URL Builder** uses templates stored as **data** (absolute vs relative, SmartPath supported) and a model-slug registry.
- **API** (FastAPI) provides `/search`, `/vin/:vin`, `/scrape/*`, `/analytics/*`, `/uploads`.
- **Frontend** (Next.js) delivers VIN Finder, VIN Detail, Insights, Ops.
- **Object Storage** (S3/MinIO) holds raw page blobs for reparse/QA.
- **DB**: Postgres with clear separation of concerns (see schema below).

## Tech Stack
- **Python 3.11**, FastAPI, SQLAlchemy, Alembic, Pydantic v2, httpx, asyncio.
- **Postgres 15**, JSONB + GIN indexes, materialized views for heavy analytics.
- **MinIO/S3** for raw HTML/markdown.
- **Node 20**, Next.js (App Router), TanStack Query/Table.
- **Tooling**: pytest, black, ruff, mypy, bandit, safety; Playwright for web E2E.

## Canonical Data Model
- `dealers(id, name, region, backend_type, homepage_url, is_active, last_scraped_at)`
- `dealer_backend_templates(backend_type, inventory_type, url_template, model_slug_rule)`
- `vehicles(vin PK, make, model, year, trim, drivetrain, transmission, colors, msrp, invoice_price, features jsonb)`
- `listings((dealer_id, vin) PK, status, advertised_price, price_delta_msrp, vdp_url, first_seen_at, last_seen_at, source_rank)`
- `observations(id, job_id, observed_at, dealer_id, vin, vdp_url, advertised_price, msrp, payload jsonb, raw_blob_key, source)`
- `price_events(id, dealer_id, vin, observed_at, old_price, new_price, delta, pct)`
- `scrape_jobs(id uuid, created_at, started_at, completed_at, model, region, status, target_count, success_count, fail_count)`
- `scrape_tasks(id, job_id, dealer_id, url, attempt, status, http_status, error, started_at, completed_at)`
- `uploads(id, uploaded_at, filename, dealer_id, rows_ingested, rows_updated, errors jsonb)`

**Rules**  
- If `advertised_price` missing, set `advertised_price = msrp` and record `payload.assumptions.ad_price_equals_msrp=true`.
- `listings.source_rank`: `vdp(10) < inventory_list(50) < upload(80)`; lower rank wins on conflict.
- Sold detection: missing twice across scrapes ⇒ `sold`; transfer if appears at another dealer within 7 days.

## APIs (v1)
- `POST /scrape/jobs {model, region?}` → `{job_id}`
- `GET /scrape/jobs/:id`
- `GET /search?model=&year=&trim=&region=&features=&below_msrp=true&status=available&page=&size=`
- `GET /vin/:vin` → vehicle + all active listings + price timeline + last observation
- `POST /uploads` (CSV/XLSX) → lineage; rows become `observations` with `source='upload'`
- `POST /vdp-scrape {dealer_id, vin}` → focused VDP scrape
- `GET /analytics/sold?model=Tacoma&period=last_month`

## Coding Best Practices (strict)
- **Typing** mandatory. No function without type hints.
- **Docstrings** (Google-style) for all public functions/classes. Include examples when helpful.
- **Small PRs** (<400 LOC) with unit tests and runnable acceptance commands.
- **Determinism**: parsers backed by fixtures; golden tests for URL generation.
- **No secrets in code**. Use `.env` with clear keys.
- **Resilience**: backoff with jitter; graceful timeouts; structured logs.
- **Observability**: add Prometheus counters for any new service behavior.
- **Data migrations**: Alembic with downgrade path; note in PR.
- **Feature flags**: keep risky behavior behind env flags.

## Documentation Best Practices
- Each package/module has a **`README.md`** explaining purpose, key types, and example usage.
- Public functions/classes include a **docstring** with: *Args*, *Returns*, *Raises*, *Notes*.
- Add **Architecture Decision Records** (ADRs) for non-obvious choices.
- Update **PRD.md** checklist when a milestone lands.
- Include **“How to run”** in every PR description and link to related tests.

## Test Strategy
- **Unit**: 1 test per branch; edge cases enumerated.
- **Property-based** (where useful) for idempotency and state transitions.
- **Integration**: scrape → parse → observe → upsert → query.
- **E2E**: basic VIN Finder smoke with Playwright.
- **Performance**: benchmark search on 50k listings p95 < 500ms.

## Linting & CI
- black, ruff, mypy, bandit, safety, pytest (coverage ≥ 80%).
- No PR merges if any check fails.

---

**You are done reading.** Implement tasks with the above contracts. When in doubt, prefer **data + tests** over branching logic in code.
