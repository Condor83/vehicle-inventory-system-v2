# Vehicle Intelligence Starter

This repository is a starter pack for a **Vehicle SKU Database & Intelligence** platform. Unzip and drop into your new repo, then follow the steps below.

## Quick Start

```bash
# 1) Python env
pyenv install -s 3.11.9
pyenv virtualenv 3.11.9 vin-intel
pyenv local vin-intel
pip install -r requirements.txt

# 2) Infra (PG + MinIO)
docker compose up -d

# 3) Seed from your spreadsheet export (drop any "Vehicle Locator" spreadsheets into data/raw for dealer enrichment first)
python scripts/seed_from_export.py --input ./data/raw/database_export.xlsx --out ./data/seeds

# 4) Load seeds into Postgres (runs Alembic migrations automatically)
python scripts/seed_from_export.py --load --in-dir ./data/seeds

# 5) Run API
uvicorn backend.app.api.main:app --reload
```

## What’s Inside
- `agents.md` — one-page directive for coding agents.
- `PRDs/` — phased product specs with acceptance criteria.
- `backend/` — FastAPI app, services, parsers, and DB models (skeletons).
- `scripts/` — seeding and utilities.
- `data/` — model slug registry, backend templates (examples), seed mapping.
- `docs/` — ADRs and detailed seeding instructions.
- `docker/` — compose file for Postgres + MinIO.
- `Makefile` — common dev tasks.

## Requirements
- Python 3.11, Node 20 (for web later).
- Postgres 15, MinIO (or S3-compatible), Docker Desktop.

## Coding Standards
- Typed Python, Google-style docstrings, black/ruff/mypy.
- Tests via pytest; coverage ≥ 80% for merge.
- No secrets in code; use `.env` (see `.env.example`).

## Environment
Create a `.env` in repo root:

```
DATABASE_URL=postgresql+psycopg://postgres:postgres@localhost:5432/vinintel
OBJECT_STORE_URL=http://localhost:9000
OBJECT_STORE_BUCKET=vin-raw
OBJECT_STORE_KEY=minioadmin
OBJECT_STORE_SECRET=minioadmin
FIRECRAWL_API_KEY=replace-me
```

## Next Steps
- Confirm seed generation on a subset of dealers.
- Implement DealerInspire parser and run a small scrape job.
- Build `GET /search` and wire the VIN Finder page.
