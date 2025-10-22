# PRD 01 — Schema & Seeding

## Deliverables
1) Postgres schema: dealers, dealer_backend_templates, vehicles, listings, observations, price_events, scrape_jobs, scrape_tasks, uploads.
2) Seed data imported from `database_export.xlsx` (cleaned).
3) URL golden snapshot test (466 dealers × 4 models).

## Key Requirements
- Observations are append-only, include `source` and `payload`.
- Listings maintain `(dealer_id, vin)` as PK, with `first_seen_at`/`last_seen_at` and `status`.
- Price events are derived (old/new, delta, pct).

## Seeding Rules
- Normalize backend names (`DEALERINSPIRE`→`DEALER_INSPIRE`, `DEALERALCHEMIST.COM`→`DEALER_ALCHEMY`).
- Add `scraping_config.template_scope` = `absolute|relative` based on URL template; `uses_smartpath` where applicable.
- Restrict placeholders to: `{homepage_url}`, `{model_slug}`, `{model_plus}`, `{model_name_encoded}`.
- Drop rows with null VIN; lowercase statuses.
- Pricing: if `advertised_price` missing and `msrp` present → set ad price = msrp and record `payload.assumptions.ad_price_equals_msrp=true`.

## Acceptance
- `scripts/seed_from_export.py` produces CSVs and loads tables.
- URL snapshot generated successfully; builder passes snapshot test.
