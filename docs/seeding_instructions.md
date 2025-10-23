# Seeding Instructions & Spreadsheet Cleanup

This document explains how to convert your existing `database_export.xlsx` into clean seeds for the fresh repo.

## 1) Normalize Dealer Backends
Map spelling variants:
- `DEALERINSPIRE` → `DEALER_INSPIRE`
- `DEALERALCHEMIST.COM` → `DEALER_ALCHEMY`

> Tip: drop the latest "Vehicle Locator" spreadsheets into `data/raw/` before running the seeding script. The loader will merge them (via `Dealer Code`) to enrich each dealer with region codes, district numbers, and phone details so URL builders can inject the correct tokens.

## 2) Mark Template Scope & SmartPath
For each row:
- If `inventory_url_template` starts with `http`, set `scraping_config.template_scope = "absolute"`, else `"relative"`.
- If hostname contains `smartpath`, set `scraping_config.uses_smartpath = true`.

Store `scraping_config` as a JSON string column in `dealers.csv` (seed).

## 3) Standardize Placeholders
Ensure all `inventory_url_template` values only use:
- `{homepage_url}`
- `{model_slug}` (kebab: `land-cruiser`, `4runner`, `tacoma`, `tundra`)
- `{model_plus}` (`Land+Cruiser`, `4Runner`, `Tacoma`, `Tundra`)
- `{model_name_encoded}` (`Land%20Cruiser`, etc.)

Convert custom tokens like `{modelParam}` to one of the above.

## 4) Remove Null VINs
Drop any row with a null VIN before inserting into `vehicles`.

## 5) Status Mapping
Convert to lowercase:
- AVAILABLE → available
- PENDING → pending
- IN_TRANSIT → in_transit
- SOLD → sold

## 6) Price Rules
- If `advertised_price` present → keep it.
- Else if `msrp` present → set `advertised_price = msrp` and record `assumptions.ad_price_equals_msrp=true` in `observations.payload`.
- Else leave price fields null (they won’t match “below MSRP” until scraped).

## 7) Outputs
The seeding script writes:
- `data/seeds/dealers.csv`
- `data/seeds/vehicles.csv`
- `data/seeds/listings.csv`
- `data/seeds/observations.csv` (source='import')
- `data/seeds/price_events.csv` (optional backfill)
- `data/snapshots/inventory_urls.snap` (466×4 golden URLs)

## 8) Load
Run:
```bash
python scripts/seed_from_export.py --input ./data/raw/database_export.xlsx --out ./data/seeds
python scripts/seed_from_export.py --load --in-dir ./data/seeds
```

This runs the Alembic migrations (creating tables if needed) and then COPYs the CSVs into Postgres.

### Upload Operational Spreadsheets (PRD 01.2 ✅)
After loading the seeds you can ingest the live Vehicle Locator spreadsheets:
```bash
curl -F "file=@data/raw/Vehicle Locator - 2025-09-26T124431.839.xlsx" http://localhost:8000/uploads
curl -F "file=@data/raw/Vehicle Locator - 2025-09-26T124608.878.xlsx" http://localhost:8000/uploads
```
The upload service normalizes each VIN row (dealer, model code, colors, MSRP, invoice, allocation/arrival) and upserts into `vehicles`, `listings` (`source_rank=80`), and `observations` (`source='upload'` with full row payload). Dealer region/district/phone are refreshed as well.

Once those spreadsheets are in, run the scrapers to layer in advertised prices and VDP URLs so price deltas remain accurate.

## 9) Validation
- Inspect a random sample of dealers to confirm absolute vs relative URLs.
- Open `data/snapshots/inventory_urls.snap` and spot-check a few SmartPath dealers.
- Query counts by backend and by status to ensure no drastic loss vs the Excel totals.
