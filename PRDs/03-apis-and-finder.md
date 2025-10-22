# PRD 03 — APIs & VIN Finder

## Deliverables
- `GET /search` with filters: model, year, trim, region, features[], max_mileage, below_msrp, status (default: available); pagination + sorting.
- `GET /vin/:vin` (specs, active listings, price timeline, last observation).
- `POST /scrape/jobs`, `GET /scrape/jobs/:id`.
- Next.js VIN Finder page (filters, table, CSV export); VIN Detail page (timeline).

## Acceptance
- API tests with seeded data; p95 < 500ms on 50k synthetic listings.
- E2E test (Playwright): filter + below MSRP + export works.
