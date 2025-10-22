# PRD 00 — Overview & Goals

## Summary
We will build a platform to aggregate Toyota dealer inventory for Land Cruiser, 4Runner, Tacoma, Tundra; enrich VIN data; and enable sales managers to find VINs with desired features below MSRP. Over time we accumulate "VIN intelligence" (sold counts, price trends, feature popularity).

## Goals
- Accurate, rate-limited scraping via Firecrawl.
- Single source of truth: `observations` (history), `listings` (current).
- Simple, fast search API + frontend.
- Upload spreadsheets to upsert trusted VIN metadata.
- Analytics for sold counts and top features.

## Non-Goals (v1)
- Negotiation/quoting workflows.
- Consumer UI.
- ML price optimization beyond deltas vs MSRP.

## Success Metrics
- ≥95% parse rate per job; <5% false sold/missing per month.
- Search p95 < 500ms for 50k listings.
- Agent-authored PRs small, deterministic; <1% parser regressions.
