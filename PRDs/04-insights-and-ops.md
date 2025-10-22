# PRD 04 — Insights & Ops

## Deliverables
- Sold detection: missing twice → sold; 7-day reappearance at another dealer → transfer.
- `GET /analytics/sold`, `GET /analytics/top-features` with materialized views.
- Ops page: start jobs, monitor tasks, view failures, retry; spreadsheet upload flow with lineage.

## Acceptance
- Unit tests for state transitions and transfers.
- Materialized views refresh successfully and power Insights page.
