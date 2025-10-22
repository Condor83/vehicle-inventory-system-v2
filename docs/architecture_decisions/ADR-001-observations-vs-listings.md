# ADR-001: Observations vs Listings

We store every scrape result in an append-only `observations` table. A separate `listings` table holds the latest per `(dealer, vin)`. This keeps history immutable and the current view fast, enabling reproducible analytics and simpler code. Price changes become `price_events` derived from consecutive observations.
