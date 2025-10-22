# ADR-003: Adopt Alembic For Database Migrations

## Status
Accepted — 2025-10-21

## Context
The initial bootstrap relied on a hand-maintained `backend/app/db/schema.sql` script to create tables. This approach made it difficult to evolve the schema safely, lacked downgrade paths, and conflicted with our standards that call for Alembic-backed migrations with explicit PR notes.

We frequently adjust the canonical data model (e.g., `observations` → `listings` derivations, analytics views, new indexes), so we need repeatable, versioned migrations that integrate with CI and local seeding.

## Decision
We introduced an Alembic environment (`alembic.ini`, `backend/app/db/migrations/`) and captured the existing schema in an initial revision `0001_initial`. The seeding script now runs Alembic before COPY-ing data, removing any dependency on the old SQL file. Future schema updates must be delivered via Alembic revisions with downgrade paths.

## Consequences
- Schema changes become auditable and reversible.
- Onboarding scripts (`scripts/seed_from_export.py --load`) automatically migrate databases to head.
- CI can invoke `alembic upgrade head` to validate migrations.
- Engineers must create migrations alongside SQLAlchemy model changes.
