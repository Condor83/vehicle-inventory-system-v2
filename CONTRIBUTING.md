# Contributing

## Branching & PRs
- Use `feat/<area>-<ticket>` branches.
- Keep PRs < 400 LOC and single-purpose.
- Include tests and a runnable command in the PR description.

## Checks
- `make fmt && make lint && make test` must pass.
- For schema changes, include Alembic migrations with downgrade.

## Docs
- Update package README and docstrings for new modules.
- Add ADRs for non-trivial decisions.

## Security
- No secrets in code; use `.env.example`.
- Use `safety check` and `bandit` locally for risky changes.
