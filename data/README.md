# Data Directory

- `models.yaml` — model slug registry used by URL builder and tests.
- `backend_templates.yaml` — example CMS templates (you may keep per-dealer overrides instead).
- `seed_mapping.yaml` — column mapping hints for the seeding script.
- `seeds/` — generated CSVs will be written here by `scripts/seed_from_export.py`.
- `raw/` — place your `database_export.xlsx` here before running the seed script.
