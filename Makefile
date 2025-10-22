.PHONY: fmt lint test seed up api

up:
	docker compose -f docker/compose.yml up -d

fmt:
	black .
	ruff check --fix .

lint:
	ruff check .
	mypy backend

test:
	pytest -q

api:
	uvicorn backend.app.api.main:app --reload

seed:
	python scripts/seed_from_export.py --input ./data/raw/database_export.xlsx --out ./data/seeds
	python scripts/seed_from_export.py --load --in-dir ./data/seeds
