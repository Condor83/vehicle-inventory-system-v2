"""Add supporting indexes for search queries.

Revision ID: 0002_search_indexes
Revises: 0001_initial
Create Date: 2025-10-22 03:45:00
"""

from alembic import op


revision = "0002_search_indexes"
down_revision = "0001_initial"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_index("idx_listings_status_price", "listings", ["status", "advertised_price"])
    op.create_index("idx_listings_price_delta", "listings", ["price_delta_msrp"])
    op.create_index("idx_vehicles_model_year", "vehicles", ["model", "year"])
    op.create_index("idx_dealers_region", "dealers", ["region"])
    op.create_index("idx_vehicles_features_gin", "vehicles", ["features"], postgresql_using="gin")


def downgrade() -> None:
    op.drop_index("idx_vehicles_features_gin", table_name="vehicles")
    op.drop_index("idx_dealers_region", table_name="dealers")
    op.drop_index("idx_vehicles_model_year", table_name="vehicles")
    op.drop_index("idx_listings_price_delta", table_name="listings")
    op.drop_index("idx_listings_status_price", table_name="listings")
