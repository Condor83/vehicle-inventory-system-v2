"""Add dealer enrichment columns

Revision ID: 0003_dealer_enrichment
Revises: 0002_search_indexes
Create Date: 2025-10-22 00:00:00
"""

from alembic import op
import sqlalchemy as sa


revision = "0003_dealer_enrichment"
down_revision = "0002_search_indexes"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("dealers") as batch_op:
        batch_op.add_column(sa.Column("district_code", sa.Text()))
        batch_op.add_column(sa.Column("phone", sa.Text()))
        batch_op.add_column(sa.Column("city", sa.Text()))
        batch_op.add_column(sa.Column("state", sa.Text()))
        batch_op.add_column(sa.Column("postal_code", sa.Text()))


def downgrade() -> None:
    with op.batch_alter_table("dealers") as batch_op:
        batch_op.drop_column("postal_code")
        batch_op.drop_column("state")
        batch_op.drop_column("city")
        batch_op.drop_column("phone")
        batch_op.drop_column("district_code")
