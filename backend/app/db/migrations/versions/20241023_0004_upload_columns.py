"""Add status metadata to uploads

Revision ID: 0004_upload_columns
Revises: 0003_dealer_enrichment
Create Date: 2025-10-23 00:00:00
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "0004_upload_columns"
down_revision = "0003_dealer_enrichment"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "uploads",
        sa.Column("status", sa.Text(), nullable=False, server_default="processing"),
    )
    op.add_column(
        "uploads",
        sa.Column("processed_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "uploads",
        sa.Column("row_errors", postgresql.JSONB(astext_type=sa.Text()), nullable=False, server_default="[]"),
    )
    op.alter_column("uploads", "status", server_default=None)
    op.alter_column("uploads", "row_errors", server_default=None)


def downgrade() -> None:
    op.drop_column("uploads", "row_errors")
    op.drop_column("uploads", "processed_at")
    op.drop_column("uploads", "status")
