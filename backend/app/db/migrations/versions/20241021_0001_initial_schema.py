"""Initial schema extracted from bootstrap SQL.

Revision ID: 0001_initial
Revises: None
Create Date: 2025-10-22 02:56:00
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "0001_initial"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "dealers",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("code", sa.Text(), nullable=True),
        sa.Column("region", sa.Text(), nullable=True),
        sa.Column("homepage_url", sa.Text(), nullable=True),
        sa.Column("backend_type", sa.Text(), nullable=False),
        sa.Column("is_active", sa.Boolean(), nullable=True, server_default=sa.text("true")),
        sa.Column("last_scraped_at", sa.DateTime(timezone=True), nullable=True),
        sa.UniqueConstraint("code"),
    )

    op.create_table(
        "dealer_backend_templates",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("backend_type", sa.Text(), nullable=False),
        sa.Column("inventory_type", sa.Text(), nullable=False),
        sa.Column("url_template", sa.Text(), nullable=False),
        sa.Column("model_format", sa.Text(), nullable=False),
        sa.Column("requires_body_style", sa.Boolean(), server_default=sa.text("false")),
        sa.Column("requires_model_id", sa.Boolean(), server_default=sa.text("false")),
        sa.UniqueConstraint("backend_type", "inventory_type", "url_template"),
    )

    op.create_table(
        "vehicles",
        sa.Column("vin", sa.String(length=17), primary_key=True),
        sa.Column("make", sa.Text(), nullable=False),
        sa.Column("model", sa.Text(), nullable=False),
        sa.Column("year", sa.Integer(), nullable=True),
        sa.Column("trim", sa.Text(), nullable=True),
        sa.Column("drivetrain", sa.Text(), nullable=True),
        sa.Column("transmission", sa.Text(), nullable=True),
        sa.Column("exterior_color", sa.Text(), nullable=True),
        sa.Column("interior_color", sa.Text(), nullable=True),
        sa.Column("msrp", sa.Numeric(10, 2), nullable=True),
        sa.Column("invoice_price", sa.Numeric(10, 2), nullable=True),
        sa.Column("features", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
    )

    op.create_table(
        "price_events",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("dealer_id", sa.Integer(), nullable=False),
        sa.Column("vin", sa.String(length=17), nullable=False),
        sa.Column("observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("old_price", sa.Numeric(10, 2), nullable=True),
        sa.Column("new_price", sa.Numeric(10, 2), nullable=True),
        sa.Column("delta", sa.Numeric(10, 2), nullable=True),
        sa.Column("pct", sa.Numeric(6, 2), nullable=True),
    )

    op.create_table(
        "scrape_jobs",
        sa.Column("id", postgresql.UUID(as_uuid=False), primary_key=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("model", sa.Text(), nullable=True),
        sa.Column("region", sa.Text(), nullable=True),
        sa.Column("status", sa.Text(), nullable=True),
        sa.Column("target_count", sa.Integer(), nullable=True),
        sa.Column("success_count", sa.Integer(), nullable=True),
        sa.Column("fail_count", sa.Integer(), nullable=True),
        sa.Column("notes", sa.Text(), nullable=True),
    )

    op.create_table(
        "uploads",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("uploaded_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=True),
        sa.Column("filename", sa.Text(), nullable=True),
        sa.Column("dealer_id", sa.Integer(), nullable=True),
        sa.Column("rows_ingested", sa.Integer(), nullable=True),
        sa.Column("rows_updated", sa.Integer(), nullable=True),
        sa.Column("errors", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    )

    op.create_table(
        "listings",
        sa.Column("dealer_id", sa.Integer(), nullable=False),
        sa.Column("vin", sa.String(length=17), nullable=False),
        sa.Column("vdp_url", sa.Text(), nullable=True),
        sa.Column("stock_number", sa.Text(), nullable=True),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("advertised_price", sa.Numeric(10, 2), nullable=True),
        sa.Column("price_delta_msrp", sa.Numeric(10, 2), nullable=True),
        sa.Column("first_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("source_rank", sa.Integer(), server_default=sa.text("100"), nullable=True),
        sa.ForeignKeyConstraint(["dealer_id"], ["dealers.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["vin"], ["vehicles.vin"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("dealer_id", "vin"),
    )

    op.create_table(
        "observations",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("job_id", postgresql.UUID(as_uuid=False), nullable=False),
        sa.Column("observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("dealer_id", sa.Integer(), nullable=False),
        sa.Column("vin", sa.String(length=17), nullable=False),
        sa.Column("vdp_url", sa.Text(), nullable=True),
        sa.Column("advertised_price", sa.Numeric(10, 2), nullable=True),
        sa.Column("msrp", sa.Numeric(10, 2), nullable=True),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("raw_blob_key", sa.Text(), nullable=True),
        sa.Column("source", sa.Text(), nullable=False),
        sa.ForeignKeyConstraint(["dealer_id"], ["dealers.id"]),
    )

    op.create_table(
        "scrape_tasks",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("job_id", postgresql.UUID(as_uuid=False), nullable=True),
        sa.Column("dealer_id", sa.Integer(), nullable=False),
        sa.Column("url", sa.Text(), nullable=False),
        sa.Column("attempt", sa.Integer(), server_default=sa.text("1"), nullable=True),
        sa.Column("status", sa.Text(), nullable=True),
        sa.Column("http_status", sa.Integer(), nullable=True),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(["job_id"], ["scrape_jobs.id"], ondelete="CASCADE"),
    )

    op.create_index("idx_listings_status", "listings", ["status"])
    op.create_index("idx_listings_dealer_last", "listings", ["dealer_id", "last_seen_at"])
    op.create_index("idx_observations_dvtime", "observations", ["dealer_id", "vin", "observed_at"], unique=False)
    op.create_index("idx_price_events_vin_time", "price_events", ["vin", "observed_at"], unique=False)


def downgrade() -> None:
    op.drop_index("idx_price_events_vin_time", table_name="price_events")
    op.drop_index("idx_observations_dvtime", table_name="observations")
    op.drop_index("idx_listings_dealer_last", table_name="listings")
    op.drop_index("idx_listings_status", table_name="listings")
    op.drop_table("scrape_tasks")
    op.drop_table("observations")
    op.drop_table("listings")
    op.drop_table("uploads")
    op.drop_table("scrape_jobs")
    op.drop_table("price_events")
    op.drop_table("vehicles")
    op.drop_table("dealer_backend_templates")
    op.drop_table("dealers")
