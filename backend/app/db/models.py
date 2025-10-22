from sqlalchemy import (
    Column, Integer, BigInteger, String, Numeric, Boolean, Text, DateTime, ForeignKey, UniqueConstraint, text
)
from sqlalchemy.orm import declarative_base
from sqlalchemy.dialects.postgresql import JSONB, UUID

Base = declarative_base()

class Dealer(Base):
    __tablename__ = "dealers"
    id = Column(Integer, primary_key=True)
    name = Column(Text, nullable=False)
    code = Column(Text, unique=True)
    region = Column(Text)
    homepage_url = Column(Text)
    backend_type = Column(Text, nullable=False)
    is_active = Column(Boolean, default=True)
    last_scraped_at = Column(DateTime(timezone=True))

class DealerBackendTemplate(Base):
    __tablename__ = "dealer_backend_templates"
    id = Column(Integer, primary_key=True)
    backend_type = Column(Text, nullable=False)
    inventory_type = Column(Text, nullable=False)  # new|used|certified
    url_template = Column(Text, nullable=False)
    model_format = Column(Text, nullable=False)    # kebab|space_plus|underscore|passthrough
    requires_body_style = Column(Boolean, default=False)
    requires_model_id = Column(Boolean, default=False)
    __table_args__ = (UniqueConstraint("backend_type","inventory_type","url_template"),)

class Vehicle(Base):
    __tablename__ = "vehicles"
    vin = Column(String(17), primary_key=True)
    make = Column(Text, nullable=False)
    model = Column(Text, nullable=False)
    year = Column(Integer)
    trim = Column(Text)
    drivetrain = Column(Text)
    transmission = Column(Text)
    exterior_color = Column(Text)
    interior_color = Column(Text)
    msrp = Column(Numeric(10,2))
    invoice_price = Column(Numeric(10,2))
    features = Column(JSONB)
    created_at = Column(DateTime(timezone=True), server_default=text("now()"))
    updated_at = Column(DateTime(timezone=True))

class Listing(Base):
    __tablename__ = "listings"
    dealer_id = Column(Integer, ForeignKey("dealers.id", ondelete="CASCADE"), primary_key=True)
    vin = Column(String(17), ForeignKey("vehicles.vin", ondelete="CASCADE"), primary_key=True)
    vdp_url = Column(Text)
    stock_number = Column(Text)
    status = Column(Text, nullable=False)  # available|sold|missing|pending|in_transit|hold
    advertised_price = Column(Numeric(10,2))
    price_delta_msrp = Column(Numeric(10,2))
    first_seen_at = Column(DateTime(timezone=True), nullable=False)
    last_seen_at = Column(DateTime(timezone=True), nullable=False)
    source_rank = Column(Integer, default=100)

class Observation(Base):
    __tablename__ = "observations"
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    job_id = Column(UUID(as_uuid=True), nullable=False)
    observed_at = Column(DateTime(timezone=True), nullable=False)
    dealer_id = Column(Integer, ForeignKey("dealers.id"), nullable=False)
    vin = Column(String(17), nullable=False)
    vdp_url = Column(Text)
    advertised_price = Column(Numeric(10,2))
    msrp = Column(Numeric(10,2))
    payload = Column(JSONB)
    raw_blob_key = Column(Text)
    source = Column(Text, nullable=False)  # inventory_list | vdp | upload

class PriceEvent(Base):
    __tablename__ = "price_events"
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    dealer_id = Column(Integer, nullable=False)
    vin = Column(String(17), nullable=False)
    observed_at = Column(DateTime(timezone=True), nullable=False)
    old_price = Column(Numeric(10,2))
    new_price = Column(Numeric(10,2))
    delta = Column(Numeric(10,2))
    pct = Column(Numeric(6,2))

class ScrapeJob(Base):
    __tablename__ = "scrape_jobs"
    id = Column(UUID(as_uuid=True), primary_key=True)
    created_at = Column(DateTime(timezone=True), server_default=text("now()"))
    started_at = Column(DateTime(timezone=True))
    completed_at = Column(DateTime(timezone=True))
    model = Column(Text)
    region = Column(Text)
    status = Column(Text) # pending|running|success|partial|failed
    target_count = Column(Integer)
    success_count = Column(Integer)
    fail_count = Column(Integer)
    notes = Column(Text)

class ScrapeTask(Base):
    __tablename__ = "scrape_tasks"
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    job_id = Column(UUID(as_uuid=True), ForeignKey("scrape_jobs.id", ondelete="CASCADE"))
    dealer_id = Column(Integer, nullable=False)
    url = Column(Text, nullable=False)
    attempt = Column(Integer, default=1)
    status = Column(Text)  # pending|running|success|retry|failed
    http_status = Column(Integer)
    error = Column(Text)
    started_at = Column(DateTime(timezone=True))
    completed_at = Column(DateTime(timezone=True))

class Upload(Base):
    __tablename__ = "uploads"
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    uploaded_at = Column(DateTime(timezone=True), server_default=text("now()"))
    filename = Column(Text)
    dealer_id = Column(Integer)
    rows_ingested = Column(Integer)
    rows_updated = Column(Integer)
    errors = Column(JSONB)
