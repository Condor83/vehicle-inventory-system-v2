#!/usr/bin/env python3
"""Seed generator & loader for VIN Intelligence.

Usage:
  python scripts/seed_from_export.py --input ./data/raw/database_export.xlsx --out ./data/seeds
  python scripts/seed_from_export.py --load --in-dir ./data/seeds

This script:
  1) Reads the Excel export.
  2) Normalizes backend types, statuses, and URL templates.
  3) Emits CSVs for dealers, vehicles, listings, observations, price_events.
  4) Generates a 466×4 URL snapshot using backend/app/parsers/url_builder.py.
  5) Optionally loads CSVs into Postgres via Alembic migrations.

Edit `data/seed_mapping.yaml` to align your Excel column names.
"""
import argparse, os, sys, json, re
from pathlib import Path
from datetime import datetime, timezone
import pandas as pd
import yaml
import psycopg
from psycopg import sql
from alembic.config import Config as AlembicConfig
from alembic import command as alembic_command

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
SEEDS = DATA / "seeds"
SNAPSHOTS = DATA / "snapshots"

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

BACKEND_NORMALIZATION = {
    "DEALERINSPIRE": "DEALER_INSPIRE",
    "DEALERALCHEMIST.COM": "DEALER_ALCHEMY",
}

STATUS_MAP = {
    "AVAILABLE": "available",
    "PENDING": "pending",
    "IN_TRANSIT": "in_transit",
    "SOLD": "sold",
}

SUPPORTED_PLACEHOLDERS = ["{homepage_url}", "{model_slug}", "{model_plus}", "{model_name_encoded}"]

def any_col(df, candidates):
    for c in candidates:
        if c in df.columns:
            return c
    return None

def normalize_backend(v: str) -> str:
    if not isinstance(v, str): return v
    return BACKEND_NORMALIZATION.get(v.strip(), v.strip())

def map_status(v: str) -> str | None:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    s = str(v).strip().upper()
    return STATUS_MAP.get(s, s.lower())

def infer_scope(url_template: str) -> str:
    if not isinstance(url_template, str): return "relative"
    return "absolute" if url_template.startswith("http") else "relative"

def detect_smartpath(url_template: str) -> bool:
    if not isinstance(url_template, str): return False
    return "smartpath" in url_template.lower()

def restrict_placeholders(url_template: str) -> str:
    if not isinstance(url_template, str): return url_template
    # Replace common variants to supported tokens
    repl = {
        "{modelParam}": "{model_plus}",
        "{model_param}": "{model_plus}",
        "{model}": "{model_plus}",
        "{MODEL}": "{model_plus}",
        "{ModelSlug}": "{model_slug}",
        "{model_slugified}": "{model_slug}",
    }
    out = url_template
    for k,v in repl.items():
        out = out.replace(k, v)
    # Warn on unknown placeholders (leave as-is)
    return out

def load_mapping() -> dict:
    with open(DATA / "seed_mapping.yaml", "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def read_excel(path: Path) -> dict[str, pd.DataFrame]:
    xl = pd.ExcelFile(path)
    sheets = {name: xl.parse(name) for name in xl.sheet_names}
    return sheets

def write_csv(df: pd.DataFrame, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)

def generate_url_snapshot(dealers_df: pd.DataFrame, out_file: Path):
    from backend.app.parsers.url_builder import build_inventory_url
    models = ["Land Cruiser","4Runner","Tacoma","Tundra"]
    lines = []
    for _, row in dealers_df.iterrows():
        d = {
            "id": row["dealer_id"],
            "homepage_url": row.get("homepage_url", ""),
            "inventory_url_template": row.get("inventory_url_template", ""),
            "scraping_config": json.loads(row.get("scraping_config") or "{}"),
        }
        for m in models:
            url = build_inventory_url(d, m)
            lines.append(f"{d['id']}	{m}	{url}")
    out_file.parent.mkdir(parents=True, exist_ok=True)
    out_file.write_text("\n".join(lines), encoding="utf-8")

def load_to_postgres(in_dir: Path):
    raw_dsn = os.getenv("DATABASE_URL", "postgresql+psycopg://postgres:postgres@localhost:5432/vinintel")
    psycopg_dsn = raw_dsn
    if "+psycopg" in psycopg_dsn:
        psycopg_dsn = psycopg_dsn.replace("postgresql+psycopg", "postgresql", 1)

    alembic_cfg = AlembicConfig(str(ROOT / "alembic.ini"))
    alembic_cfg.set_main_option("sqlalchemy.url", raw_dsn)
    alembic_command.upgrade(alembic_cfg, "head")

    with psycopg.connect(psycopg_dsn) as conn:
        for name in ["dealers","vehicles","listings","observations","price_events"]:
            csvp = in_dir / f"{name}.csv"
            if not csvp.exists(): continue
            with conn.cursor() as cur, open(csvp, "r", encoding="utf-8") as f:
                cur.copy(sql.SQL("COPY {} FROM STDIN WITH CSV HEADER").format(sql.Identifier(name)), f)
            conn.commit()
    print("Loaded seeds into Postgres.")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", type=str, help="Path to database_export.xlsx")
    ap.add_argument("--out", type=str, help="Output dir for seed CSVs", default=str(SEEDS))
    ap.add_argument("--load", action="store_true", help="Load CSVs into Postgres")
    ap.add_argument("--in-dir", type=str, help="Input dir for loading")
    args = ap.parse_args()

    if args.load:
        in_dir = Path(args.in_dir or SEEDS)
        load_to_postgres(in_dir)
        sys.exit(0)

    if not args.input:
        print("--input is required unless using --load")
        sys.exit(1)

    mapping = load_mapping()["columns"]
    sheets = read_excel(Path(args.input))

    # ---- Dealers ----
    # Prefer a sheet explicitly named 'dealers'; otherwise try to infer via columns.
    dealers_sheet = None
    for preferred in ["dealers", "Dealers", "DEALERS"]:
        if preferred in sheets:
            dealers_sheet = sheets[preferred]
            break
    if dealers_sheet is None:
        for name, df in sheets.items():
            cols = {c.lower() for c in df.columns}
            if "backend_type" in cols and ("inventory_url_template" in cols or "vehicle_url_template" in cols):
                dealers_sheet = df
                break
    if dealers_sheet is None:
        for name, df in sheets.items():
            if any(c.lower() == "dealer_id" for c in df.columns):
                dealers_sheet = df
                break
    if dealers_sheet is None:
        raise RuntimeError("Unable to locate a dealers sheet with backend/template metadata.")

    def pick(df, key):
        col = None
        for c in mapping.get(key, []):
            if c in df.columns:
                col = c; break
        return col

    d = dealers_sheet.copy()
    d = d.rename(columns={
        pick(d,"dealer_id"): "dealer_id",
        pick(d,"dealer_name"): "name",
        pick(d,"region"): "region",
        pick(d,"backend_type"): "backend_type",
        pick(d,"homepage_url"): "homepage_url",
        pick(d,"inventory_url_template"): "inventory_url_template",
        pick(d,"vehicle_url_template"): "vehicle_url_template",
    })
    # normalize backend and templates
    # Fill optional columns if the export omits them to keep downstream schema happy.
    if "region" not in d.columns:
        d["region"] = None
    if "vehicle_url_template" not in d.columns:
        d["vehicle_url_template"] = None

    d["backend_type"] = d["backend_type"].apply(normalize_backend)
    d["inventory_url_template"] = d["inventory_url_template"].apply(restrict_placeholders)
    d["template_scope"] = d["inventory_url_template"].apply(infer_scope)
    d["uses_smartpath"] = d["inventory_url_template"].apply(detect_smartpath)
    d["scraping_config"] = d.apply(lambda r: json.dumps({"template_scope": r["template_scope"], "uses_smartpath": bool(r["uses_smartpath"]) }), axis=1)
    dealers_out = d[["dealer_id","name","region","backend_type","homepage_url","inventory_url_template","vehicle_url_template","scraping_config"]].copy()

    # ---- Vehicles ----
    veh_sheet = None
    for preferred in ["vehicles", "Vehicles", "VEHICLES"]:
        if preferred in sheets:
            veh_sheet = sheets[preferred]
            break
    if veh_sheet is None:
        for name, df in sheets.items():
            cols = {c.lower() for c in df.columns}
            if "vin" in cols and {"make","model","year"}.issubset(cols):
                veh_sheet = df
                break
    if veh_sheet is None:
        raise RuntimeError("Unable to locate vehicles sheet with VIN/make/model columns.")
    v = veh_sheet.copy()
    v = v.rename(columns={
        pick(v,"vin"): "vin",
        pick(v,"make"): "make",
        pick(v,"model"): "model",
        pick(v,"year"): "year",
        pick(v,"trim"): "trim",
        pick(v,"drivetrain"): "drivetrain",
        pick(v,"transmission"): "transmission",
        pick(v,"exterior_color"): "exterior_color",
        pick(v,"interior_color"): "interior_color",
        pick(v,"msrp"): "msrp",
        pick(v,"invoice_price"): "invoice_price",
        pick(v,"features"): "features",
    })
    v = v[~v["vin"].isna()]  # drop null VINs
    vehicles_out = v[["vin","make","model","year","trim","drivetrain","transmission","exterior_color","interior_color","msrp","invoice_price","features"]].copy()

    # ---- Listings ----
    inv_sheet = None
    for name, df in sheets.items():
        cols = [c.lower() for c in df.columns]
        if "vin" in cols and "dealer_id" in cols and ("status" in cols or "Status" in df.columns):
            inv_sheet = df; break
    li = inv_sheet.copy()
    li = li.rename(columns={
        pick(li,"dealer_id"): "dealer_id",
        pick(li,"vin"): "vin",
        pick(li,"status"): "status",
        pick(li,"advertised_price"): "advertised_price",
        pick(li,"vdp_url"): "vdp_url",
        pick(li,"first_seen_at"): "first_seen_at",
        pick(li,"last_seen_at"): "last_seen_at",
    })
    li["status"] = li["status"].apply(map_status)
    # price fallback rule applied later in observations
    listings_out = li[["dealer_id","vin","vdp_url","status","advertised_price","first_seen_at","last_seen_at"]].copy()

    # compute price_delta_msrp by joining vehicles
    vm = vehicles_out[["vin","msrp"]].copy()
    listings_out = listings_out.merge(vm, how="left", on="vin")
    listings_out["price_delta_msrp"] = listings_out.apply(
        lambda r: (float(r["advertised_price"]) - float(r["msrp"])) if pd.notna(r["advertised_price"]) and pd.notna(r["msrp"]) else None,
        axis=1
    )

    # ---- Observations ---- (one per listing row, source='import')
    def obs_row(r):
        payload = {}
        if pd.isna(r.get("advertised_price")) and pd.notna(r.get("msrp")):
            payload = {"assumptions": {"ad_price_equals_msrp": True}}
        return {
            "job_id": "00000000-0000-0000-0000-000000000000",
            "observed_at": r.get("last_seen_at") or r.get("first_seen_at") or datetime.now(timezone.utc).isoformat(),
            "dealer_id": r["dealer_id"],
            "vin": r["vin"],
            "vdp_url": r.get("vdp_url"),
            "advertised_price": r.get("advertised_price") if pd.notna(r.get("advertised_price")) else (r.get("msrp") if pd.notna(r.get("msrp")) else None),
            "msrp": r.get("msrp"),
            "payload": json.dumps(payload),
            "raw_blob_key": None,
            "source": "import",
        }

    merged = listings_out.merge(vm, how="left", on="vin", suffixes=("","_veh"))
    observations_out = pd.DataFrame([obs_row(r) for _, r in merged.iterrows()])

    # ---- Price Events (optional) ----
    price_events_out = pd.DataFrame(columns=["dealer_id","vin","observed_at","old_price","new_price","delta","pct"])

    # Ensure directories
    outdir = Path(args.out)
    outdir.mkdir(parents=True, exist_ok=True)
    SNAPSHOTS.mkdir(parents=True, exist_ok=True)

    # Write CSVs
    write_csv(dealers_out, outdir / "dealers.csv")
    write_csv(vehicles_out, outdir / "vehicles.csv")
    write_csv(listings_out[["dealer_id","vin","vdp_url","status","advertised_price","price_delta_msrp","first_seen_at","last_seen_at"]], outdir / "listings.csv")
    write_csv(observations_out, outdir / "observations.csv")
    write_csv(price_events_out, outdir / "price_events.csv")

    # URL snapshot
    snapshot_file = SNAPSHOTS / "inventory_urls.snap"
    generate_url_snapshot(dealers_out, snapshot_file)

    print(f"Seeds written to {outdir}")
    print(f"URL snapshot written to {snapshot_file}")

if __name__ == "__main__":
    main()
