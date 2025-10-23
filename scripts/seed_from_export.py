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
import argparse, os, sys, json, re, unicodedata, io
from pathlib import Path
from datetime import datetime, timezone
import pandas as pd
import yaml
import psycopg
from psycopg import sql
from alembic.config import Config as AlembicConfig
from alembic import command as alembic_command
from typing import Any, Dict

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
SEEDS = DATA / "seeds"
SNAPSHOTS = DATA / "snapshots"

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

BACKEND_NORMALIZATION = {
    "DEALERINSPIRE": "DEALER_INSPIRE",
    "DEALERALCHEMIST.COM": "DEALER_ALCHEMY",
    "DEALER ON": "DEALERON",
    "DEALER_ON": "DEALERON",
    "DEALERON": "DEALERON",
}

STATUS_MAP = {
    "AVAILABLE": "available",
    "PENDING": "pending",
    "IN_TRANSIT": "in_transit",
    "SOLD": "sold",
}

SUPPORTED_PLACEHOLDERS = [
    "{homepage_url}",
    "{model_slug}",
    "{model_plus}",
    "{model_name_plus}",
    "{model_name_encoded}",
    "{model_encoded}",
    "{model_series}",
    "{model_id}",
    "{model_underscore}",
    "{city}",
    "{state}",
    "{city_code}",
    "{dealer_code}",
]

LOCATOR_GLOB = "Vehicle Locator*.xlsx"
TOKEN_OVERRIDES = {
    143: {"city": "lansing", "state": "mi", "city_code": "48911"},
}

TEAM_VELOCITY_DEALER_IDS = {
    91,
    109,
    117,
    208,
    445,
}

DEALER_OVERRIDES = {
    355: {
        "homepage_url": "https://www.downtowntoyota.com",
        "inventory_url_template": "https://www.downtowntoyota.com/new-inventory/index.htm?model={model_plus}",
        "vehicle_url_template": "https://www.downtowntoyota.com/new-inventory/index.htm?vin={vin}",
        "scraping_config": {
            "template_scope": "absolute",
            "firecrawl": {"proxy": "stealth"},
        },
    },
    358: {
        "homepage_url": "https://www.fremonttoyota.com",
        "inventory_url_template": "https://www.fremonttoyota.com/search/new-toyota-{model_slug}-fremont-ca/?s:df=1&cy=94538&tp=new&md={model_id}",
        "vehicle_url_template": "https://www.fremonttoyota.com/new-inventory/index.htm?vin={vin}",
        "scraping_config": {
            "template_scope": "absolute",
            "firecrawl": {"proxy": "stealth"},
        },
    },
    365: {
        "homepage_url": "https://www.toyotapaloalto.com",
        "inventory_url_template": "https://www.toyotapaloalto.com/new-inventory/index.htm?model={model_plus}",
        "vehicle_url_template": "https://www.toyotapaloalto.com/new-inventory/index.htm?vin={vin}",
        "scraping_config": {
            "template_scope": "absolute",
            "firecrawl": {"proxy": "stealth"},
        },
    },
    393: {
        "homepage_url": "https://www.toyotaoftacoma.com",
        "inventory_url_template": "https://www.toyotaoftacoma.com/search/new-toyota-{model_slug}-tacoma-wa/?cy=98409&md={model_id}&mk=63&tp=new",
        "vehicle_url_template": "https://www.toyotaoftacoma.com/new-inventory/index.htm?vin={vin}",
        "scraping_config": {
            "template_scope": "absolute",
            "firecrawl": {"proxy": "stealth"},
        },
    },
    461: {
        "homepage_url": "https://www.toyotacarlsbad.com",
        "inventory_url_template": "https://www.toyotacarlsbad.com/new-inventory/index.htm?model={model_plus}",
        "vehicle_url_template": "https://www.toyotacarlsbad.com/new-inventory/index.htm?vin={vin}",
        "scraping_config": {
            "template_scope": "absolute",
            "firecrawl": {"proxy": "stealth"},
        },
    },
}

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


def slugify(value) -> str | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    text = str(value).strip()
    if not text:
        return None
    normalized = unicodedata.normalize("NFKD", text)
    ascii_text = normalized.encode("ascii", "ignore").decode("ascii")
    ascii_text = re.sub(r"[^\w\s-]", "", ascii_text).strip().lower()
    slug = re.sub(r"[-\s]+", "-", ascii_text)
    return slug or None


def clean_zip(value) -> str | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    text = str(value).strip()
    if text.endswith(".0"):
        text = text[:-2]
    digits = re.sub(r"\D", "", text)
    if not digits:
        return None
    if len(digits) in {4,5}:
        return digits.zfill(5)
    return digits


def stringify(value) -> str | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    if isinstance(value, float) and value.is_integer():
        value = int(value)
    text = str(value).strip()
    return text or None

def infer_scope(url_template: str) -> str:
    if not isinstance(url_template, str): return "relative"
    return "absolute" if url_template.startswith("http") else "relative"

def detect_smartpath(url_template: str) -> bool:
    if not isinstance(url_template, str): return False
    return "smartpath" in url_template.lower()


def coerce_backend(row: pd.Series) -> str:
    backend = row.get("backend_type")
    backend_str = backend.strip() if isinstance(backend, str) else (backend or "")
    template = row.get("inventory_url_template") or ""
    homepage = row.get("homepage_url") or ""

    template_lower = str(template).lower()
    homepage_lower = str(homepage).lower()

    if backend_str == "DEALER_SOCKET":
        if "dealeron" in template_lower or "dealeron" in homepage_lower or "searchnew.aspx" in template_lower:
            return "DEALERON"

    return backend_str


def classify_backend(row: pd.Series) -> str:
    backend = row.get("backend_type")
    backend_str = backend.strip() if isinstance(backend, str) else (backend or "")
    template = row.get("inventory_url_template") or ""
    dealer_id_raw = row.get("dealer_id")
    try:
        dealer_id = int(dealer_id_raw)
    except Exception:
        dealer_id = None

    if detect_smartpath(template):
        return "SMARTPATH"
    if dealer_id in TEAM_VELOCITY_DEALER_IDS:
        return "TEAM_VELOCITY"
    return backend_str

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
        "{model_name_plus}": "{model_plus}",
        "{model_encoded}": "{model_name_encoded}",
    }
    out = url_template
    for k,v in repl.items():
        out = out.replace(k, v)
    # Warn on unknown placeholders (leave as-is)
    return out

def apply_dealer_overrides(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    updated = df.copy()
    normalized_ids = pd.to_numeric(updated["dealer_id"], errors="coerce")
    for dealer_id, override in DEALER_OVERRIDES.items():
        mask = normalized_ids == dealer_id
        if not mask.any():
            continue
        for column, value in override.items():
            if column == "scraping_config":
                existing_raw = updated.loc[mask, column].iloc[0]
                try:
                    existing_cfg = json.loads(existing_raw) if isinstance(existing_raw, str) else (existing_raw or {})
                except json.JSONDecodeError:
                    existing_cfg = {}
                patch_cfg = value or {}
                merged_cfg = {**existing_cfg, **{k: v for k, v in patch_cfg.items() if k != "firecrawl"}}
                if "firecrawl" in patch_cfg:
                    existing_firecrawl = existing_cfg.get("firecrawl")
                    if not isinstance(existing_firecrawl, dict):
                        existing_firecrawl = {}
                    merged_cfg["firecrawl"] = {**existing_firecrawl, **patch_cfg["firecrawl"]}
                updated.loc[mask, column] = json.dumps(merged_cfg)
            else:
                updated.loc[mask, column] = value
    return updated

def load_mapping() -> dict:
    with open(DATA / "seed_mapping.yaml", "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def read_excel(path: Path) -> dict[str, pd.DataFrame]:
    xl = pd.ExcelFile(path)
    sheets = {name: xl.parse(name) for name in xl.sheet_names}
    return sheets


def load_vehicle_locator_data() -> pd.DataFrame:
    from pathlib import Path

    frames = []
    for path in (DATA / "raw").glob(LOCATOR_GLOB):
        try:
            frames.append(pd.read_excel(path, sheet_name=0))
        except Exception as exc:
            print(f"Warning: unable to read {path}: {exc}")
    if not frames:
        return pd.DataFrame(columns=["dealer_code", "region", "district", "phone"])
    combined = pd.concat(frames, ignore_index=True)
    combined = combined.rename(columns={
        "Dealer Code": "dealer_code",
        "Region": "locator_region",
        "District": "locator_district",
        "Dealer Phone": "locator_phone",
    })
    combined["dealer_code"] = combined["dealer_code"].astype(str).str.strip()
    grouped = combined.groupby("dealer_code").agg({
        "locator_region": "first",
        "locator_district": "first",
        "locator_phone": "first",
    }).reset_index()
    grouped = grouped.rename(columns={
        "locator_region": "region_code",
        "locator_district": "district_code",
        "locator_phone": "phone",
    })
    return grouped


def extract_city_codes(inv_sheet: pd.DataFrame | None) -> dict[int, str]:
    if inv_sheet is None:
        return {}

    dealer_col = None
    if "dealer_id" in inv_sheet.columns:
        dealer_col = "dealer_id"
    else:
        for candidate in ["DealerID", "dealerId"]:
            if candidate in inv_sheet.columns:
                dealer_col = candidate
                break
    url_col = None
    for candidate in ["vehicle_url", "VehicleURL", "Vehicle Url"]:
        if candidate in inv_sheet.columns:
            url_col = candidate
            break
    if dealer_col is None or url_col is None:
        return {}

    city_map: dict[int, str] = {}
    for _, row in inv_sheet.iterrows():
        dealer_id = row.get(dealer_col)
        if pd.isna(dealer_id):
            continue
        try:
            dealer_id = int(dealer_id)
        except ValueError:
            continue
        url = row.get(url_col)
        if not isinstance(url, str):
            continue
        match = re.search(r"[?&]cy=(\d{4,5})", url)
        if match:
            city_map.setdefault(dealer_id, match.group(1))
    return city_map

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
        with conn.cursor() as cur:
            cur.execute(
                "TRUNCATE TABLE observations, price_events, listings, vehicles, dealers RESTART IDENTITY CASCADE"
            )
            conn.commit()

        for name in ["dealers","vehicles","listings","observations","price_events"]:
            csvp = in_dir / f"{name}.csv"
            if not csvp.exists():
                continue
            if name == "dealers":
                df = pd.read_csv(csvp)
                subset = df[[
                    "dealer_id",
                    "code",
                    "name",
                    "region",
                    "homepage_url",
                    "backend_type",
                    "district_code",
                    "phone",
                    "city",
                    "state",
                    "postal_code",
                ]].rename(columns={"dealer_id": "id"})
                subset["code"] = subset["code"].where(subset["code"].notna(), subset["id"].astype(str))
                subset["code"] = subset["code"].astype(str).str.strip()
                subset = subset[[
                    "id",
                    "name",
                    "code",
                    "region",
                    "homepage_url",
                    "backend_type",
                    "district_code",
                    "phone",
                    "city",
                    "state",
                    "postal_code",
                ]]
                subset = subset.where(pd.notnull(subset), None)
                records = subset.to_dict(orient="records")
                with conn.cursor() as cur:
                    cur.executemany(
                        """
                        INSERT INTO dealers (id, name, code, region, homepage_url, backend_type, district_code, phone, city, state, postal_code)
                        VALUES (%(id)s, %(name)s, %(code)s, %(region)s, %(homepage_url)s, %(backend_type)s, %(district_code)s, %(phone)s, %(city)s, %(state)s, %(postal_code)s)
                        """,
                        records,
                    )
                conn.commit()
                continue

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
    locator_df = load_vehicle_locator_data()
    city_code_map = extract_city_codes(sheets.get("dealer_inventory"))

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

    rename_targets = {
        "dealer_id": "dealer_id",
        "dealer_name": "name",
        "region": "region",
        "backend_type": "backend_type",
        "homepage_url": "homepage_url",
        "inventory_url_template": "inventory_url_template",
        "vehicle_url_template": "vehicle_url_template",
        "dealer_code": "dealer_code",
        "city": "city",
        "state": "state",
        "zip_code": "zip_code",
        "phone": "phone",
    }
    rename_map = {}
    for key, alias in rename_targets.items():
        col = pick(dealers_sheet, key)
        if col and col != alias:
            rename_map[col] = alias

    d = dealers_sheet.copy().rename(columns=rename_map)
    # normalize backend and templates
    # Fill optional columns if the export omits them to keep downstream schema happy.
    if "region" not in d.columns:
        d["region"] = None
    if "vehicle_url_template" not in d.columns:
        d["vehicle_url_template"] = None
    for optional in ["dealer_code", "city", "state", "zip_code", "phone"]:
        if optional not in d.columns:
            d[optional] = None

    d["dealer_code"] = d["dealer_code"].apply(stringify)
    d = d.merge(locator_df, how="left", on="dealer_code")
    if "phone_y" in d.columns:
        d["phone"] = d["phone_y"].combine_first(d.get("phone_x"))
    elif "phone_x" in d.columns:
        d["phone"] = d["phone_x"]
    if "phone" in d.columns:
        d["phone"] = d["phone"].apply(stringify)
    d = d.drop(columns=[c for c in ["phone_x", "phone_y"] if c in d.columns])
    if "region_code" in d.columns:
        d["region"] = d["region"].fillna(d["region_code"])
        d["region_code"] = d["region_code"].apply(stringify)
    if "district_code" in d.columns:
        d["district_code"] = d["district_code"].apply(stringify)
    if "zip_code" in d.columns:
        d["postal_code"] = d["zip_code"]
        d = d.drop(columns=["zip_code"])

    d["backend_type"] = d["backend_type"].apply(normalize_backend)
    d["backend_type"] = d.apply(coerce_backend, axis=1)
    d["backend_type"] = d.apply(classify_backend, axis=1)
    d["inventory_url_template"] = d["inventory_url_template"].apply(restrict_placeholders)
    d["template_scope"] = d["inventory_url_template"].apply(infer_scope)
    d["uses_smartpath"] = d["inventory_url_template"].apply(detect_smartpath)
    def build_scraping_config(row: pd.Series) -> str:
        cfg: Dict[str, Any] = {
            "template_scope": row["template_scope"],
            "uses_smartpath": bool(row["uses_smartpath"]),
        }
        tokens: Dict[str, Any] = {}
        dealer_code = stringify(row.get("dealer_code"))
        if dealer_code:
            tokens["dealer_code"] = dealer_code
        if row.get("region_code"):
            tokens.setdefault("region", str(row.get("region_code")))
        if row.get("district_code"):
            tokens.setdefault("district_code", str(row.get("district_code")))
        if row.get("phone"):
            cfg.setdefault("contact", {})
            cfg["contact"]["phone"] = str(row.get("phone"))
        city_slug = slugify(row.get("city"))
        if city_slug:
            tokens["city"] = city_slug
        state_val = row.get("state")
        if isinstance(state_val, str) and state_val.strip():
            tokens["state"] = state_val.strip().lower()
        zip_val = clean_zip(row.get("postal_code"))
        if zip_val:
            tokens["city_code"] = zip_val
        dealer_id = row.get("dealer_id")
        if pd.notna(dealer_id):
            dealer_id_int = int(dealer_id)
            if dealer_id_int in city_code_map and "city_code" not in tokens:
                tokens["city_code"] = city_code_map[dealer_id_int]
            override = TOKEN_OVERRIDES.get(dealer_id_int)
            if override:
                tokens.update({k: v for k, v in override.items() if v})
        if tokens:
            cfg["tokens"] = tokens
        return json.dumps(cfg)

    d["scraping_config"] = d.apply(build_scraping_config, axis=1)
    dealer_columns = [
        "dealer_id",
        "name",
        "region",
        "backend_type",
        "homepage_url",
        "inventory_url_template",
        "vehicle_url_template",
        "scraping_config",
        "dealer_code",
        "district_code",
        "phone",
        "city",
        "state",
        "postal_code",
    ]
    dealers_out = d[dealer_columns].copy()
    dealers_out["code"] = dealers_out["dealer_code"]
    dealers_out = dealers_out[
        [
            "dealer_id",
            "name",
            "code",
            "region",
            "backend_type",
            "homepage_url",
            "inventory_url_template",
            "vehicle_url_template",
            "scraping_config",
            "dealer_code",
            "district_code",
            "phone",
            "city",
            "state",
            "postal_code",
        ]
    ]
    dealers_out = apply_dealer_overrides(dealers_out)

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
