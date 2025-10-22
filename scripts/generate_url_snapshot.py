#!/usr/bin/env python3
from pathlib import Path
import json
import pandas as pd
from backend.app.parsers.url_builder import build_inventory_url

def main():
    dealers_csv = Path("data/seeds/dealers.csv")
    out = Path("data/snapshots/inventory_urls.snap")
    df = pd.read_csv(dealers_csv)
    lines = []
    for _, r in df.iterrows():
        d = {
            "id": r["dealer_id"],
            "homepage_url": r.get("homepage_url",""),
            "inventory_url_template": r.get("inventory_url_template",""),
            "scraping_config": json.loads(r.get("scraping_config") or "{}")
        }
        for m in ["Land Cruiser","4Runner","Tacoma","Tundra"]:
            lines.append(f"{d['id']}	{m}	{build_inventory_url(d, m)}")
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text("\n".join(lines), encoding="utf-8")
    print(f"Wrote snapshot to {out}")

if __name__ == "__main__":
    main()
