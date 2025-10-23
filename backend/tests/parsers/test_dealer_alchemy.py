from __future__ import annotations

import json
from pathlib import Path

from backend.app.parsers.dealer_alchemy import extract_typesense_config, parse_typesense_hits

FIXTURE_DIR = Path(__file__).parent / "fixtures" / "dealer_alchemy"


def test_extract_typesense_config():
    html = (FIXTURE_DIR / "config_snippet.html").read_text(encoding="utf-8")
    config = extract_typesense_config(html)
    assert config is not None
    assert config.api_key == "TEST_API_KEY"
    assert config.host == "example.typesense.net"
    assert config.port == 443
    assert config.protocol == "https"
    assert config.index_name == "vehicles-TOY30036"
    assert config.query_by == "vin,stockNumber,model"
    assert config.condition == "New"
    assert config.hits_per_page == 24


def test_parse_typesense_hits():
    payload = json.loads((FIXTURE_DIR / "typesense_response.json").read_text(encoding="utf-8"))
    rows = parse_typesense_hits(payload, page_url="https://www.amigotoyota.com/new-vehicles/?model=Land%20Cruiser")
    assert len(rows) == 1
    row = rows[0]
    assert row["vin"] == "JTEABFAJXTK051728"
    assert row["advertised_price"] == 73194.0
    assert row["msrp"] == 73194.0
    assert row["status"] == "in_transit"
    assert (
        row["vdp_url"]
        == "https://www.amigotoyota.com/vehicle/New/2026/Toyota/Land-Cruiser/JTEABFAJXTK051728/"
    )
    assert row["make"] == "Toyota"
    assert row["model"] == "Land Cruiser"
    assert row["year"] == "2026"
    assert row["trim"] == "First Edition"
    assert row["image_url"] == "https://example.com/img.jpg"
    assert "Adaptive Cruise Control" in (row["features"] or [])
