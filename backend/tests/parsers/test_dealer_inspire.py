import json
from pathlib import Path

from backend.app.parsers.dealer_inspire import (
    parse_inventory,
    extract_algolia_config,
    build_algolia_params,
    parse_algolia_hits,
)

FIXTURE_DIR = Path(__file__).parent / "fixtures" / "dealer_inspire"


def _row_map(rows):
    return {row["vin"]: row for row in rows}


def test_parse_inventory_extracts_core_fields():
    content = (FIXTURE_DIR / "sample_inventory.md").read_text(encoding="utf-8")
    rows = parse_inventory(content)
    by_vin = _row_map(rows)

    assert "JTENU5JR4R5299999" in by_vin
    first = by_vin["JTENU5JR4R5299999"]
    assert first["advertised_price"] == 47500.0
    assert first["msrp"] == 51230.0
    assert first["stock_number"] == "T12345"
    assert first["status"] == "in_transit"
    assert first["vdp_url"].startswith("https://www.tetontoyota.com/inventory/")
    assert first["image_url"] == "https://media.example.com/inventory/4runner-sr5.jpg"

    second = by_vin["JTEABFAJ9RK001234"]
    assert second["advertised_price"] == 87250.0
    assert second["msrp"] == 89500.0
    assert second["status"] == "sold"
    assert second["stock_number"] == "LC9876"
    assert second["image_url"] == "https://media.example.com/inventory/land-cruiser.jpg"

    third = by_vin["JTENU5JR3R5311111"]
    assert third["advertised_price"] is None  # no explicit sale price
    assert third["msrp"] == 62110.0
    assert third["status"] == "in_transit"
    assert third["image_url"] is None


def test_extract_algolia_config_from_settings():
    html = (FIXTURE_DIR / "westboro_srp.html").read_text(encoding="utf-8")
    config = extract_algolia_config(html)
    assert config is not None
    assert config.app_id == "SEWJN80HTN"
    assert config.api_key == "179608f32563367799314290254e3e44"
    assert config.index == "westborotoyota-sbm0624_production_inventory"
    assert config.refinements.get("model") == ["4Runner"]


def test_extract_algolia_config_from_helper_div():
    html = (FIXTURE_DIR / "jaywolfe_srp.html").read_text(encoding="utf-8")
    config = extract_algolia_config(html)
    assert config is not None
    assert config.app_id == "EHWUW84XVK"
    assert config.api_key == "fb58227032e79f03b9b820cbaea7f8fb"
    assert config.index == "jaywolfetoyotakansascity-sbm0624_production_inventory"


def test_build_algolia_params_adds_default_filters():
    html = (FIXTURE_DIR / "jaywolfe_srp.html").read_text(encoding="utf-8")
    config = extract_algolia_config(html)
    params = build_algolia_params(config, model="4Runner", hits_per_page=25)
    assert "model:4Runner" in params
    assert "make:Toyota" in params
    assert "type:New" in params
    assert params.endswith("hitsPerPage=25")


def test_parse_algolia_hits_produces_inventory_rows():
    response = json.loads((FIXTURE_DIR / "jaywolfe_algolia_response.json").read_text(encoding="utf-8"))
    rows = parse_algolia_hits(response, base_url="https://www.jaywolfetoyota.com")
    assert rows
    sample = next(row for row in rows if row["vin"] == "JTEVA5AR2S5006557")
    assert sample["advertised_price"] == 41988.0
    assert sample["msrp"] == 43733.0
    assert sample["status"] == "available"
    assert sample["vdp_url"].startswith("https://www.jaywolfetoyota.com/inventory/")
