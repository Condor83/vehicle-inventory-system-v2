from pathlib import Path

from backend.app.parsers.dealer_inspire import parse_inventory

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

    second = by_vin["JTEABFAJ9RK001234"]
    assert second["advertised_price"] == 87250.0
    assert second["msrp"] == 89500.0
    assert second["status"] == "sold"
    assert second["stock_number"] == "LC9876"

    third = by_vin["JTENU5JR3R5311111"]
    assert third["advertised_price"] is None  # no explicit sale price
    assert third["msrp"] == 62110.0
    assert third["status"] == "in_transit"
