from pathlib import Path

from backend.app.parsers.dealer_com import parse_inventory

FIXTURE_DIR = Path(__file__).parent / "fixtures" / "dealer_com"


def test_dealer_com_parser_extracts_prices():
    content = (FIXTURE_DIR / "sample_inventory.md").read_text(encoding="utf-8")
    rows = parse_inventory(content)
    data = {row["vin"]: row for row in rows}

    first = data["5TFJC5DB7RX001111"]
    assert first["advertised_price"] == 58995.0
    assert first["msrp"] == 61500.0
    assert first["status"] == "available"

    second = data["JTEABFAJ1RK002222"]
    assert second["advertised_price"] == 89750.0
    assert second["status"] == "in_transit"
