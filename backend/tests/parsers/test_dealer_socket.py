from pathlib import Path

from backend.app.parsers.dealer_socket import parse_inventory

FIXTURE = Path(__file__).parent / "fixtures" / "dealer_socket" / "sample.md"


def test_dealer_socket_parser_extracts_vins_and_prices():
    content = FIXTURE.read_text(encoding="utf-8")
    rows = parse_inventory(content)
    assert len(rows) == 8
    vins = {row["vin"] for row in rows}
    assert "JTEVB5BRXS5016328" in vins
    first = next(row for row in rows if row["vin"] == "JTEVB5BRXS5016328")
    assert first["advertised_price"] == 64464.0
    assert first["msrp"] == 64219.0
    assert first["stock_number"] == "T28067"
    assert first["status"] == "available"
    assert first["image_url"] is None
    second = next(row for row in rows if row["stock_number"] == "T28244")
    assert second["advertised_price"] == 46195.0
