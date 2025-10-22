from pathlib import Path

from backend.app.parsers.cdk import parse_inventory

FIXTURE_DIR = Path(__file__).parent / "fixtures" / "cdk"


def test_cdk_parser_extracts_status_and_price():
    content = (FIXTURE_DIR / "sample_inventory.md").read_text(encoding="utf-8")
    rows = parse_inventory(content)
    data = {row["vin"]: row for row in rows}

    first = data["JTENU5JR3R5312345"]
    assert first["advertised_price"] == 44995.0
    assert first["msrp"] == 46500.0
    assert first["status"] == "in_transit"

    second = data["JTENU5JR4R5323456"]
    assert second["advertised_price"] == 52110.0
    assert second["status"] == "in_transit"
