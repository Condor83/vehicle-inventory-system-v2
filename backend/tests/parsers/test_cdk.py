from pathlib import Path

import json
from urllib.parse import urljoin

from backend.app.parsers.cdk import (
    parse_inventory,
    extract_inventory_request,
    parse_inventory_json,
)

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


def test_cdk_extracts_inventory_request_from_html():
    html = (FIXTURE_DIR / "young_toyota_srp.html").read_text(encoding="utf-8")
    request = extract_inventory_request(html)
    assert request is not None
    assert request.endpoint == "/api/widget/ws-inv-data/getInventory"
    assert request.payload["inventoryParameters"]["model"] == "4Runner"


def test_cdk_parses_inventory_json_response():
    response = json.loads((FIXTURE_DIR / "young_toyota_response.json").read_text(encoding="utf-8"))
    base_url = "https://www.youngtoyota.com"
    rows = parse_inventory_json(response, base_url=base_url)
    assert rows, "Expected inventory rows from JSON payload"
    vin_map = {row["vin"]: row for row in rows}
    sample = vin_map["JTEVA5BR8S5057981"]
    assert sample["advertised_price"] == 63081.0
    assert sample["msrp"] == 63863.0
    assert sample["vdp_url"] == urljoin(base_url, "/new/Toyota/2025-Toyota-4Runner-Logan-UT-1d1095e3ac183d5c6d318c5bb8295c6e.htm")
    assert sample["status"] in {"available", "in_transit"}
