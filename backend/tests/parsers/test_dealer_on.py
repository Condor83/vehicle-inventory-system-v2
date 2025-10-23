from pathlib import Path
from typing import Dict, Any

import pytest

from backend.app.parsers import dealer_on

FIXTURE_DIR = Path(__file__).parent / "fixtures" / "dealer_on"


def _load_fixture(name: str) -> str:
    return (FIXTURE_DIR / name).read_text(encoding="utf-8")


def _load_json_fixture(name: str) -> Dict[str, Any]:
    import json

    return json.loads((FIXTURE_DIR / name).read_text(encoding="utf-8"))


def test_parse_inventory_fetches_api(monkeypatch: pytest.MonkeyPatch) -> None:
    html = _load_fixture("sample_srp.html")
    api_payload = _load_json_fixture("api_response.json")

    called: Dict[str, Any] = {}

    def fake_fetch(url: str, params: Dict[str, str]) -> Dict[str, Any]:  # type: ignore[override]
        called["url"] = url
        called["params"] = params
        return api_payload

    monkeypatch.setattr(dealer_on, "_fetch_inventory_json", fake_fetch)

    rows = dealer_on.parse_inventory(html)

    assert called["url"].endswith("/vehicle-pages/cosmos/srp/vehicles/11409/559658")
    assert called["params"]["host"] == "www.petersontoyota.com"
    assert called["params"]["Model"] == "4Runner"

    assert len(rows) == 1
    row = rows[0]
    assert row["vin"] == "JTEVA5BR0S5057991"
    assert row["advertised_price"] == 64140.0
    assert row["msrp"] == 64140.0
    assert row["vdp_url"].startswith("https://www.petersontoyota.com/new-Lumberton-2025-Toyota-4Runner")
    assert row["image_url"] == "https://www.petersontoyota.com/inventoryphotos/1409/jteva5br0s5057991/ip/1.jpg"
    assert row["status"] == "available"
