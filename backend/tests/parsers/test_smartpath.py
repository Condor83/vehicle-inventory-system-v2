from pathlib import Path
from typing import Dict, Any

import pytest

from backend.app.parsers import smartpath

FIXTURE_DIR = Path(__file__).parent / "fixtures" / "smartpath"


def _read_html(name: str) -> str:
    return (FIXTURE_DIR / name).read_text(encoding="utf-8")


def _read_json(name: str) -> Dict[str, Any]:
    import json

    return json.loads((FIXTURE_DIR / name).read_text(encoding="utf-8"))


def test_smartpath_parser(monkeypatch: pytest.MonkeyPatch) -> None:
    html = _read_html("sample.html")
    payload = _read_json("api_response.json")

    def fake_fetch(base_url: str, api_key: str, index_name: str, model_filter: str | None):
        assert base_url == "https://abc123.typesense.net"
        assert api_key == "TEST_TYPESENSE_KEY"
        assert index_name == "vehicles-TOY12345"
        assert model_filter == "4Runner"
        return [payload["hits"][0]["document"]]

    monkeypatch.setattr(smartpath, "_fetch_typesense_documents", fake_fetch)

    rows = smartpath.parse_inventory(html)
    assert len(rows) == 1
    row = rows[0]
    assert row["vin"] == "JTEVA5BR0S5057991"
    assert row["advertised_price"] == 42128.0
    assert row["msrp"] == 45143.0
    assert row["vdp_url"] == "https://www.exampletoyota.com/vehicle/New/2025/Toyota/4Runner/JTEVA5BR0S5057991/"
    assert row["image_url"] == "https://images.example.com/4runner.jpg"
    assert row["status"] == "available"
