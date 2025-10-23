from pathlib import Path

from backend.app.parsers import team_velocity

FIXTURE_DIR = Path(__file__).parent / "fixtures" / "team_velocity"


def test_team_velocity_parser_extracts_car_data() -> None:
    html = (FIXTURE_DIR / "sample.html").read_text(encoding="utf-8")
    rows = team_velocity.parse_inventory(html)
    assert len(rows) == 1
    row = rows[0]
    assert row["vin"] == "JTEVA5BR0S5057991"
    assert row["advertised_price"] == 46848.0
    assert row["msrp"] is None
    assert row["vdp_url"] == "https://www.exampledealer.com/viewdetails/new/jteva5br0s5057991"
    assert row["image_url"] == "https://cdn.example.com/4runner.jpg"
