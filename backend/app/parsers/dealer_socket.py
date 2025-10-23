"""DealerSocket inventory parser."""

from __future__ import annotations

import re
from typing import Dict, List, Optional

from ._inventory_common import ParsedRow

HEADER_PATTERN = re.compile(r"## \[.*?\]\((?P<vdp>[^)]+)\).*?\n", re.DOTALL)
SECTION_PATTERN = re.compile(
    r"## \[.*?\]\((?P<vdp>[^)]+)\).*?\n(?P<body>.*?)(?=\n## \[|\Z)",
    re.DOTALL,
)
VIN_PATTERN = re.compile(r"\|\s*VIN\s*\|\s*([A-HJ-NPR-Z0-9]{17})\s*\|")
TABLE_FIELD_PATTERN = re.compile(r"\|\s*([^|]+?)\s*\|\s*([^|]+?)\s*\|")
PRICE_PATTERN = re.compile(r"Your Price\s*\n\$(\d[\d,]*)")
MSRP_PATTERN = re.compile(r"(?:MSRP|TSRP)\s*\n\$(\d[\d,]*)")


def _parse_table(body: str) -> Dict[str, str]:
    table: Dict[str, str] = {}
    for label, value in TABLE_FIELD_PATTERN.findall(body):
        table[label.strip().lower()] = value.strip()
    return table


def _parse_price(body: str, pattern: re.Pattern[str]) -> Optional[float]:
    match = pattern.search(body)
    if not match:
        return None
    value = match.group(1).replace(",", "")
    try:
        return float(value)
    except ValueError:
        return None


def parse_inventory(markdown_or_html: str) -> List[ParsedRow]:
    if not markdown_or_html:
        return []

    rows: List[ParsedRow] = []
    for match in SECTION_PATTERN.finditer(markdown_or_html):
        body = match.group("body")
        vin_match = VIN_PATTERN.search(body)
        if not vin_match:
            continue
        vin = vin_match.group(1).upper()

        table = _parse_table(body)
        advertised_price = _parse_price(body, PRICE_PATTERN)
        msrp = _parse_price(body, MSRP_PATTERN)

        row: ParsedRow = {
            "vin": vin,
            "advertised_price": advertised_price,
            "msrp": msrp,
            "vdp_url": match.group("vdp"),
            "stock_number": table.get("stock #"),
            "trim": table.get("trim"),
            "status": "available",
            "features": None,
            "model": table.get("model"),
            "image_url": None,
        }
        rows.append(row)

    return rows
