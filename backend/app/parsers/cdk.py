"""CDK Global inventory parser."""

from __future__ import annotations

from typing import List

from ._inventory_common import ParserConfig, parse_inventory_with_config, ParsedRow

STATUS_MAP = {
    "IN TRANSIT": "in_transit",
    "IN-TRANSIT": "in_transit",
    "IN ROUTE": "in_transit",
    "ARRIVING SOON": "in_transit",
    "SOLD": "sold",
    "AVAILABLE": "available",
    "IN STOCK": "available",
    "ON ORDER": "in_transit",
}

PRICE_KEYWORDS_PRIORITY = [
    ("web price", 1),
    ("sale price", 1),
    ("dealer price", 2),
    ("your price", 2),
    ("price", 4),
]

CONFIG = ParserConfig(
    status_map=STATUS_MAP,
    price_keywords_priority=PRICE_KEYWORDS_PRIORITY,
)


def parse_inventory(markdown_or_html: str) -> List[ParsedRow]:
    """Parse CDK inventory markup."""
    return parse_inventory_with_config(markdown_or_html, CONFIG)
