"""DealerInspire CMS inventory parser."""

from __future__ import annotations

from typing import List, Dict, Optional, Union

from ._inventory_common import ParserConfig, parse_inventory_with_config, ParsedRow

STATUS_MAP = {
    "IN TRANSIT": "in_transit",
    "IN-TRANSIT": "in_transit",
    "COMING SOON": "in_transit",
    "SOLD": "sold",
    "AVAILABLE": "available",
    "IN STOCK": "available",
}

PRICE_KEYWORDS_PRIORITY = [
    ("sale price", 1),
    ("our price", 1),
    ("internet price", 2),
    ("special price", 2),
    ("market price", 3),
    ("dealer price", 3),
    ("price", 4),
]

CONFIG = ParserConfig(
    status_map=STATUS_MAP,
    price_keywords_priority=PRICE_KEYWORDS_PRIORITY,
)


def parse_inventory(markdown_or_html: str) -> List[ParsedRow]:
    """Parse DealerInspire inventory markup captured as markdown or HTML."""
    return parse_inventory_with_config(markdown_or_html, CONFIG)
