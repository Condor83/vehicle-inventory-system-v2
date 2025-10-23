"""DealerInspire CMS inventory parser."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin

from ._inventory_common import ParserConfig, ParsedRow, parse_inventory_with_config

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


@dataclass(frozen=True)
class AlgoliaConfig:
    app_id: str
    api_key: str
    index: str
    refinements: Dict[str, List[str]] = field(default_factory=dict)


def extract_algolia_config(html: str) -> Optional[AlgoliaConfig]:
    """Extract Algolia search credentials and refinements from the SRP markup."""
    if not html:
        return None

    settings = _extract_inventory_lightning_settings(html)
    app_id: Optional[str] = None
    api_key: Optional[str] = None
    index: Optional[str] = None
    refinements: Dict[str, List[str]] = {}

    if settings:
        app_id = settings.get("appId")
        api_key = settings.get("apiKeySearch")
        index = settings.get("inventoryIndex")
        raw_refinements = settings.get("refinements") or {}
        if isinstance(raw_refinements, dict):
            refinements = {
                key: list(value) if isinstance(value, (list, tuple)) else [str(value)]
                for key, value in raw_refinements.items()
            }

    helper = _extract_algolia_helper(html)
    if helper:
        app_id = helper.get("data-app-id", app_id)
        api_key = helper.get("data-search-key", api_key)
        index = helper.get("data-index", index)

    if not (app_id and api_key and index):
        return None
    return AlgoliaConfig(app_id=app_id, api_key=api_key, index=index, refinements=refinements)


def build_algolia_params(
    config: AlgoliaConfig,
    *,
    model: str,
    hits_per_page: int = 60,
    make: str = "Toyota",
    inventory_type: str = "New",
) -> str:
    """Construct the Algolia params string for the given refinements."""
    def quote(value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        text = str(value).strip()
        if not text:
            return None
        if any(ch in text for ch in (" ", ":")):
            return f'"{text}"'
        return text

    filters: List[str] = []
    for key, values in config.refinements.items():
        if not values:
            continue
        for value in values:
            quoted = quote(value)
            if quoted:
                filters.append(f"{key}:{quoted}")

    if not any(f.startswith("model:") for f in filters):
        quoted_model = quote(model)
        if quoted_model:
            filters.append(f"model:{quoted_model}")
    if make and not any(f.startswith("make:") for f in filters):
        quoted_make = quote(make)
        if quoted_make:
            filters.append(f"make:{quoted_make}")
    if inventory_type and not any(f.startswith("type:") for f in filters):
        quoted_type = quote(inventory_type)
        if quoted_type:
            filters.append(f"type:{quoted_type}")

    filter_str = " AND ".join(filters)
    params = f"hitsPerPage={hits_per_page}"
    if filter_str:
        params = f"filters={filter_str}&{params}"
    return params


def parse_algolia_hits(data: Dict[str, Any], *, base_url: str) -> List[ParsedRow]:
    """Convert Algolia search results into inventory rows."""
    hits = data.get("hits") or []
    rows: List[ParsedRow] = []
    for hit in hits:
        vin = str(hit.get("vin") or "").upper()
        if not vin:
            continue
        rows.append(
            {
                "vin": vin,
                "advertised_price": _coerce_price(
                    hit.get("our_price") or hit.get("algoliaPrice") or hit.get("price")
                ),
                "msrp": _coerce_price(hit.get("msrp")),
                "vdp_url": _normalize_link(hit.get("link"), base_url),
                "stock_number": hit.get("stock"),
                "status": _normalize_status(hit.get("vehicle_status") or hit.get("status")),
                "image_url": _extract_image(hit, base_url),
                "make": hit.get("make"),
                "model": hit.get("model"),
                "year": hit.get("year"),
                "trim": hit.get("trim"),
                "features": hit.get("features"),
            }
        )
    return rows


def _extract_inventory_lightning_settings(html: str) -> Optional[Dict[str, Any]]:
    marker = "var inventoryLightningSettings"
    start = html.find(marker)
    if start == -1:
        return None
    brace_start = html.find("{", start)
    if brace_start == -1:
        return None
    depth = 0
    for idx in range(brace_start, len(html)):
        char = html[idx]
        if char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                snippet = html[brace_start : idx + 1]
                try:
                    return json.loads(snippet)
                except json.JSONDecodeError:
                    return None
    return None


ALGOLIA_HELPER_PATTERN = re.compile(r'<div[^>]+id=["\']sb-algolia-helper["\'][^>]*>', re.IGNORECASE)


def _extract_algolia_helper(html: str) -> Optional[Dict[str, str]]:
    match = ALGOLIA_HELPER_PATTERN.search(html)
    if not match:
        return None
    tag = match.group(0)
    attrs = {}
    for attribute in ("data-app-id", "data-search-key", "data-index"):
        attr_match = re.search(fr'{attribute}="([^"]+)"', tag, flags=re.IGNORECASE)
        if attr_match:
            attrs[attribute] = attr_match.group(1)
    return attrs or None


def _normalize_link(link: Optional[str], base_url: str) -> Optional[str]:
    if not link:
        return None
    return urljoin(base_url, link)


def _coerce_price(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if not text:
        return None
    text = text.replace("$", "").replace(",", "")
    try:
        return float(text)
    except ValueError:
        return None


def _extract_image(hit: Dict[str, Any], base_url: str) -> Optional[str]:
    thumbnail = hit.get("thumbnail")
    if thumbnail:
        return _normalize_link(thumbnail, base_url)
    images = hit.get("images") or []
    for image in images:
        uri = image.get("url") or image.get("src")
        if uri:
            return _normalize_link(uri, base_url)
    return None


def _normalize_status(status: Optional[str]) -> Optional[str]:
    if not status:
        return None
    normalized = status.strip().lower()
    if normalized in {"on-lot", "available", "live"}:
        return "available"
    if normalized in {"in transit", "in-transit", "transit"}:
        return "in_transit"
    if normalized in {"sold"}:
        return "sold"
    return normalized
