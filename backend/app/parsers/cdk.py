"""CDK Global inventory parser."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin, unquote

from ._inventory_common import ParserConfig, ParsedRow, parse_inventory_with_config

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

CDK_FETCH_PATTERN = re.compile(
    r'fetch\("(?P<endpoint>/api/widget/ws-inv-data/getInventory)"\s*,\s*\{.*?body:decodeURI\("(?P<payload>[^"]+)"\)'
    r'.*?\}\)',
    re.IGNORECASE | re.DOTALL,
)

PRICE_PATTERN = re.compile(r"\$?\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]{2})?)")


@dataclass(frozen=True)
class CDKInventoryRequest:
    endpoint: str
    payload: Dict[str, Any]


def parse_inventory(markdown_or_html: str) -> List[ParsedRow]:
    """Parse CDK inventory markup."""
    return parse_inventory_with_config(markdown_or_html, CONFIG)


def extract_inventory_request(html: str) -> Optional[CDKInventoryRequest]:
    """Detect the embedded CDK inventory fetch metadata inside the SRP HTML."""
    if not html:
        return None
    match = CDK_FETCH_PATTERN.search(html)
    if not match:
        return None
    payload_raw = unquote(match.group("payload"))
    try:
        payload = json.loads(payload_raw)
    except json.JSONDecodeError:
        return None
    return CDKInventoryRequest(endpoint=match.group("endpoint"), payload=payload)


def parse_inventory_json(data: Dict[str, Any], *, base_url: str) -> List[ParsedRow]:
    """Convert the CDK inventory JSON payload into ParsedRow objects."""
    inventory = data.get("inventory") or []
    rows: List[ParsedRow] = []
    for entry in inventory:
        vin = str(entry.get("vin") or "").upper()
        if not vin:
            continue

        rows.append(
            {
                "vin": vin,
                "advertised_price": _extract_price(entry, field="final"),
                "msrp": _extract_price(entry, field="msrp"),
                "vdp_url": _resolve_vdp_url(entry, base_url),
                "stock_number": entry.get("stockNumber") or entry.get("stock"),
                "status": _normalize_status(entry.get("status")),
                "image_url": _extract_image(entry),
                "make": entry.get("make"),
                "model": entry.get("model"),
                "year": entry.get("year"),
                "trim": entry.get("trim"),
                "features": entry.get("features"),
            }
        )
    return rows


def _extract_price(entry: Dict[str, Any], *, field: str) -> Optional[float]:
    pricing = entry.get("pricing") or {}
    dprice = pricing.get("dprice") or []

    if field == "final":
        for item in dprice:
            if item.get("isFinalPrice") or item.get("typeClass") in {"askingPrice", "internetPrice", "finalPrice"}:
                price = _coerce_price(item.get("value"))
                if price is not None:
                    return price
        for key in ("salePrice", "sale_price", "askingPrice", "internetPrice", "asking_price"):
            price = _coerce_price(entry.get(key))
            if price is not None:
                return price
    elif field == "msrp":
        for item in dprice:
            if item.get("typeClass") in {"msrp", "retailPrice"}:
                price = _coerce_price(item.get("value"))
                if price is not None:
                    return price
        price = _coerce_price(pricing.get("retailPrice"))
        if price is not None:
            return price

    price = _coerce_price(pricing.get("retailPrice"))
    if price is not None:
        return price
    return _coerce_price(entry.get("price"))


def _coerce_price(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    match = PRICE_PATTERN.search(str(value))
    if not match:
        return None
    try:
        return float(match.group(1).replace(",", ""))
    except ValueError:
        return None


def _extract_image(entry: Dict[str, Any]) -> Optional[str]:
    images = entry.get("images") or []
    for image in images:
        uri = image.get("uri") or image.get("url")
        if not uri:
            continue
        if uri.startswith("//"):
            return f"https:{uri}"
        return uri
    primary = entry.get("primary_image") or {}
    uri = primary.get("uri") or primary.get("url")
    if uri:
        if uri.startswith("//"):
            return f"https:{uri}"
        return uri
    return None


def _resolve_vdp_url(entry: Dict[str, Any], base_url: str) -> Optional[str]:
    link = entry.get("link") or entry.get("vdp") or entry.get("url")
    if not link:
        return None
    return urljoin(base_url, link)


def _normalize_status(status: Optional[str]) -> Optional[str]:
    if not status:
        return None
    normalized = status.strip().upper().replace("-", " ").replace("_", " ")
    for key, value in STATUS_MAP.items():
        if key == normalized:
            return value
    if normalized in {"LIVE", "AVAILABLE"}:
        return "available"
    if normalized in {"IN TRANSIT", "ARRIVING", "TRANSFER"}:
        return "in_transit"
    return status.lower()
