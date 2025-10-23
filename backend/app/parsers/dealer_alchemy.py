"""Dealer Alchemy / Dealer Venom inventory parser."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Dict, List, Optional
from urllib.parse import urljoin, urlparse

from ._inventory_common import ParserConfig, ParsedRow, parse_inventory_with_config

STATUS_MAP = {
    "IN TRANSIT": "in_transit",
    "TRANSIT": "in_transit",
    "IN STOCK": "available",
    "AVAILABLE": "available",
    "BUILD PHASE": "build_phase",
    "PENDING SALE": "pending",
    "SOLD": "sold",
}

PRICE_KEYWORDS_PRIORITY = [
    ("advertised price", 1),
    ("sale price", 1),
    ("internet price", 1),
    ("final price", 1),
    ("tsrp", 2),
    ("msrp", 2),
    ("price", 3),
]

CONFIG = ParserConfig(
    status_map=STATUS_MAP,
    price_keywords_priority=PRICE_KEYWORDS_PRIORITY,
)


def parse_inventory(markdown_or_html: str) -> List[ParsedRow]:
    """Parse Dealer Alchemy HTML via generic heuristics."""
    return parse_inventory_with_config(markdown_or_html, CONFIG)


@dataclass(frozen=True)
class TypesenseConfig:
    api_key: str
    host: str
    port: int
    protocol: str
    index_name: str
    query_by: str
    condition: Optional[str] = None
    hits_per_page: int = 250


API_KEY_PATTERN = re.compile(r"apiKey\s*:\s*\"([^\"]+)\"", re.IGNORECASE)
NODE_PATTERN = re.compile(
    r"nodes\s*:\s*\[\s*{[^}]*host\s*:\s*['\"]([^'\"]+)['\"],\s*port\s*:\s*(\d+),\s*protocol\s*:\s*['\"]([^'\"]+)['\"][^}]*}",  # noqa: E501
    re.IGNORECASE,
)
QUERY_BY_PATTERN = re.compile(r"query_by\s*:\s*\"([^\"]+)\"", re.IGNORECASE)
INDEX_PATTERN = re.compile(r"var\s+indexName\s*=\s*\"([^\"]+)\"", re.IGNORECASE)
CONDITION_PATTERN = re.compile(r"var\s+srpCondition\s*=\s*'([^']+)'", re.IGNORECASE)
HITS_PER_PAGE_PATTERN = re.compile(r"hitsPerPage\s*=\s*(\d+)", re.IGNORECASE)


def extract_typesense_config(html: str) -> Optional[TypesenseConfig]:
    """Parse Typesense credentials embedded in the SRP."""
    if not html:
        return None

    api_match = API_KEY_PATTERN.search(html)
    node_match = NODE_PATTERN.search(html)
    query_match = QUERY_BY_PATTERN.search(html)
    index_match = INDEX_PATTERN.search(html)

    if not (api_match and node_match and query_match and index_match):
        return None

    api_key = api_match.group(1).strip()
    host = node_match.group(1).strip()
    port = int(node_match.group(2))
    protocol = node_match.group(3).strip()
    query_by = query_match.group(1).strip()
    index_name = index_match.group(1).strip()

    condition_match = CONDITION_PATTERN.search(html)
    condition = condition_match.group(1).strip() if condition_match else None

    hits_per_page_match = HITS_PER_PAGE_PATTERN.search(html)
    hits_per_page = int(hits_per_page_match.group(1)) if hits_per_page_match else 250

    return TypesenseConfig(
        api_key=api_key,
        host=host,
        port=port,
        protocol=protocol,
        index_name=index_name,
        query_by=query_by,
        condition=condition,
        hits_per_page=hits_per_page,
    )


_PRICE_NUMBER = re.compile(r"(\d[\d,]*\.?\d*)")


def _coerce_price(value: Optional[object]) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    try:
        text = str(value)
    except Exception:  # pragma: no cover
        return None
    match = _PRICE_NUMBER.search(text)
    if not match:
        return None
    normalized = match.group(1).replace(",", "")
    try:
        return float(normalized)
    except ValueError:
        return None


def _build_filter_string(*parts: str) -> Optional[str]:
    tokens = [part for part in parts if part]
    if not tokens:
        return None
    return " && ".join(tokens)


def _quote_filter_value(value: str) -> str:
    escaped = value.replace("\\", "\\\\").replace("'", "\\'")
    return f"'{escaped}'"


def _normalize_vdp_url(raw: Optional[str], page_url: str, dealer_url: Optional[str]) -> Optional[str]:
    if not raw:
        return None
    if raw.startswith("http://") or raw.startswith("https://"):
        return raw
    parsed = urlparse(page_url)
    base = ""
    if parsed.scheme and parsed.netloc:
        base = f"{parsed.scheme}://{parsed.netloc}"
    elif dealer_url:
        dealer = dealer_url.strip()
        if not dealer.startswith("http"):
            dealer = f"https://{dealer.lstrip('/')}"
        base = dealer.rstrip("/")
    if not base:
        return raw
    return urljoin(f"{base}/", raw.lstrip("/"))


def _derive_status(document: Dict[str, object]) -> Optional[str]:
    flags = document.get("flags") or {}
    if isinstance(flags, dict):
        if flags.get("hasSoldVehicles"):
            return "sold"
        if flags.get("inTransit"):
            return "in_transit"
    status = document.get("status") or document.get("condition")
    if isinstance(status, str):
        upper = status.upper()
        if "TRANSIT" in upper:
            return "in_transit"
        if "SOLD" in upper:
            return "sold"
    return "available"


def parse_typesense_hits(
    data: Dict[str, object],
    *,
    page_url: str,
) -> List[ParsedRow]:
    """Convert Typesense search payload into ParsedRow entries."""
    results = data.get("results") or []
    rows: List[ParsedRow] = []
    for result in results:
        if not isinstance(result, dict):
            continue
        hits = result.get("hits") or []
        for hit in hits:
            if not isinstance(hit, dict):
                continue
            document = hit.get("document") or {}
            if not isinstance(document, dict):
                continue
            vin = str(document.get("vin") or "").upper()
            if not vin:
                continue
            dealer = document.get("dealer") if isinstance(document.get("dealer"), dict) else {}
            vdp_url = _normalize_vdp_url(document.get("vdpUrl"), page_url, dealer.get("url") if isinstance(dealer, dict) else None)
            advertised_price = (
                _coerce_price(document.get("finalPrice"))
                or _coerce_price(document.get("advertisedPrice"))
                or _coerce_price(document.get("sellingPrice"))
            )
            msrp = _coerce_price(document.get("msrp"))
            image_urls = document.get("imageUrls") if isinstance(document.get("imageUrls"), list) else []
            features = document.get("features") if isinstance(document.get("features"), list) else None
            rows.append(
                {
                    "vin": vin,
                    "advertised_price": advertised_price,
                    "msrp": msrp,
                    "vdp_url": vdp_url,
                    "stock_number": document.get("stockNumber"),
                    "status": _derive_status(document),
                    "image_url": image_urls[0] if image_urls else None,
                    "make": document.get("make"),
                    "model": document.get("model"),
                    "year": document.get("year"),
                    "trim": document.get("trim"),
                    "exterior_color": document.get("exteriorColor"),
                    "interior_color": document.get("interiorColor"),
                    "features": features,
                }
            )
    return rows

