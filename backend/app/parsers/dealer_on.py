"""DealerOn inventory parser using Cosmos SRP API."""

from __future__ import annotations

import json
import re
from html import unescape
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import parse_qsl, urlparse

import httpx

from ._inventory_common import ParsedRow

TAGGING_DATA_SCRIPT_RE = re.compile(
    r'<script[^>]+id="dealeron_tagging_data"[^>]*>(.*?)</script>', re.DOTALL | re.IGNORECASE
)
OG_URL_META_RE = re.compile(r'<meta[^>]+property="og:url"[^>]+content="([^"]+)"', re.IGNORECASE)
CANONICAL_LINK_RE = re.compile(r'<link[^>]+rel="canonical"[^>]+href="([^"]+)"', re.IGNORECASE)

API_TIMEOUT_SECONDS = 10.0


class DealerOnParseError(RuntimeError):
    """Raised when DealerOn markup cannot be parsed into configuration."""


def _extract_tagging_data(raw_html: str) -> Dict[str, Any]:
    match = TAGGING_DATA_SCRIPT_RE.search(raw_html)
    if not match:
        return {}
    try:
        return json.loads(match.group(1))
    except json.JSONDecodeError:
        return {}


def _extract_host_and_query(raw_html: str) -> Tuple[Optional[str], str]:
    og_match = OG_URL_META_RE.search(raw_html)
    canonical_match = CANONICAL_LINK_RE.search(raw_html)

    candidate_url = None
    if og_match:
        candidate_url = og_match.group(1)
    elif canonical_match:
        candidate_url = canonical_match.group(1)

    if not candidate_url:
        return None, ""

    decoded = unescape(candidate_url)
    if "%3F" in decoded and "?" not in decoded:
        decoded = decoded.replace("%3F", "?", 1)
    parsed = urlparse(decoded)

    host = parsed.netloc or None
    query = parsed.query

    return host, query


def _fetch_inventory_json(url: str, params: Dict[str, str]) -> Dict[str, Any]:
    with httpx.Client(timeout=API_TIMEOUT_SECONDS) as client:
        response = client.get(url, params=params)
        response.raise_for_status()
        return response.json()


def _normalize_price(value: Optional[Any]) -> Optional[float]:
    if value in (None, ""):
        return None
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if numeric <= 0:
        return None
    return numeric


def parse_inventory(markdown_or_html: str) -> List[ParsedRow]:
    """Parse DealerOn SRP markup and fetch structured data via the Cosmos SRP API.

    Args:
        markdown_or_html: Raw HTML/markdown captured by Firecrawl for a DealerOn SRP page.

    Returns:
        List of normalized inventory rows ready for ingestion.

    Raises:
        DealerOnParseError: If the configuration payload cannot be located in the markup.
    """

    if not markdown_or_html:
        return []

    tagging_data = _extract_tagging_data(markdown_or_html)
    if not tagging_data:
        raise DealerOnParseError("Unable to locate dealeron_tagging_data script in markup.")

    dealer_id = tagging_data.get("dealerId", tagging_data.get("DealerId"))
    page_id = tagging_data.get("pageId", tagging_data.get("PageId"))
    if dealer_id is None or page_id is None:
        raise DealerOnParseError("dealeron_tagging_data missing dealerId or pageId.")

    try:
        dealer_id = int(str(dealer_id))
        page_id = int(str(page_id))
    except ValueError as exc:
        raise DealerOnParseError("dealerId or pageId is not numeric.") from exc

    host, query_string = _extract_host_and_query(markdown_or_html)
    if not host:
        raise DealerOnParseError("Unable to determine host for DealerOn page from markup.")

    status_code = tagging_data.get("statusCode")
    if status_code == 404:
        # DealerOn returns 404 with empty items when the filtered SRP has no inventory.
        return []

    vin_items = tagging_data.get("items")
    if isinstance(vin_items, list):
        page_size = max(len(vin_items), 12)
    else:
        page_size = 12

    params: Dict[str, str] = {
        "host": host,
        "PageNumber": "1",
        "PageSize": str(page_size),
        "displayCardsShown": str(page_size),
    }
    if query_string:
        for key, value in parse_qsl(query_string, keep_blank_values=True):
            params[key] = value

    api_url = f"https://{host}/api/vhcliaa/vehicle-pages/cosmos/srp/vehicles/{dealer_id}/{page_id}"
    try:
        payload = _fetch_inventory_json(api_url, params)
    except httpx.HTTPError as exc:
        raise DealerOnParseError(f"DealerOn API request failed: {exc}") from exc

    rows: List[ParsedRow] = []
    display_cards = payload.get("DisplayCards")
    if not isinstance(display_cards, list):
        return rows

    for card in display_cards:
        vehicle_card = card.get("VehicleCard") if isinstance(card, dict) else None
        if not isinstance(vehicle_card, dict):
            continue

        vin = vehicle_card.get("VehicleVin") or vehicle_card.get("VehicleImageModel", {}).get("Vin")
        if not isinstance(vin, str):
            continue
        vin = vin.strip().upper()

        image_model = vehicle_card.get("VehicleImageModel", {})
        image_src = image_model.get("VehiclePhotoSrc")
        image_url: Optional[str] = None
        if isinstance(image_src, str) and image_src:
            image_url = image_src if image_src.startswith("http") else f"https://{host}{image_src}"

        vdp_url = vehicle_card.get("VehicleDetailUrl") or image_model.get("VehicleDetailUrl")
        if isinstance(vdp_url, str) and not vdp_url.startswith("http"):
            vdp_url = f"https://{host}{vdp_url}"

        advertised_price = _normalize_price(vehicle_card.get("VehicleInternetPrice"))
        if advertised_price is None:
            advertised_price = _normalize_price(vehicle_card.get("TaggingPrice"))

        msrp = _normalize_price(vehicle_card.get("VehicleMsrp"))

        status = "available"
        if vehicle_card.get("VehicleInTransit") or vehicle_card.get("VehicleInProduction"):
            status = "in_transit"

        row: ParsedRow = {
            "vin": vin,
            "advertised_price": advertised_price,
            "msrp": msrp,
            "vdp_url": vdp_url,
            "stock_number": vehicle_card.get("VehicleStockNumber"),
            "status": status,
            "trim": vehicle_card.get("VehicleTrim"),
            "model": vehicle_card.get("VehicleModel"),
            "year": vehicle_card.get("VehicleYear"),
            "features": None,
            "image_url": image_url,
        }
        rows.append(row)

    return rows


__all__ = ["parse_inventory", "DealerOnParseError"]
