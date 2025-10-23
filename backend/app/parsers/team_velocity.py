"""Team Velocity inventory parser (structured data extraction)."""

from __future__ import annotations

import json
import re
from html import unescape
from typing import Dict, List, Optional
from urllib.parse import urlparse

from ._inventory_common import ParsedRow

LD_JSON_RE = re.compile(r'<script[^>]+type="application/ld\+json"[^>]*>(.*?)</script>', re.DOTALL | re.IGNORECASE)


class TeamVelocityParseError(RuntimeError):
    """Raised when Team Velocity markup cannot be parsed."""


def _extract_dealer_host(raw_html: str) -> Optional[str]:
    match = re.search(r'<link[^>]+rel="canonical"[^>]+href="([^"]+)"', raw_html, re.IGNORECASE)
    if not match:
        return None
    url = unescape(match.group(1))
    parsed = urlparse(url)
    return parsed.netloc or None


def _iter_cars(raw_html: str) -> List[Dict[str, object]]:
    cars: List[Dict[str, object]] = []
    for script in LD_JSON_RE.findall(raw_html):
        try:
            payload = json.loads(script)
        except json.JSONDecodeError:
            continue
        nodes: List[Dict[str, object]] = []
        if isinstance(payload, dict):
            nodes = [payload]
        elif isinstance(payload, list):
            nodes = [node for node in payload if isinstance(node, dict)]
        for node in nodes:
            if node.get("@type") == "Car":
                cars.append(node)
    return cars


def _parse_price(value: Optional[str]) -> Optional[float]:
    if not isinstance(value, str):
        return None
    stripped = value.replace("$", "").replace(",", "").strip()
    if not stripped:
        return None
    try:
        numeric = float(stripped)
    except ValueError:
        return None
    if numeric <= 0:
        return None
    return numeric


def parse_inventory(markdown_or_html: str) -> List[ParsedRow]:
    if not markdown_or_html:
        return []

    dealer_host = _extract_dealer_host(markdown_or_html)
    if not dealer_host:
        raise TeamVelocityParseError("Unable to determine dealer host for Team Velocity markup.")

    cars = _iter_cars(markdown_or_html)
    rows: List[ParsedRow] = []
    for car in cars:
        vin = car.get("vehicleIdentificationNumber")
        if not isinstance(vin, str):
            continue
        vin = vin.upper()

        offers = car.get("offers")
        offer = offers if isinstance(offers, dict) else None
        price = _parse_price(offer.get("price")) if offer else None

        image_obj = car.get("image")
        image_url: Optional[str] = None
        if isinstance(image_obj, dict):
            image_url = image_obj.get("contentUrl")
        elif isinstance(image_obj, str):
            image_url = image_obj

        vdp_url = offer.get("url") if offer else None
        if isinstance(vdp_url, str) and vdp_url.startswith("/"):
            vdp_url = f"https://{dealer_host}{vdp_url}"

        row: ParsedRow = {
            "vin": vin,
            "advertised_price": price,
            "msrp": None,
            "vdp_url": vdp_url,
            "stock_number": car.get("sku") if isinstance(car.get("sku"), str) else None,
            "status": "available",
            "trim": car.get("vehicleModel") or car.get("model"),
            "model": car.get("model") if isinstance(car.get("model"), str) else None,
            "year": car.get("vehicleModelDate") if isinstance(car.get("vehicleModelDate"), (str, int)) else None,
            "features": None,
            "image_url": image_url,
        }
        rows.append(row)

    return rows


__all__ = ["parse_inventory", "TeamVelocityParseError"]
