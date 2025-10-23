"""SmartPath (Typesense-backed) inventory parser."""

from __future__ import annotations

import json
import re
from html import unescape
from typing import Dict, List, Optional, Tuple
from urllib.parse import parse_qs, urlparse, unquote

import httpx

from ._inventory_common import ParsedRow

TYPENSE_TIMEOUT_SECONDS = 10.0


class SmartPathParseError(RuntimeError):
    """Raised when SmartPath markup is missing required configuration."""


_API_KEY_RE = re.compile(r"apiKey:\s*['\"]([^'\"]+)['\"]")
_HOST_RE = re.compile(r"host:\s*['\"]([^'\"]+)['\"]")
_INDEX_RE = re.compile(r"var\s+indexName\s*=\s*['\"]([^'\"]+)['\"]")
_DEALER_CD_RE = re.compile(r"dealerCd['\"]?\s*[:=]\s*['\"](\d+)['\"]")
_CANONICAL_RE = re.compile(r"<link[^>]+rel=\"canonical\"[^>]+href=\"([^\"]+)\"", re.IGNORECASE)
_OG_URL_RE = re.compile(r"<meta[^>]+property=\"og:url\"[^>]+content=\"([^\"]+)\"", re.IGNORECASE)


def _parse_typesense_config(raw_html: str) -> Tuple[str, str, str]:
    api_key_match = _API_KEY_RE.search(raw_html)
    host_match = _HOST_RE.search(raw_html)
    index_match = _INDEX_RE.search(raw_html)

    if not index_match:
        fallback_index = re.search(r"vehicles-[A-Za-z0-9]+", raw_html)
        if fallback_index:
            index_name = fallback_index.group(0)
        else:
            index_name = None
    else:
        index_name = index_match.group(1)

    if not (api_key_match and host_match and index_name):
        raise SmartPathParseError("Unable to locate Typesense configuration in SmartPath markup.")

    api_key = api_key_match.group(1)
    host = host_match.group(1)
    return api_key, host, index_name


def _extract_dealer_host(raw_html: str) -> Optional[str]:
    for pattern in (_CANONICAL_RE, _OG_URL_RE):
        match = pattern.search(raw_html)
        if match:
            url = unescape(match.group(1))
            parsed = urlparse(url)
            if parsed.netloc:
                return parsed.netloc
    return None


def _extract_model_filter(raw_html: str) -> Optional[str]:
    candidates: List[str] = []
    for pattern in (_CANONICAL_RE, _OG_URL_RE):
        match = pattern.search(raw_html)
        if not match:
            continue
        url = unescape(match.group(1))
        parsed = urlparse(url)
        if parsed.query:
            params = parse_qs(parsed.query)
            if "model" in params:
                candidates.extend(params["model"])
            # encoded _dFR parameters
            for key, values in params.items():
                if "model" in key:
                    candidates.extend(values)
        else:
            segments = [segment for segment in parsed.path.split("/") if segment]
            if segments:
                candidates.append(segments[-1])

    for candidate in candidates:
        normalized = _normalize_model(candidate)
        if normalized:
            return normalized
    return None


def _normalize_model(value: str) -> Optional[str]:
    if not value:
        return None
    decoded = unquote(value).replace("+", " ").strip().lower()
    mapping = {
        "4runner": "4Runner",
        "4 runner": "4Runner",
        "tacoma": "Tacoma",
        "tundra": "Tundra",
        "land cruiser": "Land Cruiser",
        "land-cruiser": "Land Cruiser",
    }
    return mapping.get(decoded)


def _fetch_typesense_documents(
    base_url: str,
    api_key: str,
    index_name: str,
    model_filter: Optional[str],
) -> List[Dict[str, object]]:
    filters = ["condition:='New'"]
    if model_filter:
        filters.append(f"model:='{model_filter}'")

    params = {
        "q": "*",
        "query_by": "model",
        "per_page": 250,
        "filter_by": " && ".join(filters),
    }

    url = f"{base_url}/collections/{index_name}/documents/search"
    headers = {"x-typesense-api-key": api_key}
    response = httpx.get(url, params=params, headers=headers, timeout=TYPENSE_TIMEOUT_SECONDS)
    response.raise_for_status()
    payload = response.json()
    hits = payload.get("hits")
    if not isinstance(hits, list):
        return []
    return [hit.get("document", {}) for hit in hits if isinstance(hit, dict)]


def _parse_currency(value: Optional[str]) -> Optional[float]:
    if not isinstance(value, str):
        return None
    stripped = value.replace("$", "").replace(",", "").strip()
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

    api_key, typesense_host, index_name = _parse_typesense_config(markdown_or_html)
    model_filter = _extract_model_filter(markdown_or_html)
    dealer_host = _extract_dealer_host(markdown_or_html)

    if not dealer_host:
        raise SmartPathParseError("Unable to determine dealer host for SmartPath site.")

    typesense_base = f"https://{typesense_host}"
    documents = _fetch_typesense_documents(typesense_base, api_key, index_name, model_filter)

    rows: List[ParsedRow] = []
    for doc in documents:
        if not isinstance(doc, dict):
            continue
        vin = doc.get("vin") or doc.get("id")
        if not isinstance(vin, str):
            continue
        vin = vin.upper()

        final_price = _parse_currency(doc.get("finalPrice") or doc.get("sellingPrice") or doc.get("price"))
        advertised_price = final_price or _parse_currency(doc.get("internetPrice"))
        msrp = _parse_currency(doc.get("msrp")) or _parse_currency(doc.get("price"))

        flags = doc.get("flags") if isinstance(doc.get("flags"), dict) else {}
        status = "in_transit" if isinstance(flags, dict) and flags.get("inTransit") else "available"

        images = doc.get("imageUrls")
        image_url = images[0] if isinstance(images, list) and images else None

        vdp_url = doc.get("vdpUrl") if isinstance(doc.get("vdpUrl"), str) else None
        if vdp_url and vdp_url.startswith("/"):
            vdp_url = f"https://{dealer_host}{vdp_url}"

        row: ParsedRow = {
            "vin": vin,
            "advertised_price": advertised_price,
            "msrp": msrp,
            "vdp_url": vdp_url,
            "stock_number": doc.get("stockNumber") if isinstance(doc.get("stockNumber"), str) else None,
            "status": status,
            "trim": doc.get("trim") if isinstance(doc.get("trim"), str) else None,
            "model": doc.get("model") if isinstance(doc.get("model"), str) else None,
            "year": doc.get("year") if isinstance(doc.get("year"), (int, float, str)) else None,
            "features": doc.get("features") if isinstance(doc.get("features"), list) else None,
            "image_url": image_url,
        }
        rows.append(row)

    return rows


__all__ = ["parse_inventory", "SmartPathParseError"]
