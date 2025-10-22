"""Common utilities for parsing dealer inventory pages scraped via Firecrawl."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional, Sequence, Tuple, Union

VIN_RE = re.compile(r"\b[A-HJ-NPR-Z0-9]{17}\b", re.IGNORECASE)
PRICE_RE = re.compile(r"\$[\s]*([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]{2})?)")
URL_RE = re.compile(r"https?://[^\s\"')>]+", re.IGNORECASE)

ParsedRow = Dict[str, Optional[Union[str, float]]]


@dataclass(frozen=True)
class ParserConfig:
    status_map: Dict[str, str]
    price_keywords_priority: Sequence[Tuple[str, int]]
    url_keywords: Sequence[str] = ("inventory", "vehicle", "vdp")
    stock_patterns: Sequence[re.Pattern[str]] = field(
        default_factory=lambda: (re.compile(r"(?:stock\s*(?:#|number|no\.?)\s*[:\-]?\s*)([A-Z0-9-]+)", re.IGNORECASE),)
    )


def _strip_tags(raw: str) -> str:
    return re.sub(r"<[^>]+>", " ", raw)


def _parse_price(token: str) -> Optional[float]:
    match = PRICE_RE.search(token)
    if not match:
        return None
    numeric = match.group(1).replace(",", "")
    try:
        return float(numeric)
    except ValueError:
        return None


def _extract_status(snippet: str, status_map: Dict[str, str]) -> Optional[str]:
    upper_snippet = snippet.upper()
    for pattern, normalized in status_map.items():
        if pattern in upper_snippet:
            return normalized
    return None


def _extract_stock(snippet: str, patterns: Sequence[re.Pattern[str]]) -> Optional[str]:
    for pattern in patterns:
        match = pattern.search(snippet)
        if match:
            return match.group(1).strip()
    return None


def _extract_vdp_url(snippet: str, vin: str, url_keywords: Iterable[str]) -> Optional[str]:
    for url_match in URL_RE.finditer(snippet):
        url = url_match.group(0)
        lowered = url.lower()
        if vin.lower() in lowered:
            return url
        if any(keyword in lowered for keyword in url_keywords):
            return url
    return None


def _apply_line(
    record: ParsedRow,
    line: str,
    config: ParserConfig,
) -> None:
    if not line:
        return

    lower = line.lower()
    line_price = _parse_price(line)

    if line_price is not None:
        if "msrp" in lower or "sticker price" in lower:
            if record["msrp"] is None:
                record["msrp"] = line_price
        else:
            rank = None
            for keyword, priority in config.price_keywords_priority:
                if keyword in lower:
                    rank = priority
                    break
            if rank is None and "$" in line:
                rank = 5
            if rank is not None:
                current_rank = record.get("_price_rank", float("inf"))
                current_price = record.get("advertised_price")
                if rank < current_rank or (
                    rank == current_rank and (current_price is None or line_price < current_price)
                ):
                    record["advertised_price"] = line_price
                    record["_price_rank"] = rank

    stock_number = _extract_stock(line, config.stock_patterns)
    if stock_number and not record.get("stock_number"):
        record["stock_number"] = stock_number

    status = _extract_status(line, config.status_map)
    if status:
        record["status"] = status

    if not record.get("vdp_url"):
        vdp_url = _extract_vdp_url(line, record["vin"], config.url_keywords)
        if vdp_url:
            record["vdp_url"] = vdp_url


def parse_inventory_with_config(markdown_or_html: str, config: ParserConfig) -> List[ParsedRow]:
    cleaned = _strip_tags(markdown_or_html)
    if not cleaned:
        return []

    records: Dict[str, ParsedRow] = {}
    current_vin: Optional[str] = None

    for raw_line in cleaned.splitlines():
        line = raw_line.strip()
        if not line:
            continue

        vin_match = VIN_RE.search(line)
        if vin_match:
            current_vin = vin_match.group(0).upper()
            record = records.setdefault(
                current_vin,
                {
                    "vin": current_vin,
                    "advertised_price": None,
                    "msrp": None,
                    "vdp_url": None,
                    "stock_number": None,
                    "status": None,
                    "_price_rank": float("inf"),
                },
            )
            remainder = (line[: vin_match.start()] + " " + line[vin_match.end() :]).strip()
            if remainder:
                _apply_line(record, remainder, config)
            continue

        if current_vin is None:
            continue

        record = records[current_vin]
        _apply_line(record, line, config)

    rows: List[ParsedRow] = []
    for row in records.values():
        row.pop("_price_rank", None)
        rows.append(row)

    return rows
