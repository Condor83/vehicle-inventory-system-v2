from __future__ import annotations

import json
import re
import unicodedata
from pathlib import Path
from typing import Any, Dict
from urllib.parse import urljoin

import yaml


def _slugify(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    normalized = unicodedata.normalize("NFKD", text)
    ascii_text = normalized.encode("ascii", "ignore").decode("ascii")
    ascii_text = re.sub(r"[^\w\s-]", "", ascii_text).strip().lower()
    slug = re.sub(r"[-\s]+", "-", ascii_text)
    return slug or None


def _load_model_registry() -> Dict[str, Dict[str, str]]:
    models_path = Path(__file__).resolve().parents[3] / "data" / "models.yaml"
    data = yaml.safe_load(models_path.read_text(encoding="utf-8"))
    registry: Dict[str, Dict[str, str]] = {}
    for entry in data.get("models", []):
        name = entry["name"]
        slug = entry.get("kebab")
        plus = entry.get("space_plus")
        encoded = entry.get("passthrough")
        underscore = entry.get("underscore") or (slug.replace("-", "_") if slug else None)
        series = entry.get("series") or (slug.replace("-", "") if slug else None)
        model_id = entry.get("dealer_socket_id")
        registry[name] = {
            "model_slug": slug,
            "model_plus": plus,
            "model_name_plus": plus,
            "model_name_encoded": encoded,
            "model_encoded": encoded,
            "model_underscore": underscore,
            "model_series": series,
        }
        if model_id:
            registry[name]["model_id"] = str(model_id)
    return registry


MODEL_REGISTRY = _load_model_registry()
PLACEHOLDER_PATTERN = re.compile(r"\{([^}]+)\}")

def build_inventory_url(dealer_row: dict, model: str) -> str:
    """Build an inventory URL from a dealer row and model.
    Expected fields in dealer_row:
      - homepage_url
      - inventory_url_template (may be absolute or relative)
      - scraping_config: {template_scope: 'absolute'|'relative'}
    Placeholders supported: {homepage_url}, {model_slug}, {model_plus}, {model_name_encoded}
    """
    if model not in MODEL_REGISTRY:
        raise ValueError(f"Unsupported model: {model}")
    tpl = dealer_row.get("inventory_url_template") or ""
    cfg = dealer_row.get("scraping_config") or {}
    if isinstance(cfg, str):
        try:
            cfg = json.loads(cfg)
        except json.JSONDecodeError:
            cfg = {}
    scope = cfg.get("template_scope", "relative")
    model_tokens = MODEL_REGISTRY[model].copy()
    base_tokens: Dict[str, Any] = {
        "homepage_url": dealer_row.get("homepage_url") or "",
        **{k: v for k, v in model_tokens.items() if v is not None},
    }

    cfg_tokens = {}
    if isinstance(cfg, dict):
        cfg_tokens = cfg.get("tokens") or {}
    if isinstance(cfg_tokens, dict):
        for key, value in cfg_tokens.items():
            if value is None:
                continue
            if key == "city":
                base_tokens[key] = _slugify(value)
            elif key == "state":
                base_tokens[key] = str(value).strip().lower()
            else:
                base_tokens[key] = str(value).strip()

    # Fallbacks from dealer row if not present in config tokens
    for fallback_key in ("dealer_code", "city_code", "city", "state"):
        if fallback_key in base_tokens:
            continue
        value = dealer_row.get(fallback_key)
        if value is None:
            continue
        if fallback_key == "city":
            slug = _slugify(value)
            if slug:
                base_tokens[fallback_key] = slug
        elif fallback_key == "state":
            base_tokens[fallback_key] = str(value).strip().lower()
        else:
            base_tokens[fallback_key] = str(value).strip()

    missing: set[str] = set()

    def replace(match: re.Match[str]) -> str:
        key = match.group(1)
        value = base_tokens.get(key)
        if value is None or value == "":
            missing.add(key)
            return ""
        return str(value)

    url = PLACEHOLDER_PATTERN.sub(replace, tpl)

    if missing:
        allowed_missing = {"city_code"}
        unexpected = missing - allowed_missing
        if unexpected:
            dealer_id = dealer_row.get("id")
            raise ValueError(
                f"Missing placeholder token(s) {unexpected} for dealer {dealer_id} ({dealer_row.get('homepage_url')})"
            )
        if "city_code" in missing:
            url = re.sub(r"([?&])cy=(?:(?=&)|$)", lambda m: "?" if m.group(1) == "?" else "", url)
            url = re.sub(r"([?&])cy=(?:&|$)", lambda m: "?" if m.group(1) == "?" else "", url)
        url = url.replace("?&", "?")
        url = re.sub(r"&&+", "&", url)
        if url.endswith("&") or url.endswith("?"):
            url = url.rstrip("?&")

    if scope == "relative" and not url.startswith("http"):
        return urljoin(base_tokens.get("homepage_url", ""), url)
    return url
