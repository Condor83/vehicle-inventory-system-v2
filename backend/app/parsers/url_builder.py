import json
from urllib.parse import urljoin

MODEL_REGISTRY = {
    "Land Cruiser": {
        "model_slug": "land-cruiser",
        "model_plus": "Land+Cruiser",
        "model_name_encoded": "Land%20Cruiser",
    },
    "4Runner": {
        "model_slug": "4runner",
        "model_plus": "4Runner",
        "model_name_encoded": "4Runner",
    },
    "Tacoma": {
        "model_slug": "tacoma",
        "model_plus": "Tacoma",
        "model_name_encoded": "Tacoma",
    },
    "Tundra": {
        "model_slug": "tundra",
        "model_plus": "Tundra",
        "model_name_encoded": "Tundra",
    },
}

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
    tokens = MODEL_REGISTRY[model]
    url = tpl
    for placeholder, value in (
        ("{model_slug}", tokens["model_slug"]),
        ("{model_plus}", tokens["model_plus"]),
        ("{model_name_encoded}", tokens["model_name_encoded"]),
    ):
        url = url.replace(placeholder, value)
    if "{homepage_url}" in url:
        url = url.replace("{homepage_url}", dealer_row.get("homepage_url") or "")
    if scope == "relative" and not url.startswith("http"):
        return urljoin(dealer_row.get("homepage_url") or "", url)
    return url
