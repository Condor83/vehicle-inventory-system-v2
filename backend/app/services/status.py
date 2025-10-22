from typing import Iterable, Dict, Any

def detect_sold(previous_seen: set[str], current_seen: set[str]) -> set[str]:
    """Return VINs that are candidates for sold (seen previously, now missing).
    In production, require two consecutive misses before marking as sold.
    """
    return previous_seen - current_seen
