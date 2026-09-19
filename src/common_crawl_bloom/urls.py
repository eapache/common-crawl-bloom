"""Versioned, conservative key policy shared by construction and lookup."""

import re
from urllib.parse import urlsplit

KEY_POLICY = "exact-ascii-http-url-v1"


def url_key(url: str) -> bytes | None:
    # No lossy normalization: query parameters are essential to this use case.
    if not isinstance(url, str) or not url.startswith(("https://", "http://")):
        return None
    if any(ord(c) <= 32 or ord(c) >= 127 for c in url):
        return None
    if "\\" in url or "#" in url or re.search(r"%(?![0-9a-fA-F]{2})", url):
        return None
    try:
        parts = urlsplit(url)
        if not parts.hostname or parts.username is not None or parts.password is not None:
            return None
        if parts.port is not None and not 1 <= parts.port <= 65535:
            return None
    except ValueError:
        return None
    return url.encode("ascii")
