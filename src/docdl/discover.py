from __future__ import annotations
from dataclasses import dataclass
from typing import Iterable
from urllib.parse import urljoin

import httpx
from selectolax.parser import HTMLParser

from .http import fetch, RateLimiter, HttpConfig

@dataclass
class DiscoveredItem:
    source_id: str
    title: str
    doc_url: str  # canonical page for the edition/report
    published_date: str | None = None

def discover_imf_reo_meca(index_url: str, *, cfg: HttpConfig) -> list[DiscoveredItem]:
    """
    Strategy:
    - Fetch index page
    - Find the first/latest issue link under the listing
    - Return 1 item (latest) to reduce noise
    """
    rl = RateLimiter(cfg.rps_per_domain)
    with httpx.Client(http2=True) as client:
        resp = fetch(client, rl, index_url, cfg=cfg)
        html = resp.text
    tree = HTMLParser(html)

    # IMF pages can change; we do "robust-ish" strategy:
    # Find first anchor that matches '/issues/' within publications/reo/meca
    anchors = tree.css("a")
    candidates: list[tuple[str, str]] = []
    for a in anchors:
        href = a.attributes.get("href") or ""
        text = (a.text() or "").strip()
        if "/publications/reo/meca/issues/" in href:
            candidates.append((text, urljoin(index_url, href)))

    if not candidates:
        raise RuntimeError("IMF discover: no issue links found (HTML may have changed).")

    title, issue_url = candidates[0]
    return [DiscoveredItem(source_id="imf_reo_meca", title=title or "IMF REO MECA (latest)", doc_url=issue_url)]

def discover_iea_natural_gas_reports(index_url: str, *, cfg: HttpConfig, limit: int = 5) -> list[DiscoveredItem]:
    """
    Strategy:
    - Fetch filtered report listing page
    - Extract top N report links under /reports/
    """
    rl = RateLimiter(cfg.rps_per_domain)
    with httpx.Client(http2=True) as client:
        resp = fetch(client, rl, index_url, cfg=cfg)
        html = resp.text
    tree = HTMLParser(html)

    items: list[DiscoveredItem] = []
    for a in tree.css("a"):
        href = a.attributes.get("href") or ""
        text = (a.text() or "").strip()
        if href.startswith("/reports/") and text:
            doc_url = urljoin("https://www.iea.org", href)
            items.append(DiscoveredItem(source_id="iea_gas_reports", title=text, doc_url=doc_url))
            if len(items) >= limit:
                break

    if not items:
        raise RuntimeError("IEA discover: no /reports/ links found (HTML may have changed).")

    return items
