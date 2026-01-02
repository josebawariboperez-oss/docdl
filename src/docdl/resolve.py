from __future__ import annotations
from dataclasses import dataclass
from urllib.parse import urljoin

import httpx
from selectolax.parser import HTMLParser

from .http import fetch, RateLimiter, HttpConfig

@dataclass
class ResolvedDoc:
    source_id: str
    title: str
    doc_url: str
    pdf_url: str
    paywalled: bool = False

def resolve_imf_issue_to_pdf(doc_url: str, title: str, *, cfg: HttpConfig) -> ResolvedDoc:
    """
    Find the 'DOWNLOAD FULL REPORT' link that points to the PDF.
    Your example ends at /-/media/.../text.pdf
    """
    rl = RateLimiter(cfg.rps_per_domain)
    with httpx.Client(http2=True) as client:
        resp = fetch(client, rl, doc_url, cfg=cfg)
        tree = HTMLParser(resp.text)

    # robust: any anchor href that endswith .pdf and contains '/-/media/'
    pdf_candidates = []
    for a in tree.css("a"):
        href = a.attributes.get("href") or ""
        if href.lower().endswith(".pdf") and "/-/media/" in href:
            pdf_candidates.append(urljoin(doc_url, href))

    if not pdf_candidates:
        raise RuntimeError(f"IMF resolve: no PDF link found on issue page: {doc_url}")

    return ResolvedDoc(source_id="imf_reo_meca", title=title, doc_url=doc_url, pdf_url=pdf_candidates[0])

def resolve_iea_report_to_pdf(doc_url: str, title: str, *, cfg: HttpConfig) -> ResolvedDoc:
    """
    Find 'Download PDF' button anchor; often a direct blob URL ending in .pdf
    """
    rl = RateLimiter(cfg.rps_per_domain)
    with httpx.Client(http2=True) as client:
        resp = fetch(client, rl, doc_url, cfg=cfg)
        tree = HTMLParser(resp.text)

    # Find any .pdf link; prefer azure blob
    pdf_candidates = []
    for a in tree.css("a"):
        href = a.attributes.get("href") or ""
        if href.lower().endswith(".pdf"):
            full = urljoin(doc_url, href)
            pdf_candidates.append(full)

    if not pdf_candidates:
        raise RuntimeError(f"IEA resolve: no PDF link found on report page: {doc_url}")

    return ResolvedDoc(source_id="iea_gas_reports", title=title, doc_url=doc_url, pdf_url=pdf_candidates[0])

