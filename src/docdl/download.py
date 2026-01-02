
from __future__ import annotations
import hashlib
from pathlib import Path
import httpx
from .http import fetch, RateLimiter, HttpConfig

class PaywallOrHtmlError(RuntimeError):
    pass

def stable_pdf_filename(source_id: str, pdf_url: str) -> str:
    h = hashlib.sha256(pdf_url.encode("utf-8")).hexdigest()[:12]
    return f"{source_id}_{h}.pdf"

def download_pdf(source_id: str, pdf_url: str, out_dir: Path, *, cfg: HttpConfig) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / stable_pdf_filename(source_id, pdf_url)

    rl = RateLimiter(cfg.rps_per_domain)
    with httpx.Client(http2=True) as client:
        resp = fetch(client, rl, pdf_url, cfg=cfg)

    ctype = (resp.headers.get("content-type") or "").lower()
    if "text/html" in ctype:
        raise PaywallOrHtmlError(f"Got HTML instead of PDF for {pdf_url}")

    path.write_bytes(resp.content)
    return path
