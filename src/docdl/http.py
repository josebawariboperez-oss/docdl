from __future__ import annotations
import time
import random
from dataclasses import dataclass
from urllib.parse import urlparse

import httpx

@dataclass
class HttpConfig:
    user_agent: str
    timeout_s: int = 30
    max_retries: int = 3
    rps_per_domain: float = 5.0
    backoff_statuses: tuple[int, ...] = (429, 503)

class RateLimiter:
    def __init__(self, rps_per_domain: float):
        self.rps = rps_per_domain
        self._last_by_domain: dict[str, float] = {}

    def wait(self, url: str):
        domain = urlparse(url).netloc
        now = time.time()
        last = self._last_by_domain.get(domain, 0.0)
        min_interval = 1.0 / max(self.rps, 0.1)
        sleep_s = (last + min_interval) - now
        if sleep_s > 0:
            time.sleep(sleep_s)
        self._last_by_domain[domain] = time.time()

def fetch(client: httpx.Client, rl: RateLimiter, url: str, *, cfg: HttpConfig) -> httpx.Response:
    headers = {"User-Agent": cfg.user_agent}
    last_exc = None

    for attempt in range(cfg.max_retries + 1):
        try:
            rl.wait(url)
            resp = client.get(url, headers=headers, timeout=cfg.timeout_s, follow_redirects=True)

            if resp.status_code in cfg.backoff_statuses:
                # exponential backoff with jitter
                backoff = (2 ** attempt) + random.random()
                time.sleep(backoff)
                continue

            return resp

        except Exception as e:
            last_exc = e
            backoff = (2 ** attempt) + random.random()
            time.sleep(backoff)

    raise RuntimeError(f"Failed to fetch after retries: {url}. Last error: {last_exc}")

