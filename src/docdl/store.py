from __future__ import annotations
import os
import httpx
from typing import Any

class SupabaseStore:
    def __init__(self):
        self.url = os.environ["SUPABASE_URL"].rstrip("/")
        self.key = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
        self.rest = f"{self.url}/rest/v1"

    def _headers(self) -> dict[str, str]:
        return {
            "apikey": self.key,
            "Authorization": f"Bearer {self.key}",
            "Content-Type": "application/json",
        }

    def upsert_ingest_run(self, payload: dict[str, Any]) -> None:
        # upsert by run_id (unique)
        with httpx.Client(timeout=30) as c:
            r = c.post(
                f"{self.rest}/ingest_runs?on_conflict=run_id",
                headers={**self._headers(), "Prefer": "resolution=merge-duplicates,return=minimal"},
                json=payload,
            )
            r.raise_for_status()

    def update_ingest_run(self, run_id: str, patch: dict[str, Any]) -> None:
        with httpx.Client(timeout=30) as c:
            r = c.patch(
                f"{self.rest}/ingest_runs?run_id=eq.{httpx.QueryParams({'x':run_id})['x']}",
                headers={**self._headers(), "Prefer": "return=minimal"},
                json=patch,
            )
            r.raise_for_status()

    def upsert_ingest_item(self, payload: dict[str, Any]) -> dict[str, Any]:
        # upsert by doc_url (unique). Return representation to get id.
        with httpx.Client(timeout=30) as c:
            r = c.post(
                f"{self.rest}/ingest_items?on_conflict=doc_url",
                headers={**self._headers(), "Prefer": "resolution=merge-duplicates,return=representation"},
                json=payload,
            )
            r.raise_for_status()
            data = r.json()
            return data[0] if data else {}

    def set_ingest_item_status(self, doc_url: str, status: str, *, error: str | None = None, extra: dict[str, Any] | None = None) -> None:
        patch: dict[str, Any] = {"status": status}
        if error:
            patch["error"] = error[:4000]
        if extra:
            patch.update(extra)

        # Escape doc_url by passing as param-ish: simplest is eq.<urlencoded>
        q = httpx.QueryParams({"doc_url": f"eq.{doc_url}"}).encode()
        with httpx.Client(timeout=30) as c:
            r = c.patch(
                f"{self.rest}/ingest_items?{q}",
                headers={**self._headers(), "Prefer": "return=minimal"},
                json=patch,
            )
            r.raise_for_status()

    def get_regulation_by_doc_url(self, doc_url: str) -> dict[str, Any] | None:
        q = httpx.QueryParams({"doc_url": f"eq.{doc_url}", "select": "*", "limit": "1"}).encode()
        with httpx.Client(timeout=30) as c:
            r = c.get(f"{self.rest}/regulations?{q}", headers=self._headers())
            r.raise_for_status()
            data = r.json()
            return data[0] if data else None

    def upsert_regulation(self, payload: dict[str, Any]) -> None:
        # upsert by doc_url (unique)
        with httpx.Client(timeout=30) as c:
            r = c.post(
                f"{self.rest}/regulations?on_conflict=doc_url",
                headers={**self._headers(), "Prefer": "resolution=merge-duplicates,return=minimal"},
                json=payload,
            )
            r.raise_for_status()
