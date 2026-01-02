from __future__ import annotations
import os
import json
import httpx
from typing import Any

def _openai_headers() -> dict[str, str]:
    return {
        "Authorization": f"Bearer {os.environ['OPENAI_API_KEY']}",
        "Content-Type": "application/json",
    }

def summarize_report(text: str, *, title: str, source: str) -> dict[str, Any]:
    """
    Devuelve JSON estructurado para guardar en regulations.
    Nota: endpoint/model puede variar; dejamos model configurable.
    """
    model = os.environ.get("OPENAI_MODEL", "gpt-4.1-mini")
    base_url = os.environ.get("OPENAI_BASE_URL", "https://api.openai.com").rstrip("/")

    system = (
        "You are a senior economic and energy analyst. "
        "Return STRICT JSON only, no markdown."
    )
    user = {
        "task": "Summarize and extract structured signals from the report text.",
        "source": source,
        "title": title,
        "required_json_schema": {
            "summary": "string (120-200 words, neutral, dense, no hype)",
            "key_points": ["bullet strings (6-10)"],
            "key_numbers": [{"metric":"string","value":"string/number","unit":"string|null","context":"string"}],
            "topics": ["strings"],
            "countries": ["strings"],
            "dates": {"published":"string|null","horizon":"string|null","other":"object"},
            "impact_level": "low|medium|high",
            "confidence": "number 0-1"
        },
        "constraints": [
            "If unsure, be explicit via lower confidence.",
            "Prefer extracting numbers that are clearly stated.",
            "Do not invent data."
        ],
        "text": text[:200_000],  # recorte defensivo
    }

    payload = {
        "model": model,
        "temperature": 0.2,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": json.dumps(user, ensure_ascii=False)},
        ],
        "response_format": {"type": "json_object"},
    }

    with httpx.Client(timeout=90) as c:
        r = c.post(f"{base_url}/v1/chat/completions", headers=_openai_headers(), json=payload)
        r.raise_for_status()
        data = r.json()

    content = data["choices"][0]["message"]["content"]
    return json.loads(content)
