from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional
import yaml


@dataclass(frozen=True)
class Source:
    source_id: str
    kind: str  # e.g. "imf_reo_meca", "iea_gas_market"
    series_url: str
    enabled: bool = True
    meta: Dict[str, Any] = None


def load_sources(path: str | Path) -> List[Source]:
    p = Path(path)
    data = yaml.safe_load(p.read_text(encoding="utf-8")) or {}
    items = data.get("sources", [])
    out: List[Source] = []
    for s in items:
        out.append(
            Source(
                source_id=str(s["source_id"]),
                kind=str(s["kind"]),
                series_url=str(s["series_url"]),
                enabled=bool(s.get("enabled", True)),
                meta=dict(s.get("meta", {})),
            )
        )
    return out
