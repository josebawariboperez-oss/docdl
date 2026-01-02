from __future__ import annotations
import json
import time
from pathlib import Path
from datetime import datetime, timezone

import yaml

from .http import HttpConfig
from .discover import discover_imf_reo_meca, discover_iea_natural_gas_reports
from .resolve import resolve_imf_issue_to_pdf, resolve_iea_report_to_pdf
from .download import download_pdf, PaywallOrHtmlError
from .extract import extract_text_from_pdf
from .util import sha256_text
from .store import SupabaseStore
from .enrich import summarize_report

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def main():
    cfg_path = Path("config/sources.yaml")
    conf = yaml.safe_load(cfg_path.read_text(encoding="utf-8"))

    http_cfg = HttpConfig(
        user_agent=conf["run"]["user_agent"],
        timeout_s=int(conf["run"]["timeout_s"]),
        max_retries=int(conf["run"]["max_retries"]),
        rps_per_domain=float(conf["run"]["rate_limit_per_domain_rps"]),
        backoff_statuses=tuple(conf["run"]["backoff_statuses"]),
    )

    run_id = f"run_{int(time.time())}"
    started = time.time()

    out_discovered = Path("data/discovered")
    out_raw = Path("data/raw")
    out_extracted = Path("data/extracted")
    out_enriched = Path("data/enriched")
    out_logs = Path("data/logs")
    for p in [out_discovered, out_raw, out_extracted, out_enriched, out_logs]:
        p.mkdir(parents=True, exist_ok=True)

    store = SupabaseStore()

    log = {
        "run_id": run_id,
        "ts_utc": utc_now_iso(),
        "sources": [],
        "errors": [],
        "counts": {"discovered": 0, "processed": 0, "skipped_unchanged": 0, "skipped_paywall": 0, "failed": 0},
    }

    # RUN start log (upsert)
    store.upsert_ingest_run({
        "run_id": run_id,
        "started_at": utc_now_iso(),
        "sources_count": 2,
        "success_count": 0,
        "fail_count": 0,
        "meta": {"schedule": "weekly_monday"},
    })

    # --- DISCOVER ---
    discovered = []
    try:
        discovered += discover_imf_reo_meca("https://www.imf.org/en/publications/reo/meca", cfg=http_cfg)
        discovered += discover_iea_natural_gas_reports(
            "https://www.iea.org/analysis?type=report&energySystem%5B0%5D=natural-gas",
            cfg=http_cfg,
            limit=5
        )
    except Exception as e:
        log["errors"].append({"stage": "discover", "error": str(e)})

    log["counts"]["discovered"] = len(discovered)
    (out_discovered / f"{run_id}.jsonl").write_text(
        "\n".join(json.dumps(x.__dict__, ensure_ascii=False) for x in discovered),
        encoding="utf-8"
    )

    success = 0
    failed = 0

    # --- PROCESS EACH ITEM ---
    for item in discovered:
        # Upsert ingest_item early (status discovered)
        ingest_payload = {
            "run_id": run_id,
            "source_id": item.source_id,
            "series": "IMF REO MECA" if item.source_id == "imf_reo_meca" else "IEA Natural Gas Reports",
            "title": item.title,
            "doc_url": item.doc_url,
            "language": "en",
            "artifact": "full_report_pdf" if item.source_id == "imf_reo_meca" else "pdf",
            "status": "discovered",
            "meta": {},
        }

        ingest_row = {}
        try:
            ingest_row = store.upsert_ingest_item(ingest_payload)
        except Exception as e:
            # si ni siquiera podemos loggear el item, seguimos
            log["errors"].append({"stage": "store_ingest_item", "doc_url": item.doc_url, "error": str(e)})
            failed += 1
            continue

        try:
            # Resolve PDF
            if item.source_id == "imf_reo_meca":
                resolved = resolve_imf_issue_to_pdf(item.doc_url, item.title, cfg=http_cfg)
            else:
                resolved = resolve_iea_report_to_pdf(item.doc_url, item.title, cfg=http_cfg)

            store.set_ingest_item_status(item.doc_url, "discovered", extra={"pdf_url": resolved.pdf_url})

            # DEDUPE: si ya existe regulation con mismo content_hash (tras extraer) lo saltamos.
            # Primero comprobamos si existe regulation (barato). Si existe, igual podemos saltar después de hash.
            existing = store.get_regulation_by_doc_url(item.doc_url)

            # Download
            try:
                pdf_path = download_pdf(item.source_id, resolved.pdf_url, out_raw, cfg=http_cfg)
            except PaywallOrHtmlError as e:
                # regla: si paywall, saltar
                store.set_ingest_item_status(item.doc_url, "skipped_paywall", error=str(e))
                log["counts"]["skipped_paywall"] += 1
                continue

            store.set_ingest_item_status(item.doc_url, "downloaded")

            # Extract
            text = extract_text_from_pdf(pdf_path)
            text_path = out_extracted / (pdf_path.stem + ".txt")
            text_path.write_text(text, encoding="utf-8")
            content_hash = sha256_text(text)

            store.set_ingest_item_status(
                item.doc_url,
                "extracted",
                extra={"content_hash": content_hash, "raw_text_length": len(text)},
            )

            # Dedupe por hash (si ya existe y no cambió)
            if existing and (existing.get("content_hash") == content_hash):
                log["counts"]["skipped_unchanged"] += 1
                # opcional: actualizar ingest_items a stored sin enrich
                store.set_ingest_item_status(item.doc_url, "stored")
                continue

            # Enrich (LLM)
            enriched = summarize_report(text, title=item.title, source=item.source_id)
            enriched_path = out_enriched / (pdf_path.stem + ".json")
            enriched_path.write_text(json.dumps(enriched, ensure_ascii=False, indent=2), encoding="utf-8")

            store.set_ingest_item_status(item.doc_url, "enriched")

            # Upsert regulation (1:1)
            ingest_item_id = ingest_row.get("id")
            if not ingest_item_id:
                # fallback: si no vino por return representation (raro), no rompemos
                raise RuntimeError("Missing ingest_item_id from upsert_ingest_item response.")

            reg_payload = {
                "ingest_item_id": ingest_item_id,
                "doc_url": item.doc_url,
                "source_id": item.source_id,
                "series": ingest_payload["series"],
                "title": item.title,
                "pdf_url": resolved.pdf_url,
                "language": "en",
                "summary": enriched.get("summary"),
                "key_points": enriched.get("key_points", []),
                "key_numbers": enriched.get("key_numbers", []),
                "topics": enriched.get("topics", []),
                "countries": enriched.get("countries", []),
                "dates": enriched.get("dates", {}),
                "impact_level": enriched.get("impact_level"),
                "confidence": enriched.get("confidence"),
                "raw_text_length": len(text),
                "content_hash": content_hash,
            }
            store.upsert_regulation(reg_payload)
            store.set_ingest_item_status(item.doc_url, "stored")

            log["sources"].append({
                "source_id": item.source_id,
                "title": item.title,
                "doc_url": item.doc_url,
                "pdf_url": resolved.pdf_url,
                "pdf_path": str(pdf_path),
                "text_path": str(text_path),
                "enriched_path": str(enriched_path),
                "content_hash": content_hash,
            })

            log["counts"]["processed"] += 1
            success += 1

        except Exception as e:
            failed += 1
            log["counts"]["failed"] += 1
            log["errors"].append({"stage": "process", "source_id": item.source_id, "doc_url": item.doc_url, "error": str(e)})
            try:
                store.set_ingest_item_status(item.doc_url, "failed", error=str(e))
            except Exception:
                pass
            continue

    finished = time.time()
    duration_s = finished - started
    log["duration_s"] = duration_s
    (out_logs / f"{run_id}.json").write_text(json.dumps(log, ensure_ascii=False, indent=2), encoding="utf-8")

    # Close run
    store.update_ingest_run(run_id, {
        "finished_at": utc_now_iso(),
        "success_count": success,
        "fail_count": failed,
        "duration_s": duration_s,
        "meta": {"counts": log["counts"]},
    })

    print(json.dumps(log, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    main()

