from __future__ import annotations
from pathlib import Path
import fitz  # PyMuPDF

def extract_text_from_pdf(pdf_path: Path) -> str:
    doc = fitz.open(pdf_path)
    parts = []
    for page in doc:
        parts.append(page.get_text("text"))
    doc.close()
    return "\n".join(parts).strip()
