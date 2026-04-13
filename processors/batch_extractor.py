"""
Async Batch Extractor
======================
Keeps the GPU saturated by overlapping prefill + generation across documents.
Instead of: doc1 fully → doc2 fully → doc3...
Does:        prefill doc1 → while generating doc1, prefill doc2 → ...

On a 4060 with Q6_K_M 4B this roughly doubles throughput.

Usage:
    python -m processors.batch_extractor --limit 500
    python -m processors.batch_extractor --limit 500 --workers 2
"""

import argparse
import asyncio
import json
import logging
import time
from pathlib import Path

import httpx

import config
from store.postgres import get_pending_documents, update_document_status, insert_waste_listing, insert_carbon_emission, insert_symbiosis_exchange
from processors.local_extractor import LocalExtractor, _validate_llm_record
from processors.models import WasteListingExtraction, CarbonEmissionExtraction, SymbiosisExchangeExtraction, validate_and_create

log = logging.getLogger(__name__)

# How many docs to process concurrently.
# 2 is the sweet spot for a single GPU — one prefilling while one generating.
# 3+ causes contention and slows everything down.
DEFAULT_WORKERS = 2


async def extract_one(
    client: httpx.AsyncClient,
    doc: dict,
    semaphore: asyncio.Semaphore,
) -> tuple[str, list]:
    """Extract from a single document. Returns (doc_id, results)."""
    doc_id = str(doc["id"])
    content = doc.get("content") or doc.get("raw_text") or ""

    if not content:
        # Try reading from file path
        file_path = doc.get("file_path")
        if file_path and Path(file_path).exists():
            content = Path(file_path).read_text(encoding="utf-8", errors="ignore")

    if not content or len(content.strip()) < 100:
        return doc_id, []

    # Clean HTML — use trafilatura for proper article extraction
    if content.strip().startswith("<") or "<html" in content[:500].lower():
        try:
            import trafilatura
            extracted = trafilatura.extract(content, include_tables=True, no_fallback=False)
            if extracted and len(extracted.strip()) > 100:
                content = extracted
            else:
                import re
                content = re.sub(r"<[^>]+>", " ", content)
                content = re.sub(r"\s+", " ", content).strip()
        except Exception:
            import re
            content = re.sub(r"<[^>]+>", " ", content)
            content = re.sub(r"\s+", " ", content).strip()

    if not content or len(content.strip()) < 100:
        return doc_id, []

    # Truncate to GPU-safe size
    content = content[:80_000]

    prompt = f"""Extract all industrial waste listings, carbon emissions, and symbiosis exchanges from this document.
Return JSON: {{"extractions": [array of facts]}}
Every fact needs: _schema, source_quote, extraction_confidence.
_schema must be one of: waste_listing | carbon_emission | symbiosis_exchange

--- DOCUMENT ---
{content}
--- END ---"""

    async with semaphore:
        try:
            r = await client.post("/chat/completions", json={
                "model": config.LLM_MODEL,
                "messages": [
                    {"role": "system", "content": "You are a precision industrial data extraction engine. Extract only explicitly stated facts. Return valid JSON only."},
                    {"role": "user", "content": prompt},
                ],
                "temperature": 0.0,
                "repeat_penalty": 1.1,
                "max_tokens": 4096,
            })
            r.raise_for_status()
            content_out = r.json()["choices"][0]["message"]["content"].strip()
            if not content_out:
                return doc_id, []
            # Strip markdown code fences if present
            import re as _re
            fence = _re.search(r"```(?:json)?\s*([\s\S]*?)\s*```", content_out)
            if fence:
                content_out = fence.group(1)
            # Find JSON object/array
            for pat in [r"\{[\s\S]*\}", r"\[[\s\S]*\]"]:
                m = _re.search(pat, content_out)
                if m:
                    try:
                        raw = json.loads(m.group())
                        if isinstance(raw, list):
                            return doc_id, raw
                        return doc_id, raw.get("extractions", [])
                    except json.JSONDecodeError:
                        pass
            return doc_id, []
        except Exception as e:
            log.warning(f"Doc {doc_id} failed: {e}")
            return doc_id, []


def save_results(doc_id: str, extractions: list, doc_text: str) -> int:
    """Validate and save extraction results. Returns count saved."""
    model_map = {
        "waste_listing":     (WasteListingExtraction,  insert_waste_listing),
        "carbon_emission":   (CarbonEmissionExtraction, insert_carbon_emission),
        "symbiosis_exchange":(SymbiosisExchangeExtraction, insert_symbiosis_exchange),
    }
    saved = 0
    for raw in extractions:
        schema = raw.pop("_schema", None)
        if schema not in model_map:
            continue
        raw = _validate_llm_record(raw, doc_text)
        model_cls, insert_fn = model_map[schema]
        validated = validate_and_create(model_cls, raw, doc_text)
        if validated:
            record = dict(validated)
            record["document_id"] = doc_id
            try:
                insert_fn(record)
                saved += 1
            except Exception as e:
                log.debug(f"Insert failed: {e}")
    return saved


async def run_batch(limit: int = 500, workers: int = DEFAULT_WORKERS, source: str = None):
    docs = get_pending_documents(source=source, limit=limit)
    if not docs:
        log.info("No pending documents.")
        return

    log.info(f"Processing {len(docs)} documents with {workers} concurrent workers")

    semaphore = asyncio.Semaphore(workers)
    total_saved = 0
    t0 = time.time()

    # Pre-load doc texts (file reads are sync, do before async loop)
    doc_texts: dict[str, str] = {}
    for doc in docs:
        content = doc.get("content") or ""
        if not content:
            fp = doc.get("file_path")
            if fp and Path(fp).exists():
                content = Path(fp).read_text(encoding="utf-8", errors="ignore")[:80_000]
        doc_texts[str(doc["id"])] = content

    async with httpx.AsyncClient(
        base_url=config.LLM_BASE_URL,
        headers={"Authorization": f"Bearer {config.LLM_API_KEY}"},
        timeout=600.0,
    ) as client:
        tasks = [extract_one(client, doc, semaphore) for doc in docs]

        for i, coro in enumerate(asyncio.as_completed(tasks)):
            doc_id, extractions = await coro
            doc_text = doc_texts.get(doc_id, "")

            if extractions:
                saved = save_results(doc_id, extractions, doc_text)
                total_saved += saved
                update_document_status(doc_id, "completed")
            else:
                update_document_status(doc_id, "completed_no_data")

            # Progress
            elapsed = time.time() - t0
            rate = (i + 1) / elapsed
            eta = (len(docs) - i - 1) / rate if rate > 0 else 0
            print(
                f"\r[{i+1}/{len(docs)}] {total_saved} records saved | "
                f"{rate:.1f} docs/s | ETA {eta/60:.1f}min",
                end="", flush=True,
            )

    print()
    elapsed = time.time() - t0
    log.info(f"\nDone. {total_saved} records from {len(docs)} docs in {elapsed/60:.1f} minutes")
    log.info(f"Rate: {len(docs)/elapsed:.2f} docs/sec")


if __name__ == "__main__":
    import sys
    sys.path.insert(0, str(Path(__file__).parent.parent))
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

    parser = argparse.ArgumentParser()
    parser.add_argument("--limit",   type=int, default=500)
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS,
                        help="Concurrent extractions (2 = sweet spot for single GPU)")
    parser.add_argument("--source",  type=str, default=None,
                        help="Filter by source (eprtr, csr, gri, etc.)")
    args = parser.parse_args()

    asyncio.run(run_batch(args.limit, args.workers, args.source))
