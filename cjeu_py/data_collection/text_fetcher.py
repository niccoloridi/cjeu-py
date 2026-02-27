"""
Full judgment text retrieval via the CELLAR REST API.

The Publications Office CELLAR endpoint supports content negotiation:
    GET http://publications.europa.eu/resource/celex/{CELEX}
    Accept: application/xhtml+xml, text/html
    Accept-Language: eng

This returns structured XHTML (~0.3 s per request, no DDoS challenge)
and is the officially supported machine-access method.

For each CELEX number, fetches the XHTML in the requested language(s),
parses it into numbered paragraphs, and stores as JSONL with
checkpoint-based resume.

Language support:
    CELLAR uses ISO 639-2/B three-letter codes for Accept-Language.
    The --lang flag accepts comma-separated codes with fallback priority:
        --lang eng,fra   -> try English first, then French
        --lang ita       -> Italian only
    Default is English only.
"""
import os
import logging
import asyncio
import aiohttp
from typing import List, Optional, Sequence

from cjeu_py import config
from cjeu_py.utils.logging_utils import load_existing_log, append_log
from cjeu_py.utils.text_processing import extract_paragraphs_from_html, get_full_text

logger = logging.getLogger(__name__)

CELLAR_BASE = "http://publications.europa.eu/resource/celex/"

# ISO 639-2/B codes used by CELLAR Accept-Language header
LANGUAGE_CODES = {
    "eng": "English",
    "fra": "French",
    "deu": "German",
    "ita": "Italian",
    "spa": "Spanish",
    "nld": "Dutch",
    "por": "Portuguese",
    "pol": "Polish",
    "ron": "Romanian",
    "ces": "Czech",
    "dan": "Danish",
    "ell": "Greek",
    "est": "Estonian",
    "fin": "Finnish",
    "hun": "Hungarian",
    "gle": "Irish",
    "hrv": "Croatian",
    "lit": "Lithuanian",
    "lav": "Latvian",
    "mlt": "Maltese",
    "slk": "Slovak",
    "slv": "Slovenian",
    "swe": "Swedish",
    "bul": "Bulgarian",
}


async def _try_fetch(
    session: aiohttp.ClientSession,
    url: str,
    lang: str,
    semaphore: asyncio.Semaphore,
) -> Optional[str]:
    """Try to fetch XHTML for a single URL in a single language."""
    headers = {
        "Accept": "application/xhtml+xml, text/html",
        "Accept-Language": lang,
    }
    async with semaphore:
        for attempt in range(3):
            try:
                async with session.get(
                    url, headers=headers, timeout=aiohttp.ClientTimeout(total=30)
                ) as resp:
                    if resp.status == 200:
                        return await resp.text()
                    elif resp.status == 404:
                        return None
                    else:
                        logger.warning(
                            f"{url} [{lang}]: HTTP {resp.status} (attempt {attempt + 1})"
                        )
            except asyncio.TimeoutError:
                logger.warning(f"{url} [{lang}]: Timeout (attempt {attempt + 1})")
            except Exception as e:
                logger.warning(f"{url} [{lang}]: Error {e} (attempt {attempt + 1})")
            await asyncio.sleep(1.0 * (2 ** attempt))
    return None


async def fetch_single_text(
    session: aiohttp.ClientSession,
    celex: str,
    semaphore: asyncio.Semaphore,
    languages: Sequence[str] = ("eng",),
) -> dict:
    """
    Fetch and parse a single judgment text from the CELLAR REST API.

    Tries each language in order; returns the first successful result.

    Returns:
        dict with: celex, url, language, text, paragraphs, paragraph_nums,
                   paragraph_count, char_count, status, [error]
    """
    url = f"{CELLAR_BASE}{celex}"

    for lang in languages:
        html = await _try_fetch(session, url, lang, semaphore)
        if html:
            paragraphs = extract_paragraphs_from_html(html)
            full_text = get_full_text(paragraphs)
            return {
                "celex": celex,
                "url": url,
                "language": lang,
                "text": full_text,
                "paragraphs": [p["text"] for p in paragraphs],
                "paragraph_nums": [p["num"] for p in paragraphs],
                "paragraph_count": len(paragraphs),
                "char_count": len(full_text),
                "status": "ok",
            }

    # All languages failed
    tried = ", ".join(languages)
    return {
        "celex": celex,
        "url": url,
        "language": None,
        "status": "not_found",
        "error": f"Not available in: {tried}",
    }


async def fetch_texts_async(
    celex_list: List[str],
    output_path: str,
    concurrency: int = 10,
    max_items: Optional[int] = None,
    languages: Sequence[str] = ("eng",),
):
    """
    Fetch judgment texts for a list of CELEX numbers via CELLAR REST API.

    Uses checkpoint-based resume: skips CELEX IDs already in the output JSONL.
    Default concurrency of 10 -- CELLAR has generous rate limits.

    Args:
        celex_list: CELEX identifiers to fetch
        output_path: JSONL output path (checkpoint-resumable)
        concurrency: Max concurrent requests
        max_items: Limit number of texts to fetch (0 or None = all)
        languages: Language preference order (ISO 639-2/B codes).
                   Tries each in sequence; uses the first available.
    """
    processed = load_existing_log(output_path, id_field="celex")
    remaining = [c for c in celex_list if c not in processed]

    if max_items:
        remaining = remaining[:max_items]

    lang_desc = " > ".join(f"{l} ({LANGUAGE_CODES.get(l, l)})" for l in languages)
    logger.info(
        f"Text fetcher: {len(remaining)} to fetch "
        f"({len(processed)} already done, concurrency={concurrency})"
    )
    logger.info(f"Language preference: {lang_desc}")

    if not remaining:
        logger.info("Nothing to fetch.")
        return

    semaphore = asyncio.Semaphore(concurrency)

    async with aiohttp.ClientSession() as session:
        ok_count = 0
        err_count = 0
        lang_counts = {}

        batch_size = concurrency * 3
        for batch_start in range(0, len(remaining), batch_size):
            batch = remaining[batch_start : batch_start + batch_size]
            tasks = [
                fetch_single_text(session, celex, semaphore, languages)
                for celex in batch
            ]
            results = await asyncio.gather(*tasks)

            for result in results:
                append_log(output_path, result)
                if result.get("status") == "ok":
                    ok_count += 1
                    lang = result.get("language", "?")
                    lang_counts[lang] = lang_counts.get(lang, 0) + 1
                else:
                    err_count += 1

            done = batch_start + len(batch)
            logger.info(
                f"  [{done}/{len(remaining)}] ok={ok_count} err={err_count}"
            )

    lang_summary = ", ".join(f"{v} {k}" for k, v in sorted(lang_counts.items()))
    logger.info(f"Text fetcher done: {ok_count} ok ({lang_summary}), {err_count} errors")


def fetch_texts(
    celex_list: List[str],
    output_path: str = None,
    concurrency: int = 10,
    max_items: Optional[int] = None,
    languages: Sequence[str] = ("eng",),
):
    """Synchronous wrapper for async CELLAR text fetching.

    Args:
        celex_list: CELEX identifiers to fetch
        output_path: JSONL output path (default: data/raw/texts/gc_texts.jsonl)
        concurrency: Max concurrent requests
        max_items: Limit (0 or None = all)
        languages: Language preference order (ISO 639-2/B codes)
    """
    output_path = output_path or os.path.join(
        config.RAW_TEXTS_DIR, "gc_texts.jsonl"
    )
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    asyncio.run(
        fetch_texts_async(celex_list, output_path, concurrency, max_items, languages)
    )
