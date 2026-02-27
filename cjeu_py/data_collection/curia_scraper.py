"""
Scrape judge biographical data from Curia.europa.eu.

Fetches structured data from the Court's official member pages:
- Current members: https://curia.europa.eu/site/jcms/d2_5096
- Former members:  https://curia.europa.eu/jcms/jcms/p1_217426/en/

Each entry has a name, role, and free-text biography. The raw bio text
is saved as-is; structured extraction (birth year, nationality, career)
is handled separately via LLM.
"""
import json
import logging
import os
import time
from typing import Dict, List, Optional
from urllib.request import Request, urlopen

import pandas as pd
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

CURRENT_URL = "https://curia.europa.eu/site/jcms/d2_5096"
FORMER_URL = "https://curia.europa.eu/jcms/jcms/p1_217426/en/"


def _fetch_page(url: str) -> str:
    """Fetch HTML from a URL."""
    req = Request(url, headers={"User-Agent": "cjeu-py/1.0"})
    with urlopen(req, timeout=30) as resp:
        return resp.read().decode("utf-8")


def _parse_members(html: str, is_current: bool = False) -> List[Dict]:
    """Parse member entries from a Curia HTML page.

    Args:
        html: Full page HTML
        is_current: Whether these are current (True) or former (False) members

    Returns:
        List of dicts with keys: name, role, bio_text, is_current
    """
    soup = BeautifulSoup(html, "html.parser")
    titles = soup.find_all("h3", class_="curia-cv-item-title")

    members = []
    for h3 in titles:
        details = h3.parent  # div.curia-cv-item-details

        name = h3.get_text(strip=True)

        role_tag = details.find("p", class_="curia-cv-item-function")
        role = role_tag.get_text(strip=True) if role_tag else None

        bio_div = details.find("div", class_="curia-cv-item-bio-text")
        if bio_div:
            paragraphs = bio_div.find_all("p")
            bio_text = "\n\n".join(p.get_text(strip=True) for p in paragraphs if p.get_text(strip=True))
        else:
            bio_text = None

        members.append({
            "name": name,
            "role": role,
            "bio_text": bio_text,
            "is_current": is_current,
        })

    return members


def scrape_judges(
    output_path: Optional[str] = None,
    cache_dir: Optional[str] = None,
) -> pd.DataFrame:
    """Scrape all current and former CJEU members from Curia.

    Args:
        output_path: Path for JSONL output (optional)
        cache_dir: Directory to cache raw HTML (optional, avoids re-fetching)

    Returns:
        DataFrame with columns: name, role, bio_text, is_current
    """
    all_members = []

    for label, url, is_current in [
        ("current", CURRENT_URL, True),
        ("former", FORMER_URL, False),
    ]:
        # Use cache if available
        cached = os.path.join(cache_dir, f"curia_{label}.html") if cache_dir else None
        if cached and os.path.exists(cached):
            logger.info(f"Loading cached {label} members from {cached}")
            with open(cached, "r", encoding="utf-8") as f:
                html = f.read()
        else:
            logger.info(f"Fetching {label} members from {url}...")
            html = _fetch_page(url)
            if cached:
                os.makedirs(cache_dir, exist_ok=True)
                with open(cached, "w", encoding="utf-8") as f:
                    f.write(html)
            time.sleep(1)  # polite delay

        members = _parse_members(html, is_current=is_current)
        logger.info(f"Parsed {len(members)} {label} members")
        all_members.extend(members)

    if output_path and all_members:
        os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            for m in all_members:
                f.write(json.dumps(m, ensure_ascii=False) + "\n")
        logger.info(f"Saved {len(all_members)} members to {output_path}")

    return pd.DataFrame(all_members) if all_members else pd.DataFrame()


# ── LLM-based structured extraction ────────────────────────────────────

BIO_EXTRACTION_PROMPT = """\
Extract structured biographical data from this CJEU member biography.
Return a JSON object with these fields (use null if not found):

- birth_year: integer
- birth_place: string (city)
- nationality: string (country, e.g. "Italy", "Germany")
- is_female: boolean
- education: list of strings (degrees and institutions)
- prior_careers: list of strings (positions held before CJEU)
- cjeu_roles: list of objects with {{role, start_year, end_year}} (e.g. Judge, AG, President)
- death_year: integer or null

Name: {name}
Role: {role}

Biography:
{bio_text}
"""

BIO_SCHEMA = {
    "type": "object",
    "properties": {
        "birth_year": {"type": ["integer", "null"]},
        "birth_place": {"type": ["string", "null"]},
        "nationality": {"type": ["string", "null"]},
        "is_female": {"type": ["boolean", "null"]},
        "education": {"type": "array", "items": {"type": "string"}},
        "prior_careers": {"type": "array", "items": {"type": "string"}},
        "cjeu_roles": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "role": {"type": "string"},
                    "start_year": {"type": ["integer", "null"]},
                    "end_year": {"type": ["integer", "null"]},
                },
            },
        },
        "death_year": {"type": ["integer", "null"]},
    },
}


def extract_judge_bios(
    members_path: str,
    output_path: str,
    max_items: int = 0,
) -> pd.DataFrame:
    """Extract structured biographical data from scraped member bios using Gemini.

    Args:
        members_path: Path to JSONL from scrape_judges()
        output_path: Path for enriched JSONL output
        max_items: Max members to process (0 = all)

    Returns:
        DataFrame with original fields plus extracted structured data
    """
    from cjeu_py.llm.client import get_gemini_client, classify_citation

    client = get_gemini_client()

    # Load existing progress
    done = set()
    if os.path.exists(output_path):
        with open(output_path, "r") as f:
            for line in f:
                rec = json.loads(line)
                done.add(rec["name"])
        logger.info(f"Resuming: {len(done)} already processed")

    # Load members
    members = []
    with open(members_path, "r") as f:
        for line in f:
            members.append(json.loads(line))

    if max_items > 0:
        members = members[:max_items]

    results = []
    with open(output_path, "a", encoding="utf-8") as out:
        for i, m in enumerate(members):
            if m["name"] in done:
                continue
            if not m.get("bio_text"):
                continue

            prompt = BIO_EXTRACTION_PROMPT.format(
                name=m["name"],
                role=m["role"] or "Unknown",
                bio_text=m["bio_text"],
            )

            try:
                result = classify_citation(
                    client, prompt, BIO_SCHEMA,
                )
                # Merge extracted fields with original
                enriched = {**m, **result}
                out.write(json.dumps(enriched, ensure_ascii=False) + "\n")
                out.flush()
                results.append(enriched)
                logger.info(f"[{i+1}/{len(members)}] {m['name']}: {result.get('nationality', '?')}, b.{result.get('birth_year', '?')}")
            except Exception as e:
                logger.warning(f"Failed to extract bio for {m['name']}: {e}")

    return pd.DataFrame(results) if results else pd.DataFrame()
