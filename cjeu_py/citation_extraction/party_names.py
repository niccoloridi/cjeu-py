"""
Extract party names from CELLAR judgment XHTML and detect informal case
references (e.g. 'the *Bosman* principle') in citing documents.

CELLAR judgment headers follow a consistent structure:

    <p class="coj-normal">In Case C‑XXX/XX,</p>
    <p class="coj-normal">REQUEST for a preliminary ruling ... in the proceedings</p>
    <p class="coj-normal"><span class="coj-bold">Applicant Name,</span></p>
    <p class="coj-pnormal">v</p>
    <p class="coj-normal"><span class="coj-bold">Defendant Name,</span></p>
    <p class="coj-normal">THE COURT (...),</p>

This module:
  1. Extracts party names from judgment XHTML headers.
  2. Generates search variants (full name, short name, etc.).
  3. Builds a lookup table: CELEX → party name variants.
  4. Searches citing document text for informal references by party name.
"""

import json
import logging
import os
import re
import warnings
from typing import Dict, List, Optional, Tuple

from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning

warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

logger = logging.getLogger(__name__)

# Party names shorter than this are too common to be reliable matches
# (e.g. "Commission", "Council", "Germany")
MIN_NAME_LENGTH = 5

# Names that are too generic to serve as case identifiers
STOPLIST = {
    # EU institutions
    "commission", "council", "parliament", "court",
    "european commission", "european council", "european parliament",
    # Member states
    "germany", "france", "italy", "spain", "netherlands", "belgium",
    "luxembourg", "austria", "poland", "portugal", "greece", "ireland",
    "sweden", "denmark", "finland", "czech republic", "hungary",
    "romania", "bulgaria", "croatia", "slovenia", "slovakia", "estonia",
    "latvia", "lithuania", "malta", "cyprus",
    "federal republic of germany", "french republic",
    "republic of poland", "kingdom of spain", "kingdom of belgium",
    "kingdom of the netherlands", "republic of austria",
    "republic of finland", "kingdom of sweden", "kingdom of denmark",
    "republic of italy", "hellenic republic", "portuguese republic",
    "republic of hungary", "republic of croatia",
    # Common generic respondents / procedural
    "advocate general", "registrar",
    # Words too common to be first-word / surname variants
    "european", "union", "federal", "republic", "kingdom", "royal",
    "city", "state", "region", "county", "municipality",
    "banco", "banque", "banca", "caisse",
    "association", "federation", "fédération",
    "minister", "ministero", "ministerstvo", "ministerium",
    "queen", "dignity", "massa",
    "administration", "authority", "agency", "office", "service",
    "general", "national", "public", "central",
    # Common first names that appear in party names
    "jean-marc", "jean", "marc", "pierre", "maria", "giuseppe",
    "hans", "peter", "klaus", "stefan", "andreas", "michael",
}


def _extract_from_meta_description(soup: BeautifulSoup) -> Dict:
    """Fallback: extract party names from DC.description meta tag (old EUR-Lex).

    The meta tag typically has the form::

        Judgment of the Court of 15 December 1995.  -  Union royale belge
        des sociétés de football association ASBL v Jean-Marc Bosman ...
        -  Reference for a preliminary ruling ...  -  Case C-415/93.

    Party names sit between the first " - " separator (after the date line)
    and the second " - " separator (before the subject-matter keywords).
    """
    empty = {"applicants": [], "defendants": [], "all_names": []}
    meta = soup.find("meta", attrs={"name": "DC.description"})
    if not meta:
        return empty

    desc = meta.get("content", "")
    if not desc:
        return empty

    # Split on "  -  " (the EUR-Lex separator with double spaces)
    parts = re.split(r"\s{2,}-\s{2,}", desc)
    if len(parts) < 2:
        return empty

    # The party string is typically the second part (after "Judgment of...")
    party_str = parts[1].strip().rstrip(".")

    # Split on " v " to separate all parties across joined cases
    # e.g. "A v B, C v B and others and D v B"
    segments = re.split(r"\s+v\s+", party_str)

    # Collect all individual names from all segments
    all_names = []
    for seg in segments:
        # Further split on ", " to separate multiple parties on the same side
        for name in re.split(r",\s+", seg):
            name = name.strip().rstrip(",.")
            name = re.sub(r"\s+and others$", "", name)
            # Skip empty or very short fragments
            if name and len(name) > 2:
                all_names.append(name)

    # Deduplicate while preserving order
    seen = set()
    unique_names = []
    for n in all_names:
        if n.lower() not in seen:
            seen.add(n.lower())
            unique_names.append(n)

    return {
        "applicants": unique_names[:1] if unique_names else [],
        "defendants": unique_names[1:] if len(unique_names) > 1 else [],
        "all_names": unique_names,
    }


def extract_party_names_from_html(html: str) -> Dict:
    """Extract party names from a CELLAR judgment XHTML document.

    Returns::

        {
            "applicants": ["BONVER WIN, a.s."],
            "defendants": ["Ministerstvo financí ČR"],
            "all_names": ["BONVER WIN, a.s.", "Ministerstvo financí ČR"],
        }

    Returns empty lists if party names cannot be extracted (e.g. old-format
    documents, AG opinions, or orders without a standard header).
    """
    soup = BeautifulSoup(html, "lxml")

    # Find the header region: between "In Case" / "In Joined Cases" and
    # "THE COURT" / "composed of"
    header_paragraphs = []
    in_header = False

    for p in soup.find_all("p"):
        text = p.get_text().strip()

        if not in_header:
            if re.match(r"In (Case|Joined Cases)", text):
                in_header = True
            continue

        # End of header
        if text.startswith("THE COURT") or text.startswith("composed of"):
            break

        header_paragraphs.append(p)

    if not header_paragraphs:
        # Fallback for old-format EUR-Lex HTML: parse DC.description meta tag
        # which typically contains "Applicant v Defendant. - Reference for ..."
        return _extract_from_meta_description(soup)

    # Split into applicants (before "v") and defendants (after "v")
    applicants = []
    defendants = []
    past_v = False

    for p in header_paragraphs:
        text = p.get_text().strip()

        # The "v" separator is in <p class="coj-pnormal"> or <p class="pnormal">
        cls = p.get("class", [])
        if isinstance(cls, list):
            cls = " ".join(cls)
        if "pnormal" in cls and text.lower() in ("v", "v."):
            past_v = True
            continue

        # Party names are in <span class="coj-bold"> or <span class="bold">
        bold = p.find("span", class_=re.compile(r"(coj-)?bold"))
        if bold:
            name = bold.get_text().strip()
            # Clean trailing punctuation
            name = name.rstrip(",;.")
            # Skip procedural text that happens to be bold
            if name.startswith("THE COURT") or name.startswith("composed"):
                break
            if name and len(name) > 1:
                if past_v:
                    defendants.append(name)
                else:
                    applicants.append(name)

    all_names = applicants + defendants

    # If no bold-formatted names found, fall back to meta description
    if not all_names:
        return _extract_from_meta_description(soup)

    return {
        "applicants": applicants,
        "defendants": defendants,
        "all_names": all_names,
    }


def generate_name_variants(names: List[str]) -> List[str]:
    """Generate search variants from a list of party names.

    For each name, produces:
      - The full name (cleaned)
      - First substantive word (for short-form references like "Bosman")
      - Name without common suffixes (Ltd, GmbH, SA, etc.)

    Filters out names that are too short or too generic.
    """
    # Common corporate suffixes to strip
    suffixes = re.compile(
        r"\s*\b("
        r"Ltd|Limited|GmbH|AG|SA|SL|Sàrl|SAS|BV|NV|plc|Inc|LP|LLC|LLP"
        r"|a\.s\.|s\.r\.o\.|Oy|AB|ApS|Kft|Sp\.\s*z\s*o\.o\."
        r"|and Others|e\.a\.|u\.a\."
        r")\b\.?\s*$",
        re.IGNORECASE,
    )

    variants = set()

    for raw_name in names:
        name = raw_name.strip()
        if not name:
            continue

        # Skip names that are too generic
        if name.lower() in STOPLIST:
            continue

        # Full name
        if len(name) >= MIN_NAME_LENGTH:
            variants.add(name)

        # Name without corporate suffix
        stripped = suffixes.sub("", name).strip().rstrip(",")
        if stripped and len(stripped) >= MIN_NAME_LENGTH:
            variants.add(stripped)

        # First substantive token (for short-form references)
        tokens = name.split()
        if tokens:
            first = tokens[0].rstrip(",")
            if (len(first) >= MIN_NAME_LENGTH
                    and first.lower() not in STOPLIST
                    and not first[0].islower()):
                variants.add(first)

        # Last word as surname for person names (e.g. "Jean-Marc Bosman" → "Bosman")
        # Heuristic: person names have 2-3 title-case words, no corporate words
        _corp_words = {
            "company", "management", "group", "fund", "trust", "holding",
            "partners", "capital", "investments", "financial", "services",
            "international", "global", "asset", "credit", "insurance",
            "restructuring", "opportunities", "offshore", "master",
        }
        clean_tokens = [t.rstrip(",") for t in tokens if not suffixes.match(t)]
        has_corp_word = any(t.lower() in _corp_words for t in clean_tokens)
        if (2 <= len(clean_tokens) <= 3
                and not has_corp_word
                and all(t[0].isupper() or t[0] == "'" for t in clean_tokens
                        if len(t) > 1)):
            last = clean_tokens[-1]
            if (len(last) >= MIN_NAME_LENGTH
                    and last.lower() not in STOPLIST):
                variants.add(last)

    return sorted(variants)


def build_lookup_table(
    xhtml_dir: str,
    celex_list: Optional[List[str]] = None,
) -> Dict[str, Dict]:
    """Build a CELEX → party names lookup table from downloaded XHTML files.

    Args:
        xhtml_dir: Directory containing XHTML files named ``{CELEX}.xhtml``
                   or ``{CELEX}.html``.
        celex_list: Optional list of CELEX numbers to process.  If None,
                    processes all files in the directory.

    Returns:
        Dict mapping CELEX to party info::

            {
                "62019CJ0311": {
                    "applicants": ["BONVER WIN, a.s."],
                    "defendants": ["Ministerstvo financí ČR"],
                    "all_names": ["BONVER WIN, a.s.", "Ministerstvo financí ČR"],
                    "search_variants": ["BONVER WIN", "BONVER", "Ministerstvo financí ČR", ...],
                },
                ...
            }
    """
    lookup = {}

    if celex_list is not None:
        files_to_process = []
        for celex in celex_list:
            for ext in (".xhtml", ".html"):
                path = os.path.join(xhtml_dir, celex + ext)
                if os.path.exists(path):
                    files_to_process.append((celex, path))
                    break
    else:
        files_to_process = []
        for fname in os.listdir(xhtml_dir):
            if fname.endswith((".xhtml", ".html")):
                celex = fname.rsplit(".", 1)[0]
                files_to_process.append((celex, os.path.join(xhtml_dir, fname)))

    for celex, path in files_to_process:
        try:
            with open(path, "r", encoding="utf-8") as f:
                html = f.read()
            info = extract_party_names_from_html(html)
            info["search_variants"] = generate_name_variants(info["all_names"])
            if info["search_variants"]:
                lookup[celex] = info
        except Exception as e:
            logger.warning(f"Failed to extract party names from {celex}: {e}")

    logger.info(
        f"Built party name lookup: {len(lookup)} cases with extractable names "
        f"out of {len(files_to_process)} files"
    )
    return lookup


def save_lookup_table(lookup: Dict, output_path: str) -> None:
    """Save lookup table to JSON."""
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(lookup, f, ensure_ascii=False, indent=2)
    logger.info(f"Saved party name lookup to {output_path}")


def load_lookup_table(path: str) -> Dict:
    """Load lookup table from JSON."""
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def find_name_references(
    text: str,
    cited_celex_list: List[str],
    lookup: Dict[str, Dict],
) -> List[Dict]:
    """Search citing document text for informal references by party name.

    Args:
        text: The full text of the citing document (with ``*italic*`` markers).
        cited_celex_list: CELEX numbers of cases known to be cited (from
                          ``cdm:work_cites_work`` metadata).
        lookup: Party name lookup table (from ``build_lookup_table``).

    Returns:
        List of detected informal references::

            [
                {
                    "cited_celex": "62015CJ0006",
                    "matched_name": "Bosman",
                    "context": "...the *Bosman* principle requires...",
                    "position": 1423,
                    "in_italics": True,
                },
                ...
            ]
    """
    references = []
    text_lower = text.lower()

    for celex in cited_celex_list:
        entry = lookup.get(celex)
        if not entry:
            continue

        for variant in entry.get("search_variants", []):
            # Case-insensitive search for the variant
            variant_lower = variant.lower()

            # Skip if name is in the stoplist (double-check)
            if variant_lower in STOPLIST:
                continue

            start = 0
            while True:
                pos = text_lower.find(variant_lower, start)
                if pos == -1:
                    break

                # Check word boundaries to avoid partial matches
                if pos > 0 and text[pos - 1].isalnum():
                    start = pos + 1
                    continue
                end_pos = pos + len(variant)
                if end_pos < len(text) and text[end_pos].isalnum():
                    start = pos + 1
                    continue

                # Check if the match is within italic markers
                in_italics = False
                # Look backwards for an opening * and forwards for a closing *
                before = text[max(0, pos - 50):pos]
                after = text[end_pos:min(len(text), end_pos + 50)]
                if "*" in before and "*" in after:
                    # More precise: find the nearest * before and after
                    last_star_before = before.rfind("*")
                    first_star_after = after.find("*")
                    if last_star_before >= 0 and first_star_after >= 0:
                        in_italics = True

                # Extract surrounding context (80 chars each side)
                ctx_start = max(0, pos - 80)
                ctx_end = min(len(text), end_pos + 80)
                context = text[ctx_start:ctx_end]

                references.append({
                    "cited_celex": celex,
                    "matched_name": variant,
                    "context": context,
                    "position": pos,
                    "in_italics": in_italics,
                })

                start = end_pos  # advance past this match

    # Deduplicate: if the same position matches multiple variants of the
    # same CELEX, keep only the longest match
    if references:
        references.sort(key=lambda r: (r["position"], -len(r["matched_name"])))
        deduped = []
        seen_positions = {}
        for ref in references:
            key = (ref["cited_celex"], ref["position"])
            if key not in seen_positions:
                seen_positions[key] = True
                deduped.append(ref)
        references = deduped

    return references
