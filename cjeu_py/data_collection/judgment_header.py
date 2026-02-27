"""
Parse structured metadata from CJEU judgment/opinion XHTML headers.

Extracts: document type, date, case numbers, parties, court composition,
representatives, advocate general, registrar, and procedural dates.
"""
import json
import logging
import os
import re
from typing import Dict, List, Optional

import pandas as pd
from bs4 import BeautifulSoup, Tag

logger = logging.getLogger(__name__)

# Date patterns: "26 January 2021", "28 July 2016"
_MONTH_MAP = {
    "january": "01", "february": "02", "march": "03", "april": "04",
    "may": "05", "june": "06", "july": "07", "august": "08",
    "september": "09", "october": "10", "november": "11", "december": "12",
}
_DATE_RE = re.compile(
    r"(\d{1,2})\s+(" + "|".join(_MONTH_MAP) + r")\s+(\d{4})", re.IGNORECASE
)

# Case number patterns (handles en-dash, em-dash, hyphen, non-breaking hyphen)
_CASE_NUM_RE = re.compile(r"([CTF])[‑–—\-](\d+)/(\d{2,4})")

# Representative title patterns
_REP_TITLES = re.compile(
    r",?\s*(?:acting as Agents?|"
    r"avvocat[io]|adwoka[tc][iy]?|Rechtsanw[aä]lt(?:in)?|"
    r"abogad[oa]s?|advogad[oa]s?|"
    r"Barrister|Solicitor|"
    r"radca prawny|dikigoro[isu]|"
    r"advokát[ay]?|ügyvéd|"
    r"avocat(?:e)?s?\s+au\s+barreau|"
    r"of\s+the\s+Bar)\b",
    re.IGNORECASE,
)


def _match_class(tag: Tag, suffix: str) -> bool:
    """Match CSS class with or without 'coj-' prefix."""
    classes = tag.get("class", [])
    if isinstance(classes, str):
        classes = classes.split()
    return suffix in classes or f"coj-{suffix}" in classes


def _has_bold(tag: Tag) -> bool:
    """Check if tag contains a bold span."""
    bold = tag.find("span", class_=lambda c: c and ("bold" in c or "coj-bold" in c))
    return bold is not None


def _get_bold_text(tag: Tag) -> str:
    """Extract text from bold spans within a tag."""
    bolds = tag.find_all("span", class_=lambda c: c and ("bold" in c or "coj-bold" in c))
    return " ".join(b.get_text(strip=True) for b in bolds)


def _extract_header_paragraphs(soup: BeautifulSoup) -> List[Tag]:
    """Extract all paragraph tags from the header (before first numbered paragraph)."""
    body = soup.find("body")
    if not body:
        return []

    header_tags = []
    for tag in body.children:
        if not isinstance(tag, Tag):
            continue

        # Stop at first numbered paragraph (table with count digit, not "–" or empty)
        if tag.name == "table":
            count = tag.find("p", class_=lambda c: c and ("count" in c or "coj-count" in c))
            if count:
                count_text = count.get_text(strip=True).rstrip(".")
                if count_text.isdigit():
                    break
        # Also stop at the "Judgment" / "Order" heading that signals end of header
        if tag.name == "p":
            text = tag.get_text(strip=True)
            if _match_class(tag, "sum-title-1") and text in ("Judgment", "Order"):
                break

        header_tags.append(tag)

    return header_tags


def _parse_date(text: str) -> Optional[str]:
    """Extract date in YYYY-MM-DD format from text."""
    m = _DATE_RE.search(text)
    if m:
        day = m.group(1).zfill(2)
        month = _MONTH_MAP[m.group(2).lower()]
        year = m.group(3)
        return f"{year}-{month}-{day}"
    return None


def _parse_case_numbers(text: str) -> List[str]:
    """Extract case numbers like C-16/19, T-344/19 from text."""
    matches = _CASE_NUM_RE.findall(text)
    return [f"{court}-{num}/{year}" for court, num, year in matches]


def _parse_composition(text: str) -> List[Dict[str, str]]:
    """Parse 'composed of ...' text into list of judges with roles.

    Handles patterns like:
        'composed of K. Lenaerts, President, R. Silva de Lapuerta, Vice-President,
         A. Prechal, M. Vilaras, E. Regan and M. Ilešič, Presidents of Chambers,
         E. Juhász, T. von Danwitz (Rapporteur), S. Rodin, F. Biltgen,
         K. Jürimäe, C. Lycourgos and N. Jääskinen, Judges,'
    """
    idx = text.lower().find("composed of")
    if idx < 0:
        return []
    comp_text = text[idx + len("composed of"):].strip().rstrip(",.")

    # Role keywords that appear after a group of names
    role_keywords = [
        "President of the Chamber", "Presidents of Chambers",
        "President of Chamber", "Vice-President", "President",
        "Judges", "Judge",
    ]

    judges = []

    # Strategy: split into segments ending with a role keyword or (Rapporteur)
    # First, handle parenthetical roles: "T. von Danwitz (Rapporteur)"
    # Replace them with a marker to avoid confusion
    paren_roles = {}
    for m in re.finditer(r"(\([^)]+\))", comp_text):
        marker = f"__ROLE{len(paren_roles)}__"
        paren_roles[marker] = m.group(1)[1:-1]  # strip parens
        comp_text = comp_text.replace(m.group(0), marker, 1)

    # Now split by role keywords: each keyword applies to the names before it
    remaining = comp_text
    while remaining.strip():
        # Find the next role keyword
        best_pos = len(remaining)
        best_kw = None
        for kw in role_keywords:
            pos = remaining.find(kw)
            if pos >= 0 and pos < best_pos:
                best_pos = pos
                best_kw = kw

        if best_kw:
            names_part = remaining[:best_pos].strip().rstrip(",")
            remaining = remaining[best_pos + len(best_kw):].strip().lstrip(",").strip()
        else:
            # No more role keywords — remaining names are plain judges
            names_part = remaining.strip().rstrip(",.")
            remaining = ""
            best_kw = None

        # Split names on commas and "and"
        name_strs = re.split(r",\s*|\s+and\s+", names_part)
        for ns in name_strs:
            ns = ns.strip().rstrip(",.")
            if not ns or len(ns) < 3:
                continue
            # Check for paren role marker
            role = best_kw
            for marker, prole in paren_roles.items():
                if marker in ns:
                    ns = ns.replace(marker, "").strip()
                    role = prole
                    break
            if ns and len(ns) >= 2:
                judges.append({"name": ns, "role": role})

    return judges


def _parse_representatives(text: str) -> List[Dict[str, str]]:
    """Parse 'by A. Name, B. Name and C. Name, avvocati' into rep list."""
    # Find "by " at the start or after comma
    by_idx = text.find(", by ")
    if by_idx < 0:
        by_idx = text.find(" by ")
    if by_idx < 0:
        return []

    rep_text = text[by_idx + 4:].strip() if text[by_idx:by_idx + 4] == ", by" else text[by_idx + 4:].strip()

    # Remove trailing title/role
    title = None
    title_match = _REP_TITLES.search(rep_text)
    if title_match:
        title = title_match.group(0).strip(", ")
        rep_text = rep_text[:title_match.start()].strip().rstrip(",")

    # Remove trailing qualifiers like "with an address for service in Luxembourg"
    rep_text = re.sub(r",?\s*with an address for service.*$", "", rep_text)

    # Split on commas and "and"
    names = re.split(r",\s*(?:and\s+)?|\s+and\s+", rep_text)
    reps = []
    for name in names:
        name = name.strip().rstrip(",.")
        # Filter out noise (too short, looks like a qualifier)
        if name and len(name) > 2 and not name.startswith("with "):
            reps.append({"name": name, "title": title})

    return reps


def parse_judgment_header(xhtml: str) -> Dict:
    """Parse structured metadata from a CJEU judgment/opinion XHTML header.

    Returns dict with keys:
        doc_type, date, case_numbers, formation, parties,
        composition, advocate_general, registrar, representatives,
        hearing_date, ag_opinion_date
    """
    soup = BeautifulSoup(xhtml, "html.parser")
    header = _extract_header_paragraphs(soup)

    result = {
        "doc_type": None,
        "date": None,
        "case_numbers": [],
        "formation": None,
        "parties": {"applicants": [], "defendants": [], "interveners": []},
        "composition": [],
        "advocate_general": None,
        "registrar": None,
        "representatives": [],
        "hearing_date": None,
        "ag_opinion_date": None,
    }

    # Track party parsing state
    current_side = "applicants"  # before "v"
    in_interveners = False

    for tag in header:
        if not isinstance(tag, Tag) or tag.name != "p":
            # Handle tables that contain representative lists (observation tables)
            if isinstance(tag, Tag) and tag.name == "table":
                for p in tag.find_all("p"):
                    text = p.get_text(strip=True)
                    if not text or text == "–":
                        continue
                    # Representative entries in observation tables
                    if ", by " in text or " by " in text:
                        # Extract party name (bold) and representatives
                        party_name = _get_bold_text(p) if _has_bold(p) else None
                        if not party_name:
                            # Try text before "by"
                            comma_by = text.find(", by ")
                            if comma_by > 0:
                                party_name = text[:comma_by].strip()
                        reps = _parse_representatives(text)
                        if party_name and reps:
                            result["representatives"].append({
                                "party": party_name,
                                "representatives": reps,
                            })
            continue

        text = tag.get_text(strip=True)
        if not text:
            continue

        # Document type and date
        if _match_class(tag, "sum-title-1"):
            if "JUDGMENT" in text.upper():
                result["doc_type"] = "judgment"
                # Extract formation from parentheses
                fm = re.search(r"\(([^)]+)\)", text)
                if fm:
                    result["formation"] = fm.group(1)
            elif "OPINION" in text.upper() and "ADVOCATE" in text.upper():
                result["doc_type"] = "ag_opinion"
            elif "ORDER" in text.upper():
                result["doc_type"] = "order"
                fm = re.search(r"\(([^)]+)\)", text)
                if fm:
                    result["formation"] = fm.group(1)
            # Try to extract date from any sum-title-1
            d = _parse_date(text)
            if d:
                result["date"] = d

        # AG opinion: AG name is on its own sum-title-1 line (all caps, e.g. "PITRUZZELLA")
        if _match_class(tag, "sum-title-1") and result["doc_type"] == "ag_opinion":
            if text.isupper() and len(text.split()) <= 3 and "OPINION" not in text and "ADVOCATE" not in text:
                result["advocate_general"] = text.title()

        # Case numbers
        if "In Case" in text or "In Joined Cases" in text:
            result["case_numbers"] = _parse_case_numbers(text)

        # Subtitle elements (used in AG opinions for parties)
        if _match_class(tag, "subtitle"):
            case_nums = _parse_case_numbers(text)
            if case_nums and not result["case_numbers"]:
                result["case_numbers"] = case_nums
            elif text.lower() == "v":
                current_side = "defendants"
            elif text.lower() == "and":
                pass  # skip bare "and" separators between sub-cases
            elif not case_nums:
                # Strip leading "and " conjunction, trailing comma, case suffixes like "(C-53/19 P)"
                party_name = re.sub(r"\s*\([^)]*\)\s*$", "", text).strip()
                if party_name.lower().startswith("and "):
                    party_name = party_name[4:].strip()
                party_name = party_name.rstrip(",. ")
                if party_name:
                    # Avoid duplicate party names
                    if party_name not in result["parties"][current_side]:
                        result["parties"][current_side].append(party_name)

        # Status markers: "applicant,", "defendant,", "intervener,"
        if _match_class(tag, "pstatus"):
            tl = text.lower().rstrip(",. ")
            if "applicant" in tl:
                current_side = "applicants"
            elif "defendant" in tl or "respondent" in tl:
                current_side = "defendants"
            elif "intervener" in tl:
                in_interveners = False  # done with interveners block

        # "v" separator
        if _match_class(tag, "pnormal") and text.lower().strip() == "v":
            current_side = "defendants"

        # Normal paragraphs — parties, composition, AG, registrar, representatives
        if _match_class(tag, "normal"):
            # "supported by:" signals interveners coming next
            if text.lower().startswith("supported by"):
                in_interveners = True
                continue

            # Party with bold name and representative
            if _has_bold(tag):
                party_name = _get_bold_text(tag).rstrip(",. ")

                # Determine which side this party is on
                if in_interveners:
                    result["parties"]["interveners"].append(party_name)
                elif current_side == "defendants":
                    result["parties"]["defendants"].append(party_name)
                else:
                    result["parties"]["applicants"].append(party_name)

                # Extract representatives if present
                if ", by " in text or " represented by " in text:
                    reps = _parse_representatives(text)
                    if reps:
                        result["representatives"].append({
                            "party": party_name,
                            "representatives": reps,
                        })

            # Court composition
            if text.startswith("THE COURT") or text.startswith("composed of"):
                # Extract formation if not already set
                if not result["formation"]:
                    fm = re.search(r"\(([^)]+)\)", text)
                    if fm:
                        result["formation"] = fm.group(1)
                # Parse judges
                if "composed of" in text:
                    judges = _parse_composition(text)
                    if judges:
                        result["composition"] = judges

            # Advocate General
            if text.startswith("Advocate General"):
                ag_name = text.replace("Advocate General:", "").replace("Advocate General", "").strip().rstrip(",.")
                if ag_name:
                    result["advocate_general"] = ag_name

            # Registrar
            if text.startswith("Registrar"):
                reg = text.replace("Registrar:", "").strip().rstrip(",.")
                if reg:
                    result["registrar"] = reg

            # Procedural dates
            hearing_match = re.search(r"hearing on (\d{1,2}\s+\w+\s+\d{4})", text)
            if hearing_match:
                result["hearing_date"] = _parse_date(hearing_match.group(1))

            ag_date_match = re.search(
                r"(?:Opinion|Conclusions?).*sitting on (\d{1,2}\s+\w+\s+\d{4})", text
            )
            if ag_date_match:
                result["ag_opinion_date"] = _parse_date(ag_date_match.group(1))

    # Normalise non-breaking spaces throughout
    def _normalise(obj):
        if isinstance(obj, str):
            return obj.replace("\xa0", " ")
        if isinstance(obj, list):
            return [_normalise(v) for v in obj]
        if isinstance(obj, dict):
            return {k: _normalise(v) for k, v in obj.items()}
        return obj

    return _normalise(result)


def parse_all_headers(
    xhtml_dir: str,
    output_path: Optional[str] = None,
    limit: int = 0,
) -> pd.DataFrame:
    """Parse headers from all XHTML files in a directory.

    Args:
        xhtml_dir: Directory containing .xhtml files
        output_path: Path for JSONL output (optional)
        limit: Max files to process (0 = all)

    Returns:
        DataFrame with one row per file, columns from parse_judgment_header()
    """
    files = sorted(f for f in os.listdir(xhtml_dir) if f.endswith(".xhtml"))
    if limit > 0:
        files = files[:limit]

    records = []
    for i, fname in enumerate(files):
        celex = fname.replace(".xhtml", "")
        fpath = os.path.join(xhtml_dir, fname)
        try:
            with open(fpath, "r", encoding="utf-8") as f:
                xhtml = f.read()
            meta = parse_judgment_header(xhtml)
            meta["celex"] = celex
            records.append(meta)
        except Exception as e:
            logger.warning(f"Failed to parse {fname}: {e}")
            continue

        if (i + 1) % 100 == 0:
            logger.info(f"Parsed {i + 1}/{len(files)} headers")

    logger.info(f"Parsed {len(records)}/{len(files)} XHTML headers")

    if output_path and records:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            for rec in records:
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")
        logger.info(f"Saved header metadata to {output_path}")

    return pd.DataFrame(records) if records else pd.DataFrame()


# ── Derived tables ──────────────────────────────────────────────────────


def flatten_assignments(headers: List[Dict]) -> pd.DataFrame:
    """Flatten composition data into one row per judge per decision.

    Args:
        headers: List of dicts from parse_judgment_header() (must include 'celex')

    Returns:
        DataFrame with columns: celex, judge_name, role, is_rapporteur
    """
    rows = []
    for rec in headers:
        celex = rec.get("celex", "")
        for judge in rec.get("composition", []):
            rows.append({
                "celex": celex,
                "judge_name": judge["name"],
                "role": judge["role"],
                "is_rapporteur": judge["role"] == "Rapporteur",
            })
    return pd.DataFrame(rows) if rows else pd.DataFrame()


def derive_case_names(headers: List[Dict]) -> pd.DataFrame:
    """Derive case names from parsed party data (applicant v defendant).

    Args:
        headers: List of dicts from parse_judgment_header() (must include 'celex')

    Returns:
        DataFrame with columns: celex, case_name, applicants, defendants
    """
    rows = []
    for rec in headers:
        parties = rec.get("parties", {})
        applicants = parties.get("applicants", [])
        defendants = parties.get("defendants", [])
        if applicants and defendants:
            case_name = f"{applicants[0]} v {defendants[0]}"
        elif applicants:
            case_name = applicants[0]
        else:
            case_name = None
        rows.append({
            "celex": rec.get("celex", ""),
            "case_name": case_name,
            "applicants": "; ".join(applicants),
            "defendants": "; ".join(defendants),
        })
    return pd.DataFrame(rows) if rows else pd.DataFrame()


def extract_operative_part(xhtml: str) -> Optional[str]:
    """Extract the operative part of a judgment (after 'On those grounds').

    The operative part contains the Court's ruling: dismisses, annuls,
    sets aside, declares inadmissible, etc. It starts after the paragraph
    matching 'On those/these grounds' and ends at '[Signatures]'.

    Args:
        xhtml: Full XHTML content of a judgment

    Returns:
        The operative text as a single string, or None if not found
    """
    soup = BeautifulSoup(xhtml, "html.parser")
    body = soup.find("body")
    if not body:
        return None

    found = False
    parts = []
    for tag in body.descendants:
        if not isinstance(tag, Tag) or tag.name != "p":
            continue
        text = tag.get_text(strip=True)
        if not text:
            continue

        if not found:
            if re.match(r"(?i)on\s+thos[ee]\s+grounds", text):
                found = True
            continue

        # Stop at signatures
        if text.startswith("[Signatures]") or text.startswith("(*"):
            break

        # Skip paragraph numbers
        if text.rstrip(".").isdigit():
            continue

        parts.append(text)

    return " ".join(parts) if parts else None
