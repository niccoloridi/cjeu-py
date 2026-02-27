"""
Text processing utilities for CJEU judgment XHTML from the CELLAR REST API.
Paragraph extraction, cleaning, and normalisation.

CELLAR returns well-structured XHTML with CSS classes like coj-normal,
coj-sum-title-1, etc. Numbered paragraphs sit inside <table> elements:
    <table><tr><td>1</td><td><p class="coj-normal">Text...</p></td></tr></table>

Footnotes sit after an <hr class="[coj-]note"/> separator as
<p class="[coj-]note"> elements.  Inline references are
<span class="[coj-]note"> with <a> anchors inside main-text paragraphs.

Italic formatting (<span class="[coj-]italic">) is preserved as markdown
*text* markers in the extracted text.  This allows downstream consumers to
identify case names, Latin terms, and other emphasised text that may signal
informal case-law references (e.g. 'the *Bosman* principle').
"""
import re
import logging
import warnings
from typing import List, Dict, Optional, Tuple
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning

warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

logger = logging.getLogger(__name__)


def _preserve_formatting(html: str) -> str:
    """Convert CELLAR italic spans to markdown *text* markers in raw HTML.

    Handles both old-format ``<span class="italic">`` and new-format
    ``<span class="coj-italic">``.  Called before BeautifulSoup parsing
    so that ``get_text()`` preserves the markers in the plain-text output.
    """
    return re.sub(
        r'<span\s+class="(?:coj-)?italic"[^>]*>(.*?)</span>',
        r'*\1*',
        html,
        flags=re.DOTALL,
    )


def clean_html_text(html: str) -> str:
    """Strip HTML tags and normalise whitespace."""
    soup = BeautifulSoup(html, "lxml")
    text = soup.get_text(separator=" ")
    text = re.sub(r"\s+", " ", text).strip()
    return text


def extract_paragraphs_from_html(html: str) -> List[Dict]:
    """
    Extract numbered paragraphs from a CELLAR XHTML judgment.

    CELLAR uses two-column <table> elements for numbered paragraphs:
      col 1 = paragraph number (bare integer in a <td>)
      col 2 = paragraph body text (may contain nested <p>, <table>, etc.)

    Also handles the EUR-Lex frontend format where paragraphs start with
    a number followed by a space inside <p> or <td> elements.

    Returns a list of dicts: [{"num": int, "text": str}, ...]
    """
    html = _preserve_formatting(html)
    soup = BeautifulSoup(html, "lxml")
    paragraphs = []
    seen_nums = set()

    # Strategy 1 (CELLAR XHTML): numbered paragraphs in <table> elements
    # Each numbered paragraph is a top-level table with:
    #   first <td> = bare integer, second <td> = paragraph body
    for table in soup.find_all("table"):
        rows = table.find_all("tr", recursive=False)
        if not rows:
            # Some tables have <td> directly without <tr>
            rows = [table]

        for row in rows:
            tds = row.find_all("td", recursive=False)
            if len(tds) < 2:
                continue

            num_text = tds[0].get_text().strip().rstrip(".")
            if not re.match(r"^\d{1,4}$", num_text):
                continue

            num = int(num_text)
            if num in seen_nums:
                continue

            body = tds[1].get_text(separator=" ").strip()
            body = re.sub(r"\s+", " ", body)
            if body:
                paragraphs.append({"num": num, "text": body})
                seen_nums.add(num)

    # Sort by paragraph number (tables may appear out of order)
    paragraphs.sort(key=lambda p: p["num"])

    if paragraphs:
        return paragraphs

    # Strategy 2 (EUR-Lex frontend HTML): number at start of <p>/<td> text
    all_paras = soup.find_all(["p", "td"])
    current_num = 0
    para_pattern = re.compile(r"^\s*(\d+)\s+")

    for elem in all_paras:
        text = elem.get_text(separator=" ").strip()
        if not text or len(text) < 5:
            continue

        match = para_pattern.match(text)
        if match:
            num = int(match.group(1))
            if num > 0 and (num == current_num + 1 or num <= current_num + 5):
                current_num = num
                text_body = text[match.end():].strip()
                if text_body:
                    paragraphs.append({"num": num, "text": text_body})

    if paragraphs:
        return paragraphs

    # Strategy 3 (Playwright plain-text dump): number on its own line
    body = soup.find("body") or soup
    full_text = body.get_text(separator="\n")
    lines = [l.strip() for l in full_text.split("\n") if l.strip()]

    current_num = None
    current_parts = []

    for line in lines:
        num_match = re.match(r"^(\d{1,3})\s*$", line)
        if num_match and int(num_match.group(1)) < 500:
            if current_num is not None and current_parts:
                paragraphs.append({
                    "num": current_num,
                    "text": " ".join(current_parts),
                })
            current_num = int(num_match.group(1))
            current_parts = []
        elif current_num is not None:
            current_parts.append(line)

    if current_num is not None and current_parts:
        paragraphs.append({
            "num": current_num,
            "text": " ".join(current_parts),
        })

    # Strategy 4 (last resort): split on double newlines
    if not paragraphs:
        for i, line in enumerate(lines, 1):
            if len(line) > 20:
                paragraphs.append({"num": i, "text": line})

    return paragraphs


def extract_paragraphs_with_footnotes(html: str) -> Dict:
    """
    Extract numbered paragraphs AND footnotes from CELLAR XHTML.

    CELLAR documents have two parts separated by ``<hr class="[coj-]note"/>``:
      * **Main text**: numbered paragraphs in ``<table>`` two-column layout.
      * **Footnotes**: ``<p class="[coj-]note">`` elements after the ``<hr>``.

    Inline footnote references (``<span class="[coj-]note">``) inside
    main-text paragraphs are detected so that each footnote can be linked
    back to the paragraph(s) it annotates.

    Handles both the older class names (``note``, ``count``) and the
    newer ``coj-`` prefixed variants (``coj-note``, ``coj-count``).

    Returns::

        {
            "main_paragraphs": [
                {"num": 1, "text": "...", "location": "main_text",
                 "footnote_refs": [2, 3]},
                ...
            ],
            "footnotes": [
                {"num": 1001, "text": "...", "location": "footnote",
                 "footnote_num": 1, "parent_paras": [5]},
                ...
            ],
        }

    Footnote ``num`` values are offset by 1000 to avoid collisions with
    main-text paragraph numbers.
    """
    html = _preserve_formatting(html)
    soup = BeautifulSoup(html, "lxml")

    # Detect class prefix (old vs new format)
    note_cls = "coj-note" if soup.find(class_="coj-note") else "note"

    # Locate footnote separator
    hr_sep = soup.find("hr", class_=note_cls)

    # ── Main-text paragraphs ──────────────────────────────────────
    main_paragraphs: List[Dict] = []
    seen_nums: set = set()

    for table in soup.find_all("table"):
        # Skip tables that appear after the footnote separator
        if hr_sep and hr_sep in table.previous_elements:
            continue

        rows = table.find_all("tr", recursive=False)
        if not rows:
            rows = [table]

        for row in rows:
            tds = row.find_all("td", recursive=False)
            if len(tds) < 2:
                continue

            num_text = tds[0].get_text().strip().rstrip(".")
            if not re.match(r"^\d{1,4}$", num_text):
                continue

            num = int(num_text)
            if num in seen_nums:
                continue

            body = tds[1].get_text(separator=" ").strip()
            body = re.sub(r"\s+", " ", body)

            # Find inline footnote references in this paragraph
            fn_refs: List[int] = []
            for span in tds[1].find_all("span", class_=note_cls):
                a_tag = span.find("a")
                if a_tag:
                    fn_text = a_tag.get_text().strip()
                    if fn_text.isdigit():
                        fn_refs.append(int(fn_text))

            if body:
                main_paragraphs.append({
                    "num": num,
                    "text": body,
                    "location": "main_text",
                    "footnote_refs": fn_refs,
                })
                seen_nums.add(num)

    main_paragraphs.sort(key=lambda p: p["num"])

    # ── Footnote-to-paragraph mapping ─────────────────────────────
    fn_to_paras: Dict[int, List[int]] = {}
    for para in main_paragraphs:
        for fn_num in para.get("footnote_refs", []):
            fn_to_paras.setdefault(fn_num, []).append(para["num"])

    # ── Footnote bodies ───────────────────────────────────────────
    footnotes: List[Dict] = []
    fn_elements = (
        hr_sep.find_all_next("p", class_=note_cls) if hr_sep
        else [p for p in soup.find_all("p", class_=note_cls)
              if not p.find_parent("table")]
    )

    for p_elem in fn_elements:
        fn_num = None
        span = p_elem.find("span", class_=note_cls)
        if span:
            a_tag = span.find("a")
            if a_tag:
                fn_text = a_tag.get_text().strip()
                if fn_text.isdigit():
                    fn_num = int(fn_text)

        text = p_elem.get_text(separator=" ").strip()
        text = re.sub(r"\s+", " ", text)
        # Strip leading "(N)" or "(*N)" number prefix
        text = re.sub(r"^\(\s*\*?\d+\s*\)\s*", "", text)

        if text and fn_num is not None:
            footnotes.append({
                "num": 1000 + fn_num,  # offset to avoid collisions
                "text": text,
                "location": "footnote",
                "footnote_num": fn_num,
                "parent_paras": fn_to_paras.get(fn_num, []),
            })

    logger.debug(
        f"Extracted {len(main_paragraphs)} main paragraphs, "
        f"{len(footnotes)} footnotes"
    )
    return {"main_paragraphs": main_paragraphs, "footnotes": footnotes}


def get_full_text(paragraphs: List[Dict]) -> str:
    """Join all paragraphs into a single text string."""
    return "\n\n".join(f"{p['num']}. {p['text']}" for p in paragraphs)


def extract_judgment_body(html: str) -> Optional[str]:
    """
    Extract the main judgment body from CELLAR XHTML,
    excluding header metadata, procedural boilerplate, etc.
    """
    soup = BeautifulSoup(html, "lxml")

    body = soup.find("body")
    if not body:
        return None

    return body.get_text(separator="\n")
