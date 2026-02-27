"""
Regex-based citation extraction from CJEU judgment texts.

Identifies case references in various formats: Case C-xxx/xx, ECLI,
ECR, Joined Cases, and named references.
"""
import re
import logging
from typing import List, Dict

logger = logging.getLogger(__name__)

# ── Citation patterns ──────────────────────────────────────────────────────
# Order matters: more specific patterns first to avoid partial matches

CITATION_PATTERNS = [
    # ECLI format: EU:C:2016:555
    (r'ECLI:EU:[CTFP]:\d{4}:\d+', 'ecli'),
    
    # Joined Cases (modern, with C- prefix)
    (r'[Jj]oined\s+[Cc]ases\s+C[-‑–]\d+/\d+(?:\s*(?:,|and|et)\s*C[-‑–]\d+/\d+)+', 'joined_modern'),
    
    # Joined Cases (pre-1989, no prefix)
    (r'[Jj]oined\s+[Cc]ases\s+\d+/\d+(?:\s*(?:,|and|et)\s*\d+/\d+)+', 'joined_old'),
    
    # Modern ECJ cases: Case C-xxx/xx
    (r'[Cc]ase\s+C[-‑–]\d+/\d+', 'case_cj'),
    
    # General Court: Case T-xxx/xx
    (r'[Cc]ase\s+T[-‑–]\d+/\d+', 'case_gc'),
    
    # Civil Service Tribunal: Case F-xxx/xx
    (r'[Cc]ase\s+F[-‑–]\d+/\d+', 'case_cst'),
    
    # Pre-1989 cases (no prefix): Case xxx/xx
    (r'[Cc]ase\s+\d+/\d+', 'case_old'),
    
    # ECR references: [yyyy] ECR I-xxxx or ECR I-xxxx
    (r'\[\d{4}\]\s*ECR\s+(?:I[-‑–])?\d+', 'ecr_bracketed'),
    (r'ECR\s+(?:I[-‑–])?\d+', 'ecr'),
    
    # Paragraph pinpoints: paragraph(s) xx (attached to previous citation)
    (r'paragraph(?:s)?\s+\d+(?:\s*(?:to|and|,)\s*\d+)*', 'para_pinpoint'),
]

# Compiled patterns
_COMPILED_PATTERNS = [(re.compile(p, re.UNICODE), name) for p, name in CITATION_PATTERNS]


def extract_citations_from_text(text: str) -> List[Dict]:
    """
    Extract all case citations from a text string.
    
    Returns:
        List of dicts: [{citation_string, pattern_type, span_start, span_end}, ...]
    """
    citations = []
    seen_spans = set()  # avoid overlapping matches
    
    for pattern, pattern_type in _COMPILED_PATTERNS:
        for match in pattern.finditer(text):
            start, end = match.span()
            
            # Skip if this span overlaps with an already-found citation
            if any(s <= start < e or s < end <= e for s, e in seen_spans):
                continue
            
            citations.append({
                "citation_string": match.group(),
                "pattern_type": pattern_type,
                "span_start": start,
                "span_end": end,
            })
            seen_spans.add((start, end))
    
    # Sort by position in text
    citations.sort(key=lambda c: c["span_start"])
    return citations


def extract_citations_from_paragraphs(
    paragraphs: List[Dict],
    citing_celex: str = None,
) -> List[Dict]:
    """
    Extract citations from a list of numbered paragraphs.
    
    Args:
        paragraphs: List of {num: int, text: str}
        citing_celex: CELEX of the document being analysed
    
    Returns:
        List of dicts with: citing_celex, paragraph_num, citation_string,
        pattern_type, span_start, span_end
    """
    all_citations = []
    
    for para in paragraphs:
        para_citations = extract_citations_from_text(para["text"])
        for cit in para_citations:
            cit["citing_celex"] = citing_celex
            cit["paragraph_num"] = para["num"]
            all_citations.append(cit)
    
    logger.debug(f"Extracted {len(all_citations)} citations from {len(paragraphs)} paragraphs")
    return all_citations


def normalise_case_reference(citation_string: str) -> str:
    """
    Normalise a case reference string for matching.
    E.g. "Case C‑6/15" → "C-6/15"
    """
    # Replace various dash types
    s = citation_string.replace("‑", "-").replace("–", "-").replace("—", "-")
    # Remove "Case " prefix
    s = re.sub(r'^[Cc]ase\s+', '', s)
    # Remove "Joined Cases " prefix
    s = re.sub(r'^[Jj]oined\s+[Cc]ases\s+', '', s)
    return s.strip()
