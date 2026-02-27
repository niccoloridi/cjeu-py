"""
Citation context window extraction.

For each citation found in a judgment, extracts the paragraph containing
the citation plus the previous and next paragraphs as context for
downstream LLM classification.
"""
import logging
from typing import List, Dict

logger = logging.getLogger(__name__)


def extract_context_windows(
    paragraphs: List[Dict],
    citations: List[Dict],
    window_size: int = 1,
) -> List[Dict]:
    """
    Enrich each citation with surrounding paragraph context.
    
    Args:
        paragraphs: List of {num: int, text: str} for the entire judgment
        citations: List of citation dicts from regex_extractor
        window_size: Number of paragraphs before/after to include (default 1)
    
    Returns:
        Enriched citation dicts with added fields:
        - context_text: concatenation of surrounding paragraphs
        - context_paragraphs: list of paragraph nums included
        - citing_paragraph_text: the specific paragraph text
    """
    # Build paragraph index by number
    para_by_num = {p["num"]: p["text"] for p in paragraphs}
    para_nums = sorted(para_by_num.keys())
    
    enriched = []
    
    for cit in citations:
        para_num = cit.get("paragraph_num")
        if para_num is None:
            continue
        
        # Find surrounding paragraph numbers
        try:
            idx = para_nums.index(para_num)
        except ValueError:
            # Paragraph not in index, just use what we have
            cit["context_text"] = para_by_num.get(para_num, "")
            cit["context_paragraphs"] = [para_num]
            cit["citing_paragraph_text"] = para_by_num.get(para_num, "")
            enriched.append(cit)
            continue
        
        # Collect context window
        start_idx = max(0, idx - window_size)
        end_idx = min(len(para_nums) - 1, idx + window_size)
        
        context_nums = para_nums[start_idx:end_idx + 1]
        context_parts = []
        for n in context_nums:
            prefix = ">>>" if n == para_num else "   "
            context_parts.append(f"[{n}] {prefix} {para_by_num[n]}")
        
        cit["context_text"] = "\n\n".join(context_parts)
        cit["context_paragraphs"] = context_nums
        cit["citing_paragraph_text"] = para_by_num.get(para_num, "")
        
        enriched.append(cit)
    
    logger.debug(f"Enriched {len(enriched)} citations with context windows (±{window_size})")
    return enriched
