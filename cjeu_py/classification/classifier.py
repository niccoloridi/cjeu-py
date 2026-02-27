"""
Unified citation classifier — classifies precision, use, treatment,
and topic in a single Gemini call per citation.
"""
import logging
from typing import Dict

from cjeu_py.llm.client import get_gemini_client, classify_citation
from cjeu_py.classification.prompts import (
    SYSTEM_PROMPT,
    CITATION_CLASSIFICATION_SCHEMA,
    build_classification_prompt,
)

logger = logging.getLogger(__name__)

# Module-level client (lazy init)
_client = None


def _get_client():
    global _client
    if _client is None:
        _client = get_gemini_client()
    return _client


def classify_single_citation(item: Dict) -> Dict:
    """
    Classify a single citation item.
    
    Args:
        item: Dict with keys:
            - citing_celex, citing_date, formation, procedure_type
            - citation_string, paragraph_num, context_text
    
    Returns:
        Original item + classification fields + _meta
    """
    client = _get_client()
    
    prompt = build_classification_prompt(
        citing_celex=item.get("citing_celex", ""),
        citing_date=item.get("citing_date", ""),
        formation=item.get("formation", ""),
        procedure_type=item.get("procedure_type", ""),
        citation_string=item.get("citation_string", ""),
        context_text=item.get("context_text", ""),
    )
    
    # Prepend system prompt
    full_prompt = f"{SYSTEM_PROMPT}\n\n{prompt}"
    
    result = classify_citation(
        client=client,
        prompt=full_prompt,
        response_schema=CITATION_CLASSIFICATION_SCHEMA,
    )
    
    # Merge classification into original item
    output = {**item}
    output["precision"] = result.get("precision")
    output["use"] = result.get("use")
    output["treatment"] = result.get("treatment")
    output["topic"] = result.get("topic")
    output["confidence"] = result.get("confidence")
    output["reasoning"] = result.get("reasoning")
    output["_meta"] = result.get("_meta", {})
    
    return output
