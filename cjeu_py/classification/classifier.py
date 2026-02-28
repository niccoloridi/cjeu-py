"""
Unified citation classifier — classifies precision, use, treatment,
and topic in a single LLM call per citation.

Supports Gemini (default) and OpenAI-compatible providers (Ollama, vLLM, etc.).
"""
import logging
from typing import Dict, Optional

from cjeu_py.classification.prompts import (
    SYSTEM_PROMPT,
    CITATION_CLASSIFICATION_SCHEMA,
    build_classification_prompt,
)

logger = logging.getLogger(__name__)

# Module-level clients (lazy init)
_gemini_client = None
_openai_client = None

# Provider config (set via configure_provider before pipeline runs)
_provider = "gemini"
_model = None
_api_base = None
_api_key = None


def configure_provider(provider: str = "gemini", model: str = None,
                       api_base: str = None, api_key: str = None):
    """Configure the LLM provider for classification.

    Called from main.py before the pipeline starts.
    """
    global _provider, _model, _api_base, _api_key
    _provider = provider
    _model = model
    _api_base = api_base
    _api_key = api_key


def _get_client():
    global _gemini_client, _openai_client
    if _provider == "openai":
        if _openai_client is None:
            from cjeu_py.llm.client import get_openai_client
            _openai_client = get_openai_client(api_base=_api_base, api_key=_api_key)
        return _openai_client
    else:
        if _gemini_client is None:
            from cjeu_py.llm.client import get_gemini_client
            _gemini_client = get_gemini_client()
        return _gemini_client


def classify_single_citation(item: Dict) -> Dict:
    """
    Classify a single citation item using the configured provider.

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

    if _provider == "openai":
        from cjeu_py.llm.client import classify_citation_openai
        result = classify_citation_openai(
            client=client,
            prompt=full_prompt,
            response_schema=CITATION_CLASSIFICATION_SCHEMA,
            model=_model,
        )
    else:
        from cjeu_py.llm.client import classify_citation
        result = classify_citation(
            client=client,
            prompt=full_prompt,
            response_schema=CITATION_CLASSIFICATION_SCHEMA,
            model=_model,
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
