"""
Unified citation classifier — classifies citations using either the
Jacob taxonomy (default, 5-layer) or the legacy taxonomy (3-dimension).

Supports Gemini (default) and OpenAI-compatible providers (Ollama, vLLM, etc.).
"""
import logging
from typing import Dict, Optional

from cjeu_py.classification.prompts import (
    SYSTEM_PROMPT,
    CITATION_CLASSIFICATION_SCHEMA,
    build_classification_prompt,
    SYSTEM_PROMPT_LEGACY,
    CITATION_CLASSIFICATION_SCHEMA_LEGACY,
    build_classification_prompt_legacy,
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

# Taxonomy config
_taxonomy = "jacob"


def configure_provider(provider: str = "gemini", model: str = None,
                       api_base: str = None, api_key: str = None,
                       taxonomy: str = "jacob"):
    """Configure the LLM provider and taxonomy for classification.

    Called from main.py before the pipeline starts.

    Args:
        provider: "gemini" or "openai"
        model: Model name override
        api_base: API base URL (OpenAI provider only)
        api_key: API key (OpenAI provider only)
        taxonomy: "jacob" (default, 5-layer) or "legacy" (3-dimension)
    """
    global _provider, _model, _api_base, _api_key, _taxonomy
    _provider = provider
    _model = model
    _api_base = api_base
    _api_key = api_key
    _taxonomy = taxonomy


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


def _get_taxonomy_config():
    """Return (schema, system_prompt, prompt_builder) for the active taxonomy."""
    if _taxonomy == "legacy":
        return (
            CITATION_CLASSIFICATION_SCHEMA_LEGACY,
            SYSTEM_PROMPT_LEGACY,
            build_classification_prompt_legacy,
        )
    return (
        CITATION_CLASSIFICATION_SCHEMA,
        SYSTEM_PROMPT,
        build_classification_prompt,
    )


# ── Field names per taxonomy ──────────────────────────────────────────

_JACOB_FIELDS = [
    "polarity", "precision", "function",
    "distinguishing_type", "departing_grounds",
    "surface_coherence", "triangle_side",
    "topic", "confidence", "reasoning",
]

_LEGACY_FIELDS = [
    "precision", "use", "treatment",
    "topic", "confidence", "reasoning",
]


def classify_single_citation(item: Dict) -> Dict:
    """
    Classify a single citation item using the configured provider.

    Args:
        item: Dict with keys:
            - citing_celex, citing_date, formation, procedure_type
            - citation_string, paragraph_num, context_text

    Returns:
        Original item + classification fields + _taxonomy + _meta
    """
    client = _get_client()
    schema, system_prompt, prompt_builder = _get_taxonomy_config()

    prompt = prompt_builder(
        citing_celex=item.get("citing_celex", ""),
        citing_date=item.get("citing_date", ""),
        formation=item.get("formation", ""),
        procedure_type=item.get("procedure_type", ""),
        citation_string=item.get("citation_string", ""),
        context_text=item.get("context_text", ""),
    )

    # Prepend system prompt
    full_prompt = f"{system_prompt}\n\n{prompt}"

    if _provider == "openai":
        from cjeu_py.llm.client import classify_citation_openai
        result = classify_citation_openai(
            client=client,
            prompt=full_prompt,
            response_schema=schema,
            model=_model,
        )
    else:
        from cjeu_py.llm.client import classify_citation
        result = classify_citation(
            client=client,
            prompt=full_prompt,
            response_schema=schema,
            model=_model,
        )

    # Merge classification into original item
    output = {**item}
    fields = _JACOB_FIELDS if _taxonomy != "legacy" else _LEGACY_FIELDS
    for field in fields:
        output[field] = result.get(field)
    output["_meta"] = result.get("_meta", {})
    output["_taxonomy"] = _taxonomy

    return output
