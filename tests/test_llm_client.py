"""Tests for LLM client module — both Gemini and OpenAI paths."""
import json
import pytest
from unittest.mock import MagicMock, patch

from cjeu_py.llm.client import _build_schema_instruction
from cjeu_py.classification.prompts import (
    CITATION_CLASSIFICATION_SCHEMA,
    CITATION_CLASSIFICATION_SCHEMA_LEGACY,
)


def test_build_schema_instruction_contains_required_keys():
    """Schema instruction includes all required keys and enum values."""
    instruction = _build_schema_instruction(CITATION_CLASSIFICATION_SCHEMA)
    assert "polarity" in instruction
    assert "POSITIVE" in instruction
    assert "REQUIRED" in instruction
    assert "JSON object" in instruction


def test_build_schema_instruction_enum_values():
    """All enum values appear in the schema instruction (Jacob taxonomy)."""
    instruction = _build_schema_instruction(CITATION_CLASSIFICATION_SCHEMA)
    for val in ["POSITIVE", "NEGATIVE_DISTINGUISHING", "NEGATIVE_DEPARTING"]:
        assert val in instruction
    for val in ["VERBATIM", "GENERAL", "STRING", "SUBSTANTIVE"]:
        assert val in instruction
    for val in ["CLASSIFY", "STATE_LAW", "AFFIRM_CONCLUSION"]:
        assert val in instruction


def test_build_schema_instruction_legacy():
    """Legacy schema instruction includes original enum values."""
    instruction = _build_schema_instruction(CITATION_CLASSIFICATION_SCHEMA_LEGACY)
    for val in ["string_citation", "general_reference", "substantive_engagement"]:
        assert val in instruction
    for val in ["legal_test", "principle", "distinguish"]:
        assert val in instruction
    for val in ["follows", "extends", "neutral"]:
        assert val in instruction


def test_classify_citation_openai_success():
    """Mock OpenAI client returns valid JSON."""
    from cjeu_py.llm.client import classify_citation_openai

    mock_client = MagicMock()
    mock_response = MagicMock()
    mock_response.choices = [MagicMock()]
    mock_response.choices[0].message.content = json.dumps({
        "precision": "general_reference",
        "use": "principle",
        "treatment": "follows",
        "topic": "free movement",
        "confidence": 0.8,
        "reasoning": "Test reasoning",
    })
    mock_response.usage = MagicMock(prompt_tokens=100, completion_tokens=50)
    mock_client.chat.completions.create.return_value = mock_response

    result = classify_citation_openai(
        client=mock_client,
        prompt="Test prompt",
        response_schema=CITATION_CLASSIFICATION_SCHEMA_LEGACY,
        model="test-model",
    )
    assert result["precision"] == "general_reference"
    assert result["_meta"]["provider"] == "openai"
    assert result["_meta"]["input_tokens"] == 100
    assert result["_meta"]["error"] is None


def test_classify_citation_openai_jacob_schema():
    """Mock OpenAI client returns valid Jacob-taxonomy JSON."""
    from cjeu_py.llm.client import classify_citation_openai

    mock_client = MagicMock()
    mock_response = MagicMock()
    mock_response.choices = [MagicMock()]
    mock_response.choices[0].message.content = json.dumps({
        "polarity": "POSITIVE",
        "precision": "STRING",
        "function": "STATE_LAW",
        "distinguishing_type": "NONE",
        "departing_grounds": [],
        "surface_coherence": True,
        "triangle_side": "NONE",
        "topic": "free movement of goods",
        "confidence": 0.85,
        "reasoning": "String citation following settled case law.",
    })
    mock_response.usage = MagicMock(prompt_tokens=150, completion_tokens=80)
    mock_client.chat.completions.create.return_value = mock_response

    result = classify_citation_openai(
        client=mock_client,
        prompt="Test prompt",
        response_schema=CITATION_CLASSIFICATION_SCHEMA,
        model="test-model",
    )
    assert result["polarity"] == "POSITIVE"
    assert result["function"] == "STATE_LAW"
    assert result["departing_grounds"] == []
    assert result["surface_coherence"] is True


def test_classify_citation_openai_malformed_json_retry():
    """Retries on malformed JSON, succeeds on second attempt."""
    from cjeu_py.llm.client import classify_citation_openai

    mock_client = MagicMock()

    bad_response = MagicMock()
    bad_response.choices = [MagicMock()]
    bad_response.choices[0].message.content = "not json at all"

    good_response = MagicMock()
    good_response.choices = [MagicMock()]
    good_response.choices[0].message.content = json.dumps({
        "precision": "string_citation",
        "use": "other",
        "treatment": "neutral",
        "topic": "state aid",
        "confidence": 0.5,
        "reasoning": "Test",
    })
    good_response.usage = MagicMock(prompt_tokens=100, completion_tokens=50)

    mock_client.chat.completions.create.side_effect = [bad_response, good_response]

    result = classify_citation_openai(
        client=mock_client,
        prompt="Test prompt",
        response_schema=CITATION_CLASSIFICATION_SCHEMA_LEGACY,
    )
    assert result["precision"] == "string_citation"
    assert mock_client.chat.completions.create.call_count == 2


def test_classify_citation_openai_strips_code_fences():
    """Strips markdown code fences from model output."""
    from cjeu_py.llm.client import classify_citation_openai

    mock_client = MagicMock()
    mock_response = MagicMock()
    mock_response.choices = [MagicMock()]
    inner = json.dumps({
        "precision": "substantive_engagement",
        "use": "interpretation",
        "treatment": "extends",
        "topic": "data protection",
        "confidence": 0.9,
        "reasoning": "Deep analysis",
    })
    mock_response.choices[0].message.content = f"```json\n{inner}\n```"
    mock_response.usage = MagicMock(prompt_tokens=80, completion_tokens=40)
    mock_client.chat.completions.create.return_value = mock_response

    result = classify_citation_openai(
        client=mock_client,
        prompt="Test",
        response_schema=CITATION_CLASSIFICATION_SCHEMA_LEGACY,
    )
    assert result["precision"] == "substantive_engagement"


def test_configure_provider_defaults_to_gemini():
    """configure_provider defaults leave Gemini path unchanged."""
    from cjeu_py.classification.classifier import configure_provider, _provider
    configure_provider()
    from cjeu_py.classification import classifier
    assert classifier._provider == "gemini"


def test_configure_provider_openai():
    """configure_provider switches to openai."""
    from cjeu_py.classification.classifier import configure_provider
    from cjeu_py.classification import classifier
    configure_provider(provider="openai", model="gemma2", api_base="http://test:11434/v1")
    assert classifier._provider == "openai"
    assert classifier._model == "gemma2"
    assert classifier._api_base == "http://test:11434/v1"
    # Reset to default
    configure_provider()
