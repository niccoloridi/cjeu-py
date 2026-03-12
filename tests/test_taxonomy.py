"""Tests for the classification taxonomy schemas and classifier integration."""
import pytest
from unittest.mock import MagicMock, patch

from cjeu_py.classification.prompts import (
    CITATION_CLASSIFICATION_SCHEMA,
    CITATION_CLASSIFICATION_SCHEMA_LEGACY,
    SYSTEM_PROMPT,
    SYSTEM_PROMPT_LEGACY,
    build_classification_prompt,
    build_classification_prompt_legacy,
)
from cjeu_py.classification.classifier import (
    configure_provider,
    _get_taxonomy_config,
    _JACOB_FIELDS,
    _LEGACY_FIELDS,
    classify_single_citation,
)


# ── Schema structure tests ─────────────────────────────────────────────


class TestJacobSchema:
    """Verify the Jacob taxonomy JSON schema is well-formed."""

    def test_required_fields(self):
        required = CITATION_CLASSIFICATION_SCHEMA["required"]
        expected = [
            "polarity", "precision", "function",
            "distinguishing_type", "departing_grounds",
            "surface_coherence", "triangle_side",
            "topic", "confidence", "reasoning",
        ]
        assert set(required) == set(expected)

    def test_polarity_enum(self):
        enum = CITATION_CLASSIFICATION_SCHEMA["properties"]["polarity"]["enum"]
        assert "POSITIVE" in enum
        assert "NEGATIVE_DISTINGUISHING" in enum
        assert "NEGATIVE_DEPARTING" in enum
        assert len(enum) == 3

    def test_precision_enum(self):
        enum = CITATION_CLASSIFICATION_SCHEMA["properties"]["precision"]["enum"]
        assert set(enum) == {"VERBATIM", "GENERAL", "STRING", "SUBSTANTIVE"}

    def test_function_enum(self):
        enum = CITATION_CLASSIFICATION_SCHEMA["properties"]["function"]["enum"]
        assert len(enum) == 9
        assert "CLASSIFY" in enum
        assert "AFFIRM_CONCLUSION" in enum

    def test_distinguishing_type_includes_none(self):
        enum = CITATION_CLASSIFICATION_SCHEMA["properties"]["distinguishing_type"]["enum"]
        assert "NONE" in enum
        assert "DISAPPLICATION" in enum
        assert "MANIPULATION" in enum
        assert "OBITERING" in enum

    def test_departing_grounds_is_array(self):
        prop = CITATION_CLASSIFICATION_SCHEMA["properties"]["departing_grounds"]
        assert prop["type"] == "array"
        items_enum = prop["items"]["enum"]
        assert "INCORRECT" in items_enum
        assert "CHANGED_PREMISES" in items_enum
        assert len(items_enum) == 6

    def test_surface_coherence_is_boolean(self):
        prop = CITATION_CLASSIFICATION_SCHEMA["properties"]["surface_coherence"]
        assert prop["type"] == "boolean"

    def test_triangle_side_enum(self):
        enum = CITATION_CLASSIFICATION_SCHEMA["properties"]["triangle_side"]["enum"]
        assert set(enum) == {"ALPHA", "BETA", "GAMMA", "NONE"}


class TestLegacySchema:
    """Verify the legacy taxonomy schema is preserved."""

    def test_required_fields(self):
        required = CITATION_CLASSIFICATION_SCHEMA_LEGACY["required"]
        assert set(required) == {"precision", "use", "treatment", "topic", "confidence", "reasoning"}

    def test_precision_enum(self):
        enum = CITATION_CLASSIFICATION_SCHEMA_LEGACY["properties"]["precision"]["enum"]
        assert set(enum) == {"string_citation", "general_reference", "substantive_engagement"}

    def test_use_enum(self):
        enum = CITATION_CLASSIFICATION_SCHEMA_LEGACY["properties"]["use"]["enum"]
        assert "legal_test" in enum
        assert "distinguish" in enum

    def test_treatment_enum(self):
        enum = CITATION_CLASSIFICATION_SCHEMA_LEGACY["properties"]["treatment"]["enum"]
        assert "follows" in enum
        assert "departs_explicit" in enum


# ── Prompt builder tests ────────────────────────────────────────────────


class TestPromptBuilders:
    """Verify prompt builders produce well-formed prompts."""

    SAMPLE_KWARGS = dict(
        citing_celex="62019CJ0001",
        citing_date="2020-01-15",
        formation="Grand Chamber",
        procedure_type="Reference for a preliminary ruling",
        citation_string="Case C-6/15",
        context_text=">>> The Court held in Case C-6/15 that...",
    )

    def test_jacob_prompt_contains_metadata(self):
        prompt = build_classification_prompt(**self.SAMPLE_KWARGS)
        assert "62019CJ0001" in prompt
        assert "Grand Chamber" in prompt
        assert "Case C-6/15" in prompt

    def test_legacy_prompt_contains_metadata(self):
        prompt = build_classification_prompt_legacy(**self.SAMPLE_KWARGS)
        assert "62019CJ0001" in prompt
        assert "Grand Chamber" in prompt

    def test_jacob_prompt_mentions_layers(self):
        prompt = build_classification_prompt(**self.SAMPLE_KWARGS)
        assert "polarity" in prompt
        assert "distinguishing_type" in prompt

    def test_legacy_prompt_mentions_dimensions(self):
        prompt = build_classification_prompt_legacy(**self.SAMPLE_KWARGS)
        assert "precision" in prompt
        assert "treatment" in prompt

    def test_system_prompt_jacob_mentions_jacob(self):
        assert "Marc Jacob" in SYSTEM_PROMPT

    def test_system_prompt_legacy_mentions_jacob(self):
        assert "Marc Jacob" in SYSTEM_PROMPT_LEGACY

    def test_missing_metadata_defaults(self):
        prompt = build_classification_prompt(
            citing_celex="test",
            citing_date=None,
            formation=None,
            procedure_type=None,
            citation_string="C-1/00",
            context_text="text",
        )
        assert "unknown" in prompt


# ── Classifier config tests ─────────────────────────────────────────────


class TestClassifierConfig:
    """Test taxonomy switching in the classifier."""

    def test_configure_jacob_taxonomy(self):
        configure_provider(taxonomy="jacob")
        schema, sys_prompt, builder = _get_taxonomy_config()
        assert schema is CITATION_CLASSIFICATION_SCHEMA
        assert sys_prompt is SYSTEM_PROMPT
        assert builder is build_classification_prompt

    def test_configure_legacy_taxonomy(self):
        configure_provider(taxonomy="legacy")
        schema, sys_prompt, builder = _get_taxonomy_config()
        assert schema is CITATION_CLASSIFICATION_SCHEMA_LEGACY
        assert sys_prompt is SYSTEM_PROMPT_LEGACY
        assert builder is build_classification_prompt_legacy
        # Reset
        configure_provider(taxonomy="jacob")

    def test_jacob_fields(self):
        assert "polarity" in _JACOB_FIELDS
        assert "function" in _JACOB_FIELDS
        assert "distinguishing_type" in _JACOB_FIELDS
        assert "departing_grounds" in _JACOB_FIELDS
        assert "surface_coherence" in _JACOB_FIELDS
        assert "triangle_side" in _JACOB_FIELDS

    def test_legacy_fields(self):
        assert "precision" in _LEGACY_FIELDS
        assert "use" in _LEGACY_FIELDS
        assert "treatment" in _LEGACY_FIELDS
        assert "polarity" not in _LEGACY_FIELDS


# ── classify_single_citation integration tests ──────────────────────────


class TestClassifySingleCitation:
    """Test classify_single_citation with mocked LLM responses."""

    SAMPLE_ITEM = {
        "citing_celex": "62019CJ0001",
        "citing_date": "2020-01-15",
        "formation": "Grand Chamber",
        "procedure_type": "Reference for a preliminary ruling",
        "citation_string": "Case C-6/15",
        "paragraph_num": 42,
        "context_text": ">>> The Court held in Case C-6/15 that...",
    }

    @patch("cjeu_py.classification.classifier._get_client")
    @patch("cjeu_py.llm.client.classify_citation")
    def test_jacob_positive_citation(self, mock_classify, mock_client):
        """Positive citation populates all Jacob fields correctly."""
        configure_provider(taxonomy="jacob")
        mock_client.return_value = MagicMock()
        mock_classify.return_value = {
            "polarity": "POSITIVE",
            "precision": "STRING",
            "function": "STATE_LAW",
            "distinguishing_type": "NONE",
            "departing_grounds": [],
            "surface_coherence": True,
            "triangle_side": "NONE",
            "topic": "competition law",
            "confidence": 0.9,
            "reasoning": "String citation of settled case law.",
            "_meta": {"provider": "gemini"},
        }

        result = classify_single_citation(self.SAMPLE_ITEM)

        assert result["polarity"] == "POSITIVE"
        assert result["function"] == "STATE_LAW"
        assert result["departing_grounds"] == []
        assert result["surface_coherence"] is True
        assert result["_taxonomy"] == "jacob"
        assert result["citing_celex"] == "62019CJ0001"

    @patch("cjeu_py.classification.classifier._get_client")
    @patch("cjeu_py.llm.client.classify_citation")
    def test_jacob_negative_distinguishing(self, mock_classify, mock_client):
        """Negative distinguishing populates Layer 4a fields."""
        configure_provider(taxonomy="jacob")
        mock_client.return_value = MagicMock()
        mock_classify.return_value = {
            "polarity": "NEGATIVE_DISTINGUISHING",
            "precision": "SUBSTANTIVE",
            "function": "INTERPRET_CASE",
            "distinguishing_type": "MANIPULATION",
            "departing_grounds": [],
            "surface_coherence": False,
            "triangle_side": "BETA",
            "topic": "state aid",
            "confidence": 0.75,
            "reasoning": "Court narrows the precedent's ratio.",
            "_meta": {"provider": "gemini"},
        }

        result = classify_single_citation(self.SAMPLE_ITEM)

        assert result["polarity"] == "NEGATIVE_DISTINGUISHING"
        assert result["distinguishing_type"] == "MANIPULATION"
        assert result["triangle_side"] == "BETA"
        assert result["_taxonomy"] == "jacob"

    @patch("cjeu_py.classification.classifier._get_client")
    @patch("cjeu_py.llm.client.classify_citation")
    def test_jacob_negative_departing(self, mock_classify, mock_client):
        """Negative departing populates Layer 4b with multiple grounds."""
        configure_provider(taxonomy="jacob")
        mock_client.return_value = MagicMock()
        mock_classify.return_value = {
            "polarity": "NEGATIVE_DEPARTING",
            "precision": "SUBSTANTIVE",
            "function": "INTERPRET_LAW",
            "distinguishing_type": "NONE",
            "departing_grounds": ["INCORRECT", "INCOMPATIBLE_CASES"],
            "surface_coherence": False,
            "triangle_side": "GAMMA",
            "topic": "citizenship",
            "confidence": 0.8,
            "reasoning": "Multiple grounds for overruling.",
            "_meta": {"provider": "gemini"},
        }

        result = classify_single_citation(self.SAMPLE_ITEM)

        assert result["polarity"] == "NEGATIVE_DEPARTING"
        assert result["departing_grounds"] == ["INCORRECT", "INCOMPATIBLE_CASES"]
        assert result["triangle_side"] == "GAMMA"

    @patch("cjeu_py.classification.classifier._get_client")
    @patch("cjeu_py.llm.client.classify_citation")
    def test_legacy_classification(self, mock_classify, mock_client):
        """Legacy taxonomy returns precision/use/treatment fields."""
        configure_provider(taxonomy="legacy")
        mock_client.return_value = MagicMock()
        mock_classify.return_value = {
            "precision": "string_citation",
            "use": "legal_test",
            "treatment": "follows",
            "topic": "free movement",
            "confidence": 0.85,
            "reasoning": "Standard string citation.",
            "_meta": {"provider": "gemini"},
        }

        result = classify_single_citation(self.SAMPLE_ITEM)

        assert result["precision"] == "string_citation"
        assert result["use"] == "legal_test"
        assert result["treatment"] == "follows"
        assert result["_taxonomy"] == "legacy"
        assert "polarity" not in result
        # Reset
        configure_provider(taxonomy="jacob")
