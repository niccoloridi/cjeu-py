"""
Tests for the regex citation extractor.
"""
import pytest
from cjeu_py.citation_extraction.regex_extractor import (
    extract_citations_from_text,
    normalise_case_reference,
)


class TestExtractCitations:
    """Test citation extraction from text."""

    def test_case_cj_modern(self):
        """Modern CJ case: Case C-xxx/xx"""
        text = "As the Court held in Case C-6/15, the contracting authority is not required..."
        cits = extract_citations_from_text(text)
        assert len(cits) >= 1
        assert any("C-6/15" in c["citation_string"] for c in cits)
        assert any(c["pattern_type"] == "case_cj" for c in cits)

    def test_case_gc(self):
        """General Court case: Case T-xxx/xx"""
        text = "The General Court in Case T-112/98 dismissed the application."
        cits = extract_citations_from_text(text)
        assert len(cits) >= 1
        assert any("T-112/98" in c["citation_string"] for c in cits)

    def test_case_old(self):
        """Pre-1989 case: Case xxx/xx"""
        text = "In Case 26/62 (Van Gend en Loos), the Court established..."
        cits = extract_citations_from_text(text)
        assert len(cits) >= 1
        assert any("26/62" in c["citation_string"] for c in cits)

    def test_ecli(self):
        """ECLI format citation."""
        text = "See ECLI:EU:C:2016:555 for the relevant holding."
        cits = extract_citations_from_text(text)
        assert len(cits) >= 1
        assert any(c["pattern_type"] == "ecli" for c in cits)
        assert any("EU:C:2016:555" in c["citation_string"] for c in cits)

    def test_joined_cases(self):
        """Joined Cases with multiple case numbers."""
        text = "In Joined Cases C-402/05 and C-415/05 (Kadi), the Court held..."
        cits = extract_citations_from_text(text)
        assert len(cits) >= 1
        assert any("joined" in c["pattern_type"] for c in cits)

    def test_ecr_reference(self):
        """European Court Reports reference."""
        text = "See [2004] ECR I-5039 for the full text."
        cits = extract_citations_from_text(text)
        assert len(cits) >= 1
        assert any("ecr" in c["pattern_type"] for c in cits)

    def test_multiple_citations(self):
        """Text with multiple citations."""
        text = (
            "The Court held in Case C-6/15 and Case C-252/10 that "
            "the principle established in Case 26/62 continues to apply."
        )
        cits = extract_citations_from_text(text)
        assert len(cits) >= 3

    def test_no_citations(self):
        """Text with no citations."""
        text = "The applicant submitted that the decision was unlawful."
        cits = extract_citations_from_text(text)
        assert len(cits) == 0

    def test_paragraph_pinpoint(self):
        """Citation with paragraph reference."""
        text = "Case C-6/15, paragraphs 24 to 28"
        cits = extract_citations_from_text(text)
        assert len(cits) >= 1  # At least the case reference

    def test_unicode_dashes(self):
        """Handles various dash types in case references."""
        text = "Case C‑6/15 and Case C–252/10"  # en-dash and non-breaking hyphen
        cits = extract_citations_from_text(text)
        assert len(cits) >= 2


class TestNormalise:
    """Test reference normalisation."""

    def test_normalise_modern_case(self):
        assert normalise_case_reference("Case C-6/15") == "C-6/15"

    def test_normalise_unicode_dash(self):
        assert normalise_case_reference("Case C‑6/15") == "C-6/15"

    def test_normalise_joined(self):
        result = normalise_case_reference("Joined Cases C-402/05")
        assert result == "C-402/05"

    def test_normalise_old_case(self):
        assert normalise_case_reference("Case 26/62") == "26/62"
