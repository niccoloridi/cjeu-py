"""
Classification prompts for ECJ citation analysis.

Based on Marc Jacob's taxonomy (Ch. 4.D) for use, the spec's precision
and treatment categories, and paragraph-level topic coding.

All prompts are designed for Gemini structured JSON output.
"""

# ── Response schemas (for Gemini structured output) ───────────────────────

CITATION_CLASSIFICATION_SCHEMA = {
    "type": "object",
    "properties": {
        "precision": {
            "type": "string",
            "enum": [
                "string_citation",       # bare reference, no discussion
                "general_reference",     # invokes precedent for a broad principle
                "substantive_engagement" # engages with specific reasoning/paragraphs
            ],
            "description": "How precisely the judgment engages with the cited precedent."
        },
        "use": {
            "type": "string",
            "enum": [
                "legal_test",            # cites to invoke a legal test or standard
                "factual_analogy",       # draws a factual comparison
                "interpretation",        # interprets EU law following the precedent
                "procedural",            # cites for procedural rules
                "jurisdictional",        # cites to establish jurisdiction
                "definition",            # cites for a definition
                "principle",             # invokes a general principle
                "distinguish",           # cites to distinguish the case
                "other"                  # none of the above
            ],
            "description": "The functional purpose of the citation (Jacob's taxonomy)."
        },
        "treatment": {
            "type": "string",
            "enum": [
                "follows",               # applies the precedent approvingly
                "extends",               # extends the precedent to a new context
                "distinguishes_facts",   # distinguishes on facts
                "distinguishes_law",     # distinguishes on law
                "distinguishes_scope",   # distinguishes on scope
                "departs_explicit",      # explicitly overrules or departs
                "departs_implicit",      # implicitly departs without saying so
                "neutral"                # mere citation, no evaluative treatment
            ],
            "description": "How the citing judgment treats the cited precedent."
        },
        "topic": {
            "type": "string",
            "description": "The area of EU law addressed in this paragraph (e.g. 'competition law', 'free movement of goods', 'fundamental rights', 'state aid', 'preliminary ruling procedure')."
        },
        "confidence": {
            "type": "number",
            "description": "Confidence score 0-1 for the overall classification."
        },
        "reasoning": {
            "type": "string",
            "description": "Brief explanation (1-2 sentences) of why you assigned these categories."
        },
    },
    "required": ["precision", "use", "treatment", "topic", "confidence", "reasoning"],
}


# ── System prompt ─────────────────────────────────────────────────────────

SYSTEM_PROMPT = """You are an expert in European Union law and judicial citation practices.
You are analysing how the Court of Justice of the European Union (CJEU) cites its own
prior decisions — specifically the precision, use, treatment, and topical context of
each citation, following Marc Jacob's empirical framework.

You will receive:
1. The CITING paragraph (marked with >>>) plus surrounding context paragraphs
2. A citation string (e.g. "Case C-6/15" or "ECLI:EU:C:2016:555")
3. Metadata about the citing decision (formation, procedure type, date)

Your task is to classify this single citation instance according to:
- **Precision**: How closely does the judgment engage with the cited case?
- **Use**: What functional role does the citation serve?
- **Treatment**: How does the citing judgment evaluate or apply the cited precedent?
- **Topic**: What area of EU law does the citing paragraph address?

Be precise and consistent. When uncertain, prefer 'neutral' treatment and note lower confidence.
"""


# ── User prompt template ─────────────────────────────────────────────────

USER_PROMPT_TEMPLATE = """## Citation to classify

**Citing decision**: {citing_celex} ({citing_date})
**Court formation**: {formation}
**Procedure type**: {procedure_type}
**Citation found**: `{citation_string}`

## Context (the citing paragraph is marked with >>>)

{context_text}

## Instructions

Classify this citation instance. Return a JSON object with:
- `precision`: one of [string_citation, general_reference, substantive_engagement]
- `use`: one of [legal_test, factual_analogy, interpretation, procedural, jurisdictional, definition, principle, distinguish, other]
- `treatment`: one of [follows, extends, distinguishes_facts, distinguishes_law, distinguishes_scope, departs_explicit, departs_implicit, neutral]
- `topic`: the area of EU law (free-text, be specific)
- `confidence`: 0-1 score
- `reasoning`: 1-2 sentence explanation
"""


def build_classification_prompt(
    citing_celex: str,
    citing_date: str,
    formation: str,
    procedure_type: str,
    citation_string: str,
    context_text: str,
) -> str:
    """Build the full user prompt for a single citation classification."""
    return USER_PROMPT_TEMPLATE.format(
        citing_celex=citing_celex,
        citing_date=citing_date or "unknown",
        formation=formation or "unknown",
        procedure_type=procedure_type or "unknown",
        citation_string=citation_string,
        context_text=context_text,
    )
