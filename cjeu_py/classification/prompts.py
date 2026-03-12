"""
Classification prompts for ECJ citation analysis.

Two taxonomy variants:
  - **jacob** (default): Five-layer annotation scheme from Marc Jacob,
    *Precedents and Case-Based Reasoning in the European Court of Justice*
    (Cambridge, 2014), chapters 4–6.  Layers: polarity, precision,
    function, distinguishing type, departing grounds, plus meta-annotations
    (surface coherence, triangle side).
  - **legacy**: The original three-dimension scheme (precision, use,
    treatment) plus free-text topic.  Kept for backward compatibility.

All prompts are designed for Gemini structured JSON output.
"""

# ═══════════════════════════════════════════════════════════════════════
# JACOB TAXONOMY (default)
# ═══════════════════════════════════════════════════════════════════════

CITATION_CLASSIFICATION_SCHEMA = {
    "type": "object",
    "properties": {
        "polarity": {
            "type": "string",
            "enum": [
                "POSITIVE",
                "NEGATIVE_DISTINGUISHING",
                "NEGATIVE_DEPARTING",
            ],
            "description": (
                "Layer 1 — Polarity. The attitude the citing court takes "
                "toward the prior case. POSITIVE: the precedent is followed, "
                "applied, or relied upon. NEGATIVE_DISTINGUISHING: the "
                "precedent is declared inapplicable or its reach is narrowed. "
                "NEGATIVE_DEPARTING: the precedent's rationale is abandoned "
                "or replaced."
            ),
        },
        "precision": {
            "type": "string",
            "enum": [
                "VERBATIM",
                "GENERAL",
                "STRING",
                "SUBSTANTIVE",
            ],
            "description": (
                "Layer 2 — Precision. How is the citation presented? "
                "VERBATIM: word-for-word reproduction of phrases from the "
                "prior case (the 'LEGO technique'). "
                "GENERAL: reference to jurisprudence at large without tying "
                "content to a specific case ('settled case-law', 'consistent "
                "jurisprudence'). "
                "STRING: specific cases appended to a proposition without "
                "discussion of their reasoning ('see', 'see, to that effect'). "
                "SUBSTANTIVE: the reasoning, context, or facts of the prior "
                "case are discussed."
            ),
        },
        "function": {
            "type": "string",
            "enum": [
                "CLASSIFY",
                "IDENTIFY_PROVISIONS",
                "STATE_LAW",
                "INTERPRET_PROVISION",
                "INTERPRET_LAW",
                "INTERPRET_CASE",
                "JUSTIFY_INTERPRETATION",
                "ASSERT_FACT",
                "AFFIRM_CONCLUSION",
            ],
            "description": (
                "Layer 3 — Function. What argumentative work does the citation "
                "perform? "
                "CLASSIFY: helps characterise a legal issue or fact. "
                "IDENTIFY_PROVISIONS: helps determine which legal provisions "
                "govern the dispute. "
                "STATE_LAW: articulates a legal rule or principle (the major "
                "premise). "
                "INTERPRET_PROVISION: construes a specific textual provision "
                "(treaty article, directive). "
                "INTERPRET_LAW: refines a legal principle not tied to a "
                "specific provision (general principles, proportionality). "
                "INTERPRET_CASE: the meaning or reach of the prior case is "
                "itself the object of interpretation. "
                "JUSTIFY_INTERPRETATION: adduces further cases that held "
                "likewise (persuasion, not derivation). "
                "ASSERT_FACT: establishes an empirical or factual proposition. "
                "AFFIRM_CONCLUSION: appended to a conclusion already drawn "
                "from other premises (reinforcement)."
            ),
        },
        "distinguishing_type": {
            "type": "string",
            "enum": [
                "DISAPPLICATION",
                "MANIPULATION",
                "OBITERING",
                "NONE",
            ],
            "description": (
                "Layer 4a — Distinguishing type. Only meaningful when "
                "polarity = NEGATIVE_DISTINGUISHING; set to NONE otherwise. "
                "DISAPPLICATION: the precedent's conditions are simply not "
                "met; the precedent is left untouched. "
                "MANIPULATION: the precedent's rationale is retrospectively "
                "reformulated — narrowed, specified, or recharacterised. "
                "OBITERING: progressive erosion across decisions; the "
                "precedent is confined to its 'particular circumstances'."
            ),
        },
        "departing_grounds": {
            "type": "array",
            "items": {
                "type": "string",
                "enum": [
                    "INCORRECT",
                    "UNWORKABLE",
                    "INCOMPATIBLE_CASES",
                    "INCOMPATIBLE_LAW",
                    "IMBALANCE",
                    "CHANGED_PREMISES",
                ],
            },
            "description": (
                "Layer 4b — Departing grounds. Only meaningful when polarity = "
                "NEGATIVE_DEPARTING; empty array otherwise. Multiple grounds "
                "may co-occur. "
                "INCORRECT: the precedent was wrongly decided. "
                "UNWORKABLE: the precedent proved impractical. "
                "INCOMPATIBLE_CASES: subsequent case law cannot be reconciled. "
                "INCOMPATIBLE_LAW: non-judicial legal changes have overtaken it. "
                "IMBALANCE: competing principles require recalibration. "
                "CHANGED_PREMISES: non-legal changes have rendered assumptions "
                "obsolete."
            ),
        },
        "surface_coherence": {
            "type": "boolean",
            "description": (
                "Meta-annotation — Surface coherence. True when the court "
                "iterates a shared formula (e.g. 'settled case-law') without "
                "substantive demonstration that the cases are relevantly similar "
                "or that the shared test produces defensible results across "
                "different factual contexts. Flag STRING + POSITIVE + "
                "STATE_LAW/AFFIRM_CONCLUSION patterns."
            ),
        },
        "triangle_side": {
            "type": "string",
            "enum": [
                "ALPHA",
                "BETA",
                "GAMMA",
                "NONE",
            ],
            "description": (
                "Structural — Triangle side under pressure. Only meaningful "
                "when polarity is negative; set to NONE otherwise. "
                "ALPHA: Norm vs Current Case (does the situation fall within "
                "the norm?). "
                "BETA: Precedent vs Current Case (are the two cases relevantly "
                "similar?). Distinguishing operates here. "
                "GAMMA: Precedent vs Norm (did the precedent correctly "
                "interpret the norm?). Departing operates here."
            ),
        },
        "topic": {
            "type": "string",
            "description": (
                "The area of EU law addressed in the citing passage "
                "(e.g. 'competition law', 'free movement of goods', "
                "'fundamental rights', 'state aid', 'preliminary ruling "
                "procedure')."
            ),
        },
        "confidence": {
            "type": "number",
            "description": "Confidence score 0-1 for the overall classification.",
        },
        "reasoning": {
            "type": "string",
            "description": "Brief explanation (1-2 sentences) of why you assigned these categories.",
        },
    },
    "required": [
        "polarity", "precision", "function",
        "distinguishing_type", "departing_grounds",
        "surface_coherence", "triangle_side",
        "topic", "confidence", "reasoning",
    ],
}


SYSTEM_PROMPT = """\
You are an expert in European Union law and judicial citation practices.
You are annotating how the Court of Justice of the European Union (CJEU)
and its Advocates General cite prior decisions, following the multi-layer
framework developed by Marc Jacob in *Precedents and Case-Based Reasoning
in the European Court of Justice* (Cambridge University Press, 2014).

You will receive:
1. The CITING paragraph (marked with >>>) plus surrounding context
2. A citation string (e.g. "Case C-6/15" or "ECLI:EU:C:2016:555")
3. Metadata about the citing decision

Classify this single citation instance on ALL of the following layers:

**Layer 1 — POLARITY** (attitude toward the prior case):
- POSITIVE: followed, applied, relied upon
- NEGATIVE_DISTINGUISHING: declared inapplicable or reach narrowed
- NEGATIVE_DEPARTING: rationale abandoned or replaced

**Layer 2 — PRECISION** (how the citation is presented):
- VERBATIM: word-for-word reproduction of phrases from the prior case
- GENERAL: reference to jurisprudence at large ("settled case-law")
- STRING: specific cases appended without discussing reasoning ("see", "see, to that effect")
- SUBSTANTIVE: reasoning, context, or facts of the prior case are discussed

**Layer 3 — FUNCTION** (argumentative work the citation performs):
- CLASSIFY: characterises a legal issue or fact
- IDENTIFY_PROVISIONS: determines which provisions govern the dispute
- STATE_LAW: articulates a legal rule or principle (the major premise)
- INTERPRET_PROVISION: construes a specific textual provision (treaty, directive)
- INTERPRET_LAW: refines a legal principle not tied to a specific provision
- INTERPRET_CASE: the prior case's meaning or reach is itself the object of interpretation
- JUSTIFY_INTERPRETATION: adduces further cases that held likewise (persuasion)
- ASSERT_FACT: establishes an empirical or factual proposition
- AFFIRM_CONCLUSION: reinforces a conclusion already drawn

**Layer 4a — DISTINGUISHING TYPE** (only if polarity = NEGATIVE_DISTINGUISHING, else NONE):
- DISAPPLICATION: precedent's conditions not met; precedent left intact
- MANIPULATION: precedent retrospectively reformulated, narrowed, or recharacterised
- OBITERING: progressive erosion, confined to "particular circumstances"

**Layer 4b — DEPARTING GROUNDS** (only if polarity = NEGATIVE_DEPARTING, else empty []):
Multiple may apply. Options: INCORRECT, UNWORKABLE, INCOMPATIBLE_CASES,
INCOMPATIBLE_LAW, IMBALANCE, CHANGED_PREMISES

**META — SURFACE COHERENCE** (boolean):
True when the court iterates a shared formula without substantive
demonstration that the cited cases are relevantly similar. Typical pattern:
STRING + POSITIVE + STATE_LAW or AFFIRM_CONCLUSION.

**TRIANGLE SIDE** (only if polarity is negative, else NONE):
- ALPHA: Norm vs Current Case under pressure
- BETA: Precedent vs Current Case under pressure (distinguishing)
- GAMMA: Precedent vs Norm under pressure (departing)

**CRITICAL — WHOSE VOICE?**
CJEU judgments routinely report parties' submissions before giving the
Court's own reasoning. When the citing paragraph merely REPORTS a party's
argument about a precedent (signalled by phrases like "the applicant
submits that…", "according to the appellant…", "the Commission argues
that…", "X contends that the General Court erred in relying on…"),
classify the citation from THE COURT'S perspective, NOT the party's.
If the Court is neutrally recounting a party's criticism of a precedent
without itself endorsing that criticism, the polarity is typically
POSITIVE (the Court is not itself rejecting the precedent) and the
function is CLASSIFY (framing the dispute). Reserve NEGATIVE polarity
for passages where the Court or AG itself — in its own analytical
voice — distinguishes or departs from the precedent.

Be precise and consistent. For POSITIVE polarity, set distinguishing_type
to NONE, departing_grounds to [], and triangle_side to NONE."""


USER_PROMPT_TEMPLATE = """\
## Citation to classify

**Citing decision**: {citing_celex} ({citing_date})
**Court formation**: {formation}
**Procedure type**: {procedure_type}
**Citation found**: `{citation_string}`

## Context (the citing paragraph is marked with >>>)

{context_text}

## Instructions

Classify this citation instance. Return a JSON object with:
- `polarity`: POSITIVE, NEGATIVE_DISTINGUISHING, or NEGATIVE_DEPARTING
- `precision`: VERBATIM, GENERAL, STRING, or SUBSTANTIVE
- `function`: one of the 9 function categories
- `distinguishing_type`: DISAPPLICATION, MANIPULATION, OBITERING, or NONE
- `departing_grounds`: list of grounds (empty [] if not departing)
- `surface_coherence`: true/false
- `triangle_side`: ALPHA, BETA, GAMMA, or NONE
- `topic`: area of EU law (free-text)
- `confidence`: 0-1
- `reasoning`: 1-2 sentence explanation"""


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


# ═══════════════════════════════════════════════════════════════════════
# LEGACY TAXONOMY (backward-compatible)
# ═══════════════════════════════════════════════════════════════════════

CITATION_CLASSIFICATION_SCHEMA_LEGACY = {
    "type": "object",
    "properties": {
        "precision": {
            "type": "string",
            "enum": [
                "string_citation",
                "general_reference",
                "substantive_engagement",
            ],
            "description": "How precisely the judgment engages with the cited precedent.",
        },
        "use": {
            "type": "string",
            "enum": [
                "legal_test",
                "factual_analogy",
                "interpretation",
                "procedural",
                "jurisdictional",
                "definition",
                "principle",
                "distinguish",
                "other",
            ],
            "description": "The functional purpose of the citation.",
        },
        "treatment": {
            "type": "string",
            "enum": [
                "follows",
                "extends",
                "distinguishes_facts",
                "distinguishes_law",
                "distinguishes_scope",
                "departs_explicit",
                "departs_implicit",
                "neutral",
            ],
            "description": "How the citing judgment treats the cited precedent.",
        },
        "topic": {
            "type": "string",
            "description": (
                "The area of EU law addressed in this paragraph "
                "(e.g. 'competition law', 'free movement of goods')."
            ),
        },
        "confidence": {
            "type": "number",
            "description": "Confidence score 0-1 for the overall classification.",
        },
        "reasoning": {
            "type": "string",
            "description": "Brief explanation (1-2 sentences) of why you assigned these categories.",
        },
    },
    "required": ["precision", "use", "treatment", "topic", "confidence", "reasoning"],
}

SYSTEM_PROMPT_LEGACY = """\
You are an expert in European Union law and judicial citation practices.
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

Be precise and consistent. When uncertain, prefer 'neutral' treatment and note lower confidence."""

USER_PROMPT_TEMPLATE_LEGACY = """\
## Citation to classify

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
- `reasoning`: 1-2 sentence explanation"""


def build_classification_prompt_legacy(
    citing_celex: str,
    citing_date: str,
    formation: str,
    procedure_type: str,
    citation_string: str,
    context_text: str,
) -> str:
    """Build the full user prompt using the legacy taxonomy."""
    return USER_PROMPT_TEMPLATE_LEGACY.format(
        citing_celex=citing_celex,
        citing_date=citing_date or "unknown",
        formation=formation or "unknown",
        procedure_type=procedure_type or "unknown",
        citation_string=citation_string,
        context_text=context_text,
    )
