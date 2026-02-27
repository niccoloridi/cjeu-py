"""
Codebook: variable definitions for all cjeu-py pipeline tables.

Each entry maps (table, column) to a description, data type, and
possible values. Used to generate documentation and validate outputs.
"""
import json
import os
from typing import Optional

# ── Variable definitions ────────────────────────────────────────────────
#
# Format: table_name -> list of (column, type, description, values)
# where values is None for continuous/free-text, or a list for categorical.

CODEBOOK = {
    # ── CELLAR SPARQL: decisions ────────────────────────────────────────
    "decisions": [
        ("celex", "str", "CELEX identifier (primary key)", None),
        ("ecli", "str", "European Case Law Identifier", None),
        ("date", "str", "Decision date (ISO 8601)", None),
        ("court_code", "str", "Court", ["CJ", "TJ", "FJ"]),
        ("resource_type", "str", "Document type", ["JUDG", "ORDER", "OPIN_AG", "THIRDPARTY_OPIN_AG", "OPIN_JUR"]),
        ("formation_code", "str", "Court formation (CDM URI suffix)", None),
        ("judge_rapporteur", "str", "Judge-Rapporteur name", None),
        ("advocate_general", "str", "Advocate General name", None),
        ("procedure_type", "str", "Procedure type (CDM URI suffix)", None),
        ("orig_country", "str", "Country of origin (ISO 2-letter code)", None),
        ("proc_lang", "str", "Language of procedure (ISO 2-letter code)", None),
        ("case_year", "str", "Year the case was registered", None),
        ("defendant_agent", "str", "Defendant agent (corporate body code)", None),
        ("applicant_agent", "str", "Applicant agent (corporate body code)", None),
        ("referring_court", "str", "Referring national court (preliminary rulings)", None),
        ("treaty_basis", "str", "Treaty basis (e.g. TFEU_2008)", None),
        ("date_lodged", "str", "Date application was lodged", None),
        ("procjur", "str", "Procedural classification (e.g. REFER_PREL, APPEAL, ANNU_DIR)", None),
        ("published_ecr", "str", "Published in electronic Reports of Cases", ["true", "false"]),
        ("authentic_lang", "str", "Authentic/original language (ISO 639-2/B code)", None),
        ("eea_relevant", "str", "EEA relevance flag", ["true", "false"]),
        ("natural_celex", "str", "Natural case number (e.g. C-489/19)", None),
    ],

    # ── CELLAR SPARQL: citation network ─────────────────────────────────
    "citations_cellar": [
        ("citing_celex", "str", "CELEX of the citing decision", None),
        ("cited_celex", "str", "CELEX of the cited decision", None),
    ],

    # ── CELLAR SPARQL: subject matter ───────────────────────────────────
    "subjects": [
        ("celex", "str", "CELEX identifier", None),
        ("subject_code", "str", "Subject code (format depends on source)", None),
        ("subject_label", "str", "English label for the subject code", None),
        ("subject_source", "str", "Source taxonomy", [
            "eurovoc", "case_law_subject", "case_law_directory", "case_law_directory_old"]),
    ],

    # ── CELLAR SPARQL: relational tables ────────────────────────────────
    "joined_cases": [
        ("celex", "str", "CELEX identifier", None),
        ("joined_celex", "str", "CELEX of the joined case", None),
    ],
    "appeals": [
        ("celex", "str", "CELEX identifier (lower court decision)", None),
        ("appeal_celex", "str", "CELEX of the appeal decision", None),
    ],
    "annulled_acts": [
        ("celex", "str", "CELEX identifier", None),
        ("annulled_celex", "str", "CELEX of the annulled act", None),
    ],
    "interveners": [
        ("celex", "str", "CELEX identifier", None),
        ("agent_name", "str", "Intervener name (from CDM agent URI)", None),
    ],

    # ── CELLAR SPARQL: AG opinion links ─────────────────────────────────
    "ag_opinions": [
        ("celex", "str", "CELEX of the judgment", None),
        ("ag_opinion_celex", "str", "CELEX of the AG opinion", None),
    ],

    # ── CELLAR SPARQL: legislation links ────────────────────────────────
    "legislation_links": [
        ("celex", "str", "CELEX identifier", None),
        ("legislation_celex", "str", "CELEX of the linked legislation", None),
        ("link_type", "str", "Type of case-to-legislation relationship", [
            "interprets", "confirms", "requests_interpretation",
            "requests_annulment", "states_failure", "amends",
            "declares_valid", "declares_void_preliminary",
            "declares_incidentally_valid",
            "suspends", "corrects_judgment",
            "incidentally_declares_void", "interprets_judgment",
            "partially_annuls", "immediately_enforces",
            "reviews_judgment", "reexamined_by",
        ]),
    ],

    # ── CELLAR SPARQL: academic citations ───────────────────────────────
    "academic_citations": [
        ("celex", "str", "CELEX identifier", None),
        ("citation_text", "str", "Bibliographic reference (author, title, journal, year)", None),
    ],

    # ── CELLAR SPARQL: referring national judgments ──────────────────────
    "referring_judgments": [
        ("celex", "str", "CELEX identifier", None),
        ("national_judgment", "str", "Referring court name, decision type, date, and reference", None),
    ],

    # ── CELLAR SPARQL: exhaustive-tier tables ─────────────────────────
    "dossiers": [
        ("celex", "str", "CELEX identifier", None),
        ("dossier_uri", "str", "Dossier URI suffix (groups related proceedings)", None),
    ],
    "summaries": [
        ("celex", "str", "CELEX identifier", None),
        ("summary_celex", "str", "CELEX of the summary or information note (SUM/INF suffix)", None),
    ],
    "misc_info": [
        ("celex", "str", "CELEX identifier", None),
        ("info_text", "str", "Miscellaneous information (often appeal case numbers)", None),
    ],
    "successors": [
        ("celex", "str", "CELEX identifier", None),
        ("successor_celex", "str", "CELEX of the logical successor case", None),
    ],
    "incorporates": [
        ("celex", "str", "CELEX identifier", None),
        ("incorporated_celex", "str", "CELEX of the incorporated legislation", None),
    ],

    # ── CELLAR SPARQL: kitchen-sink tier ──────────────────────────────
    "admin_metadata": [
        ("celex", "str", "CELEX identifier", None),
        ("property", "str", "CDM property name", [
            "country_role", "created_by", "seq_celex", "collection", "event",
            "complex_work", "version", "embargo", "date_creation",
            "date_creation_legacy", "datetime_transmission",
            "date_creation_legacy_2", "datetime_negotiation",
            "obsolete_doc", "obsolete_notice", "comment_internal",
            "do_not_index", "document_id",
        ]),
        ("value", "str", "Property value (URIs shortened to suffix)", None),
    ],

    # ── Header parser: metadata ─────────────────────────────────────────
    "header_metadata": [
        ("celex", "str", "CELEX identifier (from filename)", None),
        ("doc_type", "str", "Document type", ["judgment", "ag_opinion", "order"]),
        ("date", "str", "Decision date (YYYY-MM-DD)", None),
        ("case_numbers", "list[str]", "Case numbers (e.g. ['C-16/19'])", None),
        ("formation", "str", "Court formation (free text)", None),
        ("parties", "dict", "Parties: {applicants, defendants, interveners}", None),
        ("composition", "list[dict]", "Judges: [{name, role}, ...]", None),
        ("advocate_general", "str", "Advocate General name", None),
        ("registrar", "str", "Registrar name", None),
        ("representatives", "list[dict]", "Legal representatives per party", None),
        ("hearing_date", "str", "Hearing date (YYYY-MM-DD)", None),
        ("ag_opinion_date", "str", "AG opinion delivery date (YYYY-MM-DD)", None),
    ],

    # ── Header parser: assignments ──────────────────────────────────────
    "assignments": [
        ("celex", "str", "CELEX identifier", None),
        ("judge_name", "str", "Judge name (as appears in header)", None),
        ("role", "str", "Role in this case", ["President", "Vice-President", "Presidents of Chambers", "Rapporteur", "Judges"]),
        ("is_rapporteur", "bool", "Whether this judge is the Rapporteur", [True, False]),
    ],

    # ── Header parser: case names ───────────────────────────────────────
    "case_names": [
        ("celex", "str", "CELEX identifier", None),
        ("case_name", "str", "Short case name (Applicant v Defendant)", None),
        ("applicants", "str", "Applicant names (semicolon-separated)", None),
        ("defendants", "str", "Defendant names (semicolon-separated)", None),
    ],

    # ── Header parser: operative parts ──────────────────────────────────
    "operative_parts": [
        ("celex", "str", "CELEX identifier", None),
        ("operative_part", "str", "Full text of the operative part (dispositif)", None),
    ],

    # ── Citation extraction ─────────────────────────────────────────────
    "citations_extracted": [
        ("citing_celex", "str", "CELEX of the citing document", None),
        ("paragraph_num", "int", "Paragraph number in the citing document", None),
        ("citation_string", "str", "Raw citation text as found in the document", None),
        ("pattern_type", "str", "Regex pattern that matched", [
            "ecli", "joined_modern", "joined_old", "case_cj", "case_gc",
            "case_cst", "case_old", "ecr_bracketed", "ecr", "para_pinpoint",
        ]),
        ("span_start", "int", "Character offset (start) in paragraph", None),
        ("span_end", "int", "Character offset (end) in paragraph", None),
        ("context_text", "str", "Surrounding paragraph text for classification", None),
    ],

    # ── Classification ──────────────────────────────────────────────────
    "classified_citations": [
        ("citing_celex", "str", "CELEX of the citing document", None),
        ("paragraph_num", "int", "Paragraph number", None),
        ("citation_string", "str", "Raw citation text", None),
        ("precision", "str", "Citation precision level", [
            "string_citation", "general_reference", "substantive_engagement",
        ]),
        ("use", "str", "How the citation is used", [
            "principle", "interpretation", "legal_test", "factual_analogy",
            "procedural", "jurisdictional", "definition", "distinguish", "other",
        ]),
        ("treatment", "str", "How the cited case is treated", [
            "follows", "extends", "distinguishes_facts", "distinguishes_law",
            "distinguishes_scope", "departs_explicit", "departs_implicit", "neutral",
        ]),
        ("topic", "str", "Area of EU law (free text)", None),
        ("confidence", "float", "LLM confidence score (0.0 -- 1.0)", None),
        ("reasoning", "str", "LLM reasoning for the classification", None),
    ],

    # ── Judge biographical data ─────────────────────────────────────────
    "judges_raw": [
        ("name", "str", "Full name as listed on Curia", None),
        ("role", "str", "Role at the Court (free text)", None),
        ("bio_text", "str", "Raw biographical text from Curia", None),
        ("is_current", "bool", "Whether currently serving", [True, False]),
    ],
    "judges_structured": [
        ("name", "str", "Full name", None),
        ("role", "str", "Role at the Court", None),
        ("is_current", "bool", "Whether currently serving", [True, False]),
        ("birth_year", "int", "Year of birth", None),
        ("birth_place", "str", "City of birth", None),
        ("nationality", "str", "Country (e.g. 'Italy', 'Germany')", None),
        ("is_female", "bool", "Gender", [True, False]),
        ("education", "list[str]", "Degrees and institutions", None),
        ("prior_careers", "list[str]", "Positions held before CJEU", None),
        ("cjeu_roles", "list[dict]", "CJEU roles: [{role, start_year, end_year}]", None),
        ("death_year", "int", "Year of death (if applicable)", None),
    ],
}


def generate_codebook_markdown() -> str:
    """Generate a Markdown codebook from the variable definitions."""
    lines = ["# cjeu-py Codebook", ""]
    lines.append("Variable definitions for all pipeline output tables.")
    lines.append("")

    for table, columns in CODEBOOK.items():
        lines.append(f"## {table}")
        lines.append("")
        lines.append("| Variable | Type | Description | Values |")
        lines.append("|----------|------|-------------|--------|")
        for col, dtype, desc, values in columns:
            if values is None:
                val_str = ""
            elif isinstance(values, list) and len(values) <= 6:
                val_str = ", ".join(f"`{v}`" for v in values)
            else:
                val_str = f"{len(values)} categories"
            lines.append(f"| `{col}` | {dtype} | {desc} | {val_str} |")
        lines.append("")

    return "\n".join(lines)


def write_codebook(output_path: Optional[str] = None) -> str:
    """Write the codebook as a Markdown file.

    Args:
        output_path: Path for output (default: CODEBOOK.md in project root)

    Returns:
        Path to the written file
    """
    if output_path is None:
        from cjeu_py import config
        output_path = os.path.join(config.PROJECT_ROOT, "CODEBOOK.md")

    md = generate_codebook_markdown()
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(md)
    return output_path
