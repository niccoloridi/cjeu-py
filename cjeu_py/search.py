"""
Search collected CJEU case-law data.

Supports full-text, party, citation graph, topic, and legislation queries.
Reads from cached Parquet/JSONL files produced by the pipeline.
"""
import json
import logging
import os
from typing import Optional

import pandas as pd

from cjeu_py.export import _find_table, PARQUET_TABLES

logger = logging.getLogger(__name__)


# ── Helpers ─────────────────────────────────────────────────────────────


def _load_parquet(data_dir: str, table_name: str) -> Optional[pd.DataFrame]:
    """Load a Parquet table by name, returning None if not found."""
    rel_path = PARQUET_TABLES.get(table_name)
    if not rel_path:
        return None
    path = _find_table(data_dir, rel_path)
    # Fallback: check raw/cellar/ for tables listed at root level
    if not path:
        alt = os.path.join("raw", "cellar", os.path.basename(rel_path))
        path = _find_table(data_dir, alt)
    if not path:
        return None
    return pd.read_parquet(path)


def _load_decisions(data_dir: str) -> Optional[pd.DataFrame]:
    """Load the decisions table (needed for date/court joins)."""
    return _load_parquet(data_dir, "decisions")


def _load_case_names(data_dir: str) -> Optional[pd.DataFrame]:
    """Load case names, deduplicated to one name per CELEX."""
    df = _load_parquet(data_dir, "case_names")
    if df is None or df.empty:
        return None
    return df.drop_duplicates(subset=["celex"], keep="first")


def _enrich(df: pd.DataFrame, data_dir: str,
            celex_col: str = "celex") -> pd.DataFrame:
    """Left-join decisions + case_names onto a result DataFrame."""
    dec = _load_decisions(data_dir)
    names = _load_case_names(data_dir)
    if dec is not None and not dec.empty:
        keep = ["celex", "date", "court_code", "formation_code",
                "judge_rapporteur", "advocate_general"]
        keep = [c for c in keep if c in dec.columns]
        dec_dedup = dec[keep].drop_duplicates(subset=["celex"], keep="first")
        df = df.merge(dec_dedup, left_on=celex_col, right_on="celex",
                      how="left", suffixes=("", "_dec"))
    if names is not None and not names.empty:
        name_cols = ["celex", "case_name"]
        if "case_id" in names.columns:
            name_cols.append("case_id")
        df = df.merge(names[name_cols], left_on=celex_col, right_on="celex",
                      how="left", suffixes=("", "_name"))
    return df


def _apply_filters(df: pd.DataFrame, date_from: str = None,
                   date_to: str = None, court: str = None) -> pd.DataFrame:
    """Apply date and court filters to a DataFrame."""
    if date_from and "date" in df.columns:
        df = df[df["date"] >= date_from]
    if date_to and "date" in df.columns:
        df = df[df["date"] <= date_to]
    if court and "court_code" in df.columns:
        df = df[df["court_code"] == court.upper()]
    return df


def _extract_snippet(text: str, query: str, context: int = 100) -> str:
    """Extract a snippet around the first match, with ellipsis."""
    idx = text.lower().find(query.lower())
    if idx < 0:
        return text[:200] + ("..." if len(text) > 200 else "")
    start = max(0, idx - context)
    end = min(len(text), idx + len(query) + context)
    prefix = "..." if start > 0 else ""
    suffix = "..." if end < len(text) else ""
    return prefix + text[start:end] + suffix


def _truncate(s, width: int) -> str:
    """Truncate a string to width, adding ... if needed."""
    s = str(s) if s is not None else ""
    if len(s) <= width:
        return s
    return s[: width - 3] + "..."


# ── Output formatting ──────────────────────────────────────────────────


def _format_results(df: pd.DataFrame, columns: list,
                    col_widths: dict, limit: int, fmt: str,
                    header_msg: str = "") -> str:
    """Format a DataFrame for terminal output."""
    if fmt == "csv":
        available = [c for c in columns if c in df.columns]
        return df[available].head(limit).to_csv(index=False)
    if fmt == "json":
        available = [c for c in columns if c in df.columns]
        return df[available].head(limit).to_json(orient="records", indent=2,
                                                  default_handler=str)

    # Table mode
    lines = []
    if header_msg:
        lines.append(header_msg)
        lines.append("")

    available = [c for c in columns if c in df.columns]
    if df.empty or not available:
        lines.append("  No results found.")
        return "\n".join(lines)

    show = df[available].head(limit)

    # Header
    header_parts = []
    sep_parts = []
    for col in available:
        w = col_widths.get(col, 20)
        label = col.upper().replace("_", " ")
        header_parts.append(f"{_truncate(label, w):<{w}}")
        sep_parts.append("\u2500" * w)
    lines.append("  " + "  ".join(header_parts))
    lines.append("  " + "  ".join(sep_parts))

    # Rows
    for _, row in show.iterrows():
        parts = []
        for col in available:
            w = col_widths.get(col, 20)
            val = row.get(col, "")
            parts.append(f"{_truncate(val, w):<{w}}")
        lines.append("  " + "  ".join(parts))

    total = len(df)
    shown = min(limit, total)
    if total > shown:
        lines.append("")
        lines.append(f"  Showing {shown} of {total} results. "
                     f"Use --limit N to see more.")
    else:
        lines.append("")
        lines.append(f"  {total} result{'s' if total != 1 else ''}.")

    return "\n".join(lines)


# ── Search modes ───────────────────────────────────────────────────────


def search_text(data_dir: str, query: str, limit: int = 25,
                date_from: str = None, date_to: str = None,
                court: str = None, verbose: bool = False,
                fmt: str = "table") -> str:
    """Full-text search across judgment paragraphs."""
    # Find text files
    texts_path = _find_table(data_dir, "raw/texts/gc_texts.jsonl")
    if not texts_path:
        # Also try ag_divergence paths
        for subdir in ["ag_divergence_full", "ag_divergence"]:
            candidate = os.path.join(data_dir, subdir, "gc_texts.jsonl")
            if os.path.exists(candidate):
                texts_path = candidate
                break
    if not texts_path:
        return "No texts downloaded yet. Run: cjeu-py fetch-texts"

    query_lower = query.lower()
    matches = []
    with open(texts_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                doc = json.loads(line)
            except json.JSONDecodeError:
                continue
            if doc.get("status") != "ok":
                continue
            celex = doc.get("celex", "")
            paragraphs = doc.get("paragraphs", [])
            para_nums = doc.get("paragraph_nums", [])
            for i, para in enumerate(paragraphs):
                if query_lower in para.lower():
                    num = para_nums[i] if i < len(para_nums) else i + 1
                    snippet = para if verbose else _extract_snippet(
                        para, query)
                    matches.append({
                        "celex": celex,
                        "para": str(num),
                        "snippet": snippet,
                    })

    if not matches:
        return f'No results for "{query}".'

    df = pd.DataFrame(matches)
    df = _enrich(df, data_dir)
    df = _apply_filters(df, date_from, date_to, court)

    if "date" in df.columns:
        df = df.sort_values("date", ascending=False, na_position="last")

    columns = ["celex", "case_name", "date", "para", "snippet"]
    widths = {"celex": 15, "case_name": 35, "date": 10, "para": 5,
              "snippet": 60}
    return _format_results(df, columns, widths, limit, fmt,
                           f'Found {len(df)} paragraph(s) matching "{query}"')


def search_party(data_dir: str, query: str, limit: int = 25,
                 date_from: str = None, date_to: str = None,
                 court: str = None, fmt: str = "table") -> str:
    """Search by party name."""
    names = _load_parquet(data_dir, "case_names")
    if names is None:
        return ("No case names found. Run: cjeu-py download-cellar-meta "
                "or cjeu-py parse-headers")

    query_lower = query.lower()
    mask = names["case_name"].str.lower().str.contains(
        query_lower, na=False)
    matches = names[mask].drop_duplicates(subset=["celex"], keep="first")

    # Also check interveners
    interveners = _load_parquet(data_dir, "interveners")
    if interveners is not None and not interveners.empty:
        iv_mask = interveners["agent_name"].str.lower().str.contains(
            query_lower, na=False)
        iv_celex = interveners[iv_mask]["celex"].unique()
        extra = names[names["celex"].isin(iv_celex)].drop_duplicates(
            subset=["celex"], keep="first")
        matches = pd.concat([matches, extra]).drop_duplicates(
            subset=["celex"], keep="first")

    if matches.empty:
        return f'No cases found for party "{query}".'

    df = _enrich(matches, data_dir)
    df = _apply_filters(df, date_from, date_to, court)

    if "date" in df.columns:
        df = df.sort_values("date", ascending=False, na_position="last")

    columns = ["celex", "case_name", "date", "court_code", "formation_code"]
    widths = {"celex": 15, "case_name": 45, "date": 10, "court_code": 5,
              "formation_code": 12}
    return _format_results(df, columns, widths, limit, fmt,
                           f'Found {len(df)} case(s) matching party "{query}"')


def search_citing(data_dir: str, celex: str, limit: int = 25,
                  fmt: str = "table") -> str:
    """Find cases that cite the given CELEX."""
    cit = _load_parquet(data_dir, "citations_cellar")
    if cit is None:
        return "No citation data found. Run: cjeu-py download-cellar"

    celex_upper = celex.upper()
    direct = cit[cit["cited_celex"] == celex_upper]["citing_celex"].unique()

    if len(direct) == 0:
        return f'No cases found citing {celex}.'

    df = pd.DataFrame({"celex": direct})
    df = _enrich(df, data_dir)

    if "date" in df.columns:
        df = df.sort_values("date", ascending=False, na_position="last")

    columns = ["celex", "case_name", "date", "court_code",
                "judge_rapporteur"]
    widths = {"celex": 15, "case_name": 40, "date": 10, "court_code": 5,
              "judge_rapporteur": 20}
    return _format_results(df, columns, widths, limit, fmt,
                           f'{len(direct)} case(s) citing {celex}')


def search_cited_by(data_dir: str, celex: str, limit: int = 25,
                    fmt: str = "table") -> str:
    """Find cases cited by the given CELEX."""
    cit = _load_parquet(data_dir, "citations_cellar")
    if cit is None:
        return "No citation data found. Run: cjeu-py download-cellar"

    celex_upper = celex.upper()
    cited = cit[cit["citing_celex"] == celex_upper]["cited_celex"].unique()

    if len(cited) == 0:
        return f'{celex} does not cite any cases in the dataset.'

    df = pd.DataFrame({"celex": cited})
    df = _enrich(df, data_dir)

    if "date" in df.columns:
        df = df.sort_values("date", ascending=True, na_position="last")

    columns = ["celex", "case_name", "date"]
    widths = {"celex": 15, "case_name": 50, "date": 10}
    return _format_results(df, columns, widths, limit, fmt,
                           f'{celex} cites {len(cited)} case(s)')


def search_topic(data_dir: str, query: str, limit: int = 25,
                 date_from: str = None, date_to: str = None,
                 court: str = None, fmt: str = "table") -> str:
    """Search by subject matter code or label."""
    from cjeu_py.network_export import SUBJECT_LABELS

    subjects = _load_parquet(data_dir, "subjects")
    if subjects is None:
        return "No subject data found. Run: cjeu-py download-cellar"

    query_upper = query.upper()
    query_lower = query.lower()

    # Try exact code match first
    mask = subjects["subject_code"] == query_upper
    if mask.sum() == 0:
        # Try label substring match
        matching_codes = [
            code for code, label in SUBJECT_LABELS.items()
            if query_lower in label.lower()
        ]
        if matching_codes:
            mask = subjects["subject_code"].isin(matching_codes)
        else:
            # Try substring on code itself
            mask = subjects["subject_code"].str.contains(
                query_upper, na=False)

    if mask.sum() == 0:
        return (f'No cases found for topic "{query}". '
                f"Use: cjeu-py search list topics")

    matched = subjects[mask].copy()
    matched["subject_label"] = matched["subject_code"].map(
        SUBJECT_LABELS).fillna("")

    celex_list = matched["celex"].unique()
    df = pd.DataFrame({"celex": celex_list})
    # Carry subject info
    subject_info = matched.drop_duplicates(
        subset=["celex"], keep="first")[["celex", "subject_code",
                                          "subject_label"]]
    df = df.merge(subject_info, on="celex", how="left")
    df = _enrich(df, data_dir)
    df = _apply_filters(df, date_from, date_to, court)

    if "date" in df.columns:
        df = df.sort_values("date", ascending=False, na_position="last")

    columns = ["celex", "case_name", "date", "subject_code", "subject_label"]
    widths = {"celex": 15, "case_name": 35, "date": 10,
              "subject_code": 6, "subject_label": 25}
    return _format_results(df, columns, widths, limit, fmt,
                           f'{len(df)} case(s) matching topic "{query}"')


def search_legislation(data_dir: str, query: str, limit: int = 25,
                       fmt: str = "table") -> str:
    """Search for cases linked to a piece of legislation."""
    leg = _load_parquet(data_dir, "legislation_links")
    if leg is None:
        return ("No legislation link data found. "
                "Run: cjeu-py download-cellar-meta --detail high")

    query_upper = query.upper()
    mask = leg["legislation_celex"].str.contains(query_upper, na=False)

    if mask.sum() == 0:
        return f'No cases found linked to legislation "{query}".'

    matched = leg[mask].copy()
    df = matched.drop_duplicates(subset=["celex", "legislation_celex"],
                                  keep="first")
    df = _enrich(df, data_dir)

    if "date" in df.columns:
        df = df.sort_values("date", ascending=False, na_position="last")

    columns = ["celex", "case_name", "date", "legislation_celex", "link_type"]
    widths = {"celex": 15, "case_name": 30, "date": 10,
              "legislation_celex": 15, "link_type": 15}
    return _format_results(df, columns, widths, limit, fmt,
                           f'{len(df)} case(s) linked to "{query}"')


def list_categories(data_dir: str, category: str, fmt: str = "table") -> str:
    """List available values for a category."""
    from cjeu_py.network_export import SUBJECT_LABELS

    if category == "topics":
        subjects = _load_parquet(data_dir, "subjects")
        if subjects is None:
            return "No subject data. Run: cjeu-py download-cellar"
        counts = subjects["subject_code"].value_counts()
        rows = []
        for code, count in counts.items():
            label = SUBJECT_LABELS.get(code, "")
            rows.append({"code": code, "label": label, "cases": count})
        df = pd.DataFrame(rows)
        columns = ["code", "label", "cases"]
        widths = {"code": 6, "label": 40, "cases": 6}
        return _format_results(df, columns, widths, len(df), fmt,
                               f"{len(df)} subject codes")

    dec = _load_decisions(data_dir)
    if dec is None:
        return "No decision data. Run: cjeu-py download-cellar"

    col_map = {
        "judges": "judge_rapporteur",
        "ags": "advocate_general",
        "formations": "formation_code",
        "courts": "court_code",
        "procedures": "procedure_type",
    }
    col = col_map.get(category)
    if not col:
        valid = ", ".join(["topics"] + list(col_map.keys()))
        return f'Unknown category "{category}". Valid: {valid}'

    if col not in dec.columns:
        return f'Column "{col}" not found in decisions table.'

    counts = dec[col].value_counts()
    df = pd.DataFrame({"value": counts.index, "cases": counts.values})

    columns = ["value", "cases"]
    widths = {"value": 30, "cases": 6}
    label = category.replace("_", " ").title()
    return _format_results(df, columns, widths, len(df), fmt,
                           f"{len(df)} {label}")


def search_headnote(query: str, limit: int = 25,
                    fmt: str = "table") -> str:
    """Search CELLAR headnotes and titles remotely (no local data needed).

    Queries the CELLAR SPARQL endpoint for case-law whose headnote
    (expression_case-law_indicator_decision) or title contains the
    search term. Covers the entire CJEU corpus.
    """
    from SPARQLWrapper import SPARQLWrapper, JSON

    sparql = SPARQLWrapper(
        "https://publications.europa.eu/webapi/rdf/sparql")
    sparql.setReturnFormat(JSON)

    query_escaped = query.lower().replace('"', '\\"')

    # Search both title and headnote via UNION for broadest coverage.
    # STR() is needed because LCASE() can fail silently on typed literals.
    sparql_query = """
PREFIX cdm: <http://publications.europa.eu/ontology/cdm#>
SELECT DISTINCT ?celex ?date ?text ?parties WHERE {{
  ?work cdm:resource_legal_id_celex ?celex .
  FILTER(REGEX(STR(?celex), "^6[0-9]{{4}}(CJ|TJ|FJ)"))
  OPTIONAL {{ ?work cdm:work_date_document ?date . }}
  ?exp cdm:expression_belongs_to_work ?work .
  ?exp cdm:expression_uses_language
       <http://publications.europa.eu/resource/authority/language/ENG> .
  OPTIONAL {{ ?exp cdm:expression_case-law_parties ?parties . }}
  {{
    ?exp cdm:expression_title ?text .
    FILTER(CONTAINS(LCASE(STR(?text)), "{query_escaped}"))
  }} UNION {{
    ?exp cdm:expression_case-law_indicator_decision ?text .
    FILTER(CONTAINS(LCASE(STR(?text)), "{query_escaped}"))
  }}
}}
ORDER BY DESC(?date)
LIMIT {limit}
""".format(query_escaped=query_escaped, limit=limit)

    sparql.setQuery(sparql_query)
    logger.info("Querying CELLAR for '%s'...", query)

    try:
        results = sparql.query().convert()
    except Exception as e:
        return f"CELLAR query failed: {e}"

    bindings = results.get("results", {}).get("bindings", [])
    if not bindings:
        return f'No CELLAR results matching "{query}".'

    # Deduplicate by celex (title and headnote may both match)
    seen = set()
    rows = []
    for b in bindings:
        celex = b.get("celex", {}).get("value", "")
        if celex in seen:
            continue
        seen.add(celex)
        text = b.get("text", {}).get("value", "")
        rows.append({
            "celex": celex,
            "parties": b.get("parties", {}).get("value", ""),
            "date": b.get("date", {}).get("value", "")[:10],
            "headnote": _extract_snippet(text, query, context=80),
        })

    df = pd.DataFrame(rows)

    columns = ["celex", "parties", "date", "headnote"]
    widths = {"celex": 15, "parties": 35, "date": 10, "headnote": 60}
    return _format_results(
        df, columns, widths, limit, fmt,
        f'{len(df)} CELLAR result(s) matching "{query}" '
        f"(live query, entire corpus)")


# ── Main dispatcher ────────────────────────────────────────────────────


def run_search(data_dir: str, mode: str, query: str,
               limit: int = 25, fmt: str = "table",
               date_from: str = None, date_to: str = None,
               court: str = None, verbose: bool = False) -> str:
    """Dispatch to the appropriate search mode. Returns formatted string."""
    if mode == "text":
        return search_text(data_dir, query, limit, date_from, date_to,
                           court, verbose, fmt)
    elif mode == "headnote":
        return search_headnote(query, limit, fmt)
    elif mode == "party":
        return search_party(data_dir, query, limit, date_from, date_to,
                            court, fmt)
    elif mode == "citing":
        return search_citing(data_dir, query, limit, fmt)
    elif mode == "cited-by":
        return search_cited_by(data_dir, query, limit, fmt)
    elif mode == "topic":
        return search_topic(data_dir, query, limit, date_from, date_to,
                            court, fmt)
    elif mode == "legislation":
        return search_legislation(data_dir, query, limit, fmt)
    elif mode == "list":
        return list_categories(data_dir, query, fmt)
    else:
        return f'Unknown search mode: "{mode}"'
