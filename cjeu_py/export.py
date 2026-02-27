"""
Export pipeline data as CSV or Excel files.

Reads Parquet and JSONL outputs from the pipeline and writes them
as flat CSV (or optionally Excel) files for users who prefer
spreadsheet-based workflows.
"""
import json
import logging
import os
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)

# ── Known pipeline outputs ──────────────────────────────────────────────

PARQUET_TABLES = {
    # CELLAR SPARQL outputs — base
    "decisions": "raw/cellar/gc_decisions.parquet",
    "citations_cellar": "raw/cellar/gc_citations.parquet",
    "subjects": "raw/cellar/gc_subjects.parquet",
    # CELLAR SPARQL outputs — high tier
    "joined_cases": "raw/cellar/gc_joined_cases.parquet",
    "appeals": "raw/cellar/gc_appeals.parquet",
    "annulled_acts": "raw/cellar/gc_annulled_acts.parquet",
    "interveners": "raw/cellar/gc_interveners.parquet",
    "ag_opinions": "raw/cellar/gc_ag_opinions.parquet",
    "legislation_links": "raw/cellar/gc_legislation_links.parquet",
    # CELLAR SPARQL outputs — medium tier
    "academic_citations": "raw/cellar/gc_academic_citations.parquet",
    "referring_judgments": "raw/cellar/gc_referring_judgments.parquet",
    # CELLAR SPARQL outputs — exhaustive tier
    "dossiers": "raw/cellar/gc_dossiers.parquet",
    "summaries": "raw/cellar/gc_summaries.parquet",
    "misc_info": "raw/cellar/gc_misc_info.parquet",
    "successors": "raw/cellar/gc_successors.parquet",
    "incorporates": "raw/cellar/gc_incorporates.parquet",
    # CELLAR SPARQL outputs — kitchen_sink tier
    "admin_metadata": "raw/cellar/gc_admin_metadata.parquet",
    # Header parser outputs (default location)
    "assignments": "assignments.parquet",
    "case_names": "case_names.parquet",
    # Merged / classified
    "decisions_enriched": "processed/decisions_enriched.parquet",
    "citations_for_classification": "processed/citations_for_classification.parquet",
}

JSONL_TABLES = {
    "header_metadata": "header_metadata.jsonl",
    "operative_parts": "operative_parts.jsonl",
    "citations_extracted": "processed/citations_extracted.jsonl",
    "classified_citations": "classified/classified_citations.jsonl",
    "judges_raw": "raw/judges/curia_members.jsonl",
    "judges_structured": "raw/judges/judges_structured.jsonl",
}


def _load_jsonl(path: str) -> pd.DataFrame:
    """Load a JSONL file into a DataFrame."""
    records = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
    return pd.DataFrame(records) if records else pd.DataFrame()


def _find_table(data_root: str, relative_path: str) -> Optional[str]:
    """Search for a table file under data_root or its parent."""
    candidate = os.path.join(data_root, relative_path)
    if os.path.exists(candidate):
        return candidate
    # Also check parent (for header parser outputs alongside xhtml/)
    parent = os.path.dirname(data_root)
    candidate = os.path.join(parent, relative_path)
    if os.path.exists(candidate):
        return candidate
    # Check data subdirectories
    for subdir in ["ag_divergence_full", "ag_divergence"]:
        candidate = os.path.join(data_root, subdir, relative_path)
        if os.path.exists(candidate):
            return candidate
    return None


def export_data(
    data_dir: str,
    output_dir: str,
    fmt: str = "csv",
    tables: Optional[list] = None,
) -> dict:
    """Export pipeline data as CSV or Excel files.

    Args:
        data_dir: Root data directory (usually 'data/')
        output_dir: Directory to write exported files
        fmt: Output format ('csv' or 'xlsx')
        tables: Specific table names to export (None = all found)

    Returns:
        Dict mapping table name to (path, row_count) for exported tables
    """
    os.makedirs(output_dir, exist_ok=True)
    exported = {}

    # Export Parquet tables
    for name, rel_path in PARQUET_TABLES.items():
        if tables and name not in tables:
            continue
        path = _find_table(data_dir, rel_path)
        if not path:
            continue
        try:
            df = pd.read_parquet(path)
            out_path = os.path.join(output_dir, f"{name}.{fmt}")
            if fmt == "xlsx":
                df.to_excel(out_path, index=False)
            else:
                df.to_csv(out_path, index=False)
            exported[name] = (out_path, len(df))
            logger.info(f"Exported {name}: {len(df)} rows -> {out_path}")
        except Exception as e:
            logger.warning(f"Failed to export {name}: {e}")

    # Export JSONL tables
    for name, rel_path in JSONL_TABLES.items():
        if tables and name not in tables:
            continue
        path = _find_table(data_dir, rel_path)
        if not path:
            continue
        try:
            df = _load_jsonl(path)
            if df.empty:
                continue
            # Flatten nested dicts/lists to strings for CSV compatibility
            for col in df.columns:
                if df[col].apply(lambda x: isinstance(x, (dict, list))).any():
                    df[col] = df[col].apply(
                        lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, (dict, list)) else x
                    )
            out_path = os.path.join(output_dir, f"{name}.{fmt}")
            if fmt == "xlsx":
                df.to_excel(out_path, index=False)
            else:
                df.to_csv(out_path, index=False)
            exported[name] = (out_path, len(df))
            logger.info(f"Exported {name}: {len(df)} rows -> {out_path}")
        except Exception as e:
            logger.warning(f"Failed to export {name}: {e}")

    return exported
