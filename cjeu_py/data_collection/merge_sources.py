"""
Merge CELLAR metadata and extracted texts/citations
into a unified dataset for classification.
"""
import os
import json
import logging
import pandas as pd
from typing import Optional

from cjeu_py import config

logger = logging.getLogger(__name__)


def load_cellar_decisions(path: str = None) -> pd.DataFrame:
    """Load CELLAR decisions Parquet."""
    path = path or os.path.join(config.RAW_CELLAR_DIR, "gc_decisions.parquet")
    if not os.path.exists(path):
        logger.warning(f"No CELLAR decisions found at {path}")
        return pd.DataFrame()
    return pd.read_parquet(path)


def load_cellar_citations(path: str = None) -> pd.DataFrame:
    """Load CELLAR citations Parquet."""
    path = path or os.path.join(config.RAW_CELLAR_DIR, "gc_citations.parquet")
    if not os.path.exists(path):
        logger.warning(f"No CELLAR citations found at {path}")
        return pd.DataFrame()
    return pd.read_parquet(path)


def load_texts(path: str = None) -> pd.DataFrame:
    """Load texts JSONL as DataFrame."""
    path = path or os.path.join(config.RAW_TEXTS_DIR, "gc_texts.jsonl")
    if not os.path.exists(path):
        logger.warning(f"No texts found at {path}")
        return pd.DataFrame()
    
    records = []
    with open(path, "r") as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    rec = json.loads(line)
                    # Don't store full paragraphs in the merged table (too large)
                    rec_slim = {k: v for k, v in rec.items() if k != "paragraphs"}
                    records.append(rec_slim)
                except json.JSONDecodeError:
                    continue
    return pd.DataFrame(records)


def load_extracted_citations(path: str = None) -> pd.DataFrame:
    """Load extracted citations JSONL."""
    path = path or os.path.join(config.PROCESSED_DIR, "citations_extracted.jsonl")
    if not os.path.exists(path):
        logger.warning(f"No extracted citations at {path}")
        return pd.DataFrame()
    
    records = []
    with open(path, "r") as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
    return pd.DataFrame(records)


def merge_all(output_dir: str = None) -> pd.DataFrame:
    """
    Merge all data sources into a unified dataset.
    
    Produces:
    - decisions_enriched.parquet: CELLAR decisions + text stats
    - citations_for_classification.parquet: extracted citations + metadata
    """
    output_dir = output_dir or config.PROCESSED_DIR
    os.makedirs(output_dir, exist_ok=True)
    
    # Load sources
    decisions = load_cellar_decisions()
    texts = load_texts()
    citations_extracted = load_extracted_citations()
    
    if decisions.empty:
        logger.error("No decisions data available. Run download-cellar first.")
        return pd.DataFrame()
    
    # Merge text stats into decisions
    if not texts.empty and "celex" in texts.columns:
        text_stats = texts[["celex", "status", "paragraph_count", "char_count"]].copy()
        decisions = decisions.merge(text_stats, on="celex", how="left")
    
    # Save enriched decisions
    dec_path = os.path.join(output_dir, "decisions_enriched.parquet")
    decisions.to_parquet(dec_path, index=False)
    logger.info(f"Saved {len(decisions)} enriched decisions to {dec_path}")
    
    # Merge citation metadata for classification
    if not citations_extracted.empty and not decisions.empty:
        # Join decision metadata onto extracted citations
        meta_cols = ["celex", "ecli", "date", "formation_code", "procedure_type", 
                     "judge_rapporteur", "advocate_general"]
        meta_cols = [c for c in meta_cols if c in decisions.columns]
        
        if meta_cols:
            cit_enriched = citations_extracted.merge(
                decisions[meta_cols],
                left_on="citing_celex",
                right_on="celex",
                how="left",
                suffixes=("", "_dec")
            )
        else:
            cit_enriched = citations_extracted
        
        # Rename for downstream
        if "date" in cit_enriched.columns:
            cit_enriched = cit_enriched.rename(columns={"date": "citing_date"})
        if "formation_code" in cit_enriched.columns:
            cit_enriched = cit_enriched.rename(columns={"formation_code": "formation"})
        
        cit_path = os.path.join(output_dir, "citations_for_classification.parquet")
        cit_enriched.to_parquet(cit_path, index=False)
        logger.info(f"Saved {len(cit_enriched)} citations for classification to {cit_path}")
        
        return cit_enriched
    
    return decisions
