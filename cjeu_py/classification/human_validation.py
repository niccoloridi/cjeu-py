"""
Human validation — export stratified sample and compute inter-rater agreement.
"""
import os
import json
import logging
import random
import pandas as pd
from typing import Optional

from cjeu_py import config

logger = logging.getLogger(__name__)


def export_validation_sample(
    sample_size: int = 200,
    input_path: str = None,
    output_path: str = None,
    stratify_by: str = "precision",
    seed: int = 42,
):
    """
    Export a stratified random sample of classified citations for human review.
    
    The output CSV includes all classification fields plus the context text,
    with empty columns for the human coder to fill in.
    """
    input_path = input_path or os.path.join(config.CLASSIFIED_DIR, "classified_citations.jsonl")
    output_path = output_path or os.path.join(config.CLASSIFIED_DIR, "human_validation_sample.csv")
    
    if not os.path.exists(input_path):
        logger.error(f"No classified citations at {input_path}. Run classify first.")
        return
    
    # Load classified citations
    records = []
    with open(input_path, "r") as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    rec = json.loads(line)
                    if not rec.get("_meta", {}).get("error"):
                        records.append(rec)
                except json.JSONDecodeError:
                    continue
    
    df = pd.DataFrame(records)
    logger.info(f"Loaded {len(df)} classified citations")
    
    if len(df) == 0:
        logger.error("No valid classified citations found")
        return
    
    # Stratified sampling
    random.seed(seed)
    
    if stratify_by in df.columns:
        # Sample proportionally from each stratum
        sample = df.groupby(stratify_by, group_keys=False).apply(
            lambda x: x.sample(
                n=min(len(x), max(1, int(sample_size * len(x) / len(df)))),
                random_state=seed,
            )
        )
        # Top up if we didn't reach sample_size
        if len(sample) < sample_size:
            remaining = df[~df.index.isin(sample.index)]
            extra = remaining.sample(
                n=min(len(remaining), sample_size - len(sample)),
                random_state=seed,
            )
            sample = pd.concat([sample, extra])
    else:
        sample = df.sample(n=min(len(df), sample_size), random_state=seed)
    
    # Add human coding columns
    sample["human_precision"] = ""
    sample["human_use"] = ""
    sample["human_treatment"] = ""
    sample["human_topic"] = ""
    sample["human_notes"] = ""
    
    # Select and order columns
    key_cols = [
        "citing_celex", "citation_string", "paragraph_num",
        "citing_paragraph_text", "context_text",
        "precision", "use", "treatment", "topic", "confidence", "reasoning",
        "human_precision", "human_use", "human_treatment", "human_topic", "human_notes",
    ]
    available_cols = [c for c in key_cols if c in sample.columns]
    sample = sample[available_cols]
    
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    sample.to_csv(output_path, index=False)
    logger.info(f"Exported {len(sample)} citations for human validation → {output_path}")


def compute_agreement(
    validation_path: str = None,
) -> dict:
    """
    Compute Cohen's kappa between LLM and human codings.
    
    Returns dict with kappa scores per dimension.
    """
    validation_path = validation_path or os.path.join(
        config.CLASSIFIED_DIR, "human_validation_sample.csv"
    )
    
    if not os.path.exists(validation_path):
        logger.error(f"Validation file not found: {validation_path}")
        return {}
    
    df = pd.read_csv(validation_path)
    
    results = {}
    dimensions = ["precision", "use", "treatment", "topic"]
    
    for dim in dimensions:
        llm_col = dim
        human_col = f"human_{dim}"
        
        if llm_col not in df.columns or human_col not in df.columns:
            continue
        
        # Filter to rows where human has coded
        coded = df[(df[human_col].notna()) & (df[human_col] != "")]
        
        if len(coded) < 5:
            logger.warning(f"Too few human codings for {dim}: {len(coded)}")
            continue
        
        try:
            from sklearn.metrics import cohen_kappa_score
            kappa = cohen_kappa_score(coded[llm_col], coded[human_col])
            results[dim] = {
                "kappa": round(kappa, 3),
                "n": len(coded),
                "agreement_pct": round(
                    (coded[llm_col] == coded[human_col]).mean() * 100, 1
                ),
            }
            logger.info(f"  {dim}: κ={kappa:.3f} (n={len(coded)})")
        except Exception as e:
            logger.warning(f"Could not compute kappa for {dim}: {e}")
    
    return results
