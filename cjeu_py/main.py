"""
cjeu-py — CLI entry point.

Usage:
    python -m cjeu_py.main <command> [options]

Commands:
    download-cellar      Fetch CJEU case-law metadata from CELLAR SPARQL
    download-cellar-meta Fetch extended metadata (joins, appeals, legislation links, etc.)
    fetch-texts          Download full judgment texts from EUR-Lex
    extract-citations    Extract citations from judgment texts
    parse-headers        Parse judgment XHTML headers (composition, parties, representatives)
    merge                Merge all data sources
    classify             Run LLM classification on citations
    validate             Export citations for human validation
    scrape-judges        Scrape judge biographical data from Curia
    extract-judge-bios   Extract structured data from judge bios via LLM
"""
import argparse
import json
import logging
import os
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("cjeu-py")


def cmd_download_cellar(args):
    """Fetch CJEU decision metadata and citations from CELLAR SPARQL.

    Uses disk cache by default: if Parquet files already exist, they are
    reused without hitting the network.  Pass --force to re-download.
    """
    import pandas as pd
    from cjeu_py.data_collection.cellar_client import CellarClient
    from cjeu_py import config

    client = CellarClient()
    force = args.force

    # ── Decisions ──────────────────────────────────────────────────────
    dec_path = os.path.join(config.RAW_CELLAR_DIR, "gc_decisions.parquet")
    if not force and os.path.exists(dec_path):
        decisions = pd.read_parquet(dec_path)
        logger.info(f"Cached: {len(decisions)} decisions ({dec_path})")
    else:
        logger.info(f"Downloading decisions from CELLAR (max_items={args.max_items})...")
        decisions = client.fetch_decisions(
            court=args.court,
            resource_type=args.resource_type,
            formation=args.formation,
            judge=args.judge,
            advocate_general=args.ag,
            date_from=args.date_from,
            date_to=args.date_to,
            max_items=args.max_items,
        )
        client.save_decisions(decisions)
        logger.info(f"Downloaded {len(decisions)} decisions")

    # ── Citations ──────────────────────────────────────────────────────
    if not args.skip_citations:
        cit_path = os.path.join(config.RAW_CELLAR_DIR, "gc_citations.parquet")
        if not force and os.path.exists(cit_path):
            logger.info(f"Cached: citations ({cit_path})")
        else:
            logger.info("Downloading citation network...")
            celex_list = decisions["celex"].tolist() if not decisions.empty else None
            citations = client.fetch_citations(
                celex_list=celex_list if args.max_items else None,
                max_items=args.max_items * 10 if args.max_items else None,
            )
            client.save_citations(citations)
            logger.info(f"Downloaded {len(citations)} citation pairs")

    # ── Subject matter ─────────────────────────────────────────────────
    if not args.skip_subjects:
        sub_path = os.path.join(config.RAW_CELLAR_DIR, "gc_subjects.parquet")
        if not force and os.path.exists(sub_path):
            logger.info(f"Cached: subjects ({sub_path})")
        else:
            logger.info("Downloading subject matter codes...")
            celex_list = decisions["celex"].tolist() if not decisions.empty else None
            subjects = client.fetch_subject_matter(
                celex_list=celex_list if args.max_items else None,
                max_items=args.max_items * 5 if args.max_items else None,
            )
            client.save_subject_matter(subjects)
            logger.info(f"Downloaded {len(subjects)} subject codes")

    if not force:
        logger.info("Tip: use --force to re-download all tables from CELLAR.")


def cmd_fetch_texts(args):
    """Download full judgment texts from EUR-Lex."""
    import pandas as pd
    from cjeu_py.data_collection.text_fetcher import fetch_texts
    from cjeu_py import config
    
    # Load decisions to get CELEX list
    dec_path = os.path.join(config.RAW_CELLAR_DIR, "gc_decisions.parquet")
    if not os.path.exists(dec_path):
        logger.error("No decisions data. Run download-cellar first.")
        return
    
    decisions = pd.read_parquet(dec_path)
    celex_list = decisions["celex"].dropna().unique().tolist()
    logger.info(f"Will fetch texts for {len(celex_list)} decisions")
    
    # Parse language preference (comma-separated, e.g. "eng,fra,deu")
    languages = tuple(lang.strip() for lang in args.lang.split(",") if lang.strip())
    fetch_texts(celex_list, max_items=args.max_items, concurrency=args.concurrency,
                languages=languages)


def cmd_extract_citations(args):
    """Extract citations from downloaded judgment texts."""
    import json as json_mod
    from cjeu_py.citation_extraction.regex_extractor import extract_citations_from_paragraphs
    from cjeu_py.citation_extraction.context_window import extract_context_windows
    from cjeu_py.utils.logging_utils import append_log
    from cjeu_py import config
    
    texts_path = os.path.join(config.RAW_TEXTS_DIR, "gc_texts.jsonl")
    output_path = os.path.join(config.PROCESSED_DIR, "citations_extracted.jsonl")
    os.makedirs(config.PROCESSED_DIR, exist_ok=True)
    
    if not os.path.exists(texts_path):
        logger.error("No texts downloaded. Run fetch-texts first.")
        return
    
    total_citations = 0
    total_docs = 0
    
    with open(texts_path, "r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                doc = json_mod.loads(line)
            except json_mod.JSONDecodeError:
                continue
            
            if doc.get("status") != "ok" or not doc.get("paragraphs"):
                continue
            
            celex = doc["celex"]
            raw_paras = doc["paragraphs"]
            para_nums = doc.get("paragraph_nums")

            # Reconstruct [{num, text}] dicts from JSONL storage format
            if raw_paras and isinstance(raw_paras[0], str):
                if para_nums and len(para_nums) == len(raw_paras):
                    paragraphs = [{"num": n, "text": t} for n, t in zip(para_nums, raw_paras)]
                else:
                    paragraphs = [{"num": i + 1, "text": t} for i, t in enumerate(raw_paras)]
            else:
                paragraphs = raw_paras  # already in dict format

            # Extract citations
            citations = extract_citations_from_paragraphs(paragraphs, citing_celex=celex)

            # Enrich with context windows
            citations = extract_context_windows(paragraphs, citations, window_size=args.window)
            
            for cit in citations:
                append_log(output_path, cit)
                total_citations += 1
            
            total_docs += 1
    
    logger.info(f"Extracted {total_citations} citations from {total_docs} documents → {output_path}")


def cmd_merge(args):
    """Merge all data sources into classification-ready dataset."""
    from cjeu_py.data_collection.merge_sources import merge_all
    
    result = merge_all()
    if not result.empty:
        logger.info(f"Merged dataset: {len(result)} rows")
    else:
        logger.warning("Merge produced empty dataset")


def cmd_classify(args):
    """Run LLM classification on extracted citations."""
    import pandas as pd
    from cjeu_py.classification.classifier import classify_single_citation, configure_provider
    from cjeu_py.classification.pipeline import run_classification_pipeline
    from cjeu_py import config

    # Configure LLM provider
    configure_provider(
        provider=getattr(args, "provider", "gemini"),
        model=getattr(args, "model", None),
        api_base=getattr(args, "api_base", None),
        api_key=getattr(args, "api_key", None),
    )
    
    # Load citations for classification
    cit_path = os.path.join(config.PROCESSED_DIR, "citations_for_classification.parquet")
    if not os.path.exists(cit_path):
        logger.error("No citations ready for classification. Run merge first.")
        return
    
    df = pd.read_parquet(cit_path)
    items = df.to_dict(orient="records")
    
    output_path = os.path.join(config.CLASSIFIED_DIR, "classified_citations.jsonl")
    os.makedirs(config.CLASSIFIED_DIR, exist_ok=True)
    
    stats = run_classification_pipeline(
        items=items,
        classify_func=classify_single_citation,
        output_path=output_path,
        id_field="citing_celex",
        max_workers=args.max_workers,
        max_items=args.max_items,
    )
    
    logger.info(f"Classification complete: {json.dumps(stats, default=str)}")


def cmd_validate(args):
    """Export stratified sample for human validation."""
    from cjeu_py.classification.human_validation import export_validation_sample
    
    export_validation_sample(
        sample_size=args.sample_size,
        output_path=args.output,
    )


def cmd_download_cellar_meta(args):
    """Fetch extended CELLAR metadata at configurable detail levels.

    Uses disk cache by default.  Pass --force to re-download.

    --detail high:        procedural links + legislation links + AG opinion links + case names
    --detail medium:      (default) adds academic citations + referring judgments
    --detail all:         adds rare legislation link types
    --detail exhaustive:  adds dossiers, summaries, misc info, successors, incorporates
    --detail kitchen_sink: adds every remaining CDM property
    """
    import pandas as pd
    from cjeu_py.data_collection.cellar_client import CellarClient
    from cjeu_py import config

    client = CellarClient()
    detail = args.detail
    max_items = args.max_items
    force = args.force

    # Optionally filter to CELEX list from decisions parquet
    celex_list = None
    dec_path = os.path.join(config.RAW_CELLAR_DIR, "gc_decisions.parquet")
    if os.path.exists(dec_path):
        decisions = pd.read_parquet(dec_path)
        if not decisions.empty:
            celex_list = decisions["celex"].dropna().unique().tolist()
            logger.info(f"Filtering to {len(celex_list)} decisions from {dec_path}")

    # Map task name → (fetch_fn, save_fn, output_filename)
    high_tasks = [
        ("joined cases", client.fetch_joined_cases, client.save_joined_cases,
         "gc_joined_cases.parquet"),
        ("appeals", client.fetch_appeals, client.save_appeals,
         "gc_appeals.parquet"),
        ("interveners", client.fetch_interveners, client.save_interveners,
         "gc_interveners.parquet"),
        ("annulled acts", client.fetch_annulled_acts, client.save_annulled_acts,
         "gc_annulled_acts.parquet"),
        ("case names", client.fetch_case_names, client.save_case_names,
         "case_names.parquet"),
    ]

    # ── HIGH: procedural links + case names ────────────────────────────
    for name, fetch_fn, save_fn, filename in high_tasks:
        path = os.path.join(config.RAW_CELLAR_DIR, filename)
        if not force and os.path.exists(path):
            logger.info(f"Cached: {name} ({path})")
            continue
        logger.info(f"Downloading {name}...")
        df = fetch_fn(celex_list=celex_list, max_items=max_items)
        save_fn(df)
        logger.info(f"Downloaded {len(df)} {name}")

    # AG opinion links
    ag_path = os.path.join(config.RAW_CELLAR_DIR, "gc_ag_opinions.parquet")
    if not force and os.path.exists(ag_path):
        logger.info(f"Cached: AG opinions ({ag_path})")
    else:
        logger.info("Downloading AG opinion links...")
        ag = client.fetch_ag_opinions(celex_list=celex_list, max_items=max_items)
        client.save_ag_opinions(ag)
        logger.info(f"Downloaded {len(ag)} AG opinion links")

    # Legislation links (high-value types; --detail all+ adds rare types)
    leg_path = os.path.join(config.RAW_CELLAR_DIR, "gc_legislation_links.parquet")
    include_low = detail in ("all", "exhaustive", "kitchen_sink")
    if not force and os.path.exists(leg_path) and not include_low:
        logger.info(f"Cached: legislation links ({leg_path})")
    else:
        logger.info(f"Downloading legislation links (include_low={include_low})...")
        leg = client.fetch_legislation_links(
            celex_list=celex_list, max_items=max_items, include_low=include_low)
        client.save_legislation_links(leg)
        logger.info(f"Downloaded {len(leg)} legislation links")

    if detail == "high":
        logger.info("Detail level: high. Skipping academic citations and referring judgments.")
        return

    # ── MEDIUM: academic citations + referring judgments ───────────────
    acad_path = os.path.join(config.RAW_CELLAR_DIR, "gc_academic_citations.parquet")
    if not force and os.path.exists(acad_path):
        logger.info(f"Cached: academic citations ({acad_path})")
    else:
        logger.info("Downloading academic citations...")
        acad = client.fetch_academic_citations(celex_list=celex_list, max_items=max_items)
        client.save_academic_citations(acad)
        logger.info(f"Downloaded {len(acad)} academic citations")

    ref_path = os.path.join(config.RAW_CELLAR_DIR, "gc_referring_judgments.parquet")
    if not force and os.path.exists(ref_path):
        logger.info(f"Cached: referring judgments ({ref_path})")
    else:
        logger.info("Downloading referring national judgments...")
        ref = client.fetch_referring_judgments(celex_list=celex_list, max_items=max_items)
        client.save_referring_judgments(ref)
        logger.info(f"Downloaded {len(ref)} referring judgments")

    if detail in ("medium", "high"):
        logger.info(f"Detail level: {detail}. Extended metadata complete.")
        if not force:
            logger.info("Tip: use --force to re-download all tables from CELLAR.")
        return

    if detail == "all":
        logger.info("Detail level: all. Extended metadata complete.")
        if not force:
            logger.info("Tip: use --force to re-download all tables from CELLAR.")
        return

    # ── EXHAUSTIVE: supplementary research data ──────────────────────
    exhaustive_tasks = [
        ("dossiers", client.fetch_dossiers, client.save_dossiers,
         "gc_dossiers.parquet"),
        ("summaries", client.fetch_summaries, client.save_summaries,
         "gc_summaries.parquet"),
        ("misc info", client.fetch_misc_info, client.save_misc_info,
         "gc_misc_info.parquet"),
        ("successors", client.fetch_successors, client.save_successors,
         "gc_successors.parquet"),
        ("incorporates", client.fetch_incorporates, client.save_incorporates,
         "gc_incorporates.parquet"),
    ]

    for name, fetch_fn, save_fn, filename in exhaustive_tasks:
        path = os.path.join(config.RAW_CELLAR_DIR, filename)
        if not force and os.path.exists(path):
            logger.info(f"Cached: {name} ({path})")
            continue
        logger.info(f"Downloading {name}...")
        df = fetch_fn(celex_list=celex_list, max_items=max_items)
        save_fn(df)
        logger.info(f"Downloaded {len(df)} {name}")

    if detail == "exhaustive":
        logger.info("Detail level: exhaustive. Extended metadata complete.")
        if not force:
            logger.info("Tip: use --force to re-download all tables from CELLAR.")
        return

    # ── KITCHEN_SINK: every remaining CDM property ──────────────────
    admin_path = os.path.join(config.RAW_CELLAR_DIR, "gc_admin_metadata.parquet")
    if not force and os.path.exists(admin_path):
        logger.info(f"Cached: admin metadata ({admin_path})")
    else:
        logger.info("Downloading admin metadata (all remaining CDM properties)...")
        admin = client.fetch_admin_metadata(celex_list=celex_list, max_items=max_items)
        client.save_admin_metadata(admin)
        logger.info(f"Downloaded {len(admin)} admin metadata entries")

    logger.info("Detail level: kitchen_sink. All CDM properties collected.")
    if not force:
        logger.info("Tip: use --force to re-download all tables from CELLAR.")


def cmd_parse_headers(args):
    """Parse judgment headers from cached XHTML files."""
    from cjeu_py.data_collection.judgment_header import (
        parse_all_headers, flatten_assignments, derive_case_names,
        extract_operative_part,
    )

    xhtml_dir = args.xhtml_dir
    if not os.path.isdir(xhtml_dir):
        logger.error(f"Directory not found: {xhtml_dir}")
        return

    output_dir = os.path.normpath(args.output or os.path.join(xhtml_dir, ".."))
    os.makedirs(output_dir, exist_ok=True)

    header_path = os.path.join(output_dir, "header_metadata.jsonl")
    df = parse_all_headers(xhtml_dir, output_path=header_path, limit=args.limit)
    logger.info(f"Parsed {len(df)} judgment headers → {header_path}")

    if df.empty:
        return

    headers = df.to_dict(orient="records")

    # Assignments (judge per decision)
    assignments = flatten_assignments(headers)
    if not assignments.empty:
        apath = os.path.join(output_dir, "assignments.parquet")
        assignments.to_parquet(apath, index=False)
        logger.info(f"Saved {len(assignments)} assignments → {apath}")

    # Case names
    case_names = derive_case_names(headers)
    if not case_names.empty:
        cpath = os.path.join(output_dir, "case_names.parquet")
        case_names.to_parquet(cpath, index=False)
        logger.info(f"Saved {len(case_names)} case names → {cpath}")

    # Operative parts (judgments only)
    op_count = 0
    op_path = os.path.join(output_dir, "operative_parts.jsonl")
    import json as json_mod
    with open(op_path, "w", encoding="utf-8") as f:
        files = sorted(fn for fn in os.listdir(xhtml_dir) if fn.endswith(".xhtml"))
        if args.limit > 0:
            files = files[:args.limit]
        for fname in files:
            celex = fname.replace(".xhtml", "")
            with open(os.path.join(xhtml_dir, fname), "r", encoding="utf-8") as xf:
                op = extract_operative_part(xf.read())
            if op:
                f.write(json_mod.dumps({"celex": celex, "operative_part": op},
                                       ensure_ascii=False) + "\n")
                op_count += 1
    logger.info(f"Saved {op_count} operative parts → {op_path}")


def cmd_scrape_judges(args):
    """Scrape judge biographical data from Curia.europa.eu."""
    from cjeu_py.data_collection.curia_scraper import scrape_judges

    output_path = args.output or os.path.join("data", "raw", "judges", "curia_members.jsonl")
    cache_dir = args.cache_dir

    df = scrape_judges(output_path=output_path, cache_dir=cache_dir)
    logger.info(
        f"Scraped {len(df)} members ({df['is_current'].sum()} current, "
        f"{(~df['is_current']).sum()} former)"
    )


def cmd_extract_judge_bios(args):
    """Extract structured biographical data from scraped bios via LLM."""
    from cjeu_py.data_collection.curia_scraper import extract_judge_bios

    members_path = args.input or os.path.join("data", "raw", "judges", "curia_members.jsonl")
    output_path = args.output or os.path.join("data", "raw", "judges", "judges_structured.jsonl")

    if not os.path.exists(members_path):
        logger.error(f"No members data. Run scrape-judges first. ({members_path})")
        return

    df = extract_judge_bios(members_path, output_path, max_items=args.max_items or 0)
    logger.info(f"Extracted structured bios for {len(df)} members → {output_path}")


def cmd_export(args):
    """Export pipeline data as CSV or Excel files."""
    from cjeu_py.export import export_data
    from cjeu_py import config

    data_dir = args.data_dir or config.DATA_ROOT
    output_dir = args.output or os.path.join(data_dir, "export")
    fmt = args.format or "csv"

    exported = export_data(data_dir, output_dir, fmt=fmt)
    if exported:
        logger.info(f"Exported {len(exported)} tables to {output_dir}/ ({fmt})")
        for name, (path, n) in exported.items():
            logger.info(f"  {name}: {n} rows")
    else:
        logger.warning("No pipeline data found to export. Run the pipeline first.")


def cmd_codebook(args):
    """Generate a codebook (variable definitions) for all pipeline tables."""
    from cjeu_py.codebook import write_codebook

    output_path = args.output
    path = write_codebook(output_path)
    logger.info(f"Codebook written to {path}")


def cmd_export_network(args):
    """Export the citation network as GEXF or D3.js JSON."""
    from cjeu_py.network_export import export_network
    from cjeu_py import config

    data_dir = args.data_dir or config.DATA_ROOT
    fmt = args.format or "gexf"

    # Default output path
    if args.output:
        output_path = args.output
    else:
        ext = {"gexf": "gexf", "d3": "json", "html": "html"}[fmt]
        output_dir = os.path.join(data_dir, "export")
        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, f"citation_network.{ext}")

    path = export_network(
        data_dir=data_dir,
        output_path=output_path,
        fmt=fmt,
        topic=args.topic,
        formation=args.formation,
        court=getattr(args, "court", None),
        date_from=args.date_from,
        date_to=args.date_to,
        include_legislation=args.include_legislation,
        max_nodes=args.max_nodes,
    )
    if path:
        logger.info(f"Network exported to {path}")


def cmd_search(args):
    """Search collected case-law data."""
    from cjeu_py.search import run_search
    from cjeu_py import config

    data_dir = getattr(args, "data_dir", None) or config.DATA_ROOT

    output = run_search(
        data_dir=data_dir,
        mode=args.mode,
        query=args.query or "",
        limit=args.limit,
        fmt=args.format,
        date_from=args.date_from,
        date_to=args.date_to,
        court=args.court,
        verbose=args.verbose,
    )
    print(output)


def cmd_analyze(args):
    """Run analysis scripts (stub)."""
    logger.info("Analysis module not yet implemented.")
    logger.info("See experiments/ for available research scripts.")


# ── CLI argument parser ───────────────────────────────────────────────────

def build_parser():
    parser = argparse.ArgumentParser(
        prog="cjeu-py",
        description="cjeu-py — Python toolkit for empirical CJEU research",
    )
    subparsers = parser.add_subparsers(dest="command", help="Available commands")
    
    # download-cellar
    p = subparsers.add_parser("download-cellar", help="Fetch data from CELLAR SPARQL")
    p.add_argument("--max-items", type=int, default=None, help="Max decisions to fetch")
    p.add_argument("--court", type=str, default=None, help="Court filter: CJ, TJ, FJ")
    p.add_argument("--resource-type", type=str, default=None, help="Type: JUDG, ORDER, etc.")
    p.add_argument("--formation", type=str, default=None,
                   help="Court formation filter (substring match, e.g. GRAND_CH)")
    p.add_argument("--judge", type=str, default=None,
                   help="Judge-rapporteur name filter (case-insensitive substring)")
    p.add_argument("--ag", type=str, default=None,
                   help="Advocate general name filter (case-insensitive substring)")
    p.add_argument("--date-from", type=str, default=None,
                   help="Earliest decision date (YYYY-MM-DD)")
    p.add_argument("--date-to", type=str, default=None,
                   help="Latest decision date (YYYY-MM-DD)")
    p.add_argument("--skip-citations", action="store_true", help="Skip citation network download")
    p.add_argument("--skip-subjects", action="store_true", help="Skip subject matter download")
    p.add_argument("--force", action="store_true",
                   help="Re-download from CELLAR even if local data exists")
    
    # fetch-texts
    p = subparsers.add_parser("fetch-texts", help="Download full texts from EUR-Lex")
    p.add_argument("--max-items", type=int, default=None, help="Max texts to fetch")
    p.add_argument("--concurrency", type=int, default=20, help="Concurrent requests")
    p.add_argument("--lang", type=str, default="eng",
                   help="Language preference (comma-separated ISO 639-2/B codes, e.g. eng,fra,deu). "
                        "Tries each in order; uses the first available. Default: eng")
    
    # extract-citations
    p = subparsers.add_parser("extract-citations", help="Extract citations from texts")
    p.add_argument("--window", type=int, default=1, help="Context window size (paragraphs)")
    
    # merge
    subparsers.add_parser("merge", help="Merge all data sources")
    
    # classify
    p = subparsers.add_parser("classify", help="Run LLM classification")
    p.add_argument("--max-items", type=int, default=None, help="Max citations to classify")
    p.add_argument("--max-workers", type=int, default=None,
                   help="Parallel LLM API workers (default: 5, safe for free tier. "
                        "Tier 2 Gemini keys can use 50-100)")
    p.add_argument("--provider", type=str, default="gemini",
                   choices=["gemini", "openai"],
                   help="LLM provider: gemini (default) or openai "
                        "(any OpenAI-compatible API: Ollama, vLLM, llama.cpp)")
    p.add_argument("--model", type=str, default=None,
                   help="Model name (default: gemini-2.5-flash / gemma2)")
    p.add_argument("--api-base", type=str, default=None,
                   help="API base URL for --provider openai "
                        "(default: http://localhost:11434/v1)")
    p.add_argument("--api-key", type=str, default=None,
                   help="API key for --provider openai")
    
    # validate
    p = subparsers.add_parser("validate", help="Export human validation sample")
    p.add_argument("--sample-size", type=int, default=200, help="Number of citations to sample")
    p.add_argument("--output", type=str, default=None, help="Output CSV path")
    
    # download-cellar-meta
    p = subparsers.add_parser("download-cellar-meta",
                              help="Fetch extended metadata (joins, appeals, legislation links, etc.)")
    p.add_argument("--max-items", type=int, default=None, help="Max items per query")
    p.add_argument("--detail", type=str, default="medium",
                   choices=["high", "medium", "all", "exhaustive", "kitchen_sink"],
                   help="Detail level: high (procedural + legislation links), "
                        "medium (+ academic citations + referring judgments, default), "
                        "all (+ rare link types), "
                        "exhaustive (+ dossiers, summaries, misc info, successors, incorporates), "
                        "kitchen_sink (every remaining CDM property)")
    p.add_argument("--force", action="store_true",
                   help="Re-download from CELLAR even if local data exists")

    # parse-headers
    p = subparsers.add_parser("parse-headers", help="Parse judgment XHTML headers")
    p.add_argument("xhtml_dir", type=str, help="Directory containing .xhtml files")
    p.add_argument("--output", type=str, default=None, help="Output directory")
    p.add_argument("--limit", type=int, default=0, help="Max files to parse (0 = all)")

    # scrape-judges
    p = subparsers.add_parser("scrape-judges", help="Scrape judge bios from Curia")
    p.add_argument("--output", type=str, default=None, help="Output JSONL path")
    p.add_argument("--cache-dir", type=str, default=None, help="Cache raw HTML here")

    # extract-judge-bios
    p = subparsers.add_parser("extract-judge-bios", help="Extract structured bios via LLM")
    p.add_argument("--input", type=str, default=None, help="Input JSONL from scrape-judges")
    p.add_argument("--output", type=str, default=None, help="Output JSONL path")
    p.add_argument("--max-items", type=int, default=None, help="Max members to process")

    # export
    p = subparsers.add_parser("export", help="Export pipeline data as CSV or Excel")
    p.add_argument("--data-dir", type=str, default=None, help="Data directory")
    p.add_argument("--output", type=str, default=None, help="Output directory")
    p.add_argument("--format", type=str, default="csv", choices=["csv", "xlsx"], help="Output format")

    # export-network
    p = subparsers.add_parser("export-network",
                              help="Export citation network as GEXF (Gephi) or D3.js JSON")
    p.add_argument("--format", type=str, default="gexf", choices=["gexf", "d3", "html"],
                   help="Output format: gexf (Gephi/Gephi Lite), d3 (JSON), or html (interactive browser)")
    p.add_argument("--output", type=str, default=None, help="Output file path")
    p.add_argument("--data-dir", type=str, default=None, help="Data directory")
    p.add_argument("--topic", type=str, default=None,
                   help="Filter by subject matter (substring match)")
    p.add_argument("--formation", type=str, default=None,
                   help="Filter by court formation (e.g. GRAND_CH)")
    p.add_argument("--court", type=str, default=None,
                   help="Filter by court: CJ, TJ, FJ")
    p.add_argument("--date-from", type=str, default=None,
                   help="Earliest decision date (YYYY-MM-DD)")
    p.add_argument("--date-to", type=str, default=None,
                   help="Latest decision date (YYYY-MM-DD)")
    p.add_argument("--include-legislation", action="store_true",
                   help="Include citations to legislation/treaties (default: case-law only)")
    p.add_argument("--max-nodes", type=int, default=None,
                   help="Limit node count (keeps top N by PageRank). "
                        "Recommended for D3.js: 3000, Gephi Lite: 5000")

    # codebook
    p = subparsers.add_parser("codebook", help="Generate variable codebook (Markdown)")
    p.add_argument("--output", type=str, default=None, help="Output file path")

    # search
    p = subparsers.add_parser("search", help="Search collected case-law data")
    p.add_argument("mode", type=str,
                   choices=["text", "headnote", "party", "citing", "cited-by",
                            "topic", "legislation", "list"],
                   help="Search mode (headnote queries CELLAR live, "
                        "all others search local data)")
    p.add_argument("query", type=str, nargs="?", default="",
                   help="Search query (text/headnote/party/topic: substring, "
                        "citing/cited-by: CELEX, legislation: CELEX, "
                        "list: topics/judges/ags/formations/procedures)")
    p.add_argument("--limit", type=int, default=25, help="Max results (default: 25)")
    p.add_argument("--format", type=str, default="table",
                   choices=["table", "csv", "json"],
                   help="Output format (default: table)")
    p.add_argument("--date-from", type=str, default=None,
                   help="Earliest decision date (YYYY-MM-DD)")
    p.add_argument("--date-to", type=str, default=None,
                   help="Latest decision date (YYYY-MM-DD)")
    p.add_argument("--court", type=str, default=None,
                   help="Court filter: CJ, TJ, FJ")
    p.add_argument("--data-dir", type=str, default=None,
                   help="Data directory override")
    p.add_argument("--verbose", action="store_true",
                   help="Show full text snippets (text mode)")

    # analyze
    subparsers.add_parser("analyze", help="Run analysis scripts")

    return parser


def main():
    parser = build_parser()
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        sys.exit(1)
    
    command_map = {
        "download-cellar": cmd_download_cellar,
        "download-cellar-meta": cmd_download_cellar_meta,
        "fetch-texts": cmd_fetch_texts,
        "extract-citations": cmd_extract_citations,
        "merge": cmd_merge,
        "classify": cmd_classify,
        "validate": cmd_validate,
        "parse-headers": cmd_parse_headers,
        "scrape-judges": cmd_scrape_judges,
        "extract-judge-bios": cmd_extract_judge_bios,
        "export": cmd_export,
        "export-network": cmd_export_network,
        "codebook": cmd_codebook,
        "search": cmd_search,
        "analyze": cmd_analyze,
    }
    
    func = command_map.get(args.command)
    if func:
        func(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
