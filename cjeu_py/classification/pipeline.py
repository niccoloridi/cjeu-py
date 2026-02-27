"""
Generic classification pipeline — adapted from ECHR Lawyers.

ThreadPoolExecutor with JSONL checkpointing, cost tracking,
tqdm progress bar, rate-limit throttling, and progressive writes.

Key design: collects completed futures DURING submission (not after),
so results are written to disk immediately as they finish.
"""
import os
import time
import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Callable, Dict, List
from tqdm import tqdm

from cjeu_py import config
from cjeu_py.utils.logging_utils import load_existing_log, append_log, backup_file_if_exists

logger = logging.getLogger(__name__)


def estimate_cost(input_tokens: int, output_tokens: int, thinking_tokens: int = 0) -> float:
    """Estimate API cost in USD (includes thinking tokens if any)."""
    return (
        (input_tokens / 1_000_000) * config.PRICE_PER_M_INPUT +
        (output_tokens / 1_000_000) * config.PRICE_PER_M_OUTPUT +
        (thinking_tokens / 1_000_000) * config.PRICE_PER_M_THINKING
    )


def _collect_done(futures, output_path, id_field, stats, pbar):
    """Collect all completed futures, write to disk, update stats/progress."""
    done = [f for f in futures if f.done()]
    for future in done:
        item = futures.pop(future)
        item_id = item.get(id_field, "unknown") if isinstance(item, dict) else str(item)

        try:
            result = future.result()

            meta = result.get("_meta", {})
            inp = meta.get("input_tokens", 0) or 0
            out = meta.get("output_tokens", 0) or 0
            think = meta.get("thinking_tokens", 0) or 0
            stats["input_tokens"] += inp
            stats["output_tokens"] += out
            stats["thinking_tokens"] += think

            if meta.get("error"):
                stats["errors"] += 1

            append_log(output_path, result)

        except Exception as e:
            logger.error(f"Error processing {item_id}: {e}")
            stats["errors"] += 1
            append_log(output_path, {
                id_field: item_id,
                "_meta": {"error": str(e)},
            })

        cost = estimate_cost(stats["input_tokens"], stats["output_tokens"], stats["thinking_tokens"])
        pbar.update(1)
        pbar.set_postfix(
            cost=f"${cost:.3f}",
            err=stats["errors"],
            tok=f"{(stats['input_tokens'] + stats['output_tokens']) / 1000:.0f}k",
            refresh=False,
        )


def run_classification_pipeline(
    items: List[Dict],
    classify_func: Callable[[Dict], Dict],
    output_path: str,
    id_field: str = "celex",
    max_workers: int = None,
    max_items: int = None,
    submit_delay: float = None,
) -> Dict:
    """
    Execute classification across items using a thread pool.

    Follows the ECHR Lawyers dual-collection pattern:
    - Submits tasks with throttling (submit_delay between each)
    - Collects completed futures DURING submission (progressive writes)
    - Drains remaining futures after all submissions

    Args:
        items: List of input dicts (each must have the id_field)
        classify_func: Function that takes a dict and returns a classified dict
        output_path: Path to JSONL output file (checkpoint + results)
        id_field: Field name for unique IDs
        max_workers: Number of parallel workers
        max_items: Limit to this many items
        submit_delay: Seconds between task submissions

    Returns:
        Stats dict: {total, processed, errors, cost_usd, duration_s}
    """
    max_workers = max_workers or config.GEMINI_MAX_WORKERS
    submit_delay = submit_delay or config.GEMINI_SUBMIT_DELAY

    # Checkpoint: skip already-processed items
    processed_ids = load_existing_log(output_path, id_field=id_field)
    remaining = [item for item in items if str(item.get(id_field, "")) not in processed_ids]

    if max_items:
        remaining = remaining[:max_items]

    logger.info(
        f"Pipeline: {len(remaining)} items to process "
        f"({len(processed_ids)} already done, {max_workers} workers)"
    )

    if not remaining:
        logger.info("Nothing to process.")
        return {"total": len(items), "processed": 0, "errors": 0, "cost_usd": 0, "duration_s": 0}

    # Backup existing output
    backup_file_if_exists(output_path)

    stats = {"input_tokens": 0, "output_tokens": 0, "thinking_tokens": 0, "errors": 0}
    start_time = time.time()

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {}
        pbar = tqdm(total=len(remaining), desc="Classifying")

        # Phase 1: Submit tasks with throttling, collecting results as they finish
        for item in remaining:
            future = executor.submit(classify_func, item)
            futures[future] = item
            time.sleep(submit_delay)

            # Collect any completed futures while submitting
            _collect_done(futures, output_path, id_field, stats, pbar)

        # Phase 2: Drain remaining futures
        for future in as_completed(futures.copy()):
            # Re-use same collection logic
            if future in futures:
                item = futures.pop(future)
                item_id = item.get(id_field, "unknown") if isinstance(item, dict) else str(item)
                try:
                    result = future.result()
                    meta = result.get("_meta", {})
                    stats["input_tokens"] += (meta.get("input_tokens", 0) or 0)
                    stats["output_tokens"] += (meta.get("output_tokens", 0) or 0)
                    stats["thinking_tokens"] += (meta.get("thinking_tokens", 0) or 0)
                    if meta.get("error"):
                        stats["errors"] += 1
                    append_log(output_path, result)
                except Exception as e:
                    logger.error(f"Error processing {item_id}: {e}")
                    stats["errors"] += 1
                    append_log(output_path, {
                        id_field: item_id,
                        "_meta": {"error": str(e)},
                    })

                cost = estimate_cost(stats["input_tokens"], stats["output_tokens"], stats["thinking_tokens"])
                pbar.update(1)
                pbar.set_postfix(
                    cost=f"${cost:.3f}",
                    err=stats["errors"],
                    tok=f"{(stats['input_tokens'] + stats['output_tokens']) / 1000:.0f}k",
                    refresh=False,
                )

        pbar.close()

    duration = time.time() - start_time
    total_cost = estimate_cost(stats["input_tokens"], stats["output_tokens"], stats["thinking_tokens"])

    result_stats = {
        "total": len(items),
        "processed": len(remaining),
        "errors": stats["errors"],
        "input_tokens": stats["input_tokens"],
        "output_tokens": stats["output_tokens"],
        "thinking_tokens": stats["thinking_tokens"],
        "cost_usd": total_cost,
        "duration_s": duration,
    }

    logger.info(
        f"Pipeline complete: {result_stats['processed']} processed, "
        f"{result_stats['errors']} errors, ${result_stats['cost_usd']:.3f} cost, "
        f"{result_stats['duration_s']:.1f}s"
    )

    return result_stats
