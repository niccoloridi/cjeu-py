"""
Browse pipeline data in the terminal.

Provides table listing, row preview, column info, descriptive stats,
and text viewing — the CLI equivalent of the GUI's Browse Data tab.
"""
import json
import logging
import os
from typing import Optional

import pandas as pd

from cjeu_py.export import PARQUET_TABLES, JSONL_TABLES, _find_table, _load_jsonl

logger = logging.getLogger(__name__)

# ── Helpers ───────────────────────────────────────────────────────────


def _human_size(nbytes: int) -> str:
    """Format byte count as human-readable string."""
    for unit in ("B", "KB", "MB", "GB"):
        if nbytes < 1024:
            return f"{nbytes:.1f} {unit}" if unit != "B" else f"{nbytes} B"
        nbytes /= 1024
    return f"{nbytes:.1f} TB"


def _bar(count: int, max_count: int, width: int = 30) -> str:
    """Render a proportional ASCII bar."""
    if max_count == 0:
        return ""
    filled = round(count / max_count * width)
    return "\u2588" * filled


def _resolve_table(data_dir: str, table_name: str):
    """Look up a table by name. Returns (path, format) or (None, None)."""
    if table_name in PARQUET_TABLES:
        path = _find_table(data_dir, PARQUET_TABLES[table_name])
        if path:
            return path, "parquet"
    if table_name in JSONL_TABLES:
        path = _find_table(data_dir, JSONL_TABLES[table_name])
        if path:
            return path, "jsonl"
    return None, None


def _load_table(path: str, fmt: str) -> pd.DataFrame:
    """Load a table from disk."""
    if fmt == "parquet":
        return pd.read_parquet(path)
    return _load_jsonl(path)


def _suggest_similar(table_name: str) -> str:
    """Suggest similar table names if an exact match fails."""
    all_names = list(PARQUET_TABLES.keys()) + list(JSONL_TABLES.keys())
    matches = [n for n in all_names if table_name in n or n in table_name]
    if matches:
        return f"  Did you mean: {', '.join(matches)}?"
    return f"  Available: {', '.join(all_names)}"


# ── Public functions ──────────────────────────────────────────────────


def list_tables(data_dir: str) -> str:
    """List all available tables with row counts and file sizes."""
    rows = []

    for name, rel_path in PARQUET_TABLES.items():
        path = _find_table(data_dir, rel_path)
        if not path:
            continue
        try:
            import pyarrow.parquet as pq
            meta = pq.read_metadata(path)
            schema = pq.read_schema(path)
            n_rows = meta.num_rows
            n_cols = len(schema)
        except Exception:
            df = pd.read_parquet(path)
            n_rows, n_cols = df.shape
        size = os.path.getsize(path)
        rows.append((name, "parquet", n_rows, n_cols, size))

    for name, rel_path in JSONL_TABLES.items():
        path = _find_table(data_dir, rel_path)
        if not path:
            continue
        try:
            df = _load_jsonl(path)
            n_rows, n_cols = df.shape
        except Exception:
            continue
        size = os.path.getsize(path)
        rows.append((name, "jsonl", n_rows, n_cols, size))

    if not rows:
        return (
            f"No pipeline data found in {data_dir}\n"
            "Run 'cjeu-py download-cellar' to get started."
        )

    # Format table
    lines = [f"Available tables ({len(rows)} found in {data_dir}):", ""]
    lines.append(
        f"  {'TABLE':<30s} {'FMT':<8s} {'ROWS':>8s} {'COLS':>5s}  {'SIZE':>10s}"
    )
    lines.append("  " + "\u2500" * 65)

    for name, fmt, n_rows, n_cols, size in rows:
        lines.append(
            f"  {name:<30s} {fmt:<8s} {n_rows:>8,d} {n_cols:>5d}  {_human_size(size):>10s}"
        )

    lines.append("")
    lines.append("Use: cjeu-py browse <table> to preview rows")
    lines.append("     cjeu-py browse <table> --stats for statistics")
    lines.append("     cjeu-py browse <table> --columns for column info")
    return "\n".join(lines)


def preview_table(data_dir: str, table_name: str,
                  limit: int = 20, fmt: str = "table") -> str:
    """Show the first N rows of a table."""
    path, table_fmt = _resolve_table(data_dir, table_name)
    if not path:
        return f"Table '{table_name}' not found.\n{_suggest_similar(table_name)}"

    df = _load_table(path, table_fmt)
    n_rows, n_cols = df.shape
    header = f"{table_name}: {n_rows:,} rows \u00d7 {n_cols} columns (showing first {min(limit, n_rows)})"

    subset = df.head(limit)

    if fmt == "csv":
        return subset.to_csv(index=False)
    if fmt == "json":
        return subset.to_json(orient="records", indent=2, default_handler=str)

    # table format
    return f"{header}\n\n{subset.to_string(max_colwidth=40)}"


def show_columns(data_dir: str, table_name: str) -> str:
    """List column names, dtypes, non-null counts, and sample values."""
    path, table_fmt = _resolve_table(data_dir, table_name)
    if not path:
        return f"Table '{table_name}' not found.\n{_suggest_similar(table_name)}"

    df = _load_table(path, table_fmt)
    n_rows, n_cols = df.shape

    # Friendly dtype names
    dtype_map = {
        "object": "text", "int64": "integer", "int32": "integer",
        "float64": "float", "float32": "float", "bool": "boolean",
        "datetime64[ns]": "datetime", "datetime64[us]": "datetime",
    }

    lines = [f"{table_name}: {n_rows:,} rows, {n_cols} columns", ""]
    lines.append(
        f"  {'COLUMN':<30s} {'TYPE':<10s} {'NON-NULL':>8s} {'NULLS':>6s}  SAMPLE VALUES"
    )
    lines.append("  " + "\u2500" * 80)

    for col in df.columns:
        dtype_str = dtype_map.get(str(df[col].dtype), str(df[col].dtype))
        non_null = int(df[col].notna().sum())
        nulls = n_rows - non_null
        # Sample: first 3 unique non-null values
        uniques = df[col].dropna().unique()[:3]
        sample = ", ".join(str(v)[:30] for v in uniques)
        if len(df[col].dropna().unique()) > 3:
            sample += ", ..."
        lines.append(
            f"  {col:<30s} {dtype_str:<10s} {non_null:>8,d} {nulls:>6,d}  {sample}"
        )

    return "\n".join(lines)


def show_stats(data_dir: str, table_name: str) -> str:
    """Descriptive statistics with value distributions."""
    path, table_fmt = _resolve_table(data_dir, table_name)
    if not path:
        return f"Table '{table_name}' not found.\n{_suggest_similar(table_name)}"

    df = _load_table(path, table_fmt)
    n_rows, n_cols = df.shape
    mem_mb = df.memory_usage(deep=True).sum() / 1024 / 1024

    lines = [f"{table_name}: {n_rows:,} rows, {n_cols} columns"]

    # Date range if a 'date' column exists
    for date_col in ("date", "decision_date", "date_document"):
        if date_col in df.columns:
            try:
                dates = pd.to_datetime(df[date_col], errors="coerce").dropna()
                if len(dates):
                    lines.append(
                        f"Date range: {dates.min().strftime('%Y-%m-%d')} to "
                        f"{dates.max().strftime('%Y-%m-%d')}"
                    )
            except Exception:
                pass
            break

    lines.append(f"Memory: {mem_mb:.1f} MB")
    lines.append("")

    for col in df.columns:
        series = df[col].dropna()
        if series.empty:
            continue

        # Numeric columns
        if pd.api.types.is_numeric_dtype(series):
            lines.append(
                f"{col} (numeric): "
                f"min={series.min()}, max={series.max()}, "
                f"mean={series.mean():.2f}, median={series.median():.2f}"
            )
            lines.append("")
            continue

        # Categorical / text columns — show distribution if low cardinality
        n_unique = series.nunique()
        if n_unique > 100:
            lines.append(f"{col}: {n_unique:,} unique values (too many to display)")
            lines.append("")
            continue

        counts = series.value_counts().head(10)
        max_count = counts.iloc[0] if len(counts) else 1
        label = f"{col} ({n_unique} unique value{'s' if n_unique != 1 else ''})"
        if n_unique > 10:
            label += ", top 10"
        lines.append(f"{label}:")

        for val, count in counts.items():
            pct = count / n_rows * 100
            bar = _bar(count, max_count)
            val_str = str(val)[:30]
            lines.append(f"  {val_str:<30s} {count:>8,d}  {bar}  {pct:.1f}%")

        if n_unique > 10:
            lines.append(f"  ... and {n_unique - 10} more")
        lines.append("")

    return "\n".join(lines)


def show_text(data_dir: str, celex: str) -> str:
    """Display a judgment text from gc_texts.jsonl."""
    # Find texts file (same fallback as search.py)
    texts_path = _find_table(data_dir, "raw/texts/gc_texts.jsonl")
    if not texts_path:
        for subdir in ["ag_divergence_full", "ag_divergence"]:
            candidate = os.path.join(data_dir, subdir, "gc_texts.jsonl")
            if os.path.exists(candidate):
                texts_path = candidate
                break
    if not texts_path:
        return "No texts downloaded yet. Run: cjeu-py fetch-texts"

    celex_upper = celex.upper()
    with open(texts_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                doc = json.loads(line)
            except json.JSONDecodeError:
                continue
            if doc.get("celex", "").upper() != celex_upper:
                continue
            if doc.get("status") != "ok":
                return f"{celex}: text status is '{doc.get('status')}' (not available)"

            paragraphs = doc.get("paragraphs", [])
            para_nums = doc.get("paragraph_nums", [])
            n_chars = sum(len(p) for p in paragraphs)

            lines = [
                f"{celex} \u2014 {len(paragraphs)} paragraphs, {n_chars:,} characters",
                "",
            ]
            for i, para in enumerate(paragraphs):
                num = para_nums[i] if i < len(para_nums) else i + 1
                lines.append(f"[{num}]  {para}")
                lines.append("")
            return "\n".join(lines)

    return f"CELEX '{celex}' not found in texts."


def run_browse(data_dir: str, table: str = "",
               stats: bool = False, columns: bool = False,
               limit: int = 20, fmt: str = "table") -> str:
    """Dispatch to the appropriate browse mode. Returns formatted string."""
    if not table:
        return list_tables(data_dir)
    if columns:
        return show_columns(data_dir, table)
    if stats:
        return show_stats(data_dir, table)
    return preview_table(data_dir, table, limit=limit, fmt=fmt)
