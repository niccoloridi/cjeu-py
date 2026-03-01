"""
Tests for the browse module.

Uses synthetic Parquet and JSONL data in a temporary directory.
"""
import json
import os

import pandas as pd
import pytest

from cjeu_py.browse import (
    list_tables,
    preview_table,
    show_columns,
    show_stats,
    show_text,
    run_browse,
)


# ── Fixtures ──────────────────────────────────────────────────────────


def _make_data_dir(tmp_path):
    """Create a minimal data directory with synthetic pipeline data."""
    cellar = tmp_path / "raw" / "cellar"
    cellar.mkdir(parents=True)
    texts_dir = tmp_path / "raw" / "texts"
    texts_dir.mkdir(parents=True)

    # decisions
    decisions = pd.DataFrame({
        "celex": ["62014CJ0362", "62019CJ0311", "62020TJ0100"],
        "date": ["2015-10-06", "2020-07-16", "2021-03-10"],
        "court_code": ["CJ", "CJ", "TJ"],
        "resource_type": ["JUDG", "JUDG", "JUDG"],
    })
    decisions.to_parquet(cellar / "gc_decisions.parquet", index=False)

    # citations
    citations = pd.DataFrame({
        "citing_celex": ["62014CJ0362", "62019CJ0311"],
        "cited_celex": ["62019CJ0311", "62020TJ0100"],
    })
    citations.to_parquet(cellar / "gc_citations.parquet", index=False)

    # subjects
    subjects = pd.DataFrame({
        "celex": ["62014CJ0362", "62019CJ0311"],
        "subject_code": ["1.01", "2.03"],
        "subject_label": ["Agriculture", "Transport"],
    })
    subjects.to_parquet(cellar / "gc_subjects.parquet", index=False)

    # texts
    texts = [
        {
            "celex": "62014CJ0362",
            "status": "ok",
            "paragraphs": [
                "This is the first paragraph of the judgment.",
                "The Court recognises the autonomous legal order of EU law.",
            ],
            "paragraph_nums": [1, 2],
        },
        {
            "celex": "62019CJ0311",
            "status": "ok",
            "paragraphs": ["Data protection is a fundamental right."],
            "paragraph_nums": [1],
        },
    ]
    with open(texts_dir / "gc_texts.jsonl", "w") as f:
        for doc in texts:
            f.write(json.dumps(doc) + "\n")

    return str(tmp_path)


# ── Tests ─────────────────────────────────────────────────────────────


def test_list_tables(tmp_path):
    """list_tables shows available tables with row counts."""
    data_dir = _make_data_dir(tmp_path)
    result = list_tables(data_dir)
    assert "decisions" in result
    assert "citations_cellar" in result
    assert "subjects" in result
    assert "3" in result  # 3 rows in decisions


def test_list_tables_empty(tmp_path):
    """list_tables with no data shows helpful message."""
    result = list_tables(str(tmp_path))
    assert "No pipeline data" in result


def test_preview_table(tmp_path):
    """preview_table shows first rows of a table."""
    data_dir = _make_data_dir(tmp_path)
    result = preview_table(data_dir, "decisions")
    assert "62014CJ0362" in result
    assert "3 rows" in result


def test_preview_table_csv(tmp_path):
    """preview_table with csv format produces CSV output."""
    data_dir = _make_data_dir(tmp_path)
    result = preview_table(data_dir, "decisions", fmt="csv")
    assert "celex" in result  # CSV header
    assert "62014CJ0362" in result


def test_preview_table_json(tmp_path):
    """preview_table with json format produces valid JSON."""
    data_dir = _make_data_dir(tmp_path)
    result = preview_table(data_dir, "decisions", fmt="json")
    parsed = json.loads(result)
    assert len(parsed) == 3
    assert parsed[0]["celex"] == "62014CJ0362"


def test_preview_table_not_found(tmp_path):
    """preview_table with unknown table shows error + suggestions."""
    data_dir = _make_data_dir(tmp_path)
    result = preview_table(data_dir, "nonexistent")
    assert "not found" in result.lower()


def test_preview_table_limit(tmp_path):
    """preview_table respects --limit."""
    data_dir = _make_data_dir(tmp_path)
    result = preview_table(data_dir, "decisions", limit=1)
    assert "showing first 1" in result


def test_show_columns(tmp_path):
    """show_columns lists column names and types."""
    data_dir = _make_data_dir(tmp_path)
    result = show_columns(data_dir, "decisions")
    assert "celex" in result
    assert "court_code" in result
    assert "text" in result  # dtype


def test_show_stats(tmp_path):
    """show_stats shows value distributions."""
    data_dir = _make_data_dir(tmp_path)
    result = show_stats(data_dir, "decisions")
    assert "3 rows" in result
    assert "CJ" in result


def test_show_text(tmp_path):
    """show_text displays a judgment's paragraphs."""
    data_dir = _make_data_dir(tmp_path)
    result = show_text(data_dir, "62014CJ0362")
    assert "autonomous legal order" in result
    assert "[2]" in result


def test_show_text_not_found(tmp_path):
    """show_text with unknown CELEX shows not-found message."""
    data_dir = _make_data_dir(tmp_path)
    result = show_text(data_dir, "99999CJ0000")
    assert "not found" in result.lower()


def test_run_browse_no_table(tmp_path):
    """run_browse with no table lists all tables."""
    data_dir = _make_data_dir(tmp_path)
    result = run_browse(data_dir)
    assert "decisions" in result
    assert "Available tables" in result


def test_run_browse_stats(tmp_path):
    """run_browse with stats=True shows statistics."""
    data_dir = _make_data_dir(tmp_path)
    result = run_browse(data_dir, table="decisions", stats=True)
    assert "CJ" in result


def test_run_browse_columns(tmp_path):
    """run_browse with columns=True shows column info."""
    data_dir = _make_data_dir(tmp_path)
    result = run_browse(data_dir, table="decisions", columns=True)
    assert "celex" in result
    assert "COLUMN" in result
