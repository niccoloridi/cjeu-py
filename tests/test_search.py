"""Tests for the search module."""
import json
import os
import tempfile

import pandas as pd
import pytest

from cjeu_py.search import (
    _extract_snippet,
    _truncate,
    _format_results,
    search_text,
    search_party,
    search_citing,
    search_cited_by,
    search_topic,
    search_legislation,
    list_categories,
)


# ── Helpers ──────────────────────────────────────────────────────────────


def _make_data_dir(tmp_path):
    """Create a minimal data directory with synthetic tables."""
    cellar_dir = tmp_path / "raw" / "cellar"
    cellar_dir.mkdir(parents=True)
    texts_dir = tmp_path / "raw" / "texts"
    texts_dir.mkdir(parents=True)

    # Decisions
    decisions = pd.DataFrame({
        "celex": ["62014CJ0362", "62003CJ0349", "62019CJ0311"],
        "ecli": ["ECLI:EU:C:2015:650", "ECLI:EU:C:2006:15",
                 "ECLI:EU:C:2020:888"],
        "date": ["2015-10-06", "2006-01-10", "2020-11-11"],
        "court_code": ["CJ", "CJ", "CJ"],
        "formation_code": ["GRAND_CH", "GRAND_CH", "CHAMBER_5"],
        "judge_rapporteur": ["Bay Larsen", "Lenaerts", "Rosas"],
        "advocate_general": ["Bot", "Kokott", "Bobek"],
        "procedure_type": ["REF_PREL", "ANNULMENT", "REF_PREL"],
    })
    decisions.to_parquet(cellar_dir / "gc_decisions.parquet")

    # Case names
    case_names = pd.DataFrame({
        "celex": ["62014CJ0362", "62003CJ0349", "62019CJ0311"],
        "case_name": [
            "Maximillian Schrems v Data Protection Commissioner",
            "Commission v Council",
            "XXXX v Google LLC",
        ],
        "case_id": ["C-362/14", "C-349/03", "C-311/19"],
    })
    # case_names lives at root level per PARQUET_TABLES
    case_names.to_parquet(tmp_path / "case_names.parquet")

    # Citations
    citations = pd.DataFrame({
        "citing_celex": ["62019CJ0311", "62003CJ0349", "62019CJ0311"],
        "cited_celex": ["62014CJ0362", "62014CJ0362", "62003CJ0349"],
    })
    citations.to_parquet(cellar_dir / "gc_citations.parquet")

    # Subjects
    subjects = pd.DataFrame({
        "celex": ["62014CJ0362", "62014CJ0362", "62019CJ0311"],
        "subject_code": ["PDON", "CHDF", "PDON"],
        "subject_source": ["case_law", "case_law", "case_law"],
    })
    subjects.to_parquet(cellar_dir / "gc_subjects.parquet")

    # Legislation links
    legislation = pd.DataFrame({
        "celex": ["62014CJ0362", "62019CJ0311"],
        "legislation_celex": ["32000L0031", "32016R0679"],
        "link_type": ["interprets", "interprets"],
    })
    legislation.to_parquet(cellar_dir / "gc_legislation_links.parquet")

    # Texts (JSONL)
    texts = [
        {
            "celex": "62014CJ0362",
            "status": "ok",
            "paragraphs": [
                "The Court hereby rules as follows.",
                "The European Union is an autonomous legal order.",
                "Data protection is a fundamental right.",
            ],
            "paragraph_nums": [1, 2, 3],
        },
        {
            "celex": "62003CJ0349",
            "status": "ok",
            "paragraphs": [
                "In the present case the Commission seeks annulment.",
                "The principle of proportionality must be respected.",
            ],
            "paragraph_nums": [1, 2],
        },
    ]
    with open(texts_dir / "gc_texts.jsonl", "w") as f:
        for doc in texts:
            f.write(json.dumps(doc) + "\n")

    return str(tmp_path)


# ── Unit tests for helpers ───────────────────────────────────────────────


def test_extract_snippet_middle():
    text = "A" * 200 + "target phrase" + "B" * 200
    snippet = _extract_snippet(text, "target phrase", context=20)
    assert "target phrase" in snippet
    assert snippet.startswith("...")
    assert snippet.endswith("...")


def test_extract_snippet_start():
    text = "target phrase" + "X" * 300
    snippet = _extract_snippet(text, "target phrase", context=20)
    assert snippet.startswith("target phrase")
    assert snippet.endswith("...")


def test_extract_snippet_not_found():
    text = "nothing here"
    snippet = _extract_snippet(text, "missing")
    assert snippet == "nothing here"


def test_truncate():
    assert _truncate("short", 10) == "short"
    assert _truncate("a very long string indeed", 10) == "a very ..."
    assert _truncate(None, 10) == ""


def test_format_results_csv():
    df = pd.DataFrame({"celex": ["A", "B"], "date": ["2020", "2021"]})
    out = _format_results(df, ["celex", "date"], {"celex": 10, "date": 10},
                          25, "csv")
    assert "celex,date" in out
    assert "A,2020" in out


def test_format_results_json():
    df = pd.DataFrame({"celex": ["A"]})
    out = _format_results(df, ["celex"], {"celex": 10}, 25, "json")
    parsed = json.loads(out)
    assert len(parsed) == 1
    assert parsed[0]["celex"] == "A"


def test_format_results_empty():
    df = pd.DataFrame()
    out = _format_results(df, ["celex"], {"celex": 10}, 25, "table",
                          "Test header")
    assert "No results" in out


# ── Integration tests with synthetic data ────────────────────────────────


def test_search_text(tmp_path):
    data_dir = _make_data_dir(tmp_path)
    result = search_text(data_dir, "autonomous legal order")
    assert "autonomous legal order" in result
    assert "62014CJ0362" in result


def test_search_text_no_results(tmp_path):
    data_dir = _make_data_dir(tmp_path)
    result = search_text(data_dir, "xyznonexistent")
    assert "No results" in result


def test_search_party(tmp_path):
    data_dir = _make_data_dir(tmp_path)
    result = search_party(data_dir, "Google")
    assert "62019CJ0311" in result


def test_search_party_no_results(tmp_path):
    data_dir = _make_data_dir(tmp_path)
    result = search_party(data_dir, "Nonexistent Corp")
    assert "No cases" in result


def test_search_citing(tmp_path):
    data_dir = _make_data_dir(tmp_path)
    result = search_citing(data_dir, "62014CJ0362")
    # Both 62019CJ0311 and 62003CJ0349 cite 62014CJ0362
    assert "62019CJ0311" in result
    assert "62003CJ0349" in result


def test_search_citing_no_results(tmp_path):
    data_dir = _make_data_dir(tmp_path)
    result = search_citing(data_dir, "99999CJ0000")
    assert "No cases" in result


def test_search_cited_by(tmp_path):
    data_dir = _make_data_dir(tmp_path)
    result = search_cited_by(data_dir, "62019CJ0311")
    # 62019CJ0311 cites 62014CJ0362 and 62003CJ0349
    assert "62014CJ0362" in result
    assert "62003CJ0349" in result


def test_search_topic(tmp_path):
    data_dir = _make_data_dir(tmp_path)
    # Search by label substring
    result = search_topic(data_dir, "data protection")
    assert "62014CJ0362" in result


def test_search_topic_by_code(tmp_path):
    data_dir = _make_data_dir(tmp_path)
    result = search_topic(data_dir, "PDON")
    assert "62014CJ0362" in result


def test_search_legislation(tmp_path):
    data_dir = _make_data_dir(tmp_path)
    result = search_legislation(data_dir, "32016R0679")
    assert "62019CJ0311" in result


def test_list_topics(tmp_path):
    data_dir = _make_data_dir(tmp_path)
    result = list_categories(data_dir, "topics")
    assert "PDON" in result


def test_list_judges(tmp_path):
    data_dir = _make_data_dir(tmp_path)
    result = list_categories(data_dir, "judges")
    assert "Lenaerts" in result


def test_list_invalid_category(tmp_path):
    data_dir = _make_data_dir(tmp_path)
    result = list_categories(data_dir, "invalid")
    assert "Unknown category" in result


def test_missing_data():
    result = search_text("/nonexistent", "test")
    assert "No texts" in result
