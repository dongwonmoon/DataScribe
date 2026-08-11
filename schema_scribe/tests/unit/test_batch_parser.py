"""Adversarial tests for _parse_batch_response (panel C4/C5/C6,
2026-08-11: the parser accepted only 'N:' delimiters and failed silently
on missing lines — a model echoing the input's '1.' style zeroed out the
whole table with no warning)."""

import logging

import pytest

from schema_scribe.services.catalog_generator import CatalogGenerator

P = CatalogGenerator._parse_batch_response


def test_period_delimiter_accepted():
    """A model echoing the input's '1. name' style must parse (C6)."""
    summary, descs = P("SUMMARY: Users\n1. A unique id\n2. The name", 2)
    assert summary == "Users"
    assert descs == ["A unique id", "The name"]


def test_missing_lines_warn_and_become_empty_drafts(caplog):
    """Missing column lines are warned and become '' (never silent, C5)."""
    with caplog.at_level(logging.WARNING):
        summary, descs = P("SUMMARY: Users\n1: only one", 3)
    assert descs == ["only one", "", ""]
    assert "missing 2 column line(s)" in caplog.text
    assert "#2" in caplog.text and "#3" in caplog.text


def test_missing_summary_warns(caplog):
    with caplog.at_level(logging.WARNING):
        summary, descs = P("1: a\n2: b", 2)
    assert summary == ""
    assert descs == ["a", "b"]
    assert "missing SUMMARY line" in caplog.text


def test_duplicate_numbering_last_wins():
    summary, descs = P("SUMMARY: X\n1: first\n1: second\n2: b", 2)
    assert descs == ["second", "b"]


def test_out_of_range_and_junk_lines_ignored():
    summary, descs = P("SUMMARY: X\n1: a\n9: out\njunk\n2: b", 2)
    assert descs == ["a", "b"]


def test_case_insensitive_summary():
    summary, _ = P("summary: lower case\n1: a", 1)
    assert summary == "lower case"


def test_empty_response_is_all_empty_drafts(caplog):
    with caplog.at_level(logging.WARNING):
        summary, descs = P("", 3)
    assert summary == ""
    assert descs == ["", "", ""]
