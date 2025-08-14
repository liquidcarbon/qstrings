import pytest
from pathlib import Path
from qstrings import Q
from sqlglot.errors import ParseError


def test_parse_error():
    q = Q("SELE 42")
    assert q.errors
    with pytest.raises(ParseError):
        _ = Q("SELE 42", validate=True)


def test_select_42_ast():
    q = Q("SELECT 42")
    assert q.ast.sql() == "SELECT 42"


def test_select_42_patched_q():
    q = Q("SELECT 42")
    q1 = q.ast.from_("table").q()
    assert isinstance(q1, Q)
    assert q1 == "SELECT 42 FROM table"
    assert q1 == q.ast.from_("table").sql()
    q2 = q1.ast.from_("table").q(pretty=True)
    assert "\n" in q2
    assert q1.ast == q2.ast


def test_q_format():
    q = Q.format("SELECT {num} AS answer", num=42)
    assert q == "SELECT 42 AS answer"


def test_q_format_from_file():
    q = Q.format(Path(__file__).parent / "test_format.sql", file=True, num=42)
    assert q == "SELECT 42 AS answer"


def test_run_duckdb():
    q = Q("SELECT 42")
    result = q.run()
    assert result.fetchall() == [(42,)]
