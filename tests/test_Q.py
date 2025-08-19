import os
import pytest
from pathlib import Path
from qstrings import Q, QStringError
from sqlglot.errors import ParseError


def test_empty_string():
    q = Q("")
    assert q == ""
    assert q.ast is None
    assert q.errors


def test_keys_missing():
    s = "SELECT {num} AS {Q_name}"
    with pytest.raises(QStringError, match="values missing for keys: {'Q_name'}"):
        _ = Q(s, num=42)


def test_keys_given_in_env():
    s = "SELECT {num} AS {Q_name}"
    os.environ["Q_name"] = "answer"
    q = Q(s, num=42)
    assert q == "SELECT 42 AS answer"


def test_from_file():
    args = dict(file=Path(__file__).parent / "test_format.sql", num=42)
    with pytest.raises(QStringError, match="values missing for keys: {'foo'}"):
        _ = Q(**args)
    os.environ["foo"] = "bar"
    q = Q(**args)
    assert q == "SELECT 42 AS answer, 'bar' AS foo  -- { ignore }"


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


def test_run_duckdb():
    q = Q("SELECT 42")
    result = q.run()
    assert result.fetchall() == [(42,)]
