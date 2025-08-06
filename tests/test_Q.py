import pytest
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
    assert q1.ast == q2.ast
