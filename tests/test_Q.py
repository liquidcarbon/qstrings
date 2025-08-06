import pytest
from qstrings import Q
from sqlglot.errors import ParseError


def test_parse_error():
    q = Q("SELE 42")
    assert q.errors
    with pytest.raises(ParseError):
        _ = Q("SELE 42", validate=True)


def test_parse_select_42():
    q = Q("SELECT 42")
    assert q.ast.from_("table").sql() == "SELECT 42 FROM table"
