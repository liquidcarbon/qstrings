import duckdb
import os
import pytest
from pathlib import Path
from qstrings import Engine, Q, QStringError
from sqlglot.errors import ParseError


def test_empty_string():
    q = Q("", quiet=True)
    assert q == ""
    assert q.ast is None
    assert q.ast_errors


def test_keys_missing():
    s = "SELECT {num} AS {Q_name}"
    with pytest.raises(QStringError, match="values missing for keys: {'Q_name'}"):
        _ = Q(s, num=42)


def test_keys_given_in_env():
    s = "SELECT {num} AS {Q_name}"
    os.environ["Q_name"] = "answer"
    q = Q(s, num=42)
    assert q == "SELECT 42 AS answer"
    assert q.refs == {"num": 42, "Q_name": "answer"}
    assert q.file is None


def test_from_file():
    args = dict(file="test_does_not_exist*.sql")
    with pytest.raises(FileNotFoundError, match="File not found:"):
        _ = Q(**args)
    args = dict(file=Path(__file__).parent / "test_format.sql", num=42)
    with pytest.raises(QStringError, match="values missing for keys: {'foo'}"):
        _ = Q(**args)
    os.environ["foo"] = "bar"
    q = Q(**args)
    assert q == "SELECT 42 AS answer, 'bar' AS foo  -- { ignore }"
    assert q.refs == {"num": 42, "foo": "bar"}
    assert q.file and q.file.name == "test_format.sql"


def test_alias():
    q = Q("SELECT 42 AS answer", alias="blah")
    assert q.alias == "blah"


def test_parse_error():
    q = Q("SELE 42")
    assert q.ast_errors
    with pytest.raises(ParseError):
        _ = Q("SELE 42", validate=True)


def test_select_42_ast_transpile():
    q = Q("SELECT 42 LIMIT {n}", n=1)
    assert q.ast.sql() == "SELECT 42 LIMIT 1"
    assert q.transpile(read="duckdb", write="tsql") == Q("SELECT TOP 1 42")


def test_select_42_ast_patched_q():
    q = Q("SELECT 42")
    q1 = q.ast.from_("table").q()
    assert isinstance(q1, Q)
    assert q1 == "SELECT 42 FROM table"
    assert q1 == q.ast.from_("table").sql()
    q2 = q1.ast.from_("table").q(pretty=True)
    assert "\n" in q2
    assert q1.ast == q2.ast


def test_select_42_ast_limit():
    q = Q("SELECT 42")
    q_limit = q.limit(1)
    assert q_limit == "SELECT * FROM (SELECT 42) LIMIT 1"


def test_select_42_ast_count():
    q = Q("SELECT 42")
    q_ct = q.count
    assert q_ct == "SELECT COUNT(*) AS row_count FROM (SELECT 42)"


def test_run_duckdb_error():
    q = Q("THIS WILL FAIL")
    result = q.run(quiet=True)
    assert 'syntax error at or near "THIS"' in str(result)


def test_run_duckdb():
    q = Q("SELECT 42 AS answer")
    result = q.run(quiet=True)
    assert result.fetchall() == [(42,)]
    assert q.list(header=False, quiet=True) == [(42,)]
    assert q.list(header=True, quiet=True) == [("answer",), (42,)]


def test_run_duckdb_fails_on_closed_connection():
    Q("SELECT 42 AS answer", quiet=True).run().fetchall() == [(42,)]


def test_run_duckdb_connect_to_tmpdb():
    Q("SELECT 42 AS answer", quiet=True).run().fetchall() == [(42,)]
    tmpdb = Path(__file__).parent / "tmp.duckdb"
    q = Q("SELECT 42 AS answer")
    result = q.run(db=tmpdb, quiet=True)
    assert result.fetchall() == [(42,)]
    assert q.list(header=False, quiet=True) == [(42,)]
    assert q.list(header=True, quiet=True) == [("answer",), (42,)]
    tmpdb.unlink(missing_ok=True)


def test_run_new_engine():
    class FunnyDuckDBEngine(Engine):
        def run(q: Q):
            result = duckdb.sql(q)
            funny = "lol, running a funny query"
            return result, funny

        def df(q: Q):
            return "funny", "df"

    q = Q("SELECT 42", quiet=True)
    result, funny = q.run(engine="FunnyDuckDB")
    assert result.fetchall() == [(42,)]
    assert funny == "lol, running a funny query"
    assert q.df(engine="FunnyDuckDB") == ("funny", "df")
    with pytest.raises(NotImplementedError):
        q.list(engine="FunnyDuckDB")
    with pytest.raises(KeyError):
        q.run(engine="NonExistentEngine")


@pytest.mark.skipif(
    os.getenv("HF_API_KEY") is None,
    reason="HF_API_KEY not set",
)
def test_ai_query():
    q = Q(file=Path(__file__).parent / "test_prompt0.md", quiet=True)
    result = q.run(engine="hf", model="openai/gpt-oss-20b:fireworks-ai")
    print(result)
    assert result.isdigit()
    assert 1 <= int(result) <= 50
