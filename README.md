# Q Strings

Once upon a time, there was a [three-line class](https://medium.com/p/5d4e5b0dfabb) for running SQL queries as a method on a string.

```python
class Q(str):
    def run(self):
        return duckdb.sql(self)

# usage
Q(my_query).run()
```


## Installation

```bash
pip install qstrings  # base only
```

```bash
pip install qstrings[all]  # with ai and cli
```


## Why?

What if the string itself was the * of the show?  Q-string is simply a string extra methods and attributes that unlock a few useful tricks.


### 1. Composable queries and engines

SQL-oriented python libraries usually pass a string to a SQL engine.  If you routinely work with multiple databases, the variations in dialects and in implementation can pile up into something awkward.

Q-strings help in two ways.  First, the queries are parsed using [sqlglot](https://github.com/tobymao/sqlglot):

```python
q = Q("SELECT 42 LIMIT 1")
q.ast  # abstract syntax tree
```

The `q.ast` carries a rich toolset, including transpiling to different dialects and swapping clauses are perhaps most useful:

```python
q1 = Q("SELECT 42 FROM one_table")
q2 = q1.ast.from_("another_table").q()
assert q2 == "SELECT 42 FROM another_table"
assert q.transpile(read="duckdb", write="tsql") == Q("SELECT TOP 1 42")
```

The second helpful pattern is the template pattern for easily defining engines.  The default query engine is DuckDB, `q.run()` and `q.run(engine="duckdb")` are equivalent and will execute a DuckDB query.  It is obviously impossible to cover all possible scenarios; hard-coding the engine selection logic `if engine == "this": run_that()` only gets you so far.  To make another engine, subclass `Engine` and write the `run` method.  The new engine becomes available right away, at runtime, thanks to [autoregistry](https://github.com/BrianPugh/autoregistry).

```python
from qstrings import Engine, Q

class FunnyDuckDBEngine(Engine):
    """The engine name will be "FunnyDuckDB" (dropping 'Engine', not case sensitive)."""
    def run(q: Q):
        result = duckdb.sql(q)
        funny = "lol, running a funny query"
        return result, 
        
Q("SELECT 42 AS haha").run(engine="FunnyDuckDB")
```

A more serious example might be this implementation for Athena.  It uses [awswrangler](https://github.com/aws/aws-sdk-pandas), handles different profiles, and sets certain options:

```python
class AthenaEngine(Engine):
    def df(self, **kwargs) -> pd.DataFrame:
        import awswrangler as wr
        import boto3

        if not boto3.DEFAULT_SESSION or kwargs.get("profile"):
            boto3.setup_default_session(
                region_name=kwargs.get("region", "us-east-1"),
                profile_name=kwargs.get("profile")
            )
        df = wr.athena.read_sql_query(
            self,
            database=kwargs.get("database", "default"),
            ctas_approach=kwargs.get("ctas", False),
            keep_files=kwargs.get("keep_files", False),
        )
    return df

Q("SHOW CREATE TABLE db.table").run(engine="athena")
```


### 2. Format strings with variables

```python
from qstrings import Q

# easily swap tables, filters, column lists
Q("SELECT * FROM {table}", table="prod.whatever").run()

# or use f-strings
value = 42
Q(f"SELECT * FROM prod.whatever WHERE col = {value}").run()

# or both
cols = ",".join(["col1","col2"])
Q(f"SELECT col0,{cols} FROM prod.whatever WHERE col = {value}", value=1).run()

# use SQL templates
Q(file="tests/test_format.sql", num=42)

# variables can also come from env vars (keyword args take precedence)
Q("""
CREATE SECRET my_secret (
    TYPE s3,
    KEY_ID '{KEY_ID}'
    SECRET '{SECRET_KEY}'
)
""").run()

# easy to write unit tests
def test_keys_given_in_env():"
    s = "SELECT {num} AS {Q_name}"
    os.environ["Q_name"] = "answer"
    q = Q(s, num=42)
    assert q == "SELECT 42 AS answer"
    assert q.refs == {"num": 42, "Q_name": "answer"}
    assert q.file is None
```



## CLI

Installing `qstrings[all]` or `qstrings[cli]` gives access to executable `q`, powered by the excellent CLI library [cyclopts](https://github.com/BrianPugh/cyclopts):


```bash
q --help

Usage: run-query [ARGS] [OPTIONS]


Query anything!

╭─ Commands ─────────────────────────────────────────────────╮
│ --help -h  Display this message and exit.                  │
│ --version  Display application version.                    │
╰────────────────────────────────────────────────────────────╯
╭─ Parameters ───────────────────────────────────────────────╮
│ QUERY --query           Query string                       │
│ --file              -f  File template                      │
│ --engine            -e  Query engine [default: duckdb]     │
│ --model             -m  HuggingFace model [default:        │
│                         openai/gpt-oss-20b:fireworks-ai]   │
│                     -o  Output format (defaults to         │
│                         whatever Engine.run() returns)     │
│                         [choices: engine, csv, list, line] │
│                         [default: engine]                  │
│ --LIMIT             -L  Limit rows                         │
│ --COUNT --no-COUNT  -C  Return row count only [default:    │
│                         False]                             │
│ --[KEYWORD]                                                │
╰────────────────────────────────────────────────────────────╯
```


The crown of creation: pipe results of an LLM query to a SQL query.  The [prompt](https://github.com/liquidcarbon/qstrings/blob/main/tests/test_prompt1.md) provides an [affinity](https://github.com/liquidcarbon/affinity) data model and asks to find elements with 8 or more isotopes.

```bash
export URL="https://raw.githubusercontent.com/liquidcarbon/chembiodata/main/isotopes.csv"

q -f tests/test_prompt1.md -e hf | q -o csv
/* 250825@22:48:46.297|DEBUG|qstrings[0.2.1].Q.run:190|input_tokens=209 */
/* 250825@22:48:46.297|DEBUG|qstrings[0.2.1].Q.run:191|output_tokens=611 */
/* 250825@22:48:46.864|INFO|qstrings[0.2.1].Q.run:151|4 rows x 2 cols in 0.0000 sec */
Symbol,num_isotopes
Sn,10
Xe,9
Te,8
Cd,8
```