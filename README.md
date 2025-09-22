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

What if the string itself was the * of the show?  Q-strings are just strings carrying extra methods and attributes that unlock a few useful tricks.


## Composable queries and engines

SQL-oriented python libraries usually pass a string to a SQL engine.  If you routinely work with multiple databases, the variations in dialects and in implementation can pile up into something awkward.

Q-strings help in two ways.  First, the queries are parsed into an AST (abstract syntax tree):

```python
q = Q("SELECT 42 LIMIT 1")
q.ast  # abstract syntax tree
```

The parsing is performed with [sqlglot](https://github.com/tobymao/sqlglot) that offers a rich collection SQL tools.  Whenever you want to query different tables in different databases, it's easy to transpile and swap clauses:

```python
q1 = Q("SELECT 42 FROM one_table")
q2 = q1.ast.from_("another_table").q()
assert q2 == "SELECT 42 FROM another_table"
assert q.transpile(read="duckdb", write="tsql") == Q("SELECT TOP 1 42")
```

The second trick is the template pattern for easily defining engines.  The default query engine is DuckDB, `q.run()` and `q.run(engine="duckdb")` are equivalent and will execute a DuckDB query.  It is obviously impossible to cover all possible scenarios; hard-coding the engine selection logic `if engine == "this": run_that()` only gets you so far.  To make another engine, subclass `Engine` and write the `run` method.  The new engine becomes available right away, at runtime, thanks to [autoregistry](https://github.com/BrianPugh/autoregistry).

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

## LLM queries

Q strings can do more than SQL!  The class `HFEngine` uses OpenAI-compatible API and secret `HF_API_KEY`.

```python
Q("Pick a number from 1 to 50").run(engine="hf", model="openai/gpt-oss-20b:fireworks-ai")
# the answer is not 42!
```

Also, check out SQL+LLM query language [BlendSQL](https://github.com/parkervg/blendsql).


## QoL features

### 0. History

Qstrings maintains query history, which is an on-disk DuckDB database (default: `qstrings/history.duckdb`).  Maybe set in `pyproject.toml` under `tool.qstrings.history`.


> [!TIP]
> One may wish to switch to [MotherDuck](https://motherduck.com/product/pricing/) to have the query history from all clients in one place.


### 1. Format strings with variables

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

### 2. Alias for queries

```python
q0 = Q("SELECT 'what a beatiful query'", alias="beautiful0")
_ = q0.run()
assert q0.alias == "beautiful0"
```


### 3. Quick access to COUNT and LIMIT

When working on a potentially long-running query, you may want to check the count first, or returning a few rows.

```python
from qstrings import Q
Q("SELECT * FROM table").count.run()
Q("SELECT * FROM table").limit(9).run()
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

The query history is available through command `qh`.

The crown of creation: pipe results of an LLM query to a SQL query.  The [prompt](https://github.com/liquidcarbon/qstrings/blob/main/tests/test_prompt1.md) provides an [affinity](https://github.com/liquidcarbon/affinity) data model and asks to find elements with 8 or more isotopes.

```bash
export URL="https://raw.githubusercontent.com/liquidcarbon/chembiodata/main/isotopes.csv"

q -f tests/test_prompt1.md -e hf 
/* 250921@21:55:34|INFO|qstrings[0.4.2]:272|HFEngine: 209 input x 1011 output tokens in 4.0431 sec */
/* The dataset is loaded from the CSV URL provided. We group rows by element symbol and atomic number,
   then keep only those groups having a count of isotopes (rows) of at least eight. */
SELECT symbol, number
FROM 'https://raw.githubusercontent.com/liquidcarbon/chembiodata/main/isotopes.csv'
GROUP BY symbol, number
HAVING COUNT(*) >= 8;
```

Piping:

```
q -f tests/test_prompt1.md -e hf | q -o csv
Enter query (end with EOF / Ctrl-D):
/* 250921@21:52:56|INFO|qstrings[0.4.1]:272|DuckDBEngine: 4 rows x 2 cols in 0.4222 sec */
Symbol,isotope_count
Te,8
Sn,10
Xe,9
Cd,8
```