import duckdb
import json
import pathlib
import os
import sqlglot
import string

from abc import abstractmethod
from autoregistry import Registry
from datetime import datetime
from io import StringIO
from typing import Any, Dict, Literal, Self, Union, overload

from .config import log

PathType = Union[pathlib.Path, Any]
StrPath = Union[str, os.PathLike[str], None]


def parse_keys(s: str) -> set[str]:
    """Return a set of keys from a string formatted with {}."""
    formatter = string.Formatter()
    keys = set()
    for _, fname, _, _ in formatter.parse(s):
        if fname:
            keys.add(fname)
    return keys


class BaseQ(str):
    """Base Q-string class."""

    HISTORY = None

    def __new__(
        cls,
        s: str = "",
        *,
        file: StrPath = None,
        path_type: PathType = pathlib.Path,
        **kwargs: Dict[str, Any],
    ):
        """Create a Q string.

        Args:
            s (str): the base string
            file (StrPath, default=None): if set, read template from file
            path_type (PathType, default=pathlib.Path): Path, S3Path, etc.
        """

        if s == "" and not file and not kwargs.get("quiet"):
            log.warning("Empty Q string")
        if file:
            _path = path_type(file)
            if not _path.exists():
                raise FileNotFoundError(f"File not found: {_path}")
            with _path.open("r") as f:
                s = f.read()

        kwargs_plus_env = dict(**kwargs, **os.environ)
        keys_needed = parse_keys(s)
        keys_given = set(kwargs_plus_env)
        keys_missing = keys_needed - keys_given
        if keys_missing:
            raise QStringError(f"values missing for keys: {keys_missing}")
        refs = {k: kwargs_plus_env[k] for k in keys_needed}
        s_formatted = s.format(**refs)

        qstr = str.__new__(cls, s_formatted)
        qstr.id = int(f"{datetime.now():%y%m%d%H%M%S%f}")
        qstr.template = s
        qstr.refs = refs  # references used to create the Q string
        qstr.file = _path if file else None
        qstr.alias = kwargs.get("alias")

        try:
            qstr.ast = sqlglot.parse_one(s_formatted)
            qstr.ast_errors = None
        except sqlglot.errors.ParseError as e:
            if kwargs.get("validate"):
                raise e
            qstr.ast = None
            qstr.ast_errors = str(e)

        qstr.exec_id = 0
        qstr.duration = 0.0
        qstr.rows = 0
        qstr.cols = 0
        qstr.input_tokens = 0
        qstr.output_tokens = 0
        qstr._quiet = kwargs.get("quiet", False)
        return qstr

    def transpile(self, read: str = "duckdb", write: str = "tsql") -> Self:
        """Transpile the SQL to a different dialect using sqlglot."""
        if not self.ast:
            raise QStringError("Cannot transpile invalid SQL")
        return BaseQ(sqlglot.transpile(self.ast.sql(), read=read, write=write)[0])

    def limit(self, n: int = 5) -> Self:
        return sqlglot.subquery(self.ast).select("*").limit(n).q()

    @property
    def count(self) -> Self:
        return sqlglot.subquery(self.ast).select("COUNT(*) AS row_count").q()

    @property
    def dict(self) -> Dict[str, Any]:
        d = {}
        for k, v in self.__dict__.items():
            if k == "ast":
                d["qstr"] = str(self)
                continue
            if not k.startswith("_"):
                if isinstance(v, (int, float)) or v is None:
                    d[k] = v
                else:
                    d[k] = str(v)
        return d

    def json(self, indent: int | None = None) -> str:
        return json.dumps(self.dict, indent=indent, default=str)

    @classmethod
    def from_dict(cls, d: Dict[str, Any], **kwargs) -> Self:
        fields = []
        for k, v in d.items():
            if isinstance(v, (int, float)):
                fields.append(f"{v} AS {k}")
            elif v is None:
                fields.append(f"NULL AS {k}")
            else:
                fields.append(f"'{v}' AS {k}")
        instance = cls("SELECT " + ", ".join(fields), **kwargs)
        return instance

    @classmethod
    def from_history(cls, exec_id: int | None = None, alias: str | None = "") -> Self:
        """Retrieve most recent Q string from history matching exec_id or alias."""
        with duckdb.connect(cls.HISTORY) as con:
            if exec_id is None and alias == "":
                # no exec_id or alias - load all, return most recent
                where_ = ""
            if alias is None:
                # explicitly looking for no alias
                where_ = "alias IS NULL"
            elif alias:
                # looking for specific alias
                where_ = f"alias='{alias}'"
            elif exec_id:
                # looking for specific exec_id, overrides alias
                where_ = f"{exec_id=}"

            q_hist = (
                sqlglot.parse_one("SELECT qstr FROM q")
                .where(where_)
                .order_by("id")
                .sql()
            )
            rel = con.query(q_hist)
            qstr = rel.fetchall()[-1][0]  # return latest match
            if not qstr:
                raise QStringError(f"No history found for {exec_id=}, {alias=}")
            return cls(qstr)


class Q(BaseQ):
    """Default qstring class with timer, logger, history, and runner registry."""

    @overload
    def run(self, engine: Literal["duckdb"], db: StrPath = "", **kwargs):
        """Quack!"""
        ...

    @overload
    def run(self, engine: Literal["hf"], **kwargs): ...

    def run(self, engine=None, **kwargs):
        """Run with chosen Engine."""
        engine = engine or "duckdb"
        cls = Engine[engine]
        self._engine_cls = cls.__name__
        return cls.run(self, **kwargs)

    def list(self, engine=None, **kwargs):
        """Return the result as a list."""
        engine = engine or "duckdb"
        cls = Engine[engine]
        self._engine_cls = cls.__name__
        return cls.list(self, **kwargs)

    def df(self, engine=None, **kwargs):
        """Return the result as a DataFrame."""
        engine = engine or "duckdb"
        return Engine[engine].df(self, **kwargs)

    def save(self) -> None:
        """Save Q string execution to history."""
        if not self.HISTORY:
            return
        with duckdb.connect(self.HISTORY) as con:
            last_q = con.read_json(
                StringIO(self.json()),
                # must spell out types or NULLs forced to JSON
                columns={
                    "id": "BIGINT",
                    "template": "VARCHAR",
                    "refs": "VARCHAR",
                    "file": "VARCHAR",
                    "alias": "VARCHAR",
                    "qstr": "VARCHAR",
                    "ast_errors": "VARCHAR",
                    "exec_id": "BIGINT",
                    "duration": "DOUBLE",
                    "rows": "INT",
                    "cols": "INT",
                    "input_tokens": "INT",
                    "output_tokens": "INT",
                },
            )
            con.register("last_q", last_q)
            has_tables = any("q" in t for t in con.sql("SHOW TABLES").fetchall())
            if not has_tables:
                con.sql("CREATE TABLE IF NOT EXISTS q AS FROM last_q")
            else:
                con.sql("INSERT INTO q FROM last_q")


class Engine(Registry, suffix="Engine", overwrite=True):
    """Registry for query engines. Subclass to implement new engines.

    Overwrite helps avoid KeyCollisionError when class registration
    happens multiple times in a single session, e.g. in notebooks.
    For more details, see autoregistry docs:
    https://github.com/BrianPugh/autoregistry
    """

    @abstractmethod
    def run(q: Q):
        raise NotImplementedError

    @abstractmethod
    def list(q: Q):
        raise NotImplementedError

    @abstractmethod
    def df(q: Q):
        raise NotImplementedError

    def timer_logger(func):
        def logging_wrapper(self, *args, **kwargs):
            quiet = getattr(self, "_quiet", False) or kwargs.get("quiet", False)
            self.exec_id = int(f"{datetime.now():%y%m%d%H%M%S%f}")
            try:
                result = func(self, *args, **kwargs)
            except Exception as e:
                if not quiet:
                    log.error(f"Error: {e}")
                raise e
            t_done = int(f"{datetime.now():%y%m%d%H%M%S%f}")
            self.duration = round((t_done - self.exec_id) / 1e6, 4)

            if self.rows + self.cols > 0:
                _stats = f"{self.rows} rows x {self.cols} cols"
            elif self.input_tokens + self.output_tokens > 0:
                _it, _ot = self.input_tokens, self.output_tokens
                _stats = f"{_it} input x {_ot} output tokens"
            else:
                _stats = "no results"
            msg = f"{self._engine_cls}: {_stats} in {self.duration:.4f} sec"
            if not quiet:
                log.info(msg)

            if kwargs.get("save", True):
                # log.debug(f"saving to history: {self}")
                self.save()

            return result

        return logging_wrapper


class DuckDBEngine(Engine):
    """DuckDB engine.  By default runs using in-memory database."""

    con: duckdb.DuckDBPyConnection = None

    @Engine.timer_logger
    def run(q: Q, db: StrPath = "", **kwargs) -> duckdb.DuckDBPyRelation:
        """Run with DuckDB."""
        DuckDBEngine.con = duckdb.connect(
            database=db, read_only=kwargs.get("read_only", False)
        )
        # connection to remain attached to something, otherwise closed and gc'd
        try:
            relation = DuckDBEngine.con.sql(q)
            q.rows, q.cols = relation.shape
        except Exception as e:
            relation = duckdb.sql(f"SELECT '{q}' AS q, '{e}' AS r")
        return relation

    @staticmethod
    def df(q: Q, db: StrPath = "", **kwargs):
        return DuckDBEngine.run(q, db, **kwargs).df()

    @staticmethod
    def list(q: Q, db: StrPath = "", header=True, **kwargs):
        rel = DuckDBEngine.run(q, db, **kwargs)
        result = ([tuple(rel.columns)] if header else []) + rel.fetchall()
        return result


class AIEngine(Engine):
    """Base class for AI engines."""

    pass


class MockAIEngine(AIEngine):
    def run(q: Q, model=None):
        return "SELECT\n42 AS select"


class HFEngine(AIEngine):
    """Hugging Face OpenAI-compatible inference API engine."""

    @Engine.timer_logger
    def run(q: Q, model: str = "openai/gpt-oss-20b:fireworks-ai", **kwargs):
        """Run LLM query on HF.  Requires env var `HF_API_KEY`."""
        from openai import OpenAI

        client = OpenAI(
            base_url="https://router.huggingface.co/v1", api_key=os.getenv("HF_API_KEY")
        )
        q._response = client.responses.create(model=model, input=q)
        q.input_tokens = q._response.usage.input_tokens
        q.output_tokens = q._response.usage.output_tokens
        # if not q._quiet and not kwargs.get("quiet"):
        #     log.debug(f"{q.input_tokens=}")
        #     log.debug(f"{q.output_tokens=}")
        result = q._response.output[1].content[0].text
        return result

    @staticmethod
    def list(q: Q, model: str = "openai/gpt-oss-20b:fireworks-ai"):
        result = HFEngine.run(q, model=model)
        return [(q, result)]


class QStringError(Exception):
    pass


def sqlglot_sql_q(ex: sqlglot.expressions.Expression, *args, **kwargs):
    """Variant of sqlglot's Expression.sql that returns a Q string."""
    return Q(ex.sql(*args, **kwargs))


sqlglot.expressions.Expression.q = sqlglot_sql_q
