import duckdb
import pathlib
import os
import sqlglot
import string

from abc import abstractmethod
from autoregistry import Registry
from datetime import datetime
from typing import Any, Dict, Self, Union

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

    __file__ = __file__

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
        qstr.refs = refs  # references used to create the Q string
        qstr.file = _path if file else None
        qstr.exec_id = 0
        qstr.duration = 0.0
        qstr._quiet = kwargs.get("quiet", False)
        try:
            qstr.ast = sqlglot.parse_one(s_formatted)
            qstr.ast_errors = ""
        except sqlglot.errors.ParseError as e:
            if kwargs.get("validate"):
                raise e
            qstr.ast = None
            qstr.ast_errors = str(e)
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
        d = {k: str(v) for k, v in self.__dict__.items() if not k.startswith("_")}
        return d


class Q(BaseQ):
    """Default qstring class with timer and runner registry."""

    def timer_logger(func):
        def logging_wrapper(self, *args, **kwargs):
            quiet = getattr(self, "_quiet", False) or kwargs.get("quiet", False)
            t0 = int(f"{datetime.now():%y%m%d%H%M%S%f}")

            self.exec_id = int(f"{datetime.now():%y%m%d%H%M%S%f}")
            try:
                result = func(self, *args, **kwargs)
            except Exception as e:
                if not quiet:
                    log.error(f"Error: {e}")
                raise e

            self.duration = round((self.exec_id - t0) / 1e6, 4)
            _r, _c = getattr(self, "shape", (0, 0))
            msg = (
                f"{self._engine_cls}: {_r} rows x {_c} cols in {self.duration:.4f} sec"
            )
            if not quiet:
                log.info(msg)
            return result

        return logging_wrapper

    @timer_logger
    def run(self, engine=None, **kwargs):
        engine = engine or "duckdb"
        cls = Engine[engine]
        self._engine_cls = cls.__name__
        return cls.run(self, **kwargs)

    def list(self, engine=None, **kwargs):
        """Return the result as a list."""
        engine = engine or "duckdb"
        return Engine[engine].list(self, **kwargs)

    def df(self, engine=None, **kwargs):
        """Return the result as a DataFrame."""
        engine = engine or "duckdb"
        return Engine[engine].df(self, **kwargs)


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


class DuckDBEngine(Engine):
    """DuckDB engine.  By default runs using in-memory database."""

    def run(q: Q, db: StrPath = "", **kwargs) -> duckdb.DuckDBPyRelation:
        q.con = duckdb.connect(database=db, read_only=kwargs.get("read_only", False))
        # connection to remain attached to q, otherwise closed and gc'd
        try:
            relation = q.con.sql(q)
            q.shape = list(relation.shape)
        except Exception as e:
            relation = q.con.sql(f"SELECT '{q}' AS q, '{e}' AS r")
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

    def run(q: Q, model: str = "openai/gpt-oss-20b:fireworks-ai", **kwargs):
        """Run LLM query on HF.  Requires env var `HF_API_KEY`."""
        from openai import OpenAI

        client = OpenAI(
            base_url="https://router.huggingface.co/v1", api_key=os.getenv("HF_API_KEY")
        )
        response = client.responses.create(model=model, input=q)
        q.response = response
        # log.debug(response.model_dump_json(indent=2))
        input_tokens = response.usage.input_tokens
        output_tokens = response.usage.output_tokens
        if not q._quiet and not kwargs.get("quiet"):
            log.debug(f"{input_tokens=}")
            log.debug(f"{output_tokens=}")
        result = response.output[1].content[0].text
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
