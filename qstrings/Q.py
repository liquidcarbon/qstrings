import duckdb
import pathlib
import os
import sqlglot
import string

from abc import abstractmethod
from autoregistry import Registry
from time import time
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

    def __new__(
        cls,
        s: str = "",
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
        qstr.refs = refs  # references used to create the Q string
        qstr.file = _path if file else None
        qstr.start = 0
        qstr.duration = 0.0
        qstr.quiet = kwargs.get("quiet", False)
        try:
            qstr.ast = sqlglot.parse_one(s)
            qstr.errors = ""
        except sqlglot.errors.ParseError as e:
            if kwargs.get("validate"):
                raise e
            qstr.ast = None
            qstr.errors = str(e)
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


class Q(BaseQ):
    """Default qstring class with timer runner registry."""

    def timer(func):
        def wrapper(self, *args, **kwargs):
            self.start = time()
            result = func(self, *args, **kwargs)
            self.duration = time() - self.start
            return result

        return wrapper

    @timer
    def run(self, engine=None, **kwargs):
        engine = engine or "duckdb"
        return Engine[engine].run(self, **kwargs)

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
    def run(q: Q, **kwargs):
        try:
            relation = duckdb.sql(q)
            q.shape = relation.shape
            msg = f"{q.shape[0]} rows x {q.shape[1]} cols in {q.duration:.4f} sec"
            if not q.quiet and not kwargs.get("quiet"):
                log.info(msg)

            return relation
        except Exception as e:
            log.error(f"error {e}:\n{q}")
            return None

    @staticmethod
    def list(q: Q, header=True, **kwargs):
        rel = DuckDBEngine.run(q, **kwargs)
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
        from openai import OpenAI

        client = OpenAI(
            base_url="https://router.huggingface.co/v1", api_key=os.getenv("HF_API_KEY")
        )
        response = client.responses.create(model=model, input=q)
        q.response = response
        # log.debug(response.model_dump_json(indent=2))
        input_tokens = response.usage.input_tokens
        output_tokens = response.usage.output_tokens
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
