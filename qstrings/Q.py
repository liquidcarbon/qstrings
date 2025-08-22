import duckdb
import pathlib
import os
import sqlglot
import string

from abc import abstractmethod
from autoregistry import Registry
from typing import Any, Dict, Self, Union


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


class Q(BaseQ):
    """Default qstring class with runner registry."""

    def run(self, engine=None):
        engine = engine or "duckdb"
        return Engine[engine].run(self)

    def list(self, engine=None):
        """Return the result as a list."""
        engine = engine or "duckdb"
        return Engine[engine].list(self)

    def df(self, engine=None):
        """Return the result as a DataFrame."""
        engine = engine or "duckdb"
        return Engine[engine].df(self)


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
    def run(q: Q):
        return duckdb.sql(q)

    @staticmethod
    def list(q: Q):
        return DuckDBEngine.run(q).fetchall()


def sqlglot_sql_q(ex: sqlglot.expressions.Expression, *args, **kwargs):
    """Variant of sqlglot's Expression.sql that returns a Q string."""
    return Q(ex.sql(*args, **kwargs))


sqlglot.expressions.Expression.q = sqlglot_sql_q


class QStringError(Exception):
    pass
