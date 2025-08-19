import duckdb
import pathlib
import os
import sqlglot
import string

from typing import Any, Dict, Union

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


class Q(str):
    """Smart query string."""

    def __new__(
        cls,
        s: str = "",
        file: StrPath = None,
        path_type: PathType = pathlib.Path,
        **kwargs: Dict[str, Any],
    ):
        """Create a Q string.

        Args:
            s (str): the base string.
        """

        if file:
            _path = path_type(file)
            if not _path.exists():
                raise FileNotFoundError(f"File not found: {_path}")
            with _path.open("r") as f:
                s = f.read()

        keys_needed = parse_keys(s)
        keys_given = set(dict(**kwargs, **os.environ))
        keys_missing = keys_needed - keys_given
        if keys_missing:
            raise QStringError(f"values missing for keys: {keys_missing}")
        s_formatted = s.format(**kwargs, **os.environ)

        qstr = str.__new__(cls, s_formatted)
        try:
            qstr.ast = sqlglot.parse_one(s)
            qstr.errors = ""
        except sqlglot.errors.ParseError as e:
            if kwargs.get("validate"):
                raise e
            qstr.ast = None
            qstr.errors = str(e)
        return qstr

    def run(self):
        return duckdb.sql(self)


def sqlglot_sql_q(ex: sqlglot.expressions.Expression, *args, **kwargs):
    """Variant of sqlglot's Expression.sql that returns a Q string."""
    return Q(ex.sql(*args, **kwargs))


sqlglot.expressions.Expression.q = sqlglot_sql_q


class QStringError(Exception):
    pass
