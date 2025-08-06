import pathlib
import sqlglot

from os import PathLike
from typing import Any, Union

PathType = Union[pathlib.Path, Any]
StrPath = Union[str, PathLike[str]]


class Q(str):
    """Smart query string."""

    def __new__(cls, s: str, parser_raises: bool = True, **kwargs):
        """Create a Q string.

        Args:
            s (str): the base string.
        """
        qstr = str.__new__(cls, s)
        try:
            qstr.ast = sqlglot.parse_one(s)
            qstr.errors = ""
        except sqlglot.errors.ParseError as e:
            if kwargs.get("validate"):
                raise e
            qstr.errors = str(e)
        return qstr

    @classmethod
    def format(
        cls,
        template: StrPath,
        file: bool = False,
        path_type: PathType = pathlib.Path,
        **kwargs,
    ):
        if file:
            return cls.format_from_file(template, path_type, **kwargs)
        else:
            return cls(template.format(**kwargs))

    @classmethod
    def format_from_file(
        cls, path: StrPath, path_type: PathType = pathlib.Path, **kwargs
    ):
        _path = path_type(path)
        if not _path.exists():
            raise FileNotFoundError(f"File not found: {_path}")
        with _path.open("r") as f:
            template = f.read()
        return cls(template.format(**kwargs))


def sqlglot_sql_q(ex: sqlglot.expressions.Expression, *args, **kwargs):
    """Variant of sqlglot's Expression.sql that returns a Q string."""
    return Q(ex.sql(*args, **kwargs))


sqlglot.expressions.Expression.q = sqlglot_sql_q
