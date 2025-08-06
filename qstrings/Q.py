import sqlglot


class Q(str):
    """Smart query string."""

    def __new__(cls, s: str, **kwargs):
        instance = str.__new__(cls, s)
        try:
            instance.ast = sqlglot.parse_one(instance)
            instance.errors = ""
        except sqlglot.errors.ParseError as e:
            if kwargs.get("validate"):
                raise e
            instance.errors = str(e)
        return instance


def sqlglot_sql_q(ex: sqlglot.expressions.Expression, *args, **kwargs):
    """
    Add method to sqlglot's Expression.sql method to return a Q string.
    """
    return Q(ex.sql(*args, **kwargs))


sqlglot.expressions.Expression.q = sqlglot_sql_q
