import sqlglot


class Q(str):
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
