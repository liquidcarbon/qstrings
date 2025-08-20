from cyclopts import App, Parameter
from qstrings.Q import Q
from typing import Annotated, Literal
import platform

if platform.system() == "Windows":
    STDOUT = "CON"
else:
    STDOUT = "/dev/stdout"


app = App(help="Query anything!")


@app.default()
def run_query(
    query: Annotated[str, Parameter(help="Query string", show_default=False)] = "",
    file: Annotated[
        str, Parameter(name=["-f", "--file"], help="File template", show_default=False)
    ] = "",
    output_format: Annotated[
        Literal["csv", "list", "table"], Parameter(name=["-o"], help="Output format")
    ] = "table",
    **kwargs,
):
    q = Q(query, file=file, **kwargs)
    if output_format == "table":
        res = q.run()
    elif output_format == "list":
        res = q.list()
    elif output_format == "csv":
        q.run().to_csv(STDOUT)
        res = None
    return res


if __name__ == "__main__":
    app()
