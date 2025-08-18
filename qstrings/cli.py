from cyclopts import App, Parameter
from qstrings.Q import Q
from typing import Annotated, Literal
import platform

if platform.system() == "Windows":
    STDOUT = "CON"
else:
    STDOUT = "/dev/stdout"


app = App()


@app.default()
def run_query(
    query: Annotated[Q, Parameter(help="Query string")],
    output_format: Annotated[
        Literal["csv", "table"], Parameter(name=["-o"], help="output format")
    ] = "table",
):
    if output_format == "table":
        res = query.run()
    elif output_format == "csv":
        query.run().to_csv(STDOUT)
        res = None
    return res


if __name__ == "__main__":
    app()
