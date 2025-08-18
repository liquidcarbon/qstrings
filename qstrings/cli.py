from cyclopts import App, Parameter
from qstrings.Q import Q
from typing import Annotated, Literal


app = App()


@app.default()
def run_query(
    query: Annotated[Q, Parameter(help="Query string")],
    output_format: Annotated[
        Literal["table"], Parameter(name=["-o"], help="output format")
    ] = "table",
):
    return query.run()


if __name__ == "__main__":
    app()
