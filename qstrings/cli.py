import typer
from qstrings.Q import Q
from typing import Annotated

app = typer.Typer()


@app.command()
def run_query(
    query: Annotated[str, typer.Argument(help="Query string")],
    output_format: str = "df",
):
    q = Q(query)
    print(q.run())


if __name__ == "__main__":
    app()
