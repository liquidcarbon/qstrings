from cyclopts import App, Parameter
from typing import Annotated, Literal, Optional
import platform
import sys

from .config import setup_history, setup_logger
from .Q import Q


Q.HISTORY = setup_history()

if platform.system() == "Windows":
    STDOUT = "CON"
else:
    STDOUT = "/dev/stdout"

log = setup_logger(sink=sys.stderr)


app = App(help="Query anything!")


@app.default
def run_query(
    query: Annotated[
        Optional[str], Parameter(help="Query string", show_default=False)
    ] = "",
    *,
    file: Annotated[
        Optional[str],
        Parameter(name=["-f", "--file"], help="File template", show_default=False),
    ] = "",
    engine: Annotated[
        Optional[str], Parameter(name=["-e", "--engine"], help="Query engine")
    ] = "duckdb",
    model: Annotated[
        Optional[str], Parameter(name=["-m", "--model"], help="HuggingFace model")
    ] = "openai/gpt-oss-20b:fireworks-ai",
    limit: Annotated[
        Optional[int], Parameter(name=["-L", "--LIMIT"], help="Limit rows")
    ] = None,
    only_count: Annotated[
        bool, Parameter(name=["-C", "--COUNT"], help="Return row count only")
    ] = False,
    output_format: Annotated[
        Literal["engine", "csv", "list", "line"],
        Parameter(
            name=["-o"],
            help="Output format (defaults to whatever `Engine.run()` returns)",
        ),
    ] = "engine",
    quiet: Annotated[
        bool, Parameter(name=["-q", "--quiet"], help="Suppress logs")
    ] = False,
    **kwargs,
):
    # log.debug(f"{query=} {file=} {engine=} {model=} {output_format=}")
    if not query and not file:
        if sys.stdin.isatty():  # interactive
            print("See `q --help` or enter query below (end with `;`):")
            lines = []
            while True:
                try:
                    line = input()
                except EOFError:
                    return  # user hit Ctrl-D
                lines.append(line)
                if line.strip().endswith(";"):
                    break
            query = "\n".join(lines)
        else:
            print("Enter query (end with EOF / Ctrl-D):")
            query = sys.stdin.read()

    query_with_newlines = query.replace(r"\n", "\n")
    q = Q(query_with_newlines, file=file, quiet=quiet, **kwargs)
    if limit:
        q = q.limit(limit)
    if only_count:
        q = q.count

    if output_format == "engine":
        res = q.run(engine=engine, **kwargs)
    elif output_format == "list":
        res = q.list(engine=engine, **kwargs)
    elif output_format == "csv":
        if engine == "duckdb":
            q.run(**kwargs).to_csv(STDOUT)
            res = None
        else:
            res = "not implemented"
    elif output_format == "line":
        res = "\\n".join(
            ",".join([str(v) for v in row]) for row in q.list(engine=engine, **kwargs)
        )
    sys.stdout.write(str(res))
    return


app.command(app_history := App(name="h", help="Run query from history"))


@app_history.command(name="h", help="Run query from history / view history")
@app_history.default
def query_history(
    alias: Annotated[Optional[str], Parameter(name="-a")] = None,
    exec_id: Annotated[Optional[int], Parameter(name="-i")] = None,
    limit: Annotated[Optional[int], Parameter(name=["-L", "--LIMIT"])] = None,
):
    if alias is None and exec_id is None:
        run_query(
            "FROM q ORDER BY exec_id", db=Q.HISTORY, limit=limit, output_format="csv"
        )
        exit()

    qh = Q.from_history(alias=alias, exec_id=exec_id)
    log.debug(f"Running from history: {qh}")
    if limit:
        qh = qh.limit(limit)
    return qh.run()


if __name__ == "__main__":
    app()
