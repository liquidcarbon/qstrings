import importlib
import sys
import tomllib
from pathlib import Path
from loguru import logger as log


HISTORY = Path(__file__).parent / "history.duckdb"


def get_version():
    pyproject_path = Path(__file__).parent.parent / "pyproject.toml"
    if not pyproject_path.exists():
        package_version = None  # for pip-installed tdw
    else:
        with open(pyproject_path, "r") as pyproject:
            toml = tomllib.loads(pyproject.read())
            package_version = toml.get("project", {}).get("version")
    if not package_version:
        try:
            package_version = importlib.metadata.version("qstrings")
        except importlib.metadata.PackageNotFoundError:
            package_version = "v_unknown"
    return package_version


__version__ = get_version()


def log_format(record: dict):
    module_names = record["name"].split(".")
    module_names[0] += f"[{__version__}]"
    record["name"] = ".".join(module_names)
    msg = "/* {time:YYMMDD@HH:mm:ss.SSS}|{level}|{name}:{line}|{message} */\n"
    return msg


log.remove()
log.add(sys.stdout, format=log_format)
