import importlib
import logging
import os
import sys
import tomllib
from pathlib import Path

# HISTORY = Path(__file__).parent / "history.duckdb"


def read_pyproject() -> dict:
    pyproject_path = Path(__file__).parent.parent / "pyproject.toml"
    if not pyproject_path.exists():
        pyproject_dict = {}
    else:
        with open(pyproject_path, "r") as f:
            pyproject_dict = tomllib.loads(f.read())
    return pyproject_dict


PYPROJECT = read_pyproject()


def get_version():
    package_version = PYPROJECT.get("project", {}).get("version")
    if not package_version:
        try:
            package_version = importlib.metadata.version("qstrings")
        except importlib.metadata.PackageNotFoundError:
            package_version = "v_unknown"
    return package_version


class VersionedFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        parts = record.name.split(".")
        if "[" not in parts[0]:
            parts[0] += f"[{get_version()}]"
        record.name = ".".join(parts)
        return super().format(record)


def setup_logger(name: str = "qstrings", sink=sys.stdout, level=logging.DEBUG):
    # to disable: logging.getLogger("qstrings").setLevel(logging.CRITICAL + 1)
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sink)
        fmt = "/* %(asctime)s|%(levelname)s|%(name)s:%(lineno)d|%(message)s */"
        datefmt = "%y%m%d@%H:%M:%S"
        formatter = VersionedFormatter(fmt=fmt, datefmt=datefmt)
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(level)

    return logger


log = setup_logger()


def setup_history(history_path: str | Path | None = None) -> Path:
    """Resolve the history file path with precedence:

    1. Setting directly
    2. Env var QSTRINGS_HISTORY
    3. pyproject.toml [tool.qstrings] history
    4. Default: history.duckdb in package dir
    """
    if history_path:
        return Path(history_path)

    if env := os.getenv("QSTRINGS_HISTORY"):
        return Path(env)

    pyproject = Path(__file__).parent.parent / "pyproject.toml"
    if pyproject.exists():
        with pyproject.open("rb") as f:
            data = tomllib.load(f)
        history = data.get("tool", {}).get("qstrings", {}).get("history")
        if history:
            return Path(history)

    # fallback to package-local file
    return Path(__file__).parent / "history.duckdb"
