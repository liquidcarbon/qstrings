import importlib
import logging
import sys
import tomllib
from pathlib import Path

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


class VersionedFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        parts = record.name.split(".")
        if "[" not in parts[0]:
            parts[0] += f"[{__version__}]"
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
