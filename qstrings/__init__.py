from .config import get_version
from .Q import DuckDBEngine, Engine, Q, QStringError

__all__ = ["DuckDBEngine", "Engine", "Q", "QStringError"]
__version__ = get_version()
