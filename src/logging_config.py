import logging
import sys
from datetime import UTC, datetime
from typing import Any

from src.config import get_settings


class UtcFormatter(logging.Formatter):
    converter = staticmethod(lambda *args: datetime.now(UTC).timetuple())


def configure_logging() -> None:
    settings = get_settings()
    level = getattr(logging, settings.log_level.upper(), logging.INFO)
    root = logging.getLogger()
    root.setLevel(level)

    if not root.handlers:
        handler = logging.StreamHandler(sys.stdout)
        root.addHandler(handler)
    else:
        handler = root.handlers[0]

    if settings.log_format == "json":
        formatter = UtcFormatter('{"ts":"%(asctime)s","level":"%(levelname)s","logger":"%(name)s","message":"%(message)s"}')
    else:
        formatter = UtcFormatter("%(asctime)s %(levelname)s %(message)s")
    formatter.default_time_format = "%Y-%m-%dT%H:%M:%S"
    formatter.default_msec_format = "%s.%03dZ"
    handler.setFormatter(formatter)


def _format_value(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return str(value).lower()
    if isinstance(value, list | tuple | set):
        return "[" + ",".join(str(item) for item in value) + "]"
    text = str(value).replace("\n", " ").replace("\r", " ")
    if " " in text:
        return repr(text)
    return text


def log_event(logger: logging.Logger, event: str, level: int = logging.INFO, **fields: Any) -> None:
    parts = [event]
    parts.extend(f"{key}={_format_value(value)}" for key, value in fields.items() if value is not None)
    logger.log(level, " ".join(parts))
