from __future__ import annotations

import logging
import os
import sys
from typing import Final, TextIO

__all__ = ["get_logger", "configure_default_logging"]

_LOGGER_NAME: Final[str] = "kafklient"
_LEVEL_NAME: Final[str] = os.environ.get("KAFKLIENT_LOG_LEVEL", "INFO").upper()
_resolved_level = getattr(logging, _LEVEL_NAME, logging.INFO)
if not isinstance(_resolved_level, int):
    _resolved_level = logging.INFO
_DEFAULT_LEVEL: Final[int] = _resolved_level
_LEVEL_COLORS: Final[dict[int, str]] = {
    logging.DEBUG: "\x1b[36m",  # Cyan
    logging.INFO: "\x1b[32m",  # Green
    logging.WARNING: "\x1b[33m",  # Yellow
    logging.ERROR: "\x1b[31m",  # Red
    logging.CRITICAL: "\x1b[35m",  # Magenta
}
_RESET: Final[str] = "\x1b[0m"


def _supports_color(stream: TextIO) -> bool:
    if os.environ.get("NO_COLOR"):
        return False
    try:
        if not stream.isatty():
            return False
    except Exception:
        return False

    if sys.platform != "win32":
        return True
    return bool(
        os.environ.get("ANSICON")
        or os.environ.get("WT_SESSION")
        or os.environ.get("TERM_PROGRAM") == "vscode"
    )


class _ColorFormatter(logging.Formatter):
    def __init__(self, use_color: bool) -> None:
        super().__init__(
            fmt="%(asctime)s | %(colored_levelname)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        self._use_color = use_color

    def format(self, record: logging.LogRecord) -> str:
        level_name = record.levelname
        if self._use_color:
            color = _LEVEL_COLORS.get(record.levelno)
            if color:
                level_name = f"{color}{level_name}{_RESET}"
        setattr(record, "colored_levelname", level_name)
        return super().format(record)


def configure_default_logging() -> logging.Logger:
    logger = logging.getLogger(_LOGGER_NAME)
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(_ColorFormatter(_supports_color(handler.stream)))
        logger.addHandler(handler)
        logger.propagate = False
    logger.setLevel(_DEFAULT_LEVEL)
    return logger


def get_logger(name: str | None = None) -> logging.Logger:
    base_logger = configure_default_logging()
    if not name or name == _LOGGER_NAME:
        return base_logger
    return logging.getLogger(name)
