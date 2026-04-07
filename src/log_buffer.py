"""In-memory log buffer — son 50 satırı tutar, Telegram'a gönderilebilir."""
from __future__ import annotations

from collections import deque
from loguru import logger

_buffer: deque[str] = deque(maxlen=50)


def _sink(message: str) -> None:
    _buffer.append(message.rstrip())


logger.add(_sink, format="{time:HH:mm:ss} {level.name[0]} {message}", level="INFO")


def get_recent(n: int = 20) -> str:
    lines = list(_buffer)[-n:]
    return "\n".join(lines) if lines else "Log yok."
