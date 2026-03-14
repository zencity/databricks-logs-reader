import logging
import re
from collections.abc import Callable
from datetime import UTC, datetime, timedelta

from dbr_logs.models import LogEntry

RELATIVE_RE = re.compile(r"^(\d+)(h|m)$")

LEVEL_NAME_MAP: dict[str, int] = {
    "ERROR": logging.ERROR,
    "WARN": logging.WARNING,
    "WARNING": logging.WARNING,
    "INFO": logging.INFO,
    "DEBUG": logging.DEBUG,
}


def build_filter(
    levels: list[str] | None,
    since: datetime | None,
    tail: int | None,
) -> Callable[[list[LogEntry]], list[LogEntry]]:
    predicates: list[Callable[[list[LogEntry]], list[LogEntry]]] = []

    if levels:
        level_ints = {LEVEL_NAME_MAP[lv.upper()] for lv in levels if lv.upper() in LEVEL_NAME_MAP}
        predicates.append(lambda entries, lv=level_ints: _level_filter(entries, lv))  # type: ignore[misc]

    if since:
        predicates.append(lambda entries, s=since: _since_filter(entries, s))  # type: ignore[misc]

    def apply_all(entries: list[LogEntry]) -> list[LogEntry]:
        result = entries
        for pred in predicates:
            result = pred(result)
        if tail is not None:
            result = result[-tail:]
        return result

    return apply_all


def _level_filter(entries: list[LogEntry], levels: set[int]) -> list[LogEntry]:
    return [e for e in entries if e.level in levels]


def _since_filter(entries: list[LogEntry], since: datetime) -> list[LogEntry]:
    return [e for e in entries if e.timestamp is not None and e.timestamp >= since]


def parse_since(since_str: str) -> datetime:
    m = RELATIVE_RE.match(since_str)
    if m:
        amount, unit = int(m.group(1)), m.group(2)
        delta = timedelta(hours=amount) if unit == "h" else timedelta(minutes=amount)
        return datetime.now(tz=UTC) - delta
    dt = datetime.fromisoformat(since_str)
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)
