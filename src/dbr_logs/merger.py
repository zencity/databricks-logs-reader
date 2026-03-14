from collections.abc import Iterator
from datetime import UTC, datetime
from heapq import merge

from dbr_logs.models import LogEntry

EPOCH = datetime.min.replace(tzinfo=UTC)


def merge_entries(entries_by_file: list[list[LogEntry]]) -> list[LogEntry]:
    """Merge log entries from multiple files into a single chronologically sorted list."""
    if not entries_by_file:
        return []
    if len(entries_by_file) == 1:
        return entries_by_file[0]

    keyed_iterables = []
    for file_idx, file_entries in enumerate(entries_by_file):
        file_ts = _first_known_timestamp(file_entries)
        keyed_iterables.append(_keyed_entries(file_entries, file_ts, file_idx))

    merged = merge(*keyed_iterables, key=lambda x: x[0])
    return [entry for _, entry in merged]


def _first_known_timestamp(entries: list[LogEntry]) -> datetime:
    for entry in entries:
        if entry.timestamp is not None:
            return entry.timestamp
    return EPOCH


def _keyed_entries(
    entries: list[LogEntry],
    file_ts: datetime,
    file_idx: int,
) -> Iterator[tuple[tuple[datetime, int, int], LogEntry]]:
    for line_idx, entry in enumerate(entries):
        ts = entry.timestamp or file_ts
        yield ((ts, file_idx, line_idx), entry)
