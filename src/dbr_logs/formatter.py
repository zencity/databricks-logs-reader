import json
import logging
import sys
from typing import TextIO

from dbr_logs.models import LogEntry, SourceType

_LEVEL_INT_TO_NAME: dict[int, str] = {
    logging.ERROR: "ERROR",
    logging.WARNING: "WARN",
    logging.INFO: "INFO",
    logging.DEBUG: "DEBUG",
}


def format_text(entry: LogEntry) -> str:
    source = _source_label(entry)
    ts = entry.timestamp.strftime("%Y-%m-%dT%H:%M:%S") if entry.timestamp else "                   "
    return f"[{source:<12s} {ts}] {entry.line}"


def format_jsonl(entry: LogEntry) -> str:
    obj = {
        "source_type": entry.source_type,
        "source_detail": entry.source_detail,
        "stream": entry.stream,
        "timestamp": entry.timestamp.isoformat() if entry.timestamp else None,
        "level": _LEVEL_INT_TO_NAME.get(entry.level) if entry.level is not None else None,
        "file": entry.file_origin,
        "line": entry.line,
    }
    return json.dumps(obj, ensure_ascii=False)


def write_entries(
    entries: list[LogEntry],
    fmt: str,
    output: TextIO | None = None,
) -> None:
    """Write log entries to output in text or jsonl format."""
    if output is None:
        output = sys.stdout
    format_fn = format_jsonl if fmt == "jsonl" else format_text
    try:
        for entry in entries:
            output.write(format_fn(entry) + "\n")
            output.flush()
    except BrokenPipeError:
        sys.stderr.close()
        sys.exit(0)


def _source_label(entry: LogEntry) -> str:
    if entry.source_type == SourceType.DRIVER:
        return f"driver:{entry.stream}"
    return f"exec/{entry.source_detail}:{entry.stream}"
