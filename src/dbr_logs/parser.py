import logging
import re
from datetime import UTC, datetime

from dbr_logs.models import LogEntry, LogFile, LogSource

SPARK_LOG_RE = re.compile(
    r"(\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})"
)
SPARK_LOG_FMT = "%y/%m/%d %H:%M:%S"

ISO_BRACKET_RE = re.compile(
    r"\[(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3})[+\-]\d{4}\]"
)
ISO_BRACKET_FMT = "%Y-%m-%dT%H:%M:%S.%f"

LOG4J_RE = re.compile(
    r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3})"
)
LOG4J_FMT = "%Y-%m-%d %H:%M:%S,%f"

LEVEL_RE = re.compile(r"\b(ERROR|WARN(?:ING)?|INFO|DEBUG)\b")

LEVEL_MAP: dict[str, int] = {
    "ERROR": logging.ERROR,
    "WARN": logging.WARNING,
    "WARNING": logging.WARNING,
    "INFO": logging.INFO,
    "DEBUG": logging.DEBUG,
}

TIMESTAMP_PATTERNS = [
    (SPARK_LOG_RE, SPARK_LOG_FMT),
    (ISO_BRACKET_RE, ISO_BRACKET_FMT),
    (LOG4J_RE, LOG4J_FMT),
]


def parse_entries(content: str, source: LogSource, log_file: LogFile) -> list[LogEntry]:
    entries = []
    for raw_line in content.splitlines():
        if not raw_line.strip():
            continue
        entries.append(_parse_line(raw_line, source, log_file))
    return entries


def _parse_line(line: str, source: LogSource, log_file: LogFile) -> LogEntry:
    return LogEntry(
        source_type=source.source_type,
        source_detail=source.source_detail,
        stream=log_file.stream,
        timestamp=parse_timestamp(line),
        level=_parse_level(line),
        line=line,
        file_origin=log_file.path,
    )


def parse_timestamp(line: str) -> datetime | None:
    for pattern, fmt in TIMESTAMP_PATTERNS:
        m = pattern.search(line)
        if m:
            try:
                dt = datetime.strptime(m.group(1), fmt)
                return dt.replace(tzinfo=UTC)
            except ValueError:
                continue
    return None


def _parse_level(line: str) -> int | None:
    m = LEVEL_RE.search(line)
    if m:
        return LEVEL_MAP.get(m.group(1))
    return None
