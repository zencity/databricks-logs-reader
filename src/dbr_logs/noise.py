"""Default noise filters for Spark/Databricks log output.

When --quiet is used, these patterns suppress common JVM, Spark internal,
and Databricks platform noise that buries application logs.
"""

import re
from collections.abc import Callable

from dbr_logs.models import LogEntry

# JVM startup boilerplate
_JVM_STARTUP_PATTERNS = [
    re.compile(r"OpenJDK.*warning.*deprecated"),
    re.compile(r"^SLF4J:"),
    re.compile(r"^ANTLR Tool version"),
    re.compile(r"^chown: invalid group"),
    re.compile(r"^WARNING.*terminally deprecated"),
    re.compile(r"^WARNING.*setSecurityManager"),
    re.compile(r"^WARNING.*Please consider reporting"),
    re.compile(r"^WARNING.*will be removed"),
    re.compile(r"SyntaxWarning: invalid escape"),
]

# JVM thread dump lines (shutdown dumps)
_THREAD_DUMP_PATTERNS = [
    re.compile(r'^"[^"]+"\s+id=\d+\s+state='),
    re.compile(r"^\s+at java\.base@"),
    re.compile(r"^\s+at app//"),
    re.compile(r"^\s+- (locked|waiting on) <0x"),
    re.compile(r"^\s+Locked synchronizers:"),
]

# Spark internal task lifecycle noise
_SPARK_NOISE_SUBSTRINGS = [
    "ShuffleBlockFetcherIterator:",
    "Executor: Finished task",
    "CoarseGrainedExecutorBackend: Got assigned task",
    "BlockManager: Removing broadcast",
    "BlockManager: Removed broadcast",
    "MemoryStore: Block broadcast",
    "TorrentBroadcast: Reading broadcast variable",
    "TorrentBroadcast: Unpersisting TorrentBroadcast",
    "MapOutputTrackerWorker: Doing the fetch",
    "MapOutputTrackerWorker: Got the output locations",
    "CodeGenerator: Code generated in",
    "FilterCompat: Filtering using predicate",
    "S3AbortableInputStream: Not all bytes were read",
]

# PySpark gateway connection messages
_PYSPARK_GATEWAY_PATTERNS = [
    re.compile(r"Connection to spark from PID"),
    re.compile(r"Initialized gateway on port"),
    re.compile(r"^Connected to spark\.$"),
]


def _is_noise(line: str) -> bool:
    if not line.strip():
        return False

    for pattern in _THREAD_DUMP_PATTERNS:
        if pattern.search(line):
            return True

    for substring in _SPARK_NOISE_SUBSTRINGS:
        if substring in line:
            return True

    for pattern in _JVM_STARTUP_PATTERNS:
        if pattern.search(line):
            return True

    return any(pattern.search(line) for pattern in _PYSPARK_GATEWAY_PATTERNS)


def build_quiet_filter() -> Callable[[list[LogEntry]], list[LogEntry]]:
    """Build a filter that removes common Spark/JVM/Databricks platform noise."""
    return lambda entries: [e for e in entries if not _is_noise(e.line)]
