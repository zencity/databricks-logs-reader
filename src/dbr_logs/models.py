from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum


class SourceType(StrEnum):
    DRIVER = "driver"
    EXECUTOR = "executor"


class Stream(StrEnum):
    STDERR = "stderr"
    STDOUT = "stdout"
    LOG4J = "log4j"
    STACKTRACE = "stacktrace"


@dataclass
class LogFile:
    path: str
    stream: Stream
    is_compressed: bool
    is_active: bool
    file_timestamp: datetime | None


@dataclass
class LogSource:
    source_type: SourceType
    source_detail: str  # "" for driver, "0"-"N" for executor
    files: list[LogFile] = field(default_factory=list)


@dataclass
class LogEntry:
    source_type: SourceType
    source_detail: str
    stream: Stream
    timestamp: datetime | None
    level: int | None  # logging.ERROR, logging.WARNING, logging.INFO, logging.DEBUG
    line: str
    file_origin: str


@dataclass
class RunInfo:
    job_name: str
    run_id: int | None  # numeric Databricks run ID
    cluster_id: str  # filesystem directory name (e.g. "0311-170011-t5450avl")
    env: str
    base_path: str  # full path to the run's log directory
    has_driver: bool = False
    has_executor: bool = False
    executor_count: int = 0
    app_id: str | None = None
