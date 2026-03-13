import re
from datetime import UTC, datetime

from dbr_logs.databricks_client import DatabricksClient
from dbr_logs.models import LogFile, LogSource, RunInfo, SourceType, Stream

DRIVER_STDERR_RE = re.compile(r"^stderr(--(\d{4}-\d{2}-\d{2})--(\d{2})-?(\d{2}))?$")
DRIVER_STDOUT_RE = re.compile(r"^stdout(--(\d{4}-\d{2}-\d{2})--(\d{2})-?(\d{2}))?$")
EXECUTOR_STDERR_RE = re.compile(r"^stderr(--(\d{4}-\d{2}-\d{2})--(\d{2}))?(\.gz)?$")
EXECUTOR_STDOUT_RE = re.compile(r"^stdout(--(\d{4}-\d{2}-\d{2})--(\d{2}))?(\.gz)?$")
LOG4J_ACTIVE_RE = re.compile(r"^log4j-active\.log$")
LOG4J_ROTATED_RE = re.compile(r"^log4j-(\d{4}-\d{2}-\d{2}-\d{2})\.log\.gz$")
STACKTRACE_ACTIVE_RE = re.compile(r"^stacktrace\.log$")
STACKTRACE_ROTATED_RE = re.compile(r"^(\d{4}-\d{2}-\d{2}-\d{2})\.stacktrace\.log\.gz$")
APP_DIR_RE = re.compile(r"^app-\d{14}-\d{4}$")


def discover_sources(
    client: DatabricksClient,
    run_info: RunInfo,
    include_log4j: bool,
    include_stacktrace: bool,
) -> list[LogSource]:
    sources: list[LogSource] = []

    driver_path = f"{run_info.base_path}/driver"
    try:
        driver = _discover_driver(client, driver_path, include_log4j, include_stacktrace)
        if driver.files:
            sources.append(driver)
            run_info.has_driver = True
    except Exception:
        pass

    executor_path = f"{run_info.base_path}/executor"
    try:
        executor_sources = _discover_executors(client, executor_path, run_info)
        sources.extend(executor_sources)
        if executor_sources:
            run_info.has_executor = True
            run_info.executor_count = len(executor_sources)
    except Exception:
        pass

    return sources


def _discover_driver(
    client: DatabricksClient,
    driver_path: str,
    include_log4j: bool,
    include_stacktrace: bool,
) -> LogSource:
    source = LogSource(source_type=SourceType.DRIVER, source_detail="")

    for entry in client.list_directory(driver_path):
        classified = _classify_driver_file(entry.name, include_log4j, include_stacktrace)
        if classified:
            stream, is_compressed, is_active, file_ts = classified
            source.files.append(
                LogFile(
                    path=f"{driver_path}/{entry.name}",
                    stream=stream,
                    is_compressed=is_compressed,
                    is_active=is_active,
                    file_timestamp=file_ts,
                )
            )

    source.files.sort(key=lambda f: (f.stream, f.file_timestamp or datetime.max))
    return source


def _discover_executors(
    client: DatabricksClient,
    executor_path: str,
    run_info: RunInfo,
) -> list[LogSource]:
    sources: list[LogSource] = []

    for app_entry in client.list_directory(executor_path):
        if not app_entry.is_directory:
            continue
        if not APP_DIR_RE.match(app_entry.name):
            continue
        run_info.app_id = app_entry.name
        app_path = f"{executor_path}/{app_entry.name}"

        for exec_entry in client.list_directory(app_path):
            if not exec_entry.is_directory:
                continue
            exec_num = exec_entry.name
            exec_full = f"{app_path}/{exec_num}"
            source = _discover_single_executor(client, exec_full, exec_num)
            if source.files:
                sources.append(source)

    sources.sort(key=lambda s: int(s.source_detail) if s.source_detail.isdigit() else 0)
    return sources


def _discover_single_executor(
    client: DatabricksClient,
    exec_path: str,
    exec_num: str,
) -> LogSource:
    source = LogSource(source_type=SourceType.EXECUTOR, source_detail=exec_num)

    for entry in client.list_directory(exec_path):
        classified = _classify_executor_file(entry.name)
        if classified:
            stream, is_compressed, is_active, file_ts = classified
            source.files.append(
                LogFile(
                    path=f"{exec_path}/{entry.name}",
                    stream=stream,
                    is_compressed=is_compressed,
                    is_active=is_active,
                    file_timestamp=file_ts,
                )
            )

    source.files.sort(key=lambda f: (f.stream, f.file_timestamp or datetime.max))
    return source


def _classify_driver_file(
    name: str,
    include_log4j: bool,
    include_stacktrace: bool,
) -> tuple[Stream, bool, bool, datetime | None] | None:
    for pattern, stream in [(DRIVER_STDERR_RE, Stream.STDERR), (DRIVER_STDOUT_RE, Stream.STDOUT)]:
        m = pattern.match(name)
        if m:
            is_active = m.group(1) is None
            file_ts = _parse_driver_rotation_ts(m) if not is_active else None
            return (stream, False, is_active, file_ts)

    if include_log4j:
        if LOG4J_ACTIVE_RE.match(name):
            return (Stream.LOG4J, False, True, None)
        m = LOG4J_ROTATED_RE.match(name)
        if m:
            return (Stream.LOG4J, True, False, _parse_hourly_ts(m.group(1)))

    if include_stacktrace:
        if STACKTRACE_ACTIVE_RE.match(name):
            return (Stream.STACKTRACE, False, True, None)
        m = STACKTRACE_ROTATED_RE.match(name)
        if m:
            return (Stream.STACKTRACE, True, False, _parse_hourly_ts(m.group(1)))

    return None


def _classify_executor_file(
    name: str,
) -> tuple[Stream, bool, bool, datetime | None] | None:
    patterns = [(EXECUTOR_STDERR_RE, Stream.STDERR), (EXECUTOR_STDOUT_RE, Stream.STDOUT)]
    for pattern, stream in patterns:
        m = pattern.match(name)
        if m:
            is_active = m.group(1) is None and m.group(4) is None
            is_gz = m.group(4) == ".gz"
            file_ts = _parse_executor_rotation_ts(m) if not is_active else None
            return (stream, is_gz, is_active, file_ts)
    return None


def _parse_driver_rotation_ts(m: re.Match[str]) -> datetime | None:
    try:
        date_str = m.group(2)
        hour = int(m.group(3))
        minute = int(m.group(4)) if m.group(4) else 0
        parts = date_str.split("-")
        return datetime(
            int(parts[0]),
            int(parts[1]),
            int(parts[2]),
            hour,
            minute,
            tzinfo=UTC,
        )
    except (ValueError, IndexError):
        return None


def _parse_executor_rotation_ts(m: re.Match[str]) -> datetime | None:
    try:
        date_str = m.group(2)
        hour = int(m.group(3))
        parts = date_str.split("-")
        return datetime(
            int(parts[0]),
            int(parts[1]),
            int(parts[2]),
            hour,
            tzinfo=UTC,
        )
    except (ValueError, IndexError):
        return None


def _parse_hourly_ts(ts_str: str) -> datetime | None:
    try:
        parts = ts_str.split("-")
        return datetime(
            int(parts[0]),
            int(parts[1]),
            int(parts[2]),
            int(parts[3]),
            tzinfo=UTC,
        )
    except (ValueError, IndexError):
        return None
