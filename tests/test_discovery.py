from datetime import UTC, datetime
from unittest.mock import MagicMock

from dbr_logs.databricks_client import DirEntry
from dbr_logs.discovery import (
    _classify_driver_file,
    _classify_executor_file,
    discover_sources,
)
from dbr_logs.models import RunInfo


class TestClassifyDriverFile:
    def test_active_stderr(self) -> None:
        result = _classify_driver_file("stderr", include_log4j=False, include_stacktrace=False)
        assert result is not None
        stream, is_compressed, is_active, file_ts = result
        assert stream == "stderr"
        assert is_compressed is False
        assert is_active is True

    def test_rotated_stderr(self) -> None:
        result = _classify_driver_file(
            "stderr--2026-03-11--18-00", include_log4j=False, include_stacktrace=False
        )
        assert result is not None
        stream, is_compressed, is_active, file_ts = result
        assert stream == "stderr"
        assert is_active is False
        assert file_ts == datetime(2026, 3, 11, 18, 0, tzinfo=UTC)

    def test_log4j_excluded_by_default(self) -> None:
        result = _classify_driver_file(
            "log4j-active.log", include_log4j=False, include_stacktrace=False
        )
        assert result is None

    def test_log4j_included(self) -> None:
        result = _classify_driver_file(
            "log4j-active.log", include_log4j=True, include_stacktrace=False
        )
        assert result is not None
        assert result[0] == "log4j"

    def test_log4j_rotated(self) -> None:
        result = _classify_driver_file(
            "log4j-2026-03-11-17.log.gz", include_log4j=True, include_stacktrace=False
        )
        assert result is not None
        assert result[0] == "log4j"
        assert result[1] is True  # compressed

    def test_stacktrace_excluded_by_default(self) -> None:
        result = _classify_driver_file(
            "stacktrace.log", include_log4j=False, include_stacktrace=False
        )
        assert result is None

    def test_stacktrace_included(self) -> None:
        result = _classify_driver_file(
            "stacktrace.log", include_log4j=False, include_stacktrace=True
        )
        assert result is not None
        assert result[0] == "stacktrace"

    def test_unknown_file_ignored(self) -> None:
        result = _classify_driver_file("eventlog", include_log4j=False, include_stacktrace=False)
        assert result is None


class TestClassifyExecutorFile:
    def test_active_stderr(self) -> None:
        result = _classify_executor_file("stderr")
        assert result is not None
        stream, is_compressed, is_active, _ = result
        assert stream == "stderr"
        assert is_compressed is False
        assert is_active is True

    def test_rotated_gz_stderr(self) -> None:
        result = _classify_executor_file("stderr--2026-03-11--18.gz")
        assert result is not None
        stream, is_compressed, is_active, file_ts = result
        assert stream == "stderr"
        assert is_compressed is True
        assert is_active is False
        assert file_ts == datetime(2026, 3, 11, 18, 0, tzinfo=UTC)

    def test_active_stdout(self) -> None:
        result = _classify_executor_file("stdout")
        assert result is not None
        assert result[0] == "stdout"
        assert result[2] is True

    def test_unknown_file(self) -> None:
        assert _classify_executor_file("init_scripts") is None


class TestDiscoverSources:
    def test_discovers_driver_and_executor(self) -> None:
        client = MagicMock()
        run_info = RunInfo(
            job_name="my-spark-job",
            run_id=100,
            cluster_id="0311-170011-t5450avl",
            env="prod",
            base_path="dbfs:/Volumes/catalog/schema/logs/my-spark-job/0311-170011-t5450avl",
        )

        driver_files = [DirEntry("stderr", False), DirEntry("stdout", False)]
        app_dir = DirEntry("app-20260311170849-0000", True)
        exec_0 = DirEntry("0", True)
        exec_files = [DirEntry("stderr", False), DirEntry("stdout", False)]

        def list_dir(path: str) -> list:
            if path.endswith("/driver"):
                return driver_files
            if path.endswith("/executor"):
                return [app_dir]
            if path.endswith("-0000"):
                return [exec_0]
            if path.endswith("/0"):
                return exec_files
            return []

        client.list_directory.side_effect = list_dir

        sources = discover_sources(client, run_info, include_log4j=False, include_stacktrace=False)

        assert run_info.has_driver is True
        assert run_info.has_executor is True
        assert len(sources) >= 2

    def test_driver_only_job(self) -> None:
        client = MagicMock()
        run_info = RunInfo(
            job_name="simple-job",
            run_id=100,
            cluster_id="0311-test",
            env="prod",
            base_path="dbfs:/Volumes/test/0311-test",
        )

        client.list_directory.side_effect = [
            [DirEntry("stderr", False), DirEntry("stdout", False)],
            Exception("Not found"),
        ]

        sources = discover_sources(client, run_info, include_log4j=False, include_stacktrace=False)

        assert run_info.has_driver is True
        assert run_info.has_executor is False
        assert len(sources) == 1
