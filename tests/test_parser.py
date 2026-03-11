import logging
from datetime import UTC, datetime

from dbr_logs.models import LogFile, LogSource, SourceType, Stream
from dbr_logs.parser import parse_entries, parse_timestamp


def _make_source(source_type: SourceType = SourceType.DRIVER, detail: str = "") -> LogSource:
    return LogSource(source_type=source_type, source_detail=detail)


def _make_file(stream: Stream = Stream.STDERR, path: str = "test/stderr") -> LogFile:
    return LogFile(
        path=path, stream=stream, is_compressed=False, is_active=True, file_timestamp=None
    )


class TestParseTimestamp:
    def test_spark_log_format(self) -> None:
        line = "26/03/11 21:01:15 ERROR TransportChannelHandler: Connection timeout"
        result = parse_timestamp(line)
        assert result == datetime(2026, 3, 11, 21, 1, 15, tzinfo=UTC)

    def test_iso_bracket_format(self) -> None:
        line = "[2026-03-11T21:01:07.016+0000] [GC(1051) Pause Young]"
        result = parse_timestamp(line)
        assert result is not None
        assert result.year == 2026
        assert result.second == 7

    def test_log4j_format(self) -> None:
        line = "2026-03-11 21:00:01,234 INFO SparkContext: Running Spark version"
        result = parse_timestamp(line)
        assert result is not None
        assert result.minute == 0
        assert result.second == 1

    def test_no_timestamp(self) -> None:
        line = "some random log line without timestamps"
        assert parse_timestamp(line) is None


class TestParseEntries:
    def test_parses_lines_with_metadata(self) -> None:
        content = "26/03/11 21:01:15 ERROR Something failed\n26/03/11 21:01:16 WARN Retrying\n"
        source = _make_source(SourceType.EXECUTOR, "3")
        log_file = _make_file(Stream.STDERR, "executor/app/3/stderr")

        entries = parse_entries(content, source, log_file)

        assert len(entries) == 2
        assert entries[0].source_type == SourceType.EXECUTOR
        assert entries[0].source_detail == "3"
        assert entries[0].stream == Stream.STDERR
        assert entries[0].level == logging.ERROR
        assert entries[1].level == logging.WARNING

    def test_skips_blank_lines(self) -> None:
        content = "line1\n\n  \nline2\n"
        entries = parse_entries(content, _make_source(), _make_file())
        assert len(entries) == 2

    def test_handles_no_level(self) -> None:
        content = "just a plain message\n"
        entries = parse_entries(content, _make_source(), _make_file())
        assert entries[0].level is None
