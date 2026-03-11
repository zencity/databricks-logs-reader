import logging
from datetime import UTC, datetime, timedelta

from dbr_logs.filters import build_filter, parse_since
from dbr_logs.models import LogEntry, SourceType, Stream


def _entry(
    level: int | None = None,
    line: str = "test",
    ts_minute: int | None = None,
) -> LogEntry:
    ts = datetime(2026, 3, 11, 21, ts_minute, 0, tzinfo=UTC) if ts_minute is not None else None
    return LogEntry(
        source_type=SourceType.DRIVER,
        source_detail="",
        stream=Stream.STDERR,
        timestamp=ts,
        level=level,
        line=line,
        file_origin="test",
    )


class TestLevelFilter:
    def test_filters_by_single_level(self) -> None:
        entries = [
            _entry(logging.ERROR, "err"),
            _entry(logging.WARNING, "warn"),
            _entry(logging.INFO, "info"),
        ]
        f = build_filter(levels=["ERROR"], grep_pattern=None, since=None, tail=None)
        result = f(entries)
        assert len(result) == 1
        assert result[0].line == "err"

    def test_filters_by_multiple_levels(self) -> None:
        entries = [_entry(logging.ERROR), _entry(logging.WARNING), _entry(logging.INFO)]
        f = build_filter(levels=["ERROR", "WARN"], grep_pattern=None, since=None, tail=None)
        assert len(f(entries)) == 2


class TestGrepFilter:
    def test_filters_by_regex(self) -> None:
        entries = [_entry(line="OutOfMemoryError"), _entry(line="normal line")]
        f = build_filter(levels=None, grep_pattern="OutOfMemory", since=None, tail=None)
        result = f(entries)
        assert len(result) == 1
        assert "OutOfMemory" in result[0].line

    def test_regex_pattern(self) -> None:
        entries = [_entry(line="error code 404"), _entry(line="error code 500")]
        f = build_filter(levels=None, grep_pattern=r"code 5\d+", since=None, tail=None)
        assert len(f(entries)) == 1


class TestSinceFilter:
    def test_filters_by_time(self) -> None:
        entries = [_entry(ts_minute=0), _entry(ts_minute=30), _entry(ts_minute=59)]
        since = datetime(2026, 3, 11, 21, 30, 0, tzinfo=UTC)
        f = build_filter(levels=None, grep_pattern=None, since=since, tail=None)
        result = f(entries)
        assert len(result) == 2


class TestTailFilter:
    def test_returns_last_n(self) -> None:
        entries = [_entry(line=f"line{i}") for i in range(10)]
        f = build_filter(levels=None, grep_pattern=None, since=None, tail=3)
        result = f(entries)
        assert len(result) == 3
        assert result[0].line == "line7"


class TestParseSince:
    def test_relative_hours(self) -> None:
        result = parse_since("2h")
        expected_approx = datetime.now(tz=UTC) - timedelta(hours=2)
        assert abs((result - expected_approx).total_seconds()) < 2

    def test_relative_minutes(self) -> None:
        result = parse_since("30m")
        expected_approx = datetime.now(tz=UTC) - timedelta(minutes=30)
        assert abs((result - expected_approx).total_seconds()) < 2

    def test_iso_datetime(self) -> None:
        result = parse_since("2026-03-11T20:00:00")
        assert result.hour == 20
