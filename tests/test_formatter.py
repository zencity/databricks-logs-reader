import json
import logging
from datetime import UTC, datetime
from io import StringIO

from dbr_logs.formatter import format_jsonl, format_text, write_entries
from dbr_logs.models import LogEntry, SourceType, Stream


def _entry(
    source_type: SourceType = SourceType.DRIVER,
    source_detail: str = "",
    stream: Stream = Stream.STDERR,
    level: int | None = logging.ERROR,
    line: str = "Something failed",
) -> LogEntry:
    return LogEntry(
        source_type=source_type,
        source_detail=source_detail,
        stream=stream,
        timestamp=datetime(2026, 3, 11, 21, 1, 15, tzinfo=UTC),
        level=level,
        line=line,
        file_origin="test/stderr",
    )


class TestFormatText:
    def test_driver_stderr(self) -> None:
        result = format_text(_entry())
        assert result.startswith("[driver:stderr")
        assert "2026-03-11T21:01:15" in result
        assert "Something failed" in result

    def test_executor_format(self) -> None:
        result = format_text(_entry(source_type=SourceType.EXECUTOR, source_detail="3"))
        assert "exec/3:stderr" in result

    def test_no_timestamp(self) -> None:
        entry = _entry()
        entry.timestamp = None
        result = format_text(entry)
        assert "[driver:stderr" in result


class TestFormatJsonl:
    def test_valid_json(self) -> None:
        result = format_jsonl(_entry())
        parsed = json.loads(result)
        assert parsed["source_type"] == "driver"
        assert parsed["level"] == "ERROR"
        assert parsed["line"] == "Something failed"
        assert parsed["timestamp"] is not None

    def test_null_timestamp(self) -> None:
        entry = _entry()
        entry.timestamp = None
        parsed = json.loads(format_jsonl(entry))
        assert parsed["timestamp"] is None


class TestWriteEntries:
    def test_writes_text_format(self) -> None:
        entries = [_entry(), _entry(line="Second line")]
        output = StringIO()
        write_entries(entries, "text", output)
        lines = output.getvalue().strip().splitlines()
        assert len(lines) == 2

    def test_writes_jsonl_format(self) -> None:
        entries = [_entry()]
        output = StringIO()
        write_entries(entries, "jsonl", output)
        parsed = json.loads(output.getvalue().strip())
        assert parsed["source_type"] == "driver"
