from datetime import UTC, datetime

from dbr_logs.merger import merge_entries
from dbr_logs.models import LogEntry


def _entry(ts_minute: int | None, source: str = "driver", line: str = "") -> LogEntry:
    ts = datetime(2026, 3, 11, 21, ts_minute, 0, tzinfo=UTC) if ts_minute is not None else None
    return LogEntry(
        source_type=source,
        source_detail="",
        stream="stderr",
        timestamp=ts,
        level=None,
        line=line or f"line at minute {ts_minute}",
        file_origin="test",
    )


class TestMergeEntries:
    def test_merges_two_files_chronologically(self) -> None:
        file_a = [_entry(1, line="a1"), _entry(3, line="a3")]
        file_b = [_entry(2, line="b2"), _entry(4, line="b4")]

        merged = merge_entries([file_a, file_b])

        assert [e.line for e in merged] == ["a1", "b2", "a3", "b4"]

    def test_single_file_returns_as_is(self) -> None:
        entries = [_entry(1), _entry(2)]
        assert merge_entries([entries]) == entries

    def test_empty_input(self) -> None:
        assert merge_entries([]) == []

    def test_entries_without_timestamp_use_file_fallback(self) -> None:
        file_a = [_entry(1, line="a1"), _entry(None, line="a-none")]
        file_b = [_entry(2, line="b2")]

        merged = merge_entries([file_a, file_b])
        lines = [e.line for e in merged]
        assert lines == ["a1", "a-none", "b2"]

    def test_preserves_intra_file_order(self) -> None:
        file_a = [_entry(5, line="a-first"), _entry(5, line="a-second")]
        merged = merge_entries([file_a])
        assert [e.line for e in merged] == ["a-first", "a-second"]

    def test_merges_files_with_no_timestamps(self) -> None:
        """Files with no timestamps at all should use EPOCH fallback without crashing.

        Regression: EPOCH was timezone-naive while parsed timestamps are UTC-aware,
        causing 'can't compare offset-naive and offset-aware datetimes'.
        """
        file_a = [_entry(1, line="a1")]
        file_b = [_entry(None, line="b-no-ts")]

        merged = merge_entries([file_a, file_b])
        assert len(merged) == 2
        assert merged[0].line == "b-no-ts"
        assert merged[1].line == "a1"
