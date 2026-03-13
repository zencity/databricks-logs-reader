import gzip
from unittest.mock import MagicMock

from dbr_logs.fetcher import _apply_source_filter, _parse_source_filter, fetch_sources
from dbr_logs.models import LogFile, LogSource


def _make_source(
    source_type: str = "driver",
    detail: str = "",
    files: list[LogFile] | None = None,
) -> LogSource:
    return LogSource(
        source_type=source_type,
        source_detail=detail,
        files=files or [],
    )


def _make_file(
    path: str = "test/stderr",
    stream: str = "stderr",
    is_compressed: bool = False,
) -> LogFile:
    return LogFile(
        path=path,
        stream=stream,
        is_compressed=is_compressed,
        is_active=True,
        file_timestamp=None,
    )


class TestParseSourceFilter:
    def test_all(self) -> None:
        assert _parse_source_filter("all") == ("all", None)

    def test_driver(self) -> None:
        assert _parse_source_filter("driver") == ("driver", None)

    def test_executor_all(self) -> None:
        assert _parse_source_filter("executor") == ("executor", None)

    def test_executor_specific(self) -> None:
        assert _parse_source_filter("executor:3") == ("executor", "3")


class TestApplySourceFilter:
    def test_all_returns_everything(self) -> None:
        driver = _make_source("driver", "", [_make_file("d/stderr", "stderr")])
        executor = _make_source("executor", "0", [_make_file("e/stderr", "stderr")])
        result = _apply_source_filter([driver, executor], "all", "all")
        assert len(result) == 2

    def test_driver_only(self) -> None:
        driver = _make_source("driver", "", [_make_file("d/stderr", "stderr")])
        executor = _make_source("executor", "0", [_make_file("e/stderr", "stderr")])
        result = _apply_source_filter([driver, executor], "driver", "all")
        assert len(result) == 1
        assert result[0][0].source_type == "driver"

    def test_executor_specific(self) -> None:
        e0 = _make_source("executor", "0", [_make_file("e0/stderr")])
        e3 = _make_source("executor", "3", [_make_file("e3/stderr")])
        result = _apply_source_filter([e0, e3], "executor:3", "all")
        assert len(result) == 1
        assert result[0][0].source_detail == "3"

    def test_stream_filter(self) -> None:
        source = _make_source(
            "driver",
            "",
            [
                _make_file("d/stderr", "stderr"),
                _make_file("d/stdout", "stdout"),
            ],
        )
        result = _apply_source_filter([source], "all", "stderr")
        assert len(result) == 1
        assert result[0][1].stream == "stderr"


class TestFetchSources:
    def test_fetches_plain_text(self) -> None:
        client = MagicMock()
        content = b"ERROR Something failed"
        client.download_file.return_value = content

        lf = _make_file("dbfs:/Volumes/test/stderr", "stderr", is_compressed=False)
        source = _make_source("driver", "", [lf])

        result = fetch_sources(client, [source], "all", "all")
        assert "dbfs:/Volumes/test/stderr" in result
        assert result["dbfs:/Volumes/test/stderr"] == "ERROR Something failed"

    def test_fetches_gzipped(self) -> None:
        client = MagicMock()
        original = b"WARN GC pressure"
        compressed = gzip.compress(original)
        client.download_file.return_value = compressed

        lf = _make_file("dbfs:/Volumes/test/stderr.gz", "stderr", is_compressed=True)
        source = _make_source("executor", "0", [lf])

        result = fetch_sources(client, [source], "all", "all")
        assert result["dbfs:/Volumes/test/stderr.gz"] == "WARN GC pressure"
