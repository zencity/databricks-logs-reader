from dbr_logs.models import LogEntry, SourceType, Stream
from dbr_logs.noise import build_quiet_filter


def _entry(line: str) -> LogEntry:
    return LogEntry(
        source_type=SourceType.DRIVER,
        source_detail="",
        stream=Stream.STDERR,
        timestamp=None,
        level=None,
        line=line,
        file_origin="test",
    )


class TestQuietFilter:
    def test_keeps_application_logs(self) -> None:
        entries = [
            _entry("26/03/14 04:21:36 ERROR MyApp: Something failed"),
            _entry("26/03/14 04:21:36 WARN MyApp: Retrying connection"),
            _entry("Processing batch 42 with 1000 records"),
        ]
        quiet = build_quiet_filter()
        assert len(quiet(entries)) == 3

    def test_removes_jvm_thread_dump(self) -> None:
        entries = [
            _entry('"thread-dump-shutdown-hook-0" id=1289 state=RUNNABLE'),
            _entry("    at java.base@17.0.16/java.lang.Thread.run(Thread.java:840)"),
            _entry("    at app//com.databricks.util.StackTraceReporter.run(Foo.scala:1)"),
            _entry("    - locked <0x759597cf> (a java.util.concurrent.ThreadPoolExecutor$Worker)"),
            _entry("    - waiting on <0x427b77c5> (a java.util.concurrent.locks.Condition)"),
            _entry("    Locked synchronizers: count = 1"),
        ]
        quiet = build_quiet_filter()
        assert len(quiet(entries)) == 0

    def test_removes_spark_task_lifecycle(self) -> None:
        entries = [
            _entry("26/03/14 04:24:11 INFO ShuffleBlockFetcherIterator: Getting 0 blocks"),
            _entry("26/03/14 04:24:11 INFO Executor: Finished task 0.0 in stage 1"),
            _entry("26/03/14 04:24:11 INFO CoarseGrainedExecutorBackend: Got assigned task 123"),
            _entry("26/03/14 04:24:11 INFO BlockManager: Removing broadcast 5"),
            _entry("26/03/14 04:24:11 INFO MemoryStore: Block broadcast_5 stored"),
            _entry("26/03/14 04:24:11 INFO CodeGenerator: Code generated in 15 ms"),
        ]
        quiet = build_quiet_filter()
        assert len(quiet(entries)) == 0

    def test_removes_jvm_startup_boilerplate(self) -> None:
        entries = [
            _entry("OpenJDK 64-Bit Server VM warning: Option UseBiasedLocking was deprecated"),
            _entry("SLF4J: Failed to load class"),
            _entry("ANTLR Tool version 4.8 used for code generation"),
            _entry("chown: invalid group: ':spark-users'"),
            _entry("SyntaxWarning: invalid escape sequence"),
        ]
        quiet = build_quiet_filter()
        assert len(quiet(entries)) == 0

    def test_removes_pyspark_gateway(self) -> None:
        entries = [
            _entry("Sat Mar 14 04:21:26 2026 Connection to spark from PID  1730"),
            _entry("Sat Mar 14 04:21:26 2026 Initialized gateway on port 45023"),
            _entry("Connected to spark."),
        ]
        quiet = build_quiet_filter()
        assert len(quiet(entries)) == 0

    def test_removes_s3_and_parquet_noise(self) -> None:
        entries = [
            _entry("WARN S3AbortableInputStream: Not all bytes were read from the stream"),
            _entry("INFO FilterCompat: Filtering using predicate: noteq(col, null)"),
        ]
        quiet = build_quiet_filter()
        assert len(quiet(entries)) == 0

    def test_mixed_keeps_only_application_logs(self) -> None:
        entries = [
            _entry("OpenJDK 64-Bit Server VM warning: deprecated"),
            _entry("26/03/14 04:21:36 ERROR MyApp: Query failed"),
            _entry('"main" id=1 state=RUNNABLE'),
            _entry("    at java.base@17.0.16/java.lang.Thread.run(Thread.java:840)"),
            _entry("26/03/14 04:24:11 INFO Executor: Finished task 0.0"),
            _entry("26/03/14 04:21:37 WARN MyApp: Slow query detected"),
        ]
        quiet = build_quiet_filter()
        result = quiet(entries)
        assert len(result) == 2
        assert "Query failed" in result[0].line
        assert "Slow query" in result[1].line
