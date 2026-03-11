# dbr-logs: CLI Tool Spec

## Problem Statement

Debugging Databricks job failures requires navigating a deeply nested, inconsistently structured log directory tree via manual `databricks fs ls` / `databricks fs cat` commands. The developer must:

1. Know the job name and find the correct run directory (an opaque ID like `0311-170011-t5450avl`)
2. Discover whether the run has `driver/`, `executor/`, or both
3. For executors, navigate through `app-{timestamp}-{id}/{executor_num}/`
4. Determine which files are plain text vs gzipped (driver rotated files are plain text, executor rotated files are `.gz`)
5. Manually concatenate rotated log files in chronological order
6. Repeat this across multiple executors to find the relevant error

This tool eliminates all of that friction.

## Observed Log Directory Structure

```
dbfs:/Volumes/catalog/schema/logs/{env}/{job_name}/{run_id}/
├── driver/
│   ├── stderr                           # active file (plain text)
│   ├── stderr--2026-03-11--18-00        # rotated (plain text, NOT gzipped)
│   ├── stderr--2026-03-11--19-00
│   ├── stdout                           # active file (plain text)
│   ├── stdout--2026-03-11--18-00        # rotated (plain text)
│   ├── log4j-active.log                 # active log4j (plain text)
│   ├── log4j-2026-03-11-17.log.gz       # rotated log4j (gzipped)
│   ├── stacktrace.log                   # active stacktrace (plain text)
│   └── 2026-03-11-17.stacktrace.log.gz  # rotated stacktrace (gzipped)
├── executor/
│   └── app-20260311170849-0000/
│       ├── 0/
│       │   ├── stderr                   # active file (plain text)
│       │   ├── stderr--2026-03-11--18.gz # rotated (gzipped)
│       │   ├── stdout                   # active file (plain text)
│       │   └── stdout--2026-03-11--18.gz # rotated (gzipped)
│       ├── 1/
│       ├── 2/
│       ...
│       └── N/
├── eventlog/                            # Spark event log (not targeted by this tool)
└── init_scripts/                        # optional, some jobs only
```

**Not all files are present in every run.** Some driver directories have only `stderr`/`stdout` (no log4j, no stacktrace). Some jobs have no `executor/` directory at all. The tool must discover what exists rather than assuming a fixed structure.

### Key structural observations

| Property | Driver | Executor |
|---|---|---|
| Rotated stderr/stdout format | `stderr--YYYY-MM-DD--HH-MM` (plain text) | `stderr--YYYY-MM-DD--HH.gz` (gzipped) |
| Additional log files | `log4j-*.log.gz`, `stacktrace.log*` (may be absent) | None |
| stderr content | Spark WARN/ERROR (connection issues, task failures) | Spark WARN/ERROR (task-level, shuffle, GC) |
| stdout content | Application print output (e.g., Python `print()`) | JVM GC logs, application output |
| Executor hierarchy | N/A | `app-{id}/{executor_num}/` |
| Presence | Always present | Only on jobs with multiple nodes |

### Environments

- `prod` — production jobs
- `staging` — staging jobs
- `ondemand` — ad-hoc runs
- `cluster-logs` — cluster-level logs (out of scope)

### Profiles

Databricks CLI uses `~/.databrickscfg` profiles (e.g. `DEFAULT`, `STAGING`).

---

## CLI Interface

Outputs structured, concatenated log text to stdout for piping to `grep`, `less`, `jq`, or LLM tools.

```bash
# Fetch latest run logs for a job (all sources merged chronologically)
dbr-logs my-spark-job

# Fetch specific run
dbr-logs my-spark-job --run-id 0311-170011-t5450avl

# Filter by source
dbr-logs my-spark-job --source driver          # driver only
dbr-logs my-spark-job --source executor         # all executors
dbr-logs my-spark-job --source executor:3       # executor 3 only

# Filter by stream
dbr-logs my-spark-job --stream stderr           # stderr only
dbr-logs my-spark-job --stream stdout           # stdout only

# Filter by log level (parsed from content)
dbr-logs my-spark-job --level ERROR
dbr-logs my-spark-job --level WARN,ERROR

# Include log4j / stacktrace files from driver
dbr-logs my-spark-job --include-log4j
dbr-logs my-spark-job --include-stacktrace

# Grep-like filtering
dbr-logs my-spark-job --grep "OutOfMemoryError"

# Pipeline usage
dbr-logs my-spark-job --stream stderr | grep "ERROR" | head -50
dbr-logs my-spark-job --source executor --level ERROR | llm "summarize these errors"

# By Databricks workspace URL
dbr-logs "https://dbc-xxx.cloud.databricks.com/jobs/12345/runs/67890"

# Environment
dbr-logs my-spark-job --env staging
dbr-logs my-spark-job --env prod              # default

# JSONL output for programmatic consumption
dbr-logs my-spark-job --format jsonl          # one JSON object per line
```

---

## Output Formats

### Plain text (default)

Each log line prefixed with metadata:

```
[driver:stderr  2026-03-11T21:01:15] ERROR TransportChannelHandler: Connection to /10.177.10.232:4048 has been quiet for 600000 ms...
[driver:stderr  2026-03-11T21:01:15] ERROR TransportResponseHandler: Still have 1 requests outstanding...
[exec/0:stderr  2026-03-11T21:01:07] WARN HangingTaskDetector: Task 140961 is probably not making progress...
[exec/3:stdout  2026-03-11T21:01:07] [GC(1051) Pause Young (Allocation Failure) 16547M->13628M(17232M) 42.178ms]
```

Prefix columns:
- **source**: `driver` | `exec/{N}`
- **stream**: `stderr` | `stdout` | `log4j` | `stacktrace`
- **timestamp**: parsed from log line content (best-effort), falls back to file timestamp

### JSONL

One JSON object per line, for piping to `jq` or LLM tools:

```json
{"source_type":"executor","source_detail":"3","stream":"stderr","timestamp":"2026-03-11T21:01:07.000Z","level":"WARN","file":"stderr--2026-03-11--21.gz","line":"WARN HangingTaskDetector: Task 140961 is probably not making progress..."}
```

---

## CLI Parameters

| Param | Short | Type | Default | Description |
|---|---|---|---|---|
| `<job>` | positional | string | required | Job name or Databricks workspace URL |
| `--run-id` | `-r` | string | latest | Run ID. If omitted, fetches the latest run for the job |
| `--env` | `-e` | string | `prod` | Environment: `prod`, `staging`, `ondemand` |
| `--dbr-profile` | `-p` | string | saved profile | Databricks CLI profile name |
| `--source` | `-s` | string | `all` | `driver`, `executor`, `executor:N`, `all` |
| `--stream` | | string | `all` | `stderr`, `stdout`, `all` |
| `--level` | `-l` | string | all | Comma-separated: `ERROR`, `WARN`, `INFO`, `DEBUG` |
| `--grep` | `-g` | string | none | Filter lines matching pattern (regex) |
| `--include-log4j` | | flag | false | Include driver log4j files in output |
| `--include-stacktrace` | | flag | false | Include driver stacktrace files in output |
| `--format` | `-f` | string | `text` | Output format: `text`, `jsonl` |
| `--tail` | `-n` | int | all | Show only last N lines |
| `--since` | | string | none | Show logs since time (e.g. `1h`, `30m`, `2026-03-11T20:00`) |
| `--help` | `-h` | flag | | Show help |

---

## Configuration

### Persistent Profile Selection

Stored in `~/.config/dbr-logs/config.toml`:

```toml
[profile]
default = "DEFAULT"

[defaults]
env = "prod"
include_log4j = false
include_stacktrace = false
```

### First-run setup

On first run, if multiple profiles exist in `~/.databrickscfg`:
1. List available profiles
2. Prompt user to select a default working profile
3. Save to config file

Override anytime with `--dbr-profile`.

---

## Internal Architecture

### Modules

```
dbr_logs/
├── cli.py              # Argument parsing, entry point
├── config.py           # Profile/config management (~/.config/dbr-logs/)
├── resolver.py         # Job name → run ID resolution, URL parsing
├── discovery.py        # Walk log directory tree, discover files + structure
├── fetcher.py          # Download + decompress log files (plain text + gz)
├── parser.py           # Parse log lines: extract timestamp, level, message
├── merger.py           # Merge logs from multiple sources chronologically
├── formatter.py        # Output formatting (text, jsonl)
├── filters.py          # Level filter, grep, time range, source filter
└── models.py           # Data models (LogEntry, RunInfo, LogSource, etc.)
```

### Data Models

```python
@dataclass
class LogEntry:
    source_type: str           # "driver" | "executor"
    source_detail: str         # "" for driver, "0"-"N" for executor
    stream: str                # "stderr" | "stdout" | "log4j" | "stacktrace"
    timestamp: datetime | None # parsed from content, None if unparseable
    level: str | None          # "ERROR" | "WARN" | "INFO" | "DEBUG" | None
    line: str                  # raw log line text
    file_origin: str           # original file path for reference

@dataclass
class LogSource:
    source_type: str           # "driver" | "executor"
    source_detail: str         # executor number or ""
    files: list[LogFile]       # all log files for this source

@dataclass
class LogFile:
    path: str                  # full dbfs path
    stream: str                # "stderr" | "stdout" | "log4j" | "stacktrace"
    is_compressed: bool        # .gz file
    is_active: bool            # current (non-rotated) file
    file_timestamp: datetime   # from filename or file metadata

@dataclass
class RunInfo:
    job_name: str
    run_id: str
    env: str
    has_driver: bool
    has_executor: bool
    executor_count: int
    app_id: str | None         # e.g. "app-20260311170849-0000"
```

### Processing Pipeline

```
1. Resolve input
   ├── Job name + run_id → build base path
   ├── Job name only → list runs, pick latest → build base path
   └── URL → parse job + run_id → build base path

2. Discover structure
   ├── ls {base}/driver/     → list driver files
   │   └── log4j/stacktrace files are optional — only include if present AND requested
   ├── ls {base}/executor/   → may not exist (driver-only jobs). If absent, skip silently.
   │   └── ls app dirs → list executor nums → list files
   └── Build LogSource[] with all LogFile metadata

3. Fetch & decompress
   ├── Apply source/stream filters (skip files we don't need)
   ├── Download remaining files (parallel)
   ├── Decompress .gz files in memory
   └── Return raw text per file

4. Parse
   ├── For each file, parse lines into LogEntry[]
   ├── Extract timestamp (regex patterns for known formats)
   ├── Extract log level (ERROR/WARN/INFO/DEBUG)
   └── Attach source metadata

5. Merge
   ├── Sort all LogEntry[] by timestamp (stable sort preserving order within same timestamp)
   └── Handle entries without timestamp (keep in file order, interleave by file timestamp)

6. Filter
   ├── --level filter
   ├── --grep filter
   ├── --since filter
   └── --tail filter

7. Output
   ├── text → prefix + line to stdout
   └── jsonl → one JSON object per line to stdout
```

---

## Timestamp Parsing

Multiple log formats observed in the wild:

| Format | Example | Source |
|---|---|---|
| Spark log | `26/03/11 21:01:15 ERROR ...` | driver/executor stderr |
| ISO-ish with brackets | `[2026-03-11T21:01:07.016+0000]` | executor stdout (JVM GC) |
| Log4j | `2026-03-11 21:00:01,234 INFO ...` | driver log4j |
| File rotation timestamp | `stderr--2026-03-11--18-00` (driver) or `stderr--2026-03-11--18.gz` (executor) | filename |

Parser should try each pattern in order, fall back to file-level timestamp.

---

## URL Parsing

Support Databricks workspace URLs as input:

```
https://dbc-xxx.cloud.databricks.com/#job/<job_id>
https://dbc-xxx.cloud.databricks.com/#job/<job_id>/run/<run_id>
https://adb-xxx.azuredatabricks.net/?o=xxx#job/<job_id>/run/<run_id>
```

When a URL is provided:
1. Extract `job_id` and optionally `run_id`
2. Use the Databricks Jobs API (`databricks jobs get --job-id X`) to resolve the job name
3. If `run_id` in URL, map it to the filesystem run directory (may need to list and match)

---

## Error Handling

| Scenario | Behavior |
|---|---|
| Job name not found in log directory | Error: "Job '{name}' not found in {env}. Available jobs: ..." (list top 10 + fuzzy suggestion) |
| No runs available | Error: "No runs found for job '{name}' in {env}" |
| Run ID not found | Error: "Run '{run_id}' not found. Recent runs: ..." (list last 5) |
| No executor logs (driver-only job) | If `--source executor` specified: Warning + empty output. Otherwise: silently skip |
| Databricks CLI not installed | Error: "databricks CLI not found. Install: pip install databricks-cli" |
| Profile not configured | Error: "Profile '{name}' not found in ~/.databrickscfg. Available: ..." |
| Network/auth error | Pass through the Databricks CLI error message |
| Compressed file corruption | Warning per file, continue with remaining files |

---

## Technology Choices

| Component | Choice | Rationale |
|---|---|---|
| Language | Python 3.11+ | Matches team's data platform stack, databricks-sdk available |
| CLI framework | `click` | Widely used, good UX for nested commands |
| Databricks access | `databricks-sdk` (Python SDK) | Programmatic access, avoids subprocess calls to CLI |
| Compression | stdlib `gzip` | Built-in, sufficient for `.gz` decompression |
| Config | `tomllib` / `tomli` | Standard for Python config files |
| Packaging | `pyproject.toml` + `pip install -e .` or `pipx` | Easy install for developers |

### Why `databricks-sdk` over subprocess calls to `databricks` CLI

- Avoids process spawn overhead (important when fetching many files in parallel)
- Proper error types instead of parsing stderr
- Parallel downloads via async/threading
- Handles authentication uniformly
- Still requires `~/.databrickscfg` for auth config (shared with CLI)

---

## Performance Considerations

- **Parallel file downloads**: Executor-heavy jobs (e.g., my-spark-job with 9 executors x 4-5 files each = ~45 files) should download in parallel using `concurrent.futures.ThreadPoolExecutor`
- **Streaming for large files**: For pipe mode, stream output as files are fetched rather than waiting for all files. Merge can happen in a streaming fashion using a priority queue.
- **Lazy decompression**: Decompress `.gz` files in memory, don't write to disk
- **Early termination**: If `--tail N` or `--grep` with `--count`, can stop early
- **Caching**: Optional local cache of fetched logs in `~/.cache/dbr-logs/` to avoid re-downloading for repeated queries on the same run

---

## Installation

```bash
# From the repo
pip install -e .

# Or via pipx for isolation
pipx install .

# Creates the `dbr-logs` command
```

Entry point in `pyproject.toml`:
```toml
[project.scripts]
dbr-logs = "dbr_logs.cli:main"
```

### Prerequisites

- Python 3.11+
- `~/.databrickscfg` configured with at least one profile
- Network access to Databricks workspace

---

## Usage Examples

### Scenario: Investigating a failed my-spark-job run

```bash
# Quick look at errors from the latest run
dbr-logs my-spark-job --level ERROR

# Something in executor 0 — drill in
dbr-logs my-spark-job --source executor:0 --stream stderr

# Get full context around an OOM
dbr-logs my-spark-job --grep "OutOfMemoryError"

# Pipe to an LLM for analysis
dbr-logs my-spark-job --level ERROR,WARN --format jsonl | llm "what caused this job to fail?"
```

### Scenario: Checking a staging deployment

```bash
dbr-logs platform-collections-imports-staging --env staging -r 0310-030008-abcdef
```

### Scenario: Finding which executor had the issue

```bash
# All executor errors, grouped by source
dbr-logs my-spark-job --source executor --level ERROR --format jsonl | jq -r '.source_detail' | sort | uniq -c | sort -rn
```

---

## Open Decision: S3 Log Destinations

Some jobs have `cluster_log_conf` pointing to S3 (e.g. `s3://my-bucket-logs/prod/my-job`) rather than Unity Catalog Volumes. The current implementation only supports Volumes paths via the `w.files.*` API.

**Options to consider:**
1. Map S3 paths to a configurable Volume base path (e.g. strip S3 bucket, prepend `/Volumes/catalog/schema/logs/`)
2. Support S3 paths directly via `boto3` / presigned URLs
3. Require all jobs to use Volumes-based log destinations
4. Auto-discover the S3→Volume mapping from Unity Catalog external locations

---

## Future Enhancements (out of scope for v1)

- **Log tailing**: `--follow` / `-f` mode for in-progress runs
- **Multi-run diff**: Compare logs between two runs
- **Bookmarks**: Save commonly investigated jobs as aliases
- **Spark event log parsing**: Parse `eventlog/` for stage/task timeline visualization
- **Integration with alerting**: Auto-fetch logs when a PagerDuty/Slack alert fires
