# dbr-logs

Fetch and display Databricks job logs from Unity Catalog Volumes.

Merges driver and executor logs chronologically with source labels, so you can pipe them to `grep`, `jq`, or feed them to an LLM.

## Why

Debugging a failed Databricks job means navigating a deeply nested, inconsistently structured log directory tree:

```
dbfs:/Volumes/catalog/schema/logs/prod/my-spark-job/0311-170011-t5450avl/
├── driver/
│   ├── stderr
│   ├── stderr--2026-03-11--18-00        # rotated, plain text
│   ├── stderr--2026-03-11--19-00        # rotated, plain text
│   ├── stdout
│   ├── log4j-active.log
│   └── log4j-2026-03-11-17.log.gz      # rotated, gzipped
├── executor/
│   └── app-20260311170849-0000/         # opaque app ID
│       ├── 0/
│       │   ├── stderr
│       │   ├── stderr--2026-03-11--18.gz   # rotated, gzipped
│       │   └── stdout
│       ├── 1/
│       ├── 2/
│       ...
│       └── 8/
└── eventlog/
```

The manual process to find what went wrong:

1. **Find the run** — run IDs are opaque strings like `0311-170011-t5450avl`, not human-readable
2. **Navigate the tree** — driver logs, executor logs, or both? Which of the 9 executors had the error?
3. **Handle mixed compression** — driver rotated files are plain text, executor rotated files are `.gz`. You need to `databricks fs cp` + `gunzip` to read them
4. **Concatenate rotated files** — a single stream is split across the active file and multiple rotated files that must be read in chronological order
5. **Repeat across executors** — for a job with 9 executors, that's potentially 9 x 4 files to check
6. **Cross-reference timestamps** — the root cause is often in one source, but the symptoms appear in another

For background on how Python logging works in Databricks and why it ends up in this structure, see [Everything You Wanted to Know About Python Logging in Databricks](https://medium.com/python-in-plain-english/everything-you-wanted-to-know-about-python-logging-in-databricks-0c64da6f56c9).

There are heavier alternatives — Databricks' own [Practitioner's Ultimate Guide to Scalable Logging](https://www.databricks.com/blog/practitioners-ultimate-guide-scalable-logging) describes a full logging pipeline, and you could also route Databricks logs to Datadog or similar observability platforms. But these solutions carry significant ongoing costs and infrastructure overhead for something most teams only need occasionally when debugging a failed job. `dbr-logs` is a zero-cost, zero-infrastructure alternative: install a CLI tool, run one command, get your answer.

`dbr-logs` replaces the manual process with a single command. It discovers the log structure, downloads and decompresses all files, merges everything chronologically with source labels, and lets you filter by level, source, or regex.

## Prerequisites

- **Python 3.11+** (tested on 3.11, 3.12, 3.13, 3.14)
- **Databricks CLI** configured with at least one profile in `~/.databrickscfg` ([setup guide](https://docs.databricks.com/dev-tools/cli/index.html))
- **Unity Catalog Volumes** log destination configured on your Databricks jobs (`cluster_log_conf` pointing to a Volumes path)

## Installation

```bash
# Install as a CLI tool with uv (recommended)
uv tool install dbr-logs

# Or with pipx (isolated environment)
pipx install dbr-logs

# Or with pip (use --user to install globally without affecting your venv)
pip install --user dbr-logs

# Or run directly without installing
uvx dbr-logs <job-name>
```

## Usage

```bash
# Fetch logs for the latest run of a job
dbr-logs my-job-name

# Fetch logs from a specific run
dbr-logs my-job-name --run-id 12345

# Use a Databricks workspace URL
dbr-logs "https://dbc-xxx.cloud.databricks.com/jobs/12345/runs/67890?o=123"

# Show only errors
dbr-logs my-job-name --level ERROR

# Filter with regex
dbr-logs my-job-name --grep "OOM|OutOfMemory"

# Show only executor logs
dbr-logs my-job-name --source executor

# Show last 50 lines from a specific executor
dbr-logs my-job-name --source executor:3 --tail 50

# JSONL output for piping to jq
dbr-logs my-job-name --format jsonl | jq '.level'

# Logs since last hour
dbr-logs my-job-name --since 1h
```

## Claude Code Integration

`dbr-logs` includes a Claude Code plugin so you can fetch and analyze logs directly from a Claude conversation.

### Install the plugin

```bash
# Option 1: Claude Code Plugin
/plugin add zencity/databricks-logs-reader

# Option 2: skills.sh (works with Claude Code, Cursor, Windsurf, etc.)
npx skills add zencity/databricks-logs-reader
```

The CLI tool still needs to be installed separately (`pip install dbr-logs` or `uv tool install dbr-logs`), but the skill can also use `uvx` as a zero-install fallback.

### Example interaction

```
You: check the logs for my-spark-job

Claude:
  Runs: dbr-logs my-spark-job --level ERROR,WARN --format jsonl
  Analyzes output, then responds:

  "The latest run (0311-170011-t5450avl) has 3 errors:

  - driver/stderr: TransportChannelHandler connection timeout to 10.177.10.232 after 600s
  - executor/0-7 stderr: HangingTaskDetector reports tasks 140953, 140957 stuck for 10+ min

  Root cause: Shuffle read stall — executors are hanging on remote block fetches.

  Suggested actions:
  - Check executor at 10.177.10.232 for resource pressure
  - Enable spark.shuffle.io.retryWait / spark.shuffle.io.maxRetries
  - Review shuffle partitions to reduce per-task data volume"
```

## Options

| Option | Short | Description |
|---|---|---|
| `--run-id` | `-r` | Databricks run ID (numeric). Defaults to latest run. |
| `--env` | `-e` | Environment: prod, staging, ondemand. Default: prod. |
| `--dbr-profile` | `-p` | Databricks CLI profile name. |
| `--source` | `-s` | `driver`, `executor`, `executor:N`, or `all` (default). |
| `--stream` | | `stderr`, `stdout`, or `all` (default). |
| `--level` | `-l` | Comma-separated: ERROR, WARN, INFO, DEBUG. |
| `--grep` | `-g` | Filter lines matching regex pattern. |
| `--include-log4j` | | Include driver log4j files. |
| `--include-stacktrace` | | Include driver stacktrace files. |
| `--format` | `-f` | `text` (default) or `jsonl`. |
| `--tail` | `-n` | Show only last N lines. |
| `--since` | | Show logs since time (e.g. `1h`, `30m`, ISO datetime). |

## Configuration

On first run with multiple Databricks profiles, you'll be prompted to select a default. Config is saved to `~/.config/dbr-logs/config.toml`.

## Releasing

Version is derived from git tags via [hatch-vcs](https://github.com/ofek/hatch-vcs) — no version string to maintain in source code.

```bash
git tag v0.1.0
git push origin v0.1.0
```

This triggers the CI pipeline which: builds the package -> creates a GitHub Release with auto-generated notes -> publishes to PyPI.

## Limitations

- Only Unity Catalog Volumes log destinations are supported. S3 destinations are not yet supported.
- Jobs must have `cluster_log_conf` configured.
