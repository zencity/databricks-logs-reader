# dbr-logs

Fetch and display Databricks job logs from Unity Catalog Volumes.

Merges driver and executor logs chronologically with source labels, so you can pipe them to `grep`, `jq`, or feed them to an LLM.

## Prerequisites

- **Python 3.11+** (tested on 3.11, 3.12, 3.13, 3.14)
- **Databricks CLI** configured with at least one profile in `~/.databrickscfg` ([setup guide](https://docs.databricks.com/dev-tools/cli/index.html))
- **Unity Catalog Volumes** log destination configured on your Databricks jobs (`cluster_log_conf` pointing to a Volumes path)

## Installation

```bash
# Install with uv
uv tool install .

# Or with pip
pip install .

# Or run directly without installing
uvx --from . dbr-logs <job-name>
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

## Limitations

- Only Unity Catalog Volumes log destinations are supported. S3 destinations are not yet supported.
- Jobs must have `cluster_log_conf` configured.
