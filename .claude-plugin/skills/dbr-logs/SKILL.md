---
name: dbr-logs
description: Fetch, search, and analyze Databricks job logs. Use when user mentions "job logs", "databricks logs", "executor logs", "driver logs", "spark job failed", "check logs for", or asks to debug a Databricks job failure. Do NOT use for general Spark code questions or Databricks cluster configuration.
allowed-tools: Bash Read Grep
metadata:
  author: dbr-logs contributors
  version: 1.0.0
---

# dbr-logs: Fetch and Analyze Databricks Job Logs

You are a Databricks job log analyst. Follow these steps to fetch, analyze, and explain job logs.

## Step 0: Ensure CLI is available

Check if the `dbr-logs` CLI is accessible. Try each tier in order:

```bash
which dbr-logs
```

1. **Found** -> use `dbr-logs` directly
2. **Not found** -> check for `uvx`:
   ```bash
   which uvx
   ```
   - If `uvx` available -> use `uvx --from dbr-logs dbr-logs <args>` for all commands below
   - If `uvx` not available -> ask the user:
     > `dbr-logs` CLI not found. Install options:
     > - `uv tool install dbr-logs`
     > - `pip install dbr-logs`
     >
     > Want me to install it?
   - If user declines -> fall back to raw `databricks fs ls` / `databricks fs cat` commands. Warn: "Using raw Databricks CLI (no log merging or filtering). Install dbr-logs for a better experience." Load `references/log-structure.md` for directory layout guidance.

For the rest of these instructions, `DBR_LOGS` refers to whichever invocation method was resolved above (`dbr-logs`, `uvx --from dbr-logs dbr-logs`, etc.).

## Step 1: Resolve the target job

- If the user provides a **job name** -> use it directly
- If the user provides a **Databricks URL** -> pass the full URL as the positional argument (the CLI parses job/run from it)
- If the user **describes a failure without naming a job** -> ask which job to investigate
- If the user specifies a **source** (e.g. "check executor logs", "look at the driver") -> use `--source` accordingly
- Default environment is `prod`. Only add `--env <env>` if the user specifies a different environment.

## Step 2: Fetch logs via CLI

Run `DBR_LOGS` with appropriate flags. **Always use `--format jsonl`** when you (Claude) are consuming the output — structured data is easier to analyze. Use `--format text` only when the user wants raw output displayed directly.

**Priority: match the user's intent.** This tool is not just for error analysis. If the user asks to search for a specific string, pattern, or log message, use `--grep` and do NOT add `--level` filtering — the match may appear at any log level (INFO, DEBUG, etc.). Only default to `--level ERROR,WARN` when the user asks about failures/errors without specifying what to search for. Similarly, if the user specifies a source (e.g. "executor logs"), honor that with `--source` rather than fetching all sources.

### Common patterns

```bash
# User asks about errors/failures (no specific search term)
DBR_LOGS <job-name> --level ERROR,WARN --format jsonl

# Specific run
DBR_LOGS <job-name> --run-id <run-id> --level ERROR,WARN --format jsonl

# User says "check executor logs" (honor the source, fetch all levels)
DBR_LOGS <job-name> --source executor --format jsonl

# Executor errors specifically
DBR_LOGS <job-name> --source executor --level ERROR,WARN --format jsonl

# Single executor deep dive
DBR_LOGS <job-name> --source executor:3 --format jsonl

# User asks to search for a specific string (no --level, it may not be an error)
DBR_LOGS <job-name> --grep "partition count" --format jsonl

# Search for a specific error pattern
DBR_LOGS <job-name> --grep "OutOfMemoryError" --format jsonl

# Driver only
DBR_LOGS <job-name> --source driver --format jsonl

# Include log4j or stacktrace files
DBR_LOGS <job-name> --include-log4j --include-stacktrace --format jsonl

# Logs from the last hour
DBR_LOGS <job-name> --since 1h --format jsonl

# Staging environment
DBR_LOGS <job-name> --env staging --format jsonl
```

### CLI reference

| Option | Short | Description |
|---|---|---|
| `<job>` | positional | Job name or Databricks workspace URL |
| `--run-id` | `-r` | Run ID. Omit for latest run. |
| `--env` | `-e` | `prod` (default), `staging`, `ondemand` |
| `--dbr-profile` | `-p` | Databricks CLI profile name |
| `--source` | `-s` | `driver`, `executor`, `executor:N`, `all` (default) |
| `--stream` | | `stderr`, `stdout`, `all` (default) |
| `--level` | `-l` | Comma-separated: `ERROR`, `WARN`, `INFO`, `DEBUG` |
| `--grep` | `-g` | Filter lines matching regex pattern |
| `--include-log4j` | | Include driver log4j files |
| `--include-stacktrace` | | Include driver stacktrace files |
| `--format` | `-f` | `text` or `jsonl` |
| `--tail` | `-n` | Show only last N lines |
| `--since` | | Logs since time (e.g. `1h`, `30m`, ISO datetime) |

## Step 3: Analyze the output

Parse the JSONL output and look for these root cause patterns:

| Pattern | Likely cause | Key fields to check |
|---|---|---|
| `OutOfMemoryError` / `java.lang.OutOfMemoryError` | Executor or driver memory too small | Which source (driver vs executor), heap vs off-heap |
| `Connection refused` / `ShuffleBlockFetcher` / `TransportChannelHandler` | Network or shuffle issues, node went unhealthy | Target IP, timeout duration, which executor |
| `RESOURCE_DOES_NOT_EXIST` | Missing table, view, or path | Resource name in error message |
| `AnalysisException` | SQL/schema issues (column not found, type mismatch) | SQL statement or column name |
| `HangingTaskDetector` | Data skew or stuck tasks | Task IDs, duration, which executor |
| `FileNotFoundException` / `FileAlreadyExistsException` | Concurrent writes or stale metadata | File path |
| `SparkException: Job aborted` | Upstream task failure cascade | Root cause in "caused by" chain |
| `Py4JJavaError` | Python-side error propagated to JVM | Python traceback in the message |

When analyzing:
1. **Group errors by source** (driver vs specific executors)
2. **Identify the root cause** — often the first error chronologically is the root cause; later errors are cascading failures
3. **Note the timeline** — when errors started, how long the job ran before failing
4. **Check for patterns across executors** — same error on all executors suggests a systemic issue; one executor suggests data skew or node problem

## Step 4: Present findings and suggest next steps

Structure your response as:

1. **Summary**: What happened, which run, when
2. **Errors found**: Grouped by source, with key log lines quoted
3. **Root cause assessment**: Your best determination of why the job failed
4. **Suggested actions** based on error type:

| Error type | Suggested actions |
|---|---|
| OOM | Increase executor/driver memory, check for data skew, reduce partition size |
| Shuffle/network | Enable shuffle retry settings, check cluster health, increase shuffle partitions |
| Missing resource | Verify table/path exists, check permissions, check if upstream job ran |
| Schema/SQL | Fix column references, check for schema evolution, verify data types |
| Hanging tasks | Increase shuffle partitions, check for data skew, salting join keys |
| Concurrent write | Check for overlapping job schedules, enable Delta conflict resolution |

If the error is unclear, suggest:
- Checking a specific executor's full logs (`--source executor:N`)
- Looking at driver log4j for more context (`--include-log4j`)
- Comparing with a previous successful run
- Widening the log level to include WARN or INFO
