# Databricks Log Directory Structure

This reference is used when falling back to raw `databricks fs` commands (when `dbr-logs` CLI is not available).

## Directory layout

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
├── eventlog/                            # Spark event log (not targeted)
└── init_scripts/                        # optional
```

## Key differences between driver and executor logs

| Property | Driver | Executor |
|---|---|---|
| Rotated stderr/stdout | `stderr--YYYY-MM-DD--HH-MM` (plain text) | `stderr--YYYY-MM-DD--HH.gz` (gzipped) |
| Additional log files | `log4j-*.log.gz`, `stacktrace.log*` (may be absent) | None |
| Executor hierarchy | N/A | `app-{id}/{executor_num}/` |
| Presence | Always present | Only on multi-node jobs |

## Not all files are present in every run

Some driver directories have only `stderr`/`stdout` (no log4j, no stacktrace). Some jobs have no `executor/` directory at all. Always discover what exists rather than assuming a fixed structure.

## Environments

- `prod` — production jobs
- `staging` — staging jobs
- `ondemand` — ad-hoc runs

## Fallback workflow using raw Databricks CLI

When `dbr-logs` is not available, use these commands to manually navigate the log tree:

```bash
# 1. Find the latest run directory
databricks fs ls "dbfs:/Volumes/catalog/schema/logs/prod/<job-name>/" | tail -1

# 2. List available log sources
databricks fs ls "dbfs:/Volumes/catalog/schema/logs/prod/<job-name>/<run-id>/"

# 3. Read driver stderr (most useful starting point)
databricks fs cat "dbfs:/Volumes/catalog/schema/logs/prod/<job-name>/<run-id>/driver/stderr"

# 4. List executor directories (if they exist)
databricks fs ls "dbfs:/Volumes/catalog/schema/logs/prod/<job-name>/<run-id>/executor/"
# Then drill into: app-{id}/{executor_num}/stderr

# 5. For gzipped files, download and decompress locally
databricks fs cp "dbfs:/path/to/file.gz" /tmp/logfile.gz && gunzip -c /tmp/logfile.gz
```

Note: Without `dbr-logs`, you lose chronological merging across sources, level filtering, and regex grep. You must manually navigate each source and file.
