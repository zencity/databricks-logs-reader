# Claude Code Skill: `/dbr-logs`

A Claude Code skill that wraps `dbr-logs` so developers can fetch and analyze Databricks job logs directly from a Claude conversation, without remembering CLI flags or leaving their terminal.

**Prerequisite**: The `dbr-logs` CLI tool (see [01-cli-tool.md](01-cli-tool.md)) must be installed.

## Skill Category

**Workflow Automation** (Category 2 per Anthropic's skill guide) — a multi-step process that benefits from consistent methodology, using Bash tool execution to call the installed `dbr-logs` CLI.

## Skill Folder Structure

```
dbr-logs/
├── SKILL.md                      # Main skill file (instructions + frontmatter)
└── references/
    └── log-structure.md          # Log directory structure reference (from cli-tool spec)
```

## SKILL.md Frontmatter

```yaml
---
name: dbr-logs
description: Fetch, search, and analyze Databricks job logs. Use when user mentions "job logs", "databricks logs", "executor logs", "driver logs", "spark job failed", "check logs for", or asks to debug a Databricks job failure. Do NOT use for general Spark code questions or Databricks cluster configuration.
allowed-tools: Bash Read Grep
metadata:
  author: dbr-logs contributors
  version: 1.0.0
---
```

## Skill Instructions (SKILL.md body)

The skill body should instruct Claude to:

### Step 1: Resolve the target job

- If user provides a job name → use directly
- If user provides a Databricks URL → parse job/run from it
- If user describes a failure without naming a job → ask which job
- If `--env` not specified → default to `prod`

### Step 2: Fetch logs via CLI

Run `dbr-logs` with appropriate flags, piping to capture output:

```bash
# Default: latest run, ERROR level, all sources
dbr-logs <job-name> --level ERROR --format jsonl

# If user asks about a specific run
dbr-logs <job-name> --run-id <run-id> --format jsonl

# If user wants executor-specific info
dbr-logs <job-name> --source executor --level ERROR,WARN --format jsonl
```

Always use `--format jsonl` when Claude is consuming the output (structured data is easier to analyze). Use `--format text` only when the user wants raw output displayed.

### Step 3: Analyze the output

- Parse JSONL output, group errors by source (driver vs specific executors)
- Identify root cause patterns:
  - `OutOfMemoryError` → memory/executor sizing issue
  - `Connection refused` / `ShuffleBlockFetcher` → network/shuffle issues
  - `RESOURCE_DOES_NOT_EXIST` → missing table/path
  - `AnalysisException` → SQL/schema issues
  - `HangingTaskDetector` → data skew / stuck tasks
- Present a structured summary: what failed, where (which executor), and likely why

### Step 4: Suggest next steps

Based on the error type, suggest:
- Config changes (executor memory, shuffle partitions)
- Code fixes (schema mismatch, null handling)
- Whether to check a specific executor's full logs
- Whether to compare with a previous successful run

## Trigger Phrases

The description field includes these triggers so the skill loads automatically:

**Should trigger:**
- "check the logs for my-spark-job"
- "why did the databricks job fail"
- "show me executor logs for platform-collections"
- "what errors happened in the latest run of anomalies"
- "debug the spark job failure"
- "fetch driver stderr for my-spark-job"

**Should NOT trigger:**
- "how do I write a Spark job" (general coding)
- "configure a Databricks cluster" (infrastructure)
- "what's the Databricks pricing" (unrelated)

## Example Interactions

**Example 1: Quick error check**
```
User: "check the logs for my-spark-job"

Claude:
1. Runs: dbr-logs my-spark-job --level ERROR,WARN --format jsonl
2. Analyzes output
3. Responds:

"The latest run of my-spark-job (0311-170011-t5450avl) has errors:

- **driver/stderr**: TransportChannelHandler connection timeout to executor at 10.177.10.232
  after 600s idle. OneForOneBlockFetcher failed with IOException.
- **executor/0-7 stderr**: HangingTaskDetector reports tasks 140953, 140957, 140961 stuck
  for 10+ minutes with no metric changes.

Root cause: Shuffle read stall — executors are hanging on remote block fetches.
The connection timeout (600s) suggests a node went unhealthy.

Suggested actions:
- Check if executor at 10.177.10.232 had resource pressure
- Consider enabling spark.shuffle.io.retryWait / spark.shuffle.io.maxRetries
- Review if the job needs more shuffle partitions to reduce per-task data volume"
```

**Example 2: Specific executor deep dive**
```
User: "show me what happened on executor 3 of my-spark-job"

Claude:
1. Runs: dbr-logs my-spark-job --source executor:3 --format jsonl
2. Presents full timeline from executor 3
```

**Example 3: Cross-run comparison**
```
User: "did my-spark-job fail yesterday too?"

Claude:
1. Runs: databricks fs ls to find yesterday's run directory
2. Runs: dbr-logs my-spark-job -r <yesterday-run-id> --level ERROR --format jsonl
4. Compares error patterns with latest run
```

## Fallback Behavior

If `dbr-logs` is not installed:
- Detect with `which dbr-logs` check
- Fall back to raw `databricks fs ls` / `databricks fs cat` commands using the configured profile
- Warn user: "dbr-logs CLI not installed. Using raw Databricks CLI (slower, no merging). Install with: pip install -e /path/to/dbr_logs_viewer"

## Progressive Disclosure

Following Anthropic's three-level skill design:

| Level | Content | When loaded |
|---|---|---|
| **1. Frontmatter** | Name, description, trigger phrases | Always in system prompt |
| **2. SKILL.md body** | Workflow steps, analysis patterns, examples | When skill is triggered |
| **3. references/log-structure.md** | Full log directory structure, file naming conventions, timestamp formats | Only when Claude needs to fall back to raw `databricks fs` commands |
