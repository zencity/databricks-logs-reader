# dbr-logs

CLI tool for fetching and displaying Databricks job logs from Unity Catalog Volumes.

## Commands

```bash
uv sync --dev          # Install dependencies
uv run pytest          # Run tests (70 tests)
uv run ruff check src/ tests/   # Lint
uv run ruff format src/ tests/  # Format
uv run mypy src/       # Type check
```

## Architecture

Pipeline: `cli.py` -> `resolver.py` -> `discovery.py` -> `fetcher.py` -> `parser.py` -> `merger.py` -> `filters.py` -> `formatter.py`

- `databricks_client.py` - SOLE file importing `databricks.sdk`. All other modules use the adapter.
- `models.py` - Data models with `StrEnum` types (`SourceType`, `Stream`) and `logging` level ints
- `config.py` - Profile/config management via `~/.config/dbr-logs/config.toml`

## Gotchas

- `databricks-sdk` has incomplete type stubs — `databricks_client.py` has `ignore_errors = true` in mypy config
- Driver rotated log files are plain text; executor rotated files are `.gz`
- Level values are `logging.ERROR`/`logging.WARNING` ints, not strings
- All CI must pass: ruff check + ruff format --check + mypy strict + pytest
