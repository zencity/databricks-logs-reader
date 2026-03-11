# TUI Mode Spec (Expansion Goal)

Interactive terminal UI for browsing and searching Databricks job logs. This is an expansion of the core CLI tool (see [01-cli-tool.md](01-cli-tool.md)) — not part of v1.

**Invocation**: `dbr-logs my-spark-job --tui`

## Layout

```
┌─────────────────────────────────────────────────────────────────┐
│ dbr-logs: my-spark-job | run: 0311-170011-t5450avl | env: prod     │
├────────────┬────────────────────────────────────────────────────┤
│ Sources    │ Log Output                                         │
│            │                                                    │
│ [x] driver │ [driver:stderr] ERROR TransportChannelHandler:... │
│   stderr   │ [exec/0:stderr] WARN HangingTaskDetector: Task... │
│   stdout   │ [exec/0:stderr] WARN HangingTaskDetector: Task... │
│   log4j    │ [exec/1:stderr] ERROR ShuffleBlockFetcherIter... │
│            │ [driver:stdout] Processing batch 42...             │
│ [x] exec/0 │                                                   │
│ [x] exec/1 │                                                   │
│ [x] exec/2 │                                                   │
│ ...        │                                                    │
│            │                                                    │
├────────────┴────────────────────────────────────────────────────┤
│ Filter: _________  Level: [ALL ▼]  Stream: [ALL ▼]  Search: /  │
└─────────────────────────────────────────────────────────────────┘
```

## Features

- **Source tree panel** (left): Toggle visibility of each source (driver, executor N). Checkbox-style.
- **Log panel** (right): Scrollable, color-coded log output. Lines colored by level (red=ERROR, yellow=WARN, white=INFO).
- **Filter bar** (bottom): Real-time text filter, log level dropdown, stream filter.
- **Search** (`/`): vim-style search with `n`/`N` for next/prev match.

## Keyboard Shortcuts

| Key | Action |
|---|---|
| `q` | Quit |
| `/` | Search |
| `f` | Toggle filter bar focus |
| `1`-`9` | Toggle source visibility |
| `e` | Show only ERROR |
| `w` | Show WARN + ERROR |
| `a` | Show all levels |
| `Tab` | Switch focus between panels |
| `g` / `G` | Go to top / bottom |
| `j` / `k` or arrows | Scroll |

## Architecture

The TUI reuses the same processing pipeline as the CLI tool (resolve → discover → fetch → parse → merge → filter). The only difference is the output stage, which feeds into a Textual app instead of stdout.

### Additional modules (under `dbr_logs/tui/`)

```
tui/
├── app.py          # TUI application (Textual)
├── source_panel.py # Source tree with checkboxes
├── log_panel.py    # Scrollable log display
└── filter_bar.py   # Bottom filter controls
```

## Technology Choice

| Component | Choice | Rationale |
|---|---|---|
| TUI framework | `textual` | Modern Python TUI, rich widget library, async-native |

## Data Flow

1. On launch: full pipeline runs, all logs loaded into memory
2. Source panel toggles → re-filter in memory (no re-fetch)
3. Level/stream filter changes → re-filter in memory
4. Search → scan filtered log entries, highlight matches

## Open Questions

- Should the TUI support lazy loading for very large runs (100k+ lines)?
- Should source panel show file-level detail or just source-level?
- Should the TUI support a "follow" mode for in-progress runs?
