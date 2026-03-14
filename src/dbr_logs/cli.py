import sys
from datetime import UTC, datetime

import click

from dbr_logs.config import (
    get_default_env,
    get_default_profile,
    interactive_profile_setup,
    list_databricks_profiles,
    load_config,
)
from dbr_logs.databricks_client import DatabricksClient
from dbr_logs.discovery import discover_sources
from dbr_logs.fetcher import fetch_sources
from dbr_logs.filters import build_filter, parse_since
from dbr_logs.formatter import write_entries
from dbr_logs.merger import merge_entries
from dbr_logs.noise import build_quiet_filter
from dbr_logs.parser import parse_entries
from dbr_logs.resolver import resolve_run


@click.command()
@click.argument("job_input")
@click.option("--run-id", "-r", default=None, help="Databricks run ID (numeric)")
@click.option("--env", "-e", default=None, help="Environment: prod, staging, ondemand")
@click.option("--dbr-profile", "-p", default=None, help="Databricks CLI profile name")
@click.option("--source", "-s", default="all", help="driver, executor, executor:N, all")
@click.option("--stream", default="all", help="stderr, stdout, all")
@click.option(
    "--level",
    "-l",
    default=None,
    help="Exact match, comma-separated: ERROR,WARN,INFO,DEBUG (e.g. WARN,ERROR for both)",
)
@click.option("--include-log4j", is_flag=True, default=False, help="Include driver log4j files")
@click.option(
    "--include-stacktrace", is_flag=True, default=False, help="Include driver stacktrace files"
)
@click.option(
    "--format",
    "-f",
    "fmt",
    default="text",
    type=click.Choice(["text", "jsonl"]),
    help="Output format",
)
@click.option("--tail", "-n", default=None, type=int, help="Show only last N lines")
@click.option("--since", default=None, help="Show logs since time (e.g. 1h, 30m, ISO datetime)")
@click.option(
    "--focus",
    is_flag=True,
    default=False,
    help="Suppress Spark/JVM noise (thread dumps, shuffle, task lifecycle)",
)
def main(
    job_input: str,
    run_id: str | None,
    env: str | None,
    dbr_profile: str | None,
    source: str,
    stream: str,
    level: str | None,
    include_log4j: bool,
    include_stacktrace: bool,
    fmt: str,
    tail: int | None,
    since: str | None,
    focus: bool,
) -> None:
    """Fetch and display Databricks job logs.

    JOB_INPUT is a job name or Databricks workspace URL.
    """
    config = load_config()
    profile = _resolve_profile(dbr_profile, config)
    env = env or get_default_env(config)

    client = DatabricksClient(profile=profile)

    click.echo(f"Resolving job: {job_input}...", err=True)
    run_info = resolve_run(client, job_input, run_id, env)

    run_time = _format_run_time(run_info.start_time, run_info.end_time)
    click.echo(
        f"Run: {run_info.cluster_id} | Job: {run_info.job_name} | Env: {env} | {run_time}",
        err=True,
    )

    sources = discover_sources(client, run_info, include_log4j, include_stacktrace)
    if not sources:
        click.echo("No log sources found.", err=True)
        sys.exit(0)

    click.echo(f"Fetching logs from {sum(len(s.files) for s in sources)} files...", err=True)
    content_map = fetch_sources(client, sources, source, stream)

    if not content_map:
        click.echo("No matching log files.", err=True)
        sys.exit(0)

    all_entries = []
    for src in sources:
        for lf in src.files:
            if lf.path in content_map:
                all_entries.append(parse_entries(content_map[lf.path], src, lf))

    merged = merge_entries(all_entries)

    levels = [lv.strip() for lv in level.split(",")] if level else None
    since_dt = parse_since(since) if since else None
    if focus:
        quiet_filter = build_quiet_filter()
        before_count = len(merged)
        merged = quiet_filter(merged)
        filtered_out = before_count - len(merged)
        click.echo(
            f"--focus: filtered {filtered_out} noise lines ({len(merged)} remaining)",
            err=True,
        )

    filter_fn = build_filter(levels, since_dt, tail)
    filtered = filter_fn(merged)

    write_entries(filtered, fmt)


def _format_run_time(start_ms: int | None, end_ms: int | None) -> str:
    if start_ms is None:
        return "Started: unknown"
    start = datetime.fromtimestamp(start_ms / 1000, tz=UTC)
    start_str = start.strftime("%Y-%m-%d %H:%M:%S UTC")
    if end_ms is None:
        return f"Started: {start_str} (still running)"
    duration_s = (end_ms - start_ms) / 1000
    minutes, seconds = divmod(int(duration_s), 60)
    hours, minutes = divmod(minutes, 60)
    if hours:
        dur = f"{hours}h{minutes}m"
    elif minutes:
        dur = f"{minutes}m{seconds}s"
    else:
        dur = f"{seconds}s"
    return f"Started: {start_str} ({dur})"


def _resolve_profile(override: str | None, config: dict[str, object]) -> str | None:
    if override:
        return override

    saved = get_default_profile(config)
    if saved:
        return saved

    profiles = list_databricks_profiles()
    if len(profiles) == 1:
        return profiles[0]
    if len(profiles) > 1:
        return interactive_profile_setup(profiles)

    return None
