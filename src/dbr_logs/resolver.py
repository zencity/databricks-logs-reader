import re
from dataclasses import dataclass

import click

from dbr_logs.databricks_client import DatabricksClient
from dbr_logs.models import RunInfo

URL_PATTERNS = [
    re.compile(r"/jobs/(\d+)/runs/(\d+)"),
    re.compile(r"/jobs/(\d+)"),
    re.compile(r"#job/(\d+)/run/(\d+)"),
    re.compile(r"#job/(\d+)"),
]


@dataclass
class ParsedUrl:
    job_id: int
    run_id: int | None


def parse_databricks_url(url: str) -> ParsedUrl:
    """Extract job_id and optional run_id from a Databricks workspace URL."""
    for pattern in URL_PATTERNS:
        m = pattern.search(url)
        if m:
            groups = m.groups()
            job_id = int(groups[0])
            run_id = int(groups[1]) if len(groups) > 1 else None
            return ParsedUrl(job_id=job_id, run_id=run_id)
    raise click.UsageError(f"Could not parse Databricks URL: {url}")


def resolve_run(
    client: DatabricksClient,
    job_input: str,
    run_id_override: str | None,
    env: str,
) -> RunInfo:
    """Resolve a job name or URL to a RunInfo with cluster ID and log path."""
    is_url = job_input.startswith("http://") or job_input.startswith("https://")

    if is_url:
        parsed = parse_databricks_url(job_input)
        job_name, log_dest = client.get_job_name_and_log_destination(parsed.job_id)
        job_id = parsed.job_id
        run_id = int(run_id_override) if run_id_override else parsed.run_id
    else:
        job_id = client.find_job_by_name(job_input)
        job_name = job_input
        log_dest = client.get_log_destination(job_id)
        run_id = int(run_id_override) if run_id_override else None

    if run_id:
        rc = client.get_run_cluster(run_id)
    else:
        rc = client.get_latest_run(job_id)
        run_id = rc.run_id

    return RunInfo(
        job_name=job_name,
        run_id=run_id,
        cluster_id=rc.cluster_id,
        env=env,
        base_path=f"{log_dest}/{rc.cluster_id}",
        start_time=rc.start_time,
        end_time=rc.end_time,
    )
