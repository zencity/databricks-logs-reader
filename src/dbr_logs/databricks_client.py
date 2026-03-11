from dataclasses import dataclass
from typing import NoReturn

import click
from databricks.sdk import WorkspaceClient


@dataclass
class DirEntry:
    name: str
    is_directory: bool


@dataclass
class RunCluster:
    run_id: int
    cluster_id: str


class DatabricksClient:
    def __init__(self, profile: str | None = None) -> None:
        self._w = WorkspaceClient(profile=profile)

    def find_job_by_name(self, name: str) -> int:
        jobs = list(self._w.jobs.list(name=name))
        if not jobs:
            raise click.UsageError(f"Job '{name}' not found")
        return jobs[0].job_id

    def get_job_name(self, job_id: int) -> str:
        job = self._w.jobs.get(job_id=job_id)
        return job.settings.name

    def get_job_name_and_log_destination(self, job_id: int) -> tuple[str, str]:
        job = self._w.jobs.get(job_id=job_id)
        name = job.settings.name
        log_dest = self._extract_log_destination(job, job_id)
        return (name, log_dest)

    def get_log_destination(self, job_id: int) -> str:
        job = self._w.jobs.get(job_id=job_id)
        return self._extract_log_destination(job, job_id)

    def get_run_cluster_id(self, run_id: int) -> str:
        run = self._w.jobs.get_run(run_id=run_id)
        cluster_id = self._extract_cluster_id(run)
        if cluster_id:
            return cluster_id
        self._raise_no_cluster(run_id, run)

    def get_latest_run(self, job_id: int) -> RunCluster:
        runs = list(self._w.jobs.list_runs(job_id=job_id, limit=1))
        if not runs:
            raise click.UsageError(f"No runs found for job {job_id}")
        run_id = runs[0].run_id
        full_run = self._w.jobs.get_run(run_id=run_id)
        cluster_id = self._extract_cluster_id(full_run)
        if cluster_id:
            return RunCluster(run_id=run_id, cluster_id=cluster_id)
        self._raise_no_cluster(run_id, full_run)

    def list_directory(self, path: str) -> list[DirEntry]:
        vol_path = self._volume_path(path)
        return [
            DirEntry(name=e.name, is_directory=bool(e.is_directory))
            for e in self._w.files.list_directory_contents(vol_path)
        ]

    def download_file(self, path: str) -> bytes:
        vol_path = self._volume_path(path)
        response = self._w.files.download(vol_path)
        return response.contents.read()

    @staticmethod
    def _volume_path(dbfs_path: str) -> str:
        if dbfs_path.startswith("dbfs:"):
            return dbfs_path[5:]
        return dbfs_path

    @staticmethod
    def _raise_no_cluster(run_id: int, run: object) -> NoReturn:
        state = getattr(run, "state", None)
        life_cycle = getattr(state, "life_cycle_state", "UNKNOWN") if state else "UNKNOWN"
        raise click.UsageError(
            f"Run {run_id} has no cluster instance (state: {life_cycle}). "
            "The cluster may not be provisioned yet if the run is still starting."
        )

    @staticmethod
    def _extract_log_destination(job: object, job_id: int) -> str:
        for cluster in DatabricksClient._collect_clusters(job):
            log_conf = cluster.cluster_log_conf
            if not log_conf:
                continue
            if log_conf.volumes and log_conf.volumes.destination:
                return log_conf.volumes.destination.rstrip("/")
            if log_conf.dbfs and log_conf.dbfs.destination:
                return log_conf.dbfs.destination.rstrip("/")
            if log_conf.s3 and log_conf.s3.destination:
                raise click.UsageError(
                    f"Job {job_id} uses S3 log destination ({log_conf.s3.destination}). "
                    "S3 paths are not yet supported — only Unity Catalog Volumes."
                )
        raise click.UsageError(f"Job {job_id} has no cluster_log_conf configured")

    @staticmethod
    def _collect_clusters(job: object) -> list:
        clusters = []
        for task in job.settings.tasks or []:
            if task.new_cluster:
                clusters.append(task.new_cluster)
        for jc in job.settings.job_clusters or []:
            if jc.new_cluster:
                clusters.append(jc.new_cluster)
        return clusters

    @staticmethod
    def _extract_cluster_id(run: object) -> str | None:
        if run.cluster_instance and run.cluster_instance.cluster_id:
            return run.cluster_instance.cluster_id
        for task in run.tasks or []:
            if task.cluster_instance and task.cluster_instance.cluster_id:
                return task.cluster_instance.cluster_id
        return None
