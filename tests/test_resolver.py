from unittest.mock import MagicMock

import click
import pytest

from dbr_logs.databricks_client import RunCluster
from dbr_logs.resolver import parse_databricks_url, resolve_run


class TestParseDatabricksUrl:
    def test_job_and_run(self) -> None:
        url = "https://dbc-xxx.cloud.databricks.com/#job/12345/run/67890"
        parsed = parse_databricks_url(url)
        assert parsed.job_id == 12345
        assert parsed.run_id == 67890

    def test_job_only(self) -> None:
        url = "https://dbc-xxx.cloud.databricks.com/#job/12345"
        parsed = parse_databricks_url(url)
        assert parsed.job_id == 12345
        assert parsed.run_id is None

    def test_azure_url_with_run(self) -> None:
        url = "https://adb-xxx.azuredatabricks.net/?o=123#job/456/run/789"
        parsed = parse_databricks_url(url)
        assert parsed.job_id == 456
        assert parsed.run_id == 789

    def test_azure_url_job_only(self) -> None:
        url = "https://adb-xxx.azuredatabricks.net/?o=123#job/456"
        parsed = parse_databricks_url(url)
        assert parsed.job_id == 456
        assert parsed.run_id is None

    def test_new_url_format_job_only(self) -> None:
        url = "https://dbc-xxx.cloud.databricks.com/jobs/243814802779843?o=1234567890"
        parsed = parse_databricks_url(url)
        assert parsed.job_id == 243814802779843
        assert parsed.run_id is None

    def test_new_url_format_with_run(self) -> None:
        url = "https://dbc-xxx.cloud.databricks.com/jobs/12345/runs/67890?o=123"
        parsed = parse_databricks_url(url)
        assert parsed.job_id == 12345
        assert parsed.run_id == 67890

    def test_invalid_url(self) -> None:
        with pytest.raises(click.UsageError, match="Could not parse"):
            parse_databricks_url("https://example.com/not-databricks")


class TestResolveRun:
    def _make_client(self) -> MagicMock:
        client = MagicMock()
        client.find_job_by_name.return_value = 42
        client.get_job_name_and_log_destination.return_value = (
            "my-spark-job",
            "dbfs:/Volumes/catalog/schema/logs/my-spark-job",
        )
        client.get_log_destination.return_value = (
            "dbfs:/Volumes/catalog/schema/logs/my-spark-job"
        )
        client.get_run_cluster_id.return_value = "0311-170011-t5450avl"
        client.get_latest_run.return_value = RunCluster(
            run_id=100, cluster_id="0311-170011-t5450avl"
        )
        return client

    def test_resolve_by_name_latest_run(self) -> None:
        client = self._make_client()
        result = resolve_run(client, "my-spark-job", run_id_override=None, env="prod")

        assert result.job_name == "my-spark-job"
        assert result.cluster_id == "0311-170011-t5450avl"
        assert result.base_path == (
            "dbfs:/Volumes/catalog/schema/logs/my-spark-job/0311-170011-t5450avl"
        )
        client.get_latest_run.assert_called_once_with(42)

    def test_resolve_by_name_with_run_id(self) -> None:
        client = self._make_client()
        result = resolve_run(client, "my-spark-job", run_id_override="100", env="prod")

        assert result.cluster_id == "0311-170011-t5450avl"
        client.get_run_cluster_id.assert_called_once_with(100)

    def test_resolve_from_url(self) -> None:
        client = self._make_client()
        url = "https://dbc-xxx.cloud.databricks.com/#job/42/run/100"
        result = resolve_run(client, url, run_id_override=None, env="prod")

        assert result.job_name == "my-spark-job"
        assert result.cluster_id == "0311-170011-t5450avl"
        client.get_run_cluster_id.assert_called_once_with(100)

    def test_resolve_from_url_latest_run(self) -> None:
        client = self._make_client()
        url = "https://dbc-xxx.cloud.databricks.com/#job/42"
        result = resolve_run(client, url, run_id_override=None, env="prod")

        assert result.job_name == "my-spark-job"
        assert result.cluster_id == "0311-170011-t5450avl"
        client.get_latest_run.assert_called_once_with(42)

    def test_job_not_found(self) -> None:
        client = self._make_client()
        client.find_job_by_name.side_effect = click.UsageError("Job 'nonexistent-job' not found")

        with pytest.raises(click.UsageError, match="not found"):
            resolve_run(client, "nonexistent-job", run_id_override=None, env="prod")
