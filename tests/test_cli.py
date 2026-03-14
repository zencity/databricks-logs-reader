from unittest.mock import MagicMock, patch

from click.testing import CliRunner

from dbr_logs.cli import main
from dbr_logs.databricks_client import DirEntry, RunCluster


def _mock_client() -> MagicMock:
    client = MagicMock()

    client.find_job_by_name.return_value = 1
    client.get_job_name.return_value = "test-job"
    client.get_log_destination.return_value = "dbfs:/Volumes/catalog/schema/logs/test-job"
    client.get_run_cluster_id.return_value = "0311-test-cluster"
    client.get_latest_run.return_value = RunCluster(run_id=100, cluster_id="0311-test-cluster")

    def list_dir(path: str) -> list:
        if path.endswith("/driver"):
            return [DirEntry("stderr", False)]
        if path.endswith("/executor"):
            raise Exception("Not found")
        return []

    client.list_directory.side_effect = list_dir

    stderr_content = b"26/03/11 21:01:15 ERROR TransportChannelHandler: Connection timeout\n"
    client.download_file.return_value = stderr_content

    return client


class TestCliEndToEnd:
    @patch("dbr_logs.cli.DatabricksClient")
    @patch("dbr_logs.cli.load_config", return_value={})
    @patch("dbr_logs.cli.list_databricks_profiles", return_value=["DEFAULT"])
    def test_basic_invocation(
        self, mock_profiles: MagicMock, mock_config: MagicMock, mock_cls: MagicMock
    ) -> None:
        mock_cls.return_value = _mock_client()
        runner = CliRunner()
        result = runner.invoke(main, ["test-job"])

        assert result.exit_code == 0
        assert "Connection timeout" in result.output

    @patch("dbr_logs.cli.DatabricksClient")
    @patch("dbr_logs.cli.load_config", return_value={})
    @patch("dbr_logs.cli.list_databricks_profiles", return_value=["DEFAULT"])
    def test_level_filter(
        self, mock_profiles: MagicMock, mock_config: MagicMock, mock_cls: MagicMock
    ) -> None:
        mock_cls.return_value = _mock_client()
        runner = CliRunner()

        result = runner.invoke(main, ["test-job", "--level", "WARN"])
        assert result.exit_code == 0
        assert "Connection timeout" not in result.output

    @patch("dbr_logs.cli.DatabricksClient")
    @patch("dbr_logs.cli.load_config", return_value={})
    @patch("dbr_logs.cli.list_databricks_profiles", return_value=["DEFAULT"])
    def test_jsonl_format(
        self, mock_profiles: MagicMock, mock_config: MagicMock, mock_cls: MagicMock
    ) -> None:
        mock_cls.return_value = _mock_client()
        runner = CliRunner()

        result = runner.invoke(main, ["test-job", "--format", "jsonl"])
        assert result.exit_code == 0
        assert '"source_type"' in result.output
        assert '"driver"' in result.output

    def test_help(self) -> None:
        runner = CliRunner()
        result = runner.invoke(main, ["--help"])
        assert result.exit_code == 0
        assert "JOB_INPUT" in result.output
