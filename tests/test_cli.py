import logging
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from click.testing import CliRunner

from nightingale.cli import run, setup_logging


class TestCli(unittest.TestCase):
    def setUp(self):
        self.runner = CliRunner()
        self.temp_dir = tempfile.TemporaryDirectory()
        self.config_path = Path(self.temp_dir.name) / "test_config.toml"
        self.invalid_config_path = Path(self.temp_dir.name) / "invalid_test_config.toml"
        self.selector_data = "SELECT * FROM test_table;"
        self.selector_path = Path(self.temp_dir.name) / "test_selector.sql"
        with open(self.selector_path, "w") as f:
            f.write(self.selector_data)
        self.config_data = """
        [datasource]
        connection = "test_connection"

        [mapping]
        file = "test_mapping_file"
        ocid_prefix = "test_ocid_prefix"
        selector = "test_selector"
        force_publish = true

        [publishing]
        version = "1.0"
        publisher = "test_publisher"
        base_uri = "http://test_base_uri"

        [output]
        directory = "test_output_directory"
        """
        with open(self.config_path, "w") as f:
            f.write(self.config_data)

        self.invalid_config_data = """
        [datasource
        connection = "test_connection"
        """
        with open(self.invalid_config_path, "w") as f:
            f.write(self.invalid_config_data)

    def tearDown(self):
        self.temp_dir.cleanup()

    @patch("nightingale.cli.Config.from_file")
    @patch("nightingale.cli.OCDSDataMapper")
    @patch("nightingale.cli.DataLoader")
    @patch("nightingale.cli.DataWriter")
    @patch("nightingale.cli.DataPublisher")
    def test_run_without_package(self, mock_publisher, mock_writer, mock_loader, mock_mapper, mock_config):
        # Setup mocks
        mock_config.return_value = MagicMock()
        mock_mapper_instance = MagicMock()
        mock_mapper.return_value = mock_mapper_instance

        mock_loader_instance = MagicMock()
        mock_loader.return_value = mock_loader_instance

        mock_writer_instance = MagicMock()
        mock_writer.return_value = mock_writer_instance

        mock_mapper_instance.map.return_value = [{"dummy_data": "data"}]

        result = self.runner.invoke(run, ["--config", str(self.config_path), "--loglevel", "INFO"])

        self.assertEqual(result.exit_code, 0)
        mock_mapper.assert_called_once()
        mock_loader.assert_called_once()
        mock_writer.assert_called_once()
        mock_writer_instance.write.assert_called_once_with([{"dummy_data": "data"}])
        mock_publisher.assert_not_called()

    @patch("nightingale.cli.Config.from_file")
    @patch("nightingale.cli.OCDSDataMapper")
    @patch("nightingale.cli.DataLoader")
    @patch("nightingale.cli.DataWriter")
    @patch("nightingale.cli.DataPublisher")
    def test_run_with_package(self, mock_publisher, mock_writer, mock_loader, mock_mapper, mock_config):
        # Setup mocks
        mock_config.return_value = MagicMock()
        mock_mapper_instance = MagicMock()
        mock_mapper.return_value = mock_mapper_instance

        mock_loader_instance = MagicMock()
        mock_loader.return_value = mock_loader_instance

        mock_writer_instance = MagicMock()
        mock_writer.return_value = mock_writer_instance

        mock_publisher_instance = MagicMock()
        mock_publisher.return_value = mock_publisher_instance

        mock_mapper_instance.map.return_value = {"dummy_data": "data"}
        mock_publisher_instance.package.return_value = {"packaged_data": "data"}

        result = self.runner.invoke(run, ["--config", str(self.config_path), "--package", "--loglevel", "INFO"])

        self.assertEqual(result.exit_code, 0)
        mock_mapper.assert_called_once()
        mock_loader.assert_called_once()
        mock_writer.assert_called_once()
        mock_publisher.assert_called_once()
        mock_writer_instance.write.assert_called_once_with({"packaged_data": "data"})

    def test_setup_logging(self):
        with self.assertLogs(level="DEBUG") as log:
            setup_logging("DEBUG")
            logging.getLogger().debug("This is a debug message")

        self.assertIn("This is a debug message", log.output[0])

    @patch("nightingale.cli.Config.from_file")
    @patch("nightingale.cli.OCDSDataMapper")
    @patch("nightingale.cli.DataLoader")
    @patch("nightingale.cli.DataWriter")
    def test_run_mapping_crash(self, mock_writer, mock_loader, mock_mapper, mock_config):
        # Setup mocks
        mock_config.return_value = MagicMock()
        mock_mapper_instance = MagicMock()
        mock_mapper.return_value = mock_mapper_instance

        mock_loader_instance = MagicMock()
        mock_loader.return_value = mock_loader_instance

        mock_writer_instance = MagicMock()
        mock_writer.return_value = mock_writer_instance

        mock_mapper_instance.map.side_effect = Exception("Simulated mapping crash")

        result = self.runner.invoke(run, ["--config", str(self.config_path), "--loglevel", "INFO"])

        self.assertNotEqual(result.exit_code, 0)
        self.assertIn("Error during transformation: Simulated mapping crash", result.output)
        mock_mapper.assert_called_once()
        mock_loader.assert_called_once()

    def test_invalid_toml_file(self):
        result = self.runner.invoke(run, ["--config", str(self.invalid_config_path), "--loglevel", "INFO"])
        self.assertNotEqual(result.exit_code, 0)
        self.assertIn("Error decoding TOML", result.output)

    @patch("nightingale.cli.Config.from_file")
    @patch("nightingale.cli.OCDSDataMapper")
    @patch("nightingale.cli.DataLoader")
    @patch("nightingale.cli.DataWriter")
    def test_run_with_selector_file(self, mock_writer, mock_loader, mock_mapper, mock_config):
        # Setup mocks
        mock_config.return_value = MagicMock()
        mock_mapper_instance = MagicMock()
        mock_mapper.return_value = mock_mapper_instance

        mock_loader_instance = MagicMock()
        mock_loader.return_value = mock_loader_instance

        mock_writer_instance = MagicMock()
        mock_writer.return_value = mock_writer_instance

        mock_mapper_instance.map.return_value = [{"dummy_data": "data"}]

        result = self.runner.invoke(
            run, ["--config", str(self.config_path), "--selector", str(self.selector_path), "--loglevel", "INFO"]
        )

        self.assertEqual(result.exit_code, 0)
        mock_mapper.assert_called_once()
        mock_loader.assert_called_once()
        mock_writer.assert_called_once()
        mock_writer_instance.write.assert_called_once_with([{"dummy_data": "data"}])


if __name__ == "__main__":
    unittest.main()
