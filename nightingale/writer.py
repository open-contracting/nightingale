from datetime import datetime
from pathlib import Path

import simplejson as json

from utils import produce_package_name


def new_name(package: dict | list) -> str:
    """
    Generate a new name for the package based on its published date.

    :param package: The release package dictionary.
    :type package: dict
    :return: The generated package name.
    :rtype: str
    """

    if isinstance(package, list):
        date = datetime.now().isoformat()
    else:
        date = package.get("publishedDate", datetime.now().isoformat())
    return produce_package_name(date)


class DataWriter:
    """
    Writes release package to disk.

    :param config: Configuration object containing settings for the writer.
    :type config: Config
    """

    def __init__(self, config):
        """
        Initialize the DataWriter.

        :param config: Configuration object containing settings for the writer.
        :type config: Config
        """
        self.config = config

    def make_dirs(self) -> Path:
        """
        Create the necessary directories for storing the release package.

        :return: The base directory path.
        :rtype: Path
        """
        base = Path(self.config.directory)
        base.mkdir(parents=True, exist_ok=True)
        return base

    def get_output_path(self, package: dict) -> Path:
        """
        Get the output path for the release package.

        :param package: The release package dictionary.
        :type package: dict
        :return: The path where the package will be written.
        :rtype: Path
        """
        base = self.make_dirs()
        return base / new_name(package)

    def write(self, package: list | dict) -> None:
        """
        Write the release package to disk.

        :param package: The release package dictionary.
        :type package: dict
        """
        path = self.get_output_path(package)
        with path.open("w") as f:
            json.dump(package, f, indent=2)
