from datetime import datetime
from pathlib import Path

import simplejson as json

from nightingale.config import Output
from nightingale.utils import produce_package_name


def new_name(package: dict | list) -> str:
    """
    Generate a new name for the package based on its published date.

    :param package: The release package dictionary.
    :return: The generated package name.
    """

    if isinstance(package, list):
        date = datetime.now().isoformat()
    else:
        date = package.get("publishedDate", datetime.now().isoformat())
    return produce_package_name(date)


class DataWriter:
    """
    Writes release package to disk.
    """

    def __init__(self, config: Output):
        """
        Initialize the DataWriter.

        :param config: Configuration object containing settings for the writer.
        """
        self.config = config

    def make_dirs(self) -> Path:
        """
        Create the necessary directories for storing the release package.

        :return: The base directory path.
        """
        base = Path(self.config.directory)
        base.mkdir(parents=True, exist_ok=True)
        return base

    def get_output_path(self, package: dict | list) -> Path:
        """
        Get the output path for the release package.

        :param package: The release package dictionary.
        :return: The path where the package will be written.
        """
        base = self.make_dirs()
        return base / new_name(package)

    def write(self, package: dict | list) -> None:
        """
        Write the release package to disk.

        :param package: The release package dictionary.
        """
        path = self.get_output_path(package)
        with path.open("w") as f:
            json.dump(package, f, indent=2)
