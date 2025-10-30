import os
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
        self._file_handler = None
        self._is_first_release = True
        self._output_path = None

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
        if not self._output_path:
            base = self.make_dirs()
            self._output_path = base / new_name(package)
        return self._output_path

    def write(self, package: dict | list) -> None:
        """
        Write the release package to disk in a single operation.

        :param package: The release package dictionary or list of releases.
        """
        path = self.get_output_path(package)
        with path.open("w", encoding="utf-8") as f:
            json.dump(package, f, indent=2, ensure_ascii=False)

    def start_package_stream(self, package_metadata: dict) -> None:
        """
        Starts a streaming write session. Writes package metadata and prepares for releases.
        """
        buffer_size_str = os.getenv('APP_WRITE_BUFFER_SIZE', '8388608')
        buffer_size = int(buffer_size_str)
        path = self.get_output_path(package_metadata)
        self._file_handler = path.open("w", encoding="utf-8", buffering=buffer_size)

        # Write metadata part of the package
        metadata_items = []
        for key, value in package_metadata.items():
            metadata_items.append(f'"{key}": {json.dumps(value, ensure_ascii=False, indent=2)}')

        self._file_handler.write("{\n  " + ",\n  ".join(metadata_items) + ",\n")
        self._file_handler.write('  "releases": [\n')
        self._is_first_release = True

    def stream_release(self, release: dict) -> None:
        """
        Writes a single release to the open package file stream.
        """
        if not self._file_handler:
            raise IOError("Stream writing has not been started. Call start_package_stream() first.")

        if not self._is_first_release:
            self._file_handler.write(",\n")

        # Using simplejson for potential performance gains and better decimal handling
        json.dump(release, self._file_handler, indent=4, ensure_ascii=False)
        self._is_first_release = False
        self._file_handler.flush()


    def end_package_stream(self) -> None:
        """
        Finalizes the streaming write session by closing the JSON array and file.
        This method is safe to call even if the stream was not started or already closed.
        """
        if self._file_handler:
            self._file_handler.write("\n  ]\n}\n")
            self._file_handler.close()
            self._file_handler = None

    def is_streaming(self) -> bool:
        """
        Checks if the writer is currently in a streaming session.
        """
        return self._file_handler is not None and not self._file_handler.closed
