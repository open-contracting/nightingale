from typing import Any

from .utils import get_iso_now, produce_package_name


class DataPublisher:
    """
    Packs array of releases into a release package.

    :param config: Configuration object containing settings for the publisher.
    :type config: Config
    """

    def __init__(self, config):
        """
        Initialize the DataPublisher.

        :param config: Configuration object containing settings for the publisher.
        :type config: Config
        """
        self.config = config

    def produce_uri(self, date: str) -> str:
        """
        Produce a URI for the package based on the given date.

        :param date: The date to use for generating the URI.
        :type date: str
        :return: The produced URI.
        :rtype: str
        """
        return f"{self.config.base_uri}/{produce_package_name(date)}"

    def package(self, data: list[dict[str, Any]]) -> dict[str, Any]:
        """
        Package the given data into a release package.

        :param data: List of release dictionaries to be packaged.
        :type data: list[dict[str, Any]]
        :return: A dictionary representing the release package.
        :rtype: dict[str, Any]
        """
        now = get_iso_now()
        return {
            "uri": self.produce_uri(now),
            "version": self.config.version,
            "publisher": {"name": self.config.publisher},
            "publishedDate": now,
            "releases": data,
        }
