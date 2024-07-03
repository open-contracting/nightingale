from typing import Any

from ocdskit.combine import package_releases

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
        self.date = get_iso_now()

    def produce_uri(self) -> str:
        """
        Produce a URI for the package based on the given date.

        :param date: The date to use for generating the URI.
        :type date: str
        :return: The produced URI.
        :rtype: str
        """
        return f"{self.config.base_uri}/{produce_package_name(self.date)}"

    def package(self, data: list[dict[str, Any]]) -> dict[str, Any]:
        """
        Package the given data into a release package.

        :param data: List of release dictionaries to be packaged.
        :type data: list[dict[str, Any]]
        :return: A dictionary representing the release package.
        :rtype: dict[str, Any]
        """
        kwargs = dict(
            uri=self.produce_uri(),
            publisher=self.get_publisher(),
            published_date=self.date,
            extensions=self.get_extensions(),
        )
        if self.config.version:
            kwargs["version"] = self.config.version
        return package_releases(data, **kwargs)

    def get_publisher(self):
        publisher = {
            "name": self.config.publisher,
        }
        if self.config.publisher_scheme:
            publisher["scheme"] = self.config.publisher_scheme
        if self.config.publisher_uid:
            publisher["uid"] = self.config.publisher_uid
        if self.config.publisher_uri:
            publisher["uri"] = self.config.publisher_uri
        return publisher

    def get_extensions(self):
        return self.config.extensions

    def get_version(self):
        self.config.version
