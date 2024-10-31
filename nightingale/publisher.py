from typing import Any
from urllib.parse import urljoin

from ocdskit.combine import package_releases

from nightingale.config import Publishing
from nightingale.utils import get_iso_now, produce_package_name


class DataPublisher:
    """
    Packs array of releases into a release package.
    """

    def __init__(self, config: Publishing, mapping):
        """
        Initialize the DataPublisher.

        :param config: Configuration object containing settings for the publisher.
        """
        self.config = config
        self.mapping = mapping
        self.date = get_iso_now()

    def produce_uri(self) -> str:
        """
        Produce a URI for the package based on the given date.

        :return: The produced URI.
        """
        full_name = produce_package_name(self.date)
        return urljoin(self.config.base_uri, f"/{full_name}")

    def package(self, data: list[dict[str, Any]]) -> dict[str, Any]:
        """
        Package the given data into a release package.

        :param data: List of release dictionaries to be packaged.
        :return: A dictionary representing the release package.
        """
        kwargs = dict(
            uri=self.produce_uri(),
            publisher=self.get_publisher(),
            published_date=self.date,
            extensions=self.get_extensions(),
        )
        for key in ("version", "license", "publicationPolicy"):
            if value := getattr(self.config, key, None):
                kwargs[key] = value

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
        return [e.get("url") for e in self.mapping.extensions if e.get("url")]

    def get_version(self):
        return self.config.version
