import unittest
from unittest import mock

from nightingale.config import Publishing
from nightingale.publisher import DataPublisher


class TestDataPublisher(unittest.TestCase):
    def setUp(self):
        self.config = Publishing(
            **{
                "version": "1.0",
                "publisher": "test_publisher",
                "base_uri": "http://example.com",
                "publisher_scheme": "scheme",
                "publisher_uid": "uid",
                "publisher_uri": "uri",
                "license": "license",
                "publicationPolicy": "policy",
            }
        )
        self.mapping = mock.Mock()
        self.mapping.extensions = [{"url": "http://example.com/extension1"}, {"url": None}]
        self.publisher = DataPublisher(self.config, self.mapping)
        self.data = [{"ocid": "ocid_prefix-1234"}]

    def test_publish(self):
        package = self.publisher.package(self.data)
        self.assertEqual(package["version"], self.config.version)
        self.assertEqual(package["license"], self.config.license)
        self.assertEqual(package["publicationPolicy"], self.config.publicationPolicy)
        self.assertEqual(
            package["publisher"],
            {
                "name": self.config.publisher,
                "scheme": self.config.publisher_scheme,
                "uid": self.config.publisher_uid,
                "uri": self.config.publisher_uri,
            },
        )
        self.assertEqual(package["releases"][0]["ocid"], "ocid_prefix-1234")


if __name__ == "__main__":
    unittest.main()
