import unittest
from nightingale.publisher import DataPublisher
from nightingale.config import Publishing
class TestDataPublisher(unittest.TestCase):
    def setUp(self):
        self.config = Publishing(**{
            "ocid_prefix": "ocid_prefix",
            "version": "1.0",
            "publisher": "test_publisher"
        })
        self.publisher = DataPublisher(self.config)
        self.data = [{"ocid": "1234"}]

    def test_produce_ocid(self):
        ocid = self.publisher.produce_ocid("1234")
        self.assertEqual(ocid, "ocid_prefix-1234")

    def test_publish(self):
        package = self.publisher.publish(self.data)
        self.assertEqual(package["version"], self.config.version)
        self.assertEqual(package["publisher"], self.config.publisher)
        self.assertEqual(package["releases"][0]["ocid"], "ocid_prefix-1234")

if __name__ == '__main__':
    unittest.main()