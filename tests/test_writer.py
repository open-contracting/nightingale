import unittest
from pathlib import Path

import simplejson as json

from nightingale.config import Output
from nightingale.writer import DataWriter


class TestDataWriter(unittest.TestCase):
    def setUp(self):
        self.config = Output(**{"directory": "./test_dir"})
        self.writer = DataWriter(self.config)
        self.package = {"publishedDate": "2022-01-01", "data": "test_data"}

    def tearDown(self):
        # Clean up the test directory after each test
        test_dir = Path(self.config.directory)
        if test_dir.exists():
            for file in test_dir.iterdir():
                file.unlink()
            test_dir.rmdir()

    def test_make_dirs(self):
        self.writer.make_dirs()
        self.assertTrue(Path(self.config.directory).exists())

    def test_get_output_path(self):
        output_path = self.writer.get_output_path(self.package)
        expected_path = Path(self.config.directory) / f'release-package-{self.package["publishedDate"]}.json'
        self.assertEqual(output_path, expected_path)

    def test_write(self):
        self.writer.write(self.package)
        output_path = self.writer.get_output_path(self.package)
        self.assertTrue(output_path.exists())
        with open(output_path, "r") as f:
            data = json.load(f)
        self.assertEqual(data, self.package)


if __name__ == "__main__":
    unittest.main()
