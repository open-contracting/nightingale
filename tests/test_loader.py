import sqlite3
import unittest

from nightingale.config import Datasource
from nightingale.loader import DataLoader


class TestDataLoader(unittest.TestCase):
    def setUp(self):
        # Using an in-memory SQLite database for testing
        self.config = Datasource(connection=":memory:")
        self.connection = sqlite3.connect(self.config.connection)
        self.connection.row_factory = sqlite3.Row
        self.cursor = self.connection.cursor()

        # Set up the in-memory database and table for testing
        self.cursor.execute("CREATE TABLE test_table (column1 TEXT)")
        self.cursor.execute("INSERT INTO test_table (column1) VALUES ('value1')")
        self.connection.commit()

        # Inject the in-memory connection into DataLoader
        self.loader = DataLoader(self.config, connection=self.connection)

    def tearDown(self):
        # Close the connection after each test
        self.connection.close()

    def test_get_connection(self):
        loader = DataLoader(self.config)
        connection = loader.get_connection()
        self.assertIsNotNone(connection)
        self.assertEqual(connection.execute("PRAGMA database_list").fetchall()[0]["file"], "")

    def test_load(self):
        data = self.loader.load("SELECT * FROM test_table")
        self.assertEqual(data, [{"column1": "value1"}])


if __name__ == "__main__":
    unittest.main()
