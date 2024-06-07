import unittest
from unittest.mock import MagicMock, patch

from nightingale.config import Datasources
from nightingale.loader import DataLoader


class TestDataLoader(unittest.TestCase):
    def setUp(self):
        self.config = Datasources(connection="test.db", selector="SELECT * FROM test_table")
        self.loader = DataLoader(self.config)

    @patch("sqlite3.connect")
    def test_get_connection(self, mock_connect):
        self.loader.get_connection()
        mock_connect.assert_called_once_with(self.config.connection)

    @patch("sqlite3.connect")
    def test_get_cursor(self, mock_connect):
        mock_cursor = MagicMock()
        mock_connect.return_value.cursor.return_value = mock_cursor
        cursor = self.loader.get_cursor()
        self.assertEqual(cursor, mock_cursor)

    @patch.object(DataLoader, "get_cursor")
    def test_load(self, mock_get_cursor):
        mock_cursor = MagicMock()
        mock_get_cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = [{"column1": "value1"}]
        self.loader.load()
        mock_cursor.execute.assert_called_once_with(self.config.selector)
        self.assertEqual(self.loader.data, [{"column1": "value1"}])


if __name__ == "__main__":
    unittest.main()
