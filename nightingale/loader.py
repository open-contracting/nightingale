import logging
import sqlite3

logger = logging.getLogger(__name__)

# TODO: postgresql support


class DataLoader:
    """Load data from a database using a SQL query"""

    def __init__(self, config, connection=None):
        self.config = config
        self._connection = connection

    def load(self, selector):
        cursor = self.get_cursor()
        cursor.execute(selector)
        # logger.info(f"Loaded data from {self.config.connection}")
        return (dict(row) for row in cursor)

    def get_cursor(self):
        conn = self.get_connection()
        cursor = conn.cursor()
        return cursor

    def get_connection(self):
        if self._connection:
            return self._connection
        conn = sqlite3.connect(self.config.connection)
        conn.row_factory = sqlite3.Row
        # for tests
        self._connection = conn
        logger.info(f"Connected to {self.config.connection}")
        return conn
