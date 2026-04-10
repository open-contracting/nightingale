import logging
import sqlite3

logger = logging.getLogger(__name__)


class DataLoader:
    """Load data from a database using a SQL query."""

    def __init__(self, config, connection=None):
        self.config = config
        self._connection = connection

    def load(self, selector):
        cursor = self.get_cursor()
        cursor.execute(selector)
        return (dict(row) for row in cursor)

    def get_cursor(self):
        conn = self.get_connection()
        return conn.cursor()

    def get_connection(self):  # SQLite-specific
        if self._connection:
            return self._connection
        conn = sqlite3.connect(self.config.connection)
        conn.row_factory = sqlite3.Row
        self._connection = conn
        logger.info("Connected to %s", self.config.connection)
        return conn

    def close(self):
        if self._connection:
            self._connection.close()
            self._connection = None
