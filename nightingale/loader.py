import logging
import sqlite3

import pandas as pd

logger = logging.getLogger(__name__)

# TODO: postgresql support


class DataLoader:
    """Load data from a database using a SQL query"""

    def __init__(self, config):
        self.config = config

    def load(self, selector):
        cursor = self.get_cursor()
        cursor.execute(selector)
        return [dict(row) for row in cursor.fetchall()]

    def get_cursor(self):
        conn = self.get_connection()
        cursor = conn.cursor()
        return cursor

    def get_connection(self):
        conn = sqlite3.connect(self.config.connection)
        conn.row_factory = sqlite3.Row
        return conn

    def validate_data_elements(self, data_elements):
        """Match columns in the database and in data elements"""
        conn = sqlite3.connect(self.config.connection)
        # XXX: validator should be a separate class
        # conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        for table in tables:
            table_name = table[0]
            elements = data_elements[data_elements["Section / Table"] == table_name]
            columns = [c[1] for c in cursor.execute(f"PRAGMA table_info([{table_name}])").fetchall()[1:]]
            for column in columns:
                if column not in elements["Data element"].values:
                    logger.warning(f"Column {table_name}/{column} not in data elements")
                    # XXX: warning or exception?
                    # raise ValueError(f'Column {column[1]} not in data elements')

    def validate_selector(self, data_elements):
        """Check selected columns are used in mapping"""
        # XXX: validator should be a separate class
        if not self.data:
            return
        labels_for_mapping = data_elements["For mapping"].values
        columns = list(self.data[0].keys())

        for column in columns:
            if column not in labels_for_mapping:
                logger.warning(f"Column {column} is not mapped")


class PDLoader(DataLoader):
    """Load data from sql to a pandas dataframe"""

    def load(self, selector):
        self.data = pd.read_sql(selector, self.get_connection())
