import logging

logger = logging.getLogger(__name__)


class MappingTemplateValidator:
    """Class to validate loaded mapping against providded data"""

    def __init__(self, loader, mapping_template):
        self.loader = loader
        self.mapping_template = mapping_template

    def validate_data_elements(self) -> None:
        """Match columns in the database and in data elements from mapping template"""
        cursor = self.loader.get_cursor()
        # XXX: postgersql support?
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        for table in tables:
            table_name = table[0]
            elements = self.mapping_template.get_data_elements()
            columns = [c[1] for c in cursor.execute(f"PRAGMA table_info([{table_name}])").fetchall()[1:]]
            for column in columns:
                if column not in elements:
                    logger.warning(f"Column {table_name}/{column} is not described in data elements")

    def validate_selector(self, row) -> None:
        """Check selected columns are used in mapping"""
        elements = self.mapping_template.get_data_elements()
        labels_for_mapping = [e["for_mapping"] for e in elements.values()]
        columns = [k for k in row]

        for column in columns:
            if column not in labels_for_mapping:
                logger.warning(f"Column {column} is not mapped in mapping template")
