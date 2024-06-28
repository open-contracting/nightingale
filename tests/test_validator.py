import unittest
from unittest.mock import Mock, patch

from nightingale.mapping.v1.validator import MappingTemplateValidator


class TestMappingTemplateValidator(unittest.TestCase):
    def setUp(self):
        # Mock the loader and mapping_template
        self.loader = Mock()
        self.mapping_template = Mock()

        # Create instance of the class
        self.validator = MappingTemplateValidator(self.loader, self.mapping_template)

    @patch("nightingale.mapping.v1.validator.logger")
    def test_validate_data_elements_all_columns_described(self, mock_logger):
        # Mock the cursor and database table structure
        cursor = Mock()
        self.loader.get_cursor.return_value = cursor
        cursor.execute.side_effect = [cursor, cursor]  # First execute call  # Second execute call
        cursor.fetchall.side_effect = [
            [("table1",)],  # First fetchall call
            [
                (0, "id", "INTEGER", 0, None, 0),
                (1, "name", "TEXT", 0, None, 0),
                # (2, 'missing', 'TEXT', 0, None, 0)
            ],  # Second fetchall call
        ]

        # Mock the mapping template data elements
        self.mapping_template.get_data_elements.return_value = {
            "id": {
                "for_mapping": "mapped_id",
                "data_source": "source1",
                "data_element": "id",
                "table": "table1",
                "publish": True,
                "example": "example1",
                "description": "description1",
                "data_type": "type1",
            },
            "name": {
                "for_mapping": "mapped_name",
                "data_source": "table1",
                "data_element": "name",
                "table": "table1",
                "publish": False,
                "example": "example2",
                "description": "description2",
                "data_type": "type2",
            },
        }

        # Call the method
        self.validator.validate_data_elements()

        # Check if cursor was used correctly
        cursor.execute.assert_any_call("SELECT name FROM sqlite_master WHERE type='table';")
        cursor.execute.assert_any_call("PRAGMA table_info([table1])")

        # Ensure no warnings were logged
        mock_logger.warning.assert_not_called()

    @patch("nightingale.mapping.v1.validator.logger")
    def test_validate_data_elements_missing_columns(self, mock_logger):
        # Mock the cursor and database table structure
        cursor = Mock()
        self.loader.get_cursor.return_value = cursor
        cursor.execute.side_effect = [cursor, cursor]  # First execute call  # Second execute call
        cursor.fetchall.side_effect = [
            [("table1",)],  # First fetchall call
            [
                (0, "id", "INTEGER", 0, None, 0),
                (1, "name", "TEXT", 0, None, 0),
                (2, "missing", "TEXT", 0, None, 0),
            ],  # Second fetchall call
        ]

        # Mock the mapping template data elements
        self.mapping_template.get_data_elements.return_value = {
            "id": {
                "for_mapping": "mapped_id",
                "data_source": "source1",
                "data_element": "id",
                "table": "table1",
                "publish": True,
                "example": "example1",
                "description": "description1",
                "data_type": "type1",
            },
            "name": {
                "for_mapping": "mapped_name",
                "data_source": "table1",
                "data_element": "name",
                "table": "table1",
                "publish": False,
                "example": "example2",
                "description": "description2",
                "data_type": "type2",
            },
        }

        # Call the method
        self.validator.validate_data_elements()

        # Check if a warning was logged for the missing column
        mock_logger.warning.assert_called_with("Column table1/missing is not described in data elements")

    @patch("nightingale.mapping.v1.validator.logger")
    def test_validate_selector_all_columns_mapped(self, mock_logger):
        # Mock the mapping template data elements
        self.mapping_template.get_data_elements.return_value = {
            "id": {
                "for_mapping": "mapped_id",
                "data_source": "source1",
                "data_element": "id",
                "table": "table1",
                "publish": True,
                "example": "example1",
                "description": "description1",
                "data_type": "type1",
            },
            "name": {
                "for_mapping": "mapped_name",
                "data_source": "table1",
                "data_element": "name",
                "table": "table1",
                "publish": False,
                "example": "example2",
                "description": "description2",
                "data_type": "type2",
            },
        }

        # Create a row with mapped columns, propper name is a responsibilty of sql selector
        row = {"mapped_id": 1, "mapped_name": "Test"}

        # Call the method
        self.validator.validate_selector(row)

        # Ensure no warnings were logged
        mock_logger.warning.assert_not_called()

    @patch("nightingale.mapping.v1.validator.logger")
    def test_validate_selector_missing_columns(self, mock_logger):
        # Mock the mapping template data elements
        self.mapping_template.get_data_elements.return_value = {
            "id": {
                "for_mapping": "mapped_id",
                "data_source": "source1",
                "data_element": "id",
                "table": "table1",
                "publish": True,
                "example": "example1",
                "description": "description1",
                "data_type": "type1",
            },
            "name": {
                "for_mapping": "mapped_name",
                "data_source": "table1",
                "data_element": "name",
                "table": "table1",
                "publish": False,
                "example": "example2",
                "description": "description2",
                "data_type": "type2",
            },
        }

        # Create a row with mapped columns, propper name is a responsibilty of sql selector
        row = {"mapped_id": 1, "mapped_name": "Test", "mapped_value": "value"}

        # Call the method
        self.validator.validate_selector(row)

        # Check if a warning was logged for the unmapped column
        mock_logger.warning.assert_called_with("Column mapped_value is not mapped in mapping template")


if __name__ == "__main__":
    unittest.main()
