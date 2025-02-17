from pathlib import Path
from unittest import mock

import dict_hash
import pytest

from nightingale.config import Config, Datasource, Mapping, Output, Publishing
from nightingale.mapper import OCDSDataMapper
from nightingale.utils import get_longest_array_path


class MockMappingConfig:
    def __init__(self, config, array_paths=None):
        self.config = config
        self.array_paths = array_paths if array_paths is not None else []

    def get_paths_for_mapping(self, flat_col, force_publish=False):
        if flat_col in self.config:
            return [self.config[flat_col]]
        return []

    def get_ocid_mapping(self):
        return "/ocid"

    def get_containing_array_path(self, path):
        return get_longest_array_path(self.array_paths, path)


@pytest.fixture(autouse=True)
def mock_load_workbook():
    with mock.patch("openpyxl.load_workbook", return_value=mock.MagicMock()):
        yield


@pytest.fixture
def mock_config():
    datasource = Datasource(connection="sqlite:///:memory:")
    mapping = Mapping(name="step1", file=Path("/path/to/file1"), selector="selector1", ocid_prefix="prefix")
    publishing = Publishing(version="1.0", publisher="publisher", base_uri="http://example.com")
    output = Output(directory=Path("/path/to/output"))

    return Config(datasource=datasource, mapping=mapping, publishing=publishing, output=output)


@pytest.mark.parametrize(
    "input_data, mapping_config, flattened_schema, result, expected_output",
    [
        (
            {
                "flat_col1": "value1",
                "flat_col2": "value2",
                "flat_col3": "value3",
                "flat_col4": "value4",
                "flat_col5": "value5",
                "flat_col6": "value6",
                "flat_col_s": "value7",
                "flat_col_not_mapped": "value8",
                "flat_col_no_value": "",
            },
            MockMappingConfig(
                {
                    "flat_col1": "/object/field1",
                    "flat_col2": "/object/field2/array_field/id",
                    "flat_col3": "/object/field2/array_field/array_field2/id",
                    "flat_col4": "/object/field2/array_field/array_field2/id",
                    "flat_col5": "/object/field2/array_field/id",
                    "flat_col6": "/object/field2/array_field/array_field2/id",
                    "flat_col_s": "/object/field2/string_field",
                },
                array_paths=["/object/field2/array_field", "/object/field2/array_field/array_field2"],
            ),
            {
                "/object/field1": {"type": "string"},
                "/object/field2": {"type": "object"},
                "/object/field2/string_field": {"type": "string"},
                "/object/field2/array_field": {"type": "array"},
                "/object/field2/array_field/id": {"type": "string"},
                "/object/field2/array_field/array_field2": {"type": "array"},
                "/object/field2/array_field/array_field2/id": {"type": "string"},
            },
            None,
            {
                "object": {
                    "field1": "value1",
                    "field2": {
                        "string_field": "value7",
                        "array_field": [
                            {"id": "value2", "array_field2": [{"id": "value3"}, {"id": "value4"}]},
                            {"id": "value5", "array_field2": [{"id": "value6"}]},
                        ],
                    },
                },
            },
        ),
        (
            {"flat_col1": "a", "flat_col2": "b", "flat_col3": "c", "flat_col_s": "d"},
            MockMappingConfig(
                {
                    "flat_col1": "/object1/field1",
                    "flat_col2": "/object1/field2/id",
                    "flat_col3": "/object1/field2/id",
                    "flat_col_s": "/object1/field3",
                },
                array_paths=["/object1/field2"],
            ),
            {
                "/object1/field1": {"type": "string"},
                "/object1/field2": {"type": "array"},
                "/object1/field2/id": {"type": "string"},
                "/object1/field3": {"type": "string"},
            },
            None,
            {"object1": {"field1": "a", "field2": [{"id": "b"}, {"id": "c"}], "field3": "d"}},
        ),
        (
            {"flat_col1": "x", "flat_col2": "y"},
            MockMappingConfig({"flat_col1": "/obj/attr", "flat_col2": "/obj/arr/id"}, array_paths=["/obj/arr"]),
            {"/obj/attr": {"type": "string"}, "/obj/arr": {"type": "array"}, "/obj/arr/id": {"type": "string"}},
            None,
            {"obj": {"attr": "x", "arr": [{"id": "y"}]}},
        ),
        (
            {"flat_col1": "m", "flat_col2": "n", "flat_col3": "o"},
            MockMappingConfig(
                {
                    "flat_col1": "/level1/level2/field",
                    "flat_col2": "/level1/array/id",
                    "flat_col3": "/level1/array/id",
                },
                array_paths=["/level1/array"],
            ),
            {
                "/level1/level2/field": {"type": "string"},
                "/level1/array": {"type": "array"},
                "/level1/array/id": {"type": "string"},
            },
            None,
            {"level1": {"level2": {"field": "m"}, "array": [{"id": "n"}, {"id": "o"}]}},
        ),
        (
            {"col1": "a", "col2": "b"},
            MockMappingConfig(
                {"col1": "/obj1/obj2/attr", "col2": "/obj1/obj2/arr/id"}, array_paths=["/obj1/obj2/arr"]
            ),
            {
                "/obj1/obj2/attr": {"type": "string"},
                "/obj1/obj2/arr": {"type": "array"},
                "/obj1/obj2/arr/id": {"type": "string"},
            },
            None,
            {"obj1": {"obj2": {"attr": "a", "arr": [{"id": "b"}]}}},
        ),
        (
            {"flat_col1": "value1", "flat_col2": "value2", "flat_col3": "value3"},
            MockMappingConfig(
                {"flat_col1": "/obj/field1", "flat_col2": "/obj/field2/id", "flat_col3": "/obj/field2/id"},
                array_paths=["/obj/field2"],
            ),
            {
                "/obj/field1": {"type": "string"},
                "/obj/field2": {"type": "array"},
                "/obj/field2/id": {"type": "string"},
            },
            None,
            {"obj": {"field1": "value1", "field2": [{"id": "value2"}, {"id": "value3"}]}},
        ),
        (
            {"flat_col1": "x", "flat_col2": "y", "flat_col3": "z"},
            MockMappingConfig(
                {"flat_col1": "/obj1/field1", "flat_col2": "/obj1/array/id", "flat_col3": "/obj1/array/field"},
                array_paths=["/obj1/array"],
            ),
            {
                "/obj1/field1": {"type": "string"},
                "/obj1/array": {"type": "array"},
                "/obj1/array/id": {"type": "string"},
                "/obj1/array/field": {"type": "string"},
            },
            None,
            {"obj1": {"field1": "x", "array": [{"id": "y", "field": "z"}]}},
        ),
        (
            {"col1": "1", "col2": "2", "col3": "3", "col4": "4"},
            MockMappingConfig(
                {
                    "col1": "/data/field",
                    "col2": "/data/array/id",
                    "col3": "/data/array/id",
                    "col4": "/data/array/field",
                },
                array_paths=["/data/array"],
            ),
            {
                "/data/field": {"type": "string"},
                "/data/array": {"type": "array"},
                "/data/array/id": {"type": "string"},
                "/data/array/field": {"type": "string"},
            },
            None,
            {"data": {"field": "1", "array": [{"id": "2"}, {"id": "3", "field": "4"}]}},
        ),
        (
            {"a": "foo", "b": "bar", "c": "baz"},
            MockMappingConfig(
                {"a": "/root/field1", "b": "/root/field2", "c": "/root/field3"},
            ),
            {
                "/root/field1": {"type": "string"},
                "/root/field2": {"type": "string"},
                "/root/field3": {"type": "string"},
            },
            None,
            {"root": {"field1": "foo", "field2": "bar", "field3": "baz"}},
        ),
        (
            {"a": "1", "b": "2", "c": "3", "d": "4"},
            MockMappingConfig(
                {"a": "/lvl1/field", "b": "/lvl1/array/id", "c": "/lvl1/array/field", "d": "/lvl1/array/id"},
                array_paths=["/lvl1/array"],
            ),
            {
                "/lvl1/field": {"type": "string"},
                "/lvl1/array": {"type": "array"},
                "/lvl1/array/id": {"type": "string"},
                "/lvl1/array/field": {"type": "string"},
            },
            None,
            {"lvl1": {"field": "1", "array": [{"id": "2", "field": "3"}, {"id": "4"}]}},
        ),
        (
            {"x": "a", "y": "b"},
            MockMappingConfig({"x": "/path/attr", "y": "/path/array/id"}, array_paths=["/path/array"]),
            {"/path/attr": {"type": "string"}, "/path/array": {"type": "array"}, "/path/array/id": {"type": "string"}},
            None,
            {"path": {"attr": "a", "array": [{"id": "b"}]}},
        ),
        (
            {"f1": "val1", "f2": "val2", "f3": "val3"},
            MockMappingConfig(
                {"f1": "/obj1/obj2/attr", "f2": "/obj1/obj2/arr/id", "f3": "/obj1/obj2/arr/id"},
                array_paths=["/obj1/obj2/arr"],
            ),
            {
                "/obj1/obj2/attr": {"type": "string"},
                "/obj1/obj2/arr": {"type": "array"},
                "/obj1/obj2/arr/id": {"type": "string"},
            },
            None,
            {"obj1": {"obj2": {"attr": "val1", "arr": [{"id": "val2"}, {"id": "val3"}]}}},
        ),
        (
            {"col1": "one", "col2": "two"},
            MockMappingConfig(
                {"col1": "/root/level1/field1", "col2": "/root/level1/array/id"}, array_paths=["/root/level1/array"]
            ),
            {
                "/root/level1/field1": {"type": "string"},
                "/root/level1/array": {"type": "array"},
                "/root/level1/array/id": {"type": "string"},
            },
            None,
            {"root": {"level1": {"field1": "one", "array": [{"id": "two"}]}}},
        ),
        (
            {"col1": "one", "col2": "two", "col3": "three"},
            MockMappingConfig(
                {"col1": "/root/level1/field1", "col2": "/root/level1/array/id", "col3": "/root/level1/array/id"},
                array_paths=["/root/level1/array"],
            ),
            {
                "/root/level1/field1": {"type": "string"},
                "/root/level1/array": {"type": "array"},
                "/root/level1/array/id": {"type": "string"},
            },
            None,
            {"root": {"level1": {"field1": "one", "array": [{"id": "two"}, {"id": "three"}]}}},
        ),
        (
            {"col1": "1", "col2": "2", "col3": "3", "col4": "4"},
            MockMappingConfig(
                {
                    "col1": "/root/field1",
                    "col2": "/root/array1/id",
                    "col3": "/root/array1/field2",
                    "col4": "/root/array1/id",
                },
                array_paths=["/root/array1"],
            ),
            {
                "/root/field1": {"type": "string"},
                "/root/array1": {"type": "array"},
                "/root/array1/id": {"type": "string"},
                "/root/array1/field2": {"type": "string"},
            },
            None,
            {"root": {"field1": "1", "array1": [{"id": "2", "field2": "3"}, {"id": "4"}]}},
        ),
        (
            {
                "col1": "val1",
                "col2": "val2",
                "col3": "val3",
                "col4": "val4",
                "col5": "val5",
                "col6": "val6",
                "col7": "val7",
            },
            MockMappingConfig(
                {
                    "col1": "/object/lvl1/field",
                    "col2": "/object/lvl1/array1/field1",
                    "col3": "/object/lvl1/array1/field2",
                    "col4": "/object/lvl1/array1/id",
                    "col5": "/object/lvl1/lvl2/field",
                    "col6": "/object/lvl1/lvl2/array2/field1",
                    "col7": "/object/lvl1/lvl2/array2/id",
                },
                array_paths=["/lvl1/array"],
            ),
            {
                "/object/lvl1/field": {"type": "string"},
                "/object/lvl1/array1": {"type": "array"},
                "/object/lvl1/array1/field1": {"type": "string"},
                "/object/lvl1/array1/field2": {"type": "string"},
                "/object/lvl1/array1/id": {"type": "string"},
                "/object/lvl1/lvl2/field": {"type": "string"},
                "/object/lvl1/lvl2/array2": {"type": "array"},
                "/object/lvl1/lvl2/array2/field1": {"type": "string"},
                "/object/lvl1/lvl2/array2/id": {"type": "string"},
            },
            None,
            {
                "object": {
                    "lvl1": {
                        "field": "val1",
                        "array1": [
                            {"id": "val4", "field1": "val2", "field2": "val3"},
                        ],
                        "lvl2": {"field": "val5", "array2": [{"id": "val7", "field1": "val6"}]},
                    }
                }
            },
        ),
    ],
)
def test_transform_data(input_data, mapping_config, flattened_schema, result, expected_output, mock_config):
    mapper = OCDSDataMapper(mock_config)
    result = mapper.transform_row(input_data, mapping_config, flattened_schema, result)
    assert result == expected_output


def test_transform_data_with_shared_result(mock_config):
    input_data1 = {"col1": "value1"}
    input_data2 = {"col2": "value2"}

    mapping_config = MockMappingConfig({"col1": "/shared/object/field1", "col2": "/shared/object/field2"})

    flattened_schema = {"/shared/object/field1": {"type": "string"}, "/shared/object/field2": {"type": "string"}}

    mapper = OCDSDataMapper(mock_config)

    result = {}
    result = mapper.transform_row(input_data1, mapping_config, flattened_schema, result)
    result = mapper.transform_row(input_data2, mapping_config, flattened_schema, result)

    expected_output = {"shared": {"object": {"field1": "value1", "field2": "value2"}}}

    assert result == expected_output


@pytest.mark.parametrize(
    "input_data, expected_ocid",
    [
        ("12345", "prefix-12345"),
        ("abcde", "prefix-abcde"),
        ("67890", "prefix-67890"),
    ],
)
def test_produce_ocid(mock_config, input_data, expected_ocid):
    mapper = OCDSDataMapper(mock_config)
    assert mapper.produce_ocid(input_data) == expected_ocid


def test_date_release(mock_config):
    mapper = OCDSDataMapper(mock_config)
    curr_row = {}
    mapper.date_release(curr_row)
    assert "date" in curr_row


def test_tag_initiation_type(mock_config):
    mapper = OCDSDataMapper(mock_config)
    curr_row = {"tender": {}}
    mapper.tag_initiation_type(curr_row)
    assert curr_row["initiationType"] == "tender"


def test_tag_ocid(mock_config):
    mapper = OCDSDataMapper(mock_config)
    curr_row = {}
    mapper.tag_ocid(curr_row, "12345")
    assert curr_row["ocid"] == "prefix-12345"


def test_map(mock_config, mocker):
    loader = mocker.Mock()
    loader.load.return_value = [{"ocid": "1", "field": "value"}]

    # Mock the MappingTemplate class to avoid reading from an actual file
    with mock.patch("nightingale.mapper.MappingTemplate") as MockMapping:
        MockMapping.return_value = MockMappingConfig({})
        mapper = OCDSDataMapper(mock_config)
        result = mapper.map(loader)

    assert result == []
    loader.load.assert_called_once_with("selector1")


@pytest.mark.parametrize(
    "input_data, output_data",
    [
        (
            {
                "id": "1",
                "tender": {
                    "items": [
                        {"id": "item1", "description": "Item 1"},
                        {"description": "Item without ID"},
                    ],
                    "documents": [
                        {"id": "doc1", "title": "Document 1"},
                        {"title": "Document without ID"},
                    ],
                },
            },
            {
                "id": "1",
                "tender": {
                    "items": [
                        {"id": "item1", "description": "Item 1"},
                    ],
                    "documents": [
                        {"id": "doc1", "title": "Document 1"},
                    ],
                },
            },
        ),
        (
            {
                "id": "2",
                "tender": {
                    "items": [
                        {"description": "Item without ID"},
                    ],
                    "documents": [],
                },
            },
            {
                "id": "2",
            },
        ),
    ],
)
def test_remove_empty_id_arrays(input_data, output_data, mock_config):
    mapper = OCDSDataMapper(mock_config)
    data = mapper.remove_empty_id_arrays(input_data)
    assert data == output_data


def test_map_with_validation(mock_config):
    loader = mock.MagicMock()
    loader.load.return_value = [{"ocid": "1", "field": "value"}]

    with mock.patch("nightingale.mapper.MappingTemplateValidator") as MockValidator:
        mock_validator_instance = MockValidator.return_value
        mock_validator_instance.validate_data_elements = mock.MagicMock()
        mock_validator_instance.validate_selector = mock.MagicMock()
        with mock.patch("nightingale.mapper.MappingTemplate") as mock_mapping_template:
            mapper = OCDSDataMapper(mock_config)
            mapper.map(loader, validate_mapping=True)
            MockValidator.assert_called_with(loader, mock_mapping_template())
            mock_validator_instance.validate_data_elements.assert_called_once()
            mock_validator_instance.validate_selector.assert_called_once_with(loader.load.return_value[0])


def generate_hash(data):
    return dict_hash.sha256(data)


@mock.patch("nightingale.mapper.get_iso_now")
def test_finish_release(mock_get_iso_now, mock_config):
    mock_get_iso_now.return_value = "2022-01-01T00:00:00Z"

    mapper = OCDSDataMapper(mock_config)

    curr_release = {
        "field": "value1",
        "tender": {"id": 1},
    }
    curr_ocid = "1"
    mapped = []

    mapper.finish_release(curr_ocid, curr_release, mapped)

    expected_release = {
        "field": "value1",
        "initiationType": "tender",
        "date": "2022-01-01T00:00:00Z",
        "ocid": "prefix-1",
        "tag": ["tender"],
        "tender": {"id": 1},
        "id": generate_hash(
            {
                "field": "value1",
                "initiationType": "tender",
                "date": "2022-01-01T00:00:00Z",
                "ocid": "prefix-1",
                "tag": ["tender"],
                "tender": {"id": 1},
            }
        ),
    }

    assert len(mapped) == 1
    assert mapped[0] == expected_release


@mock.patch("nightingale.mapper.get_iso_now")
@mock.patch("nightingale.mapper.MappingTemplate")
def test_finish_release_with_tags(mock_mapping, mock_get_iso_now, mock_config):
    mock_get_iso_now.return_value = "2022-01-01T00:00:00Z"
    mock_mapping.get_ocid_mapping.return_value = "ocid"
    mapper = OCDSDataMapper(mock_config)
    mapper.transform_row = mock.Mock(
        side_effect=[
            {"ocid": "1", "field": "value1", "tender": {"id": "tender"}},
            {
                "ocid": "2",
                "field": "value2",
                "tender": {"id": "tender"},
                "awards": [{"id": "award"}],
                "contracts": [{"id": "contract"}],
            },
        ]
    )

    result = mapper.transform_data([{"ocid": 1}, {"ocid": 2}], mock_mapping)

    expected_result = [
        {
            "field": "value1",
            "tender": {"id": "tender"},
            "initiationType": "tender",
            "date": "2022-01-01T00:00:00Z",
            "ocid": "prefix-1",
            "tag": ["tender"],
            "id": generate_hash(
                {
                    "field": "value1",
                    "tender": {"id": "tender"},
                    "initiationType": "tender",
                    "date": "2022-01-01T00:00:00Z",
                    "ocid": "prefix-1",
                    "tag": ["tender"],
                }
            ),
        },
        {
            "field": "value2",
            "tender": {"id": "tender"},
            "awards": [{"id": "award"}],
            "contracts": [{"id": "contract"}],
            "initiationType": "tender",
            "date": "2022-01-01T00:00:00Z",
            "ocid": "prefix-2",
            "tag": ["tender", "award", "contract"],
            "id": generate_hash(
                {
                    "field": "value2",
                    "tender": {"id": "tender"},
                    "awards": [{"id": "award"}],
                    "contracts": [{"id": "contract"}],
                    "initiationType": "tender",
                    "date": "2022-01-01T00:00:00Z",
                    "ocid": "prefix-2",
                    "tag": ["tender", "award", "contract"],
                }
            ),
        },
    ]

    assert result == expected_result


if __name__ == "__main__":
    pytest.main()
