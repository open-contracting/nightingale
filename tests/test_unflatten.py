import pytest

from nightingale.unflatten import transform_data


class MockMappingConfig:
    def __init__(self, config):
        self.config = config

    def get_paths_for_mapping(self, flat_col, force_publish=False):
        if flat_col in self.config:
            return [self.config[flat_col]]
        return []


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
            },
            MockMappingConfig(
                {
                    "flat_col1": "/object/field1",
                    "flat_col_s": "/object/field2/string_field",
                    "flat_col2": "/object/field2/array_field/id",
                    "flat_col3": "/object/field2/array_field/array_field2/id",
                    "flat_col4": "/object/field2/array_field/array_field2/id",
                    "flat_col5": "/object/field2/array_field/id",
                    "flat_col6": "/object/field2/array_field/array_field2/id",
                }
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
                }
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
                }
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
            MockMappingConfig({"flat_col1": "/obj/attr", "flat_col2": "/obj/arr/id"}),
            {"/obj/attr": {"type": "string"}, "/obj/arr": {"type": "array"}, "/obj/arr/id": {"type": "string"}},
            None,
            {"obj": {"attr": "x", "arr": [{"id": "y"}]}},
        ),
        (
            {"flat_col1": "m", "flat_col2": "n", "flat_col3": "o"},
            MockMappingConfig(
                {"flat_col1": "/level1/level2/field", "flat_col2": "/level1/array/id", "flat_col3": "/level1/array/id"}
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
            MockMappingConfig({"col1": "/obj1/obj2/attr", "col2": "/obj1/obj2/arr/id"}),
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
                {"flat_col1": "/obj/field1", "flat_col2": "/obj/field2/id", "flat_col3": "/obj/field2/id"}
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
                {"flat_col1": "/obj1/field1", "flat_col2": "/obj1/array/id", "flat_col3": "/obj1/array/field"}
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
                }
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
            MockMappingConfig({"a": "/root/field1", "b": "/root/field2", "c": "/root/field3"}),
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
                {"a": "/lvl1/field", "b": "/lvl1/array/id", "c": "/lvl1/array/field", "d": "/lvl1/array/id"}
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
            MockMappingConfig({"x": "/path/attr", "y": "/path/array/id"}),
            {"/path/attr": {"type": "string"}, "/path/array": {"type": "array"}, "/path/array/id": {"type": "string"}},
            None,
            {"path": {"attr": "a", "array": [{"id": "b"}]}},
        ),
        (
            {"f1": "val1", "f2": "val2", "f3": "val3"},
            MockMappingConfig({"f1": "/obj1/obj2/attr", "f2": "/obj1/obj2/arr/id", "f3": "/obj1/obj2/arr/id"}),
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
            MockMappingConfig({"col1": "/root/level1/field1", "col2": "/root/level1/array/id"}),
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
                {"col1": "/root/level1/field1", "col2": "/root/level1/array/id", "col3": "/root/level1/array/id"}
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
                }
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
    ],
)
def test_transform_data(input_data, mapping_config, flattened_schema, result, expected_output):
    result = transform_data(input_data, mapping_config, flattened_schema, result)
    assert result == expected_output


def test_transform_data_with_shared_result():
    input_data1 = {"col1": "value1"}
    input_data2 = {"col2": "value2"}

    mapping_config = MockMappingConfig({"col1": "/shared/object/field1", "col2": "/shared/object/field2"})

    flattened_schema = {"/shared/object/field1": {"type": "string"}, "/shared/object/field2": {"type": "string"}}

    result = {}
    result = transform_data(input_data1, mapping_config, flattened_schema, result)
    result = transform_data(input_data2, mapping_config, flattened_schema, result)

    expected_output = {"shared": {"object": {"field1": "value1", "field2": "value2"}}}

    assert result == expected_output


if __name__ == "__main__":
    pytest.main()
