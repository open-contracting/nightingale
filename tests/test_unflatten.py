import pytest
from nightingale.unflatten import transform_data


@pytest.mark.parametrize("input_data, mapping_config, flattened_schema, expected_output", [
    (
        [{
            'flat_col1': 'value1',
            'flat_col2': 'value2',
            'flat_col3': 'value3',
            'flat_col4': 'value4',
            'flat_col5': 'value5',
            'flat_col6': 'value6',
            'flat_col_s': 'value7',
        }],
        {
            'flat_col1': '/object/field1',
            'flat_col_s': '/object/field2/string_field',
            'flat_col2': '/object/field2/array_field/id',
            'flat_col3': '/object/field2/array_field/array_field2/id',
            'flat_col4': '/object/field2/array_field/array_field2/id',
            'flat_col5': '/object/field2/array_field/id',
            'flat_col6': '/object/field2/array_field/array_field2/id',
        },
        {
            '/object/field1': 'string',
            '/object/field2': 'object',
            '/object/field2/string_field': 'string',
            '/object/field2/array_field': 'array',
            '/object/field2/array_field/id': 'string',
            '/object/field2/array_field/array_field2': 'array',
            '/object/field2/array_field/array_field2/id': 'string',
        },
        {
            'object': {
                'field1': 'value1',
                'field2': {
                    'string_field': 'value7',
                    'array_field': [
                        {
                            'id': 'value2',
                            'array_field2': [
                                {'id': 'value3'},
                                {'id': 'value4'}
                            ]
                        },
                        {
                            'id': 'value5',
                            'array_field2': [
                                {'id': 'value6'}
                            ]
                        }
                    ]
                }
            }
        }
    ),
    (
        [{'flat_col1': 'a', 'flat_col2': 'b', 'flat_col3': 'c', 'flat_col_s': 'd'}],
        {'flat_col1': '/object1/field1', 'flat_col2': '/object1/field2/id', 'flat_col3': '/object1/field2/id', 'flat_col_s': '/object1/field3'},
        {'/object1/field1': 'string', '/object1/field2': 'array', '/object1/field2/id': 'string', '/object1/field3': 'string'},
        {'object1': {'field1': 'a', 'field2': [{'id': 'b'}, {'id': 'c'}], 'field3': 'd'}}
    ),
    (
        [{'flat_col1': 'x', 'flat_col2': 'y'}],
        {'flat_col1': '/obj/attr', 'flat_col2': '/obj/arr/id'},
        {'/obj/attr': 'string', '/obj/arr': 'array', '/obj/arr/id': 'string'},
        {'obj': {'attr': 'x', 'arr': [{'id': 'y'}]}}
    ),
    (
        [{'flat_col1': 'm', 'flat_col2': 'n', 'flat_col3': 'o'}],
        {'flat_col1': '/level1/level2/field', 'flat_col2': '/level1/array/id', 'flat_col3': '/level1/array/id'},
        {'/level1/level2/field': 'string', '/level1/array': 'array', '/level1/array/id': 'string'},
        {'level1': {'level2': {'field': 'm'}, 'array': [{'id': 'n'}, {'id': 'o'}]}}
    ),
    (
        [{'col1': 'a', 'col2': 'b'}],
        {'col1': '/obj1/obj2/attr', 'col2': '/obj1/obj2/arr/id'},
        {'/obj1/obj2/attr': 'string', '/obj1/obj2/arr': 'array', '/obj1/obj2/arr/id': 'string'},
        {'obj1': {'obj2': {'attr': 'a', 'arr': [{'id': 'b'}]}}}
    ),
    (
        [{'flat_col1': 'value1', 'flat_col2': 'value2', 'flat_col3': 'value3'}],
        {'flat_col1': '/obj/field1', 'flat_col2': '/obj/field2/id', 'flat_col3': '/obj/field2/id'},
        {'/obj/field1': 'string', '/obj/field2': 'array', '/obj/field2/id': 'string'},
        {'obj': {'field1': 'value1', 'field2': [{'id': 'value2'}, {'id': 'value3'}]}}
    ),
    (
        [{'flat_col1': 'x', 'flat_col2': 'y', 'flat_col3': 'z'}],
        {'flat_col1': '/obj1/field1', 'flat_col2': '/obj1/array/id', 'flat_col3': '/obj1/array/field'},
        {'/obj1/field1': 'string', '/obj1/array': 'array', '/obj1/array/id': 'string', '/obj1/array/field': 'string'},
        {'obj1': {'field1': 'x', 'array': [{'id': 'y', 'field': 'z'}]}}
    ),
    (
        [{'col1': '1', 'col2': '2', 'col3': '3', 'col4': '4'}],
        {'col1': '/data/field', 'col2': '/data/array/id', 'col3': '/data/array/id', 'col4': '/data/array/field'},
        {'/data/field': 'string', '/data/array': 'array', '/data/array/id': 'string', '/data/array/field': 'string'},
        {'data': {'field': '1', 'array': [{'id': '2'}, {'id': '3', 'field': '4'}]}}
    ),
    (
        [{'a': 'foo', 'b': 'bar', 'c': 'baz'}],
        {'a': '/root/field1', 'b': '/root/field2', 'c': '/root/field3'},
        {'/root/field1': 'string', '/root/field2': 'string', '/root/field3': 'string'},
        {'root': {'field1': 'foo', 'field2': 'bar', 'field3': 'baz'}}
    ),
    (
        [{'a': '1', 'b': '2', 'c': '3', 'd': '4'}],
        {'a': '/lvl1/field', 'b': '/lvl1/array/id', 'c': '/lvl1/array/field', 'd': '/lvl1/array/id'},
        {'/lvl1/field': 'string', '/lvl1/array': 'array', '/lvl1/array/id': 'string', '/lvl1/array/field': 'string'},
        {'lvl1': {'field': '1', 'array': [{'id': '2', 'field': '3'}, {'id': '4'}]}}
    ),
    (
        [{'x': 'a', 'y': 'b'}],
        {'x': '/path/attr', 'y': '/path/array/id'},
        {'/path/attr': 'string', '/path/array': 'array', '/path/array/id': 'string'},
        {'path': {'attr': 'a', 'array': [{'id': 'b'}]}}
    ),
    (
        [{'f1': 'val1', 'f2': 'val2', 'f3': 'val3'}],
        {'f1': '/obj1/obj2/attr', 'f2': '/obj1/obj2/arr/id', 'f3': '/obj1/obj2/arr/id'},
        {'/obj1/obj2/attr': 'string', '/obj1/obj2/arr': 'array', '/obj1/obj2/arr/id': 'string'},
        {'obj1': {'obj2': {'attr': 'val1', 'arr': [{'id': 'val2'}, {'id': 'val3'}]}}}
    ),
    (
        [{'col1': 'one', 'col2': 'two'}],
        {'col1': '/root/level1/field1', 'col2': '/root/level1/array/id'},
        {'/root/level1/field1': 'string', '/root/level1/array': 'array', '/root/level1/array/id': 'string'},
        {'root': {'level1': {'field1': 'one', 'array': [{'id': 'two'}]}}}
    ),
    (
        [{'col1': 'one', 'col2': 'two', 'col3': 'three'}],
        {'col1': '/root/level1/field1', 'col2': '/root/level1/array/id', 'col3': '/root/level1/array/id'},
        {'/root/level1/field1': 'string', '/root/level1/array': 'array', '/root/level1/array/id': 'string'},
        {'root': {'level1': {'field1': 'one', 'array': [{'id': 'two'}, {'id': 'three'}]}}}
    ),
    (
        [{'col1': '1', 'col2': '2', 'col3': '3', 'col4': '4'}],
        {'col1': '/root/field1', 'col2': '/root/array1/id', 'col3': '/root/array1/field2', 'col4': '/root/array1/id'},
        {'/root/field1': 'string', '/root/array1': 'array', '/root/array1/id': 'string', '/root/array1/field2': 'string'},
        {'root': {'field1': '1', 'array1': [{'id': '2', 'field2': '3'}, {'id': '4'}]}}
    ),
])
def test_transform_data(input_data, mapping_config, flattened_schema, expected_output):
    result = transform_data(input_data, mapping_config, flattened_schema)
    assert result == expected_output

if __name__ == "__main__":
    pytest.main()
