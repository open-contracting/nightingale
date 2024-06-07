def transform_data(input_data, mapping_config, flattened_schema):
    # Helper function to set values in nested dictionary
    def set_nested_value(nested_dict, keys, value, schema, add_new=False):
        full_value = nested_dict
        for i, key in enumerate(keys[:-1]):
            if isinstance(nested_dict, list):
                if not nested_dict:
                    nested_dict.append({})
                nested_dict = nested_dict[-1]
            if key not in nested_dict:
                if schema.get('/' + '/'.join(keys[:i + 1])) == 'array':
                    nested_dict[key] = []
                else:
                    nested_dict[key] = {}

            nested_dict = nested_dict[key]
        last_key = keys[-1]
        if isinstance(nested_dict, list):
            if add_new:
                if last_key not in nested_dict[-1]:
                    nested_dict[-1][last_key] = []
                nested_dict[-1][last_key].append(value)
            else:
                nested_dict[-1][last_key] = value
        else:
            if last_key in nested_dict:
                if isinstance(nested_dict[last_key], list) and add_new:
                    nested_dict[last_key].append(value)
                elif isinstance(nested_dict[last_key], dict):
                    nested_dict[last_key].update(value)
                else:
                    nested_dict[last_key] = value
            else:
                nested_dict[last_key] = value

    # Helper function to determine if a path corresponds to an array in the schema
    def is_array_path(path):
        return flattened_schema.get(path) == 'array'

    result = {}
    array_counters = {}

    # Transform the input data
    for data in input_data:
        for flat_col, value in data.items():
            # XXX: this should be an array
            path = mapping_config.get(flat_col)
            if path:
                keys = path.strip('/').split('/')
                if any(is_array_path('/' + '/'.join(keys[:i + 1])) for i in range(len(keys))):
                    # in array
                    array_path = '/' + '/'.join(keys[:-1])
                    array_id_key = keys[-1]
                    array_id_value = value
                    if array_path in array_counters:
                        # count new array only if new id comes
                        # else count work with current array path
                        # we can do this because we are working with ocds and we know that each array has id
                        if array_id_key == 'id':
                            if array_counters[array_path] != array_id_value:
                                array_counters[array_path] = array_id_value
                                set_nested_value(result, keys[:-1], {}, flattened_schema, add_new=True)
                    else:
                        array_counters[array_path] = array_id_value
                        set_nested_value(result, keys[:-1], [{}], flattened_schema)

                    current = result
                    for key in keys[:-1]:
                        if isinstance(current, list):
                            current = current[-1]
                        current = current[key]
                    if isinstance(current, list):
                        current[-1][array_id_key] = value
                    else:
                        current[array_id_key] = value
                else:
                    set_nested_value(result, keys, value, flattened_schema)
    return result


if __name__ == '__main__':
    # Test the function
    input_data = [{
        'flat_col1': 'value1',
        'flat_col2': 'value2',
        'flat_col3': 'value3',
        'flat_col4': 'value4',
        'flat_col5': 'value5',
        'flat_col6': 'value6',
        'flat_col_s': 'value7',
    }]

    mapping_config = {
        'flat_col1': '/object/field1',
        'flat_col_s': '/object/field2/string_field',
        'flat_col2': '/object/field2/array_field/id',
        'flat_col3': '/object/field2/array_field/array_field2/id',
        'flat_col4': '/object/field2/array_field/array_field2/id',
        'flat_col5': '/object/field2/array_field/id',
        'flat_col6': '/object/field2/array_field/array_field2/id',
    }

    flattened_schema = {
        '/object/field1': 'string',
        '/object/field2': 'object',
        '/object/field2/string_field': 'string',
        '/object/field2/array_field': 'array',
        '/object/field2/array_field/id': 'string',
        '/object/field2/array_field/array_field2': 'array',
        '/object/field2/array_field/array_field2/id': 'string',
    }
    #
    transformed_data = transform_data(input_data, mapping_config, flattened_schema)
    print(transformed_data)
