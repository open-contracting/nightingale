def transform_data(input_data, mapping_config, flattened_schema, result=None, force_publish=False):
    # Helper function to set values in nested dictionary
    def set_nested_value(nested_dict, keys, value, schema, add_new=False):
        for i, key in enumerate(keys[:-1]):
            if isinstance(nested_dict, list):
                if not nested_dict:
                    nested_dict.append({})
                nested_dict = nested_dict[-1]
            if key not in nested_dict:
                if schema.get("/" + "/".join(keys[: i + 1]), {}).get("type") == "array":
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
        return flattened_schema.get(path, {}).get("type") == "array"

    if not result:
        result = {}
    array_counters = {}

    # Transform the input data
    for flat_col, value in input_data.items():
        # XXX: this should be an array
        # path = mapping_config.get(flat_col)
        if not value:
            continue
        paths = mapping_config.get_paths_for_mapping(flat_col, force_publish=force_publish)
        if not paths:
            continue
        for path in paths:
            keys = path.strip("/").split("/")
            if any(is_array_path("/" + "/".join(keys[: i + 1])) for i in range(len(keys))):
                # in array
                array_path = "/" + "/".join(keys[:-1])
                array_id_key = keys[-1]
                array_id_value = value
                if array_path in array_counters:
                    # count new array only if new id comes
                    # else count work with current array path
                    # we can do this because we are working with ocds and we know that each array has id
                    if array_id_key == "id":
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
