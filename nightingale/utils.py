from datetime import datetime


def produce_package_name(date) -> str:
    return f"release-package-{date}.json"


def remove_dicts_without_id(data):
    if isinstance(data, dict):
        result = {}
        for k, val in data.items():
            if cleaned_v := remove_dicts_without_id(val):
                result[k] = cleaned_v
        return result
    elif isinstance(data, list):
        return [remove_dicts_without_id(item) for item in data if not (isinstance(item, dict) and "id" not in item)]
    else:
        return data


def get_iso_now():
    now = datetime.utcnow()
    return now.isoformat() + "Z"


def is_new_array(array_counters, child_path, array_key, array_value, array_path):
    """
    Check if a new array should be created based on the given parameters.

    :param array_counters: Dictionary keeping track of array counters.
    :type array_counters: dict
    :param child_path: The child path in the schema.
    :type child_path: str
    :param array_key: The key in the array.
    :type array_key: str
    :param array_value: The value associated with the array key.
    :type array_value: str
    :param array_path: The path of the array.
    :type array_path: str
    :return: True if a new array should be created, False otherwise.
    :rtype: bool

    >>> array_counters = {'/object/field2/array_field': '1'}
    >>> child_path = '/id'
    >>> array_key = 'id'
    >>> array_value = '2'
    >>> array_path = '/object/field2/array_field'
    >>> is_new_array(array_counters, child_path, array_key, array_value, array_path)
    True

    >>> array_counters = {'/object/field2/array_field': '1'}
    >>> child_path = '/id'
    >>> array_key = 'id'
    >>> array_value = '1'
    >>> array_path = '/object/field2/array_field'
    >>> is_new_array(array_counters, child_path, array_key, array_value, array_path)
    False

    >>> array_counters = {'/object/field2/array_field': '1'}
    >>> child_path = '/name'
    >>> array_key = 'name'
    >>> array_value = 'example'
    >>> array_path = '/object/field2/array_field'
    >>> is_new_array(array_counters, child_path, array_key, array_value, array_path)
    False
    """
    if array_key == "id" and "/" + array_key == child_path and array_counters[array_path] != array_value:
        return True
    return False


def get_longest_array_path(arrays, path):  # extract for testing
    for array in reversed(sorted(arrays, key=len)):
        if path.startswith(array):
            return array
