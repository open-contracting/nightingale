from datetime import datetime

from pytz import timezone


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
        all_items = []
        for item in data:
            if not (isinstance(item, dict) and "id" not in item):
                all_items.append(remove_dicts_without_id(item))
            elif isinstance(item, dict) and item.get("verificationMethod"):
                all_items.append(remove_dicts_without_id(item))
        return all_items
    else:
        return data


def get_iso_now():
    pacific = timezone("America/Los_Angeles")
    now = datetime.now(pacific)
    return now.strftime("%Y-%m-%dT%H:%M:%S%z")[:-2] + ":" + now.strftime("%Y-%m-%dT%H:%M:%S%z")[-2:]


def is_new_array(array_counters: dict, child_path: str, array_key: str, array_value: str, array_path: str) -> bool:
    """
    Check if a new array should be created based on the given parameters.

    :param array_counters: Dictionary keeping track of array counters.
    :param child_path: The child path in the schema.
    :param array_key: The key in the array.
    :param array_value: The value associated with the array key.
    :param array_path: The path of the array.
    :return: True if a new array should be created, False otherwise.

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
