import logging
from datetime import datetime
from typing import Any

import dict_hash

from .config import Config
from .mapping.v1 import Mapping

logger = logging.getLogger(__name__)


class OCDSDataMapper:
    def __init__(self, config: Config):
        self.config = config

    def produce_ocid(self, value):
        prefix = self.config.mapping.ocid_prefix
        return f"{prefix}-{value}"

    def map(self, loader):
        config = self.config.mapping
        mapping = Mapping(config)
        data = loader.load(config.selector)
        logger.info("Start mapping data")
        return self.transform_data(data, mapping)

    def transform_data(self, data: list[dict[Any, Any]], mapping: Mapping) -> list[dict[str, Any]]:
        curr_ocid = ""
        curr_release = {}
        mapped = []

        ocid_mapping = mapping.get_ocid_mapping()
        for row in data:
            ocid = row.pop(ocid_mapping, "")
            if not ocid:
                logger.warning(f"No OCID found in row: {row}. Skipping.")
                continue

            curr_release = self.transform_row(row, mapping, mapping.get_scheme(), curr_release)
            if curr_ocid != ocid:
                if not curr_ocid:
                    curr_ocid = ocid
                    continue
                curr_ocid = ocid
                self.tag_initiation_type(curr_release)
                self.date_release(curr_release)
                self.tag_ocid(curr_release, curr_ocid)
                self.make_release_id(curr_release)
                logger.info(f"Release mapped: {curr_release['ocid']}")
                mapped.append(curr_release)
                curr_release = {}
                continue
        return mapped

    def transform_row(self, input_data, mapping_config, flattened_schema, result=None):
        def set_nested_value(nested_dict, keys, value, schema, add_new=False):
            for i, key in enumerate(keys[:-1]):
                if isinstance(nested_dict, list):
                    if not nested_dict:
                        nested_dict.append({})
                    nested_dict = nested_dict[-1]
                if key not in nested_dict:
                    nested_dict[key] = (
                        [] if schema.get("/" + "/".join(keys[: i + 1]), {}).get("type") == "array" else {}
                    )
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

        def is_array_path(path):
            return flattened_schema.get(path, {}).get("type") == "array"

        if not result:
            result = {}
        array_counters = {}

        for flat_col, value in input_data.items():
            if not value:
                continue
            paths = mapping_config.get_paths_for_mapping(flat_col, force_publish=self.config.mapping.force_publish)
            if not paths:
                continue
            for path in paths:
                keys = path.strip("/").split("/")
                if any(is_array_path("/" + "/".join(keys[: i + 1])) for i in range(len(keys))):
                    array_path = "/" + "/".join(keys[:-1])
                    array_id_key = keys[-1]
                    array_id_value = value
                    if array_path in array_counters:
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

    def make_release_id(self, curr_row):
        id_ = dict_hash.sha256(curr_row)
        curr_row["id"] = id_

    def date_release(self, curr_row):
        curr_row["date"] = datetime.now().isoformat()

    def tag_initiation_type(self, curr_row):
        if "tender" in curr_row and "initiationType" not in curr_row:
            curr_row["initiationType"] = "tender"

    def tag_ocid(self, curr_row, curr_ocid):
        curr_row["ocid"] = self.produce_ocid(curr_ocid)
