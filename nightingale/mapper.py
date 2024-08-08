import logging
from typing import Any, Optional

import dict_hash

from .codelists import CodelistsMapping
from .config import Config
from .mapping_template.v09 import MappingTemplate, MappingTemplateValidator
from .utils import get_iso_now, is_new_array, remove_dicts_without_id

logger = logging.getLogger(__name__)


class OCDSDataMapper:
    """
    Maps data from a source to the OCDS format.

    :param config: Configuration object containing settings for the mapper.
    :type config: Config
    """

    def __init__(self, config: Config):
        """
        Initialize the OCDSDataMapper.

        :param config: Configuration object containing settings for the mapper.
        :type config: Config
        """
        self.config = config

    def produce_ocid(self, value: str) -> str:
        """
        Produce an OCID based on the given value.

        :param value: The value to use for generating the OCID.
        :type value: str
        :return: The produced OCID.
        :rtype: str
        """
        prefix = self.config.mapping.ocid_prefix
        return f"{prefix}-{value}"

    def map(self, loader: Any, validate_mapping: bool = False) -> list[dict[str, Any]]:
        """
        Map data from the loader to the OCDS format.

        :param loader: Data loader object.
        :type loader: Any
        :return: List of mapped release dictionaries.
        :rtype: list[dict[str, Any]]
        """
        config = self.config.mapping
        mapping = MappingTemplate(config)
        codelists = None
        if config.codelists:
            codelists = CodelistsMapping(config)
        logger.info("MappingTemplate data loaded")
        data = loader.load(config.selector)
        logger.info("Source data is loaded...")
        if validate_mapping:
            logger.info("Validating mapping template...")
            validator = MappingTemplateValidator(loader, mapping)
            validator.validate_data_elements()
            validator.validate_selector(data[0])
        logger.info("Start mapping data")
        return self.transform_data(data, mapping, codelists=codelists)

    def transform_data(
        self, data: list[dict[Any, Any]], mapping: MappingTemplate, codelists: Optional[CodelistsMapping] = None
    ) -> list[dict[str, Any]]:
        """
        Transform the input data to the OCDS format.

        :param data: List of input data dictionaries.
        :type data: list[dict[Any, Any]]
        :param mapping: Mapping configuration object.
        :type mapping: MappingTemplate
        :return: List of transformed release dictionaries.
        :rtype: list[dict[str, Any]]
        """
        curr_ocid = ""
        curr_release = {}
        mapped = []

        ocid_mapping = mapping.get_ocid_mapping()
        for row in data:
            ocid = row.get(ocid_mapping, "")
            if not ocid:
                logger.warning(f"No OCID found in row: {row}. Skipping.")
                continue
            if not curr_ocid:
                curr_ocid = ocid
            if curr_ocid != ocid:
                self.finish_release(curr_ocid, curr_release, mapped)
                curr_ocid = ocid
                curr_release = {}

            curr_release = self.transform_row(row, mapping, mapping.get_schema(), curr_release, codelists=codelists)

        if curr_release:
            self.finish_release(curr_ocid, curr_release, mapped)
        return mapped

    def finish_release(self, curr_ocid, curr_release, mapped):
        curr_release = self.remove_empty_id_arrays(curr_release)
        self.tag_initiation_type(curr_release)
        self.date_release(curr_release)
        self.tag_ocid(curr_release, curr_ocid)
        self.tag_tags(curr_release)
        self.make_release_id(curr_release)
        logger.info(f"Release mapped: {curr_release['ocid']}")
        mapped.append(curr_release)

    def transform_row(
        self,
        input_data: dict[Any, Any],
        mapping_config: MappingTemplate,
        flattened_schema: dict[str, Any],
        result: dict = None,
        codelists: Optional[CodelistsMapping] = None,
    ) -> dict:
        """
        Transform a single row of input data to the OCDS format.

        :param input_data: Dictionary of input data.
        :type input_data: dict[Any, Any]
        :param mapping_config: Mapping configuration object.
        :type mapping_config: MappingTemplate
        :param flattened_schema: Flattened schema dictionary.
        :type flattened_schema: dict[str, Any]
        :param result: Existing result dictionary to update.
        :type result: dict, optional
        :return: Transformed row dictionary.
        :rtype: dict
        """

        def set_nested_value(nested_dict, keys, value, schema, add_new=False):
            value = self.map_codelist_value(keys, schema, codelists, value)
            for i, key in enumerate(keys[:-1]):
                if isinstance(nested_dict, list):
                    if not nested_dict:
                        nested_dict.append({})
                    nested_dict = nested_dict[-1]
                if key not in nested_dict:
                    subpath = "/" + "/".join(keys[: i + 1])
                    nested_dict[key] = [] if schema.get(subpath, {}).get("type") == "array" else {}
                nested_dict = nested_dict[key]

            last_key = keys[-1]
            if isinstance(nested_dict, list):
                if add_new:
                    if last_key not in nested_dict[-1]:
                        nested_dict[-1][last_key] = []
                    nested_dict[-1][last_key].append(value)

                else:
                    if not nested_dict:
                        nested_dict.append({})
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
                if array_path := mapping_config.get_containing_array_path(path):
                    child_path = path[len(array_path) :]
                    array_key = keys[-1]
                    array_value = value

                    if path == array_path:
                        # case for /parties/roles
                        set_nested_value(result, keys, value, flattened_schema, add_new=True)
                        continue
                    elif array_path in array_counters:
                        if add_new := is_new_array(array_counters, child_path, array_key, array_value, array_path):
                            array_counters[array_path] = array_value
                            set_nested_value(result, keys[:-1], {}, flattened_schema, add_new=add_new)
                    else:
                        if array_key == "id":
                            array_counters[array_path] = array_value
                        set_nested_value(result, keys[:-1], [{}], flattened_schema)

                    current = result
                    for i, key in enumerate(keys[:-1]):
                        if isinstance(current, list):
                            if not current:
                                current.append({})
                            current = current[-1]
                        if key not in current:
                            key_path = "/" + "/".join(keys[: i + 1])
                            current[key] = [] if flattened_schema.get(key_path, {}).get("type") == "array" else {}

                        current = current[key]
                    value = self.map_codelist_value(keys, flattened_schema, codelists, value)
                    if isinstance(current, list):
                        if not current:
                            current.append({})
                        current[-1][array_key] = value
                    else:
                        current[array_key] = value
                else:
                    set_nested_value(result, keys, value, flattened_schema)
        return result

    def make_release_id(self, curr_row: dict) -> None:
        """
        Generate and set a unique ID for the release based on its content.

        :param curr_row: The current release row dictionary.
        :type curr_row: dict
        """
        id_ = dict_hash.sha256(curr_row)
        curr_row["id"] = id_

    def date_release(self, curr_row: dict) -> None:
        """
        Set the release date to the current date and time.

        :param curr_row: The current release row dictionary.
        :type curr_row: dict
        """
        curr_row["date"] = get_iso_now()

    def tag_initiation_type(self, curr_row: dict) -> None:
        """
        Tag the initiation type of the release as 'tender' if applicable.

        :param curr_row: The current release row dictionary.
        :type curr_row: dict
        """
        if "tender" in curr_row and "initiationType" not in curr_row:
            curr_row["initiationType"] = "tender"

    def tag_ocid(self, curr_row: dict, curr_ocid: str) -> None:
        """
        Set the OCID for the release.

        :param curr_row: The current release row dictionary.
        :type curr_row: dict
        :param curr_ocid: The OCID value to set.
        :type curr_ocid: str
        """
        curr_row["ocid"] = self.produce_ocid(curr_ocid)

    def tag_tags(self, curr_row) -> None:
        if "tag" not in curr_row:
            curr_row["tag"] = []
        if "tender" in curr_row:
            curr_row["tag"].append("tender")
        if "awards" in curr_row:
            curr_row["tag"].append("award")
        if "contracts" in curr_row:
            curr_row["tag"].append("contract")

    def remove_empty_id_arrays(self, data: Any) -> Any:
        """
        Recursively remove arrays that do not contain an 'id' field.

        :param data: The data dictionary to process.
        :type data: dict[str, Any]
        """

        return remove_dicts_without_id(data)

    def map_codelist_value(self, keys, schema, codelists, value):
        path = "/" + "/".join(keys)
        if codelist := schema.get(path, {}).get("codelist"):
            codelist = codelists.get_mapping_for_codelist(codelist)
            if codelist:
                if new_value := codelist.get(value):
                    return new_value
        return value
