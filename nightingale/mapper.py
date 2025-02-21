import logging
from typing import Any, Optional

import dict_hash

from nightingale.codelists import CodelistsMapping
from nightingale.config import Config
from nightingale.mapping_template.v09 import MappingTemplate
from nightingale.mapping_template.validator import MappingTemplateValidator
from nightingale.utils import get_iso_now, is_new_array, remove_dicts_without_id

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
        self.mapping = MappingTemplate(config.mapping)
        self.codelists = None
        if self.config.mapping.codelists:
            self.codelists = CodelistsMapping(self.config.mapping)

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

        logger.info("MappingTemplate data loaded")
        data = loader.load(config.selector)
        logger.info("Start fetching rows from datasource")
        if validate_mapping:
            logger.info("Validating mapping template...")
            validator = MappingTemplateValidator(loader, self.mapping)
            validator.validate_data_elements()
            validator.validate_selector(data[0])
        logger.info("Start mapping data")
        return self.transform_data(data, self.mapping, codelists=self.codelists)

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
        array_counters = {}
        mapped = []
        count = 0

        ocid_mapping = mapping.get_ocid_mapping()
        for row in data:
            ocid = row.get(ocid_mapping, "")

            if not ocid:
                logger.warning(f"No OCID found in row: {row}. Skipping.")
                continue
            if not curr_ocid:
                curr_ocid = ocid
            if curr_ocid != ocid:
                logger.info(f"Finishing release with {count} rows")
                self.finish_release(curr_ocid, curr_release, mapped)
                curr_ocid = ocid
                curr_release = {}
                array_counters = {}

            curr_release = self.transform_row(
                row, mapping, mapping.get_schema(), curr_release, array_counters=array_counters, codelists=codelists
            )
            count += 1

        if curr_release:
            logger.info(f"Finishing release with {count} rows")
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
        array_counters: dict = None,
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

        # XXX: some duplication in code present maybe refactoring needed
        def set_nested_value(nested_dict, keys, value, schema, add_new=False, append_once=False):
            value = self.map_codelist_value(keys, schema, codelists, value)
            last_key = keys[-1]
            keys_path = "/" + "/".join(keys)

            for i, key in enumerate(keys[:-1]):
                subpath = "/" + "/".join(keys[: i + 1])
                if isinstance(nested_dict, list):
                    nested_dict = self.shift_current_array(nested_dict, subpath, array_counters)
                if key not in nested_dict:
                    nested_dict[key] = [] if schema.get(subpath, {}).get("type") == "array" else {}
                nested_dict = nested_dict[key]
            subpath = "/" + "/".join(keys[:-1])
            if schema.get(keys_path, {}).get("type") == "array" and isinstance(nested_dict, list) and nested_dict:
                nested_dict = self.shift_current_array(nested_dict, subpath, array_counters)
            if isinstance(nested_dict, list):
                nested_dict = self.shift_current_array(nested_dict, keys_path, array_counters)
                if add_new:
                    if last_key not in nested_dict:
                        nested_dict[last_key] = []
                    if value in nested_dict[last_key] and append_once:
                        return
                    nested_dict[last_key].append(value)
                else:
                    nested_dict[last_key] = value
            else:
                if last_key in nested_dict:
                    if isinstance(nested_dict[last_key], list) and add_new:
                        if value in nested_dict[last_key] and append_once:
                            return
                        nested_dict[last_key].append(value)
                    elif isinstance(nested_dict[last_key], dict):
                        nested_dict[last_key].update(value)
                    else:
                        nested_dict[last_key] = value
                else:
                    subpath = "/" + "/".join(keys)
                    if schema.get(subpath, {}).get("type") == "array" and not isinstance(value, list):
                        value = [value]
                    nested_dict[last_key] = value

        if not result:
            result = {}

        skip_criteria_processing = False

        if result:
            if result.get("tender", {}).get("selectionCriteria", {}).get("criteria", None):
                skip_criteria_processing = True

        for flat_col, value in input_data.items():
            if not value:
                continue
            paths = mapping_config.get_paths_for_mapping(flat_col, force_publish=self.config.mapping.force_publish)
            if not paths:
                continue
            for path in paths:
                if "/tender/selectionCriteria/criteria" in path and skip_criteria_processing:
                    continue
                keys = path.strip("/").split("/")
                if array_path := mapping_config.get_containing_array_path(path):
                    child_path = path[len(array_path) :]
                    last_key_name = keys[-1]
                    array_value = value
                    if path == array_path:
                        if "criteria" in path:
                            if child_path != "criteria":
                                if result["tender"].get("selectionCriteria", None):
                                    if len(result["tender"]["selectionCriteria"]["criteria"]) == 0:
                                        result["tender"]["selectionCriteria"]["criteria"].append({})
                                        result["tender"]["selectionCriteria"]["criteria"][-1][last_key_name] = [value]
                                        continue
                                    else:
                                        continue_to_next = False
                                        for index, criterion in enumerate(
                                                result["tender"]["selectionCriteria"]["criteria"]):
                                            if last_key_name not in criterion.keys():
                                                if continue_to_next:
                                                    break
                                                result["tender"]["selectionCriteria"]["criteria"][index][
                                                    last_key_name] = [value]
                                                continue_to_next = True
                                        if continue_to_next:
                                            continue
                                else:
                                    # case for /parties/roles
                                    set_nested_value(result, keys, value, flattened_schema, add_new=True,
                                                     append_once=True)
                                    continue
                            else:
                                set_nested_value(result, keys, value, flattened_schema, add_new=True, append_once=True)
                                continue
                        else:
                            set_nested_value(result, keys, value, flattened_schema, add_new=True, append_once=True)
                            continue
                    elif "criteria" in path:
                        if child_path != "criteria":
                            if result["tender"].get("selectionCriteria", None):
                                if len(result["tender"]["selectionCriteria"]["criteria"]) == 0:
                                    result["tender"]["selectionCriteria"]["criteria"].append({})
                                else:
                                    for index, criterion in enumerate(
                                            result["tender"]["selectionCriteria"]["criteria"]):
                                        if last_key_name not in criterion.keys():
                                            result["tender"]["selectionCriteria"]["criteria"].append({})
                                            break
                    elif array_path in array_counters:
                        if add_new := is_new_array(array_counters, child_path, last_key_name, array_value, array_path):
                            array_counters[array_path] = array_value
                            set_nested_value(result, keys[:-1], {}, flattened_schema, add_new=add_new)
                    else:
                        if last_key_name == "id":
                            array_counters[array_path] = array_value
                            set_nested_value(result, keys[:-1], {}, flattened_schema, True)
                        set_nested_value(result, keys[:-1], [{}], flattened_schema)

                    current = result
                    for i, key in enumerate(keys[:-1]):
                        current_path = "/" + "/".join(keys[: i + 1])
                        is_array = flattened_schema.get(current_path, {}).get("type") == "array"
                        if key not in current:
                            current[key] = [] if is_array else {}
                        current = current[key]
                        if is_array:
                            if "criteria" in path:
                                if child_path != "criteria":
                                    if result["tender"].get("selectionCriteria", None):
                                        for index, criterion in enumerate(
                                                result["tender"]["selectionCriteria"]["criteria"]):
                                            if last_key_name not in criterion.keys() or len(criterion.keys()) == 0:
                                                if last_key_name != "minimum":
                                                    current = result["tender"]["selectionCriteria"]["criteria"][index]
                                                    break
                                                else:
                                                    if len(criterion.keys()) == 0 or criterion.get("type",
                                                                                                   "") == "economic":
                                                        current = result["tender"]["selectionCriteria"]["criteria"][
                                                            index]
                                                        break

                            else:
                                current = self.shift_current_array(current, current_path, array_counters)

                    value = self.map_codelist_value(keys, flattened_schema, codelists, value)
                    if isinstance(current, list):
                        array_path = mapping_config.get_containing_array_path("/" + "/".join(keys))
                        current = self.shift_current_array(current, array_path, array_counters)
                    current[last_key_name] = value
                else:
                    set_nested_value(result, keys, value, flattened_schema)
        return result

    def shift_current_array(self, current, array_path, array_counters):
        if not current:
            current.append({})
        return find_array_element_by_id(current, array_counters.get(array_path) if array_counters else None)

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

        # if not curr_row["awards"] and not curr_row["contracts"]:
        #     curr_row["date"] = curr_row["tender"]["awardPeriod"]["startDate"]
        #     return
        #
        # if curr_row["tender"] and curr_row["awards"] and not curr_row["contracts"]:
        #     curr_row["date"] = curr_row["awards"][0]["date"]
        #     return
        #
        # if curr_row["tender"] and curr_row["contracts"] and curr_row["awards"]:
        #     oldest_start_date = None
        #     if isinstance(curr_row["contracts"], list):
        #         for contract in curr_row["contracts"]:
        #             if contract.get("period", None):
        #                 start_date = contract['period']['startDate']
        #                 parsed_date = datetime.fromisoformat(start_date.replace("Z", "+00:00"))
        #                 if oldest_start_date is None or parsed_date < oldest_start_date:
        #                     oldest_start_date = parsed_date
        #     elif isinstance(curr_row["contracts"], dict):
        #         oldest_start_date = datetime.fromisoformat(curr_row["contracts"]['period']['startDate'].replace("Z", "+00:00"))
        #
        #     curr_row["date"] = oldest_start_date.strftime('%Y-%m-%dT%H:%M:%SZ')
        #     return

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


def find_array_element_by_id(current, array_element_id):
    """
    Finds and returns the first dictionary in a list of dictionaries that contains the given 'id' value.
    If no dictionary with the matching 'id' is found, returns the last dictionary in the list.

    :param current: List[Dict], a list of dictionaries to search.
    :param array_element_id: Any, the target 'id' value to search for.
    :return: Dict, the dictionary with the matching 'id' value, or the last dictionary if not found.

    Examples:
        >>> dict_list = [{'id': 1, 'name': 'Alice'}, {'id': 2, 'name': 'Bob'}, {'id': 3, 'name': 'Charlie'}]
        >>> find_array_element_by_id(dict_list, 2)
        {'id': 2, 'name': 'Bob'}

        >>> find_array_element_by_id(dict_list, 4)
        {'id': 3, 'name': 'Charlie'}

        >>> find_array_element_by_id(dict_list, 3)
        {'id': 3, 'name': 'Charlie'}

        >>> find_array_element_by_id([], 1) is None
        True
    """
    for item in current:
        if item.get("id") == array_element_id:
            return item
    return current[-1] if current else None
