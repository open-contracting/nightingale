import flatten_dict
import dict_hash
from datetime import datetime
from collections import defaultdict
from pathlib import Path
from typing import Any
from .mapping.v1 import Mapping



class OCDSDataMapper:

    def __init__(self, mapping: Mapping):
        self.mapping = mapping

    def __do_mapping_hack(self, ocid_mapping):
        return ocid_mapping['Mapping'].replace('Requisition Header Table', 'BuySpeed Project Projects')

    def map(self, data: list[dict[str, Any]]) -> list[dict[str, Any]]:
        curr_ocid = ''
        curr_row = None
        curr_row_arrays = None
        mapped = []

        def finish_row():
            for array_name, array_data in curr_row_arrays.items():
                if array_name not in curr_row:
                    curr_row[array_name] = []
                for _obj in array_data:
                    curr_row[array_name].append(flatten_dict.unflatten(_obj, splitter='path'))
                    # curr_row[key].append(obj)
            self.make_release_id(curr_row)
            mapped.append(flatten_dict.unflatten(curr_row, splitter='path'))

        ocid_mapping = self.get_ocid_mapping()
        for row in data:
            ocid = row.get(ocid_mapping)
            if not ocid:
                continue
            if curr_ocid != ocid:
                if curr_ocid:
                    finish_row()
                curr_ocid = ocid
                curr_row = defaultdict(dict)
                curr_row_arrays = defaultdict(list)
            for key, value in row.items():

                paths = self.mapping.get_paths_for_mapping(key)

                for path in paths:

                    curr_array = self.mapping.is_in_array(path)
                    if curr_array:
                        array_path = curr_array['path']

                        child_path = path['Path'].replace(f'{curr_array['path']}/', '')
                        if array_path not in curr_row_arrays:
                            curr_row_arrays[array_path].append({})
                        else:
                            curr_array_id = curr_row_arrays[array_path][-1].get('id')
                            if child_path == 'id' and curr_array_id and curr_array_id != value:
                                curr_row_arrays[array_path].append({})
                        # TODO: nested arrays
                        curr_row_arrays[array_path][-1][child_path] = value
                        #curr_row[in_array['path']].append({path['Path'].replace(f'{in_array['path']}/', ''): value})
                    else:
                        curr_row[path['Path']] = value
            finish_row()

        return mapped

    def get_ocid_mapping(self):
        ocid_mapping = self.mapping.get_mapping_for('ocid')[0]
        # TODO: remove this when template is fixed
        ocid_mapping = self.__do_mapping_hack(ocid_mapping)
        return ocid_mapping

    def make_release_id(self, curr_row):
        id_ = dict_hash.sha256(curr_row)
        curr_row['id'] = id_
