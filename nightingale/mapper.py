import flatten_dict
import dict_hash
import logging
from datetime import datetime
from collections import defaultdict
from pathlib import Path
from typing import Any
from .mapping.v1 import Mapping
from .utils import load_data
from .config import Config
from deepmerge import Merger

merger = Merger(
    [
        (list, ["append"]),
        (dict, ["merge"]),
        (set, ["union"])
    ],
    ['override'],
    ['override']
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)


class OCDSDataMapper:

    def __init__(self, config: Config):
        self.config = config

    def map(self):
        mapped = defaultdict(dict)
        for step in self.config.steps:
            logger.info(f'Mapping step: {step.name}')
            data = load_data(self.config.datasource, step.selector)
            mapping = Mapping(step)
            data = self.map_step(data, mapping)
            for row in data:
                # mapped[row['ocid']] = merger.merge(mapped[row['ocid']], row)
        return [row for row in mapped.values()]

    def map_step(self, data: list[dict[Any, Any]], mapping: Mapping) -> list[dict[str, Any]]:
        curr_ocid = ''
        curr_row = {}
        curr_row_arrays = {}
        mapped = []

        def finish_row():
            for array_name, array_data in curr_row_arrays.items():
                if array_name not in curr_row:
                    curr_row[array_name] = []
                for _obj in array_data:
                    curr_row[array_name].append(flatten_dict.unflatten(_obj, splitter='path'))
                    # curr_row[key].append(obj)
            if curr_row:
                curr_row['ocid'] = curr_ocid
                self.make_release_id(curr_row)
                self.date_row(curr_row)
                mapped.append(flatten_dict.unflatten(curr_row, splitter='path'))

        ocid_mapping = self.get_ocid_mapping(mapping)['Mapping']
        for row in data:
            ocid = row.pop(ocid_mapping, '')
            if not ocid:
                continue
            if curr_ocid != ocid:
                if curr_ocid:
                    finish_row()
                curr_ocid = ocid
                curr_row = defaultdict(dict)
                curr_row_arrays = defaultdict(list)
            for key, value in row.items():
                if not value:
                    continue
                paths = mapping.get_paths_for_mapping(key)

                for path in paths:

                    curr_array = mapping.is_in_array(path)
                    if curr_array:
                        array_path = curr_array['path']

                        child_path = path['Path'].replace(f'{curr_array['path']}/', '')
                        if path == 'parties/identifier/id':
                            import pdb; pdb.set_trace()
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

    def get_ocid_mapping(self, mapping):
        ocid_mapping = mapping.get_mapping_for('ocid')[0]
        return ocid_mapping

    def make_release_id(self, curr_row):
        id_ = dict_hash.sha256(curr_row)
        curr_row['id'] = id_

    def date_row(self, curr_row):
        curr_row['date'] = datetime.now().isoformat()
