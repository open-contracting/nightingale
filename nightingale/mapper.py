import logging
from datetime import datetime
from typing import Any

import dict_hash

from .config import Config
from .mapping.v1 import Mapping
from .unflatten import transform_data
from .utils import load_data

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)


class OCDSDataMapper:
    def __init__(self, config: Config):
        self.config = config

    def map(self):
        mapped = []
        for step in self.config.steps:
            logger.info(f"Mapping step: {step.name}")
            data = load_data(self.config.datasource, step.selector)
            mapping = Mapping(step)
            mapped.extend(self.map_step(data, mapping, step.force_publish))
        return mapped

    def map_step(self, data: list[dict[Any, Any]], mapping: Mapping, force_publish=False) -> list[dict[str, Any]]:
        curr_ocid = ""
        curr_release = {}
        mapped = []

        ocid_mapping = mapping.get_ocid_mapping()
        for row in data:
            ocid = row.pop(ocid_mapping, "")
            if not ocid:
                logger.warn(f"No OCID found in row: {row}")
                continue

            curr_release = transform_data(row, mapping, mapping.get_scheme(), curr_release, force_publish)
            if curr_ocid != ocid:
                if not curr_ocid:
                    curr_ocid = ocid
                    continue
                curr_ocid = ocid
                self.tag_initiation_type(curr_release)
                self.date_release(curr_release)
                self.make_release_id(curr_release)
                mapped.append(curr_release)
                curr_release = {}
                continue
        return mapped

    def make_release_id(self, curr_row):
        id_ = dict_hash.sha256(curr_row)
        curr_row["id"] = id_

    def date_release(self, curr_row):
        curr_row["date"] = datetime.now().isoformat()

    def tag_initiation_type(self, curr_row):
        if "tender" in curr_row and "initiationType" not in curr_row:
            curr_row["initiationType"] = "tender"
