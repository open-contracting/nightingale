import logging
from collections import defaultdict

import openpyxl

logger = logging.getLogger(__name__)


class CodelistsMapping:
    def __init__(self, config):
        self.config = config
        self.wb = openpyxl.load_workbook(self.config.codelists, data_only=True)
        self.codelists = self.load_codelists_mapping()

    def normmalize_mapping_column(self, mappings):
        """Normalize the mapping column by setting all space separators to one space"""
        for mapping in mappings:
            if "  " in mapping["mapping"]:
                mapping["mapping"] = " ".join((p.strip() for p in mapping["mapping"].split("  ")))
        return mappings

    def get_mapping_for_codelist(self, name: str):
        return self.codelists.get(name)

    def load_codelists_mapping(self):
        mappings = {}
        for sheet in self.wb.worksheets:
            if sheet.title.lower().startswith("(ocds)"):
                mappings.update(self.read_codelists_sheet(sheet))
        return mappings

    def read_codelists_sheet(self, sheet):
        mappings = defaultdict(dict)
        in_codelist = False
        codelist = ""
        for row in sheet.iter_rows(values_only=True):
            match row[0]:
                case "codelist_name":
                    codelist = row[1].split(":")[-1].strip()
                    in_codelist = True
                    continue
                case "codelist_headers":
                    headers = {k: i for i, k in enumerate(row)}
                    ocds_i = headers["Code"]
                    source_codelist_i = headers["Source codelist"]
                    source_code_i = headers["Source code"]
                    continue
                case _:
                    if not in_codelist:
                        # skip documentation header
                        continue
                    ocds, source_codelist, source_value = row[ocds_i], row[source_codelist_i], row[source_code_i]
                    if not (source_codelist and source_value):
                        # no mapping available
                        continue
                    mappings[codelist][source_value] = ocds
        return mappings
