import csv
import pandas as pd
import tomllib
from pathlib import Path
from typing import Any
BUY_SPEED_REPLACE = [
    "Bid Header Custom Columns",
    "Bid Header Table",
    "Bid Status Dates",
    "PO Status Dates",
    "Purchase Order Header Table",
    "Req Header Custom Columns",
    "Requisition Header Table",
    "Requisition Status Dates",
    "Vendor"
]

def buy_speed_hack(value):
    for match in BUY_SPEED_REPLACE:
        if match in value:
            return value.replace(match, 'BuySpeed Project Projects')
    return value

class Mapping:

    def __init__(self, config):
        self.config = config
        self.data_sources = self.read_sheet('1. Data Sources', skiprows=3)
        self.data_elements = self.read_sheet('2. Data Elements')
        self.mapping = pd.concat([
            self.read_sheet('(OCDS) 1. General (all stages)', usecols="C,F").assign(stage='general'),
            self.read_sheet('(OCDS) 2. Planning', usecols="C,F").assign(stage='planning'),
            self.read_sheet('(OCDS) 3. Tender', usecols="C,F").assign(stage='tender'),
            self.read_sheet('(OCDS) 4. Award', usecols="C,F").assign(stage='tender'),
            self.read_sheet('(OCDS) 5. Contract', usecols="C,F").assign(stage='tender'),
        ]).dropna(subset=['Mapping'])
        self.mapping['Mapping'] = self.mapping['Mapping'].apply(lambda x: x.replace('  ', ' '))
        self.mapping['Mapping'] = self.mapping['Mapping'].apply(buy_speed_hack)
        self.schema = self.read_sheet('OCDS Schema 1.1.5', skiprows=0)
        #self.load_extensions()

    def read_sheet(self, sheet_name, usecols=None, skiprows=2):
        if usecols:
            return pd.read_excel(self.config['file'], sheet_name=sheet_name, skiprows=2, usecols=usecols)
        return pd.read_excel(self.config['file'], sheet_name=sheet_name, skiprows=skiprows)

    def get_mapping(self):
        return self.mapping.asdict()

    def get_mapping_for(self, path):
        return self.mapping[self.mapping['Path'] == path].to_dict(orient='records')

    def get_data_sources(self):
        return self.data_sources.asdict()

    def get_paths_for_mapping(self, key):
        return self.mapping[self.mapping['Mapping'] == key].to_dict(orient='records')

    def is_in_array(self, path):
        arrays = self.schema[self.schema['type'] == "array"].to_dict(orient='records')
        for array in arrays:
            if path['Path'].startswith(array['path']):
                return array
        return False

    def load_extensions(self):
        pass
