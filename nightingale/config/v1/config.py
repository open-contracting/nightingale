import csv
import tomllib
from pathlib import Path
from typing import Any


class TransformationConfig:

    def __init__(self, conf):
        self.conf = conf
        sheetnames = self.conf.get_sheet_names()
        map_conf = conf.get_map_conf()
        self.data_sources = None
        self.data_elements = None
        self.planning = None
        self.general = None
        for sheet, filename in sheetnames.items():
            p = Path(map_conf['directory']) / Path(filename)
            if p.is_file():
                with p.open() as f:
                    if map_conf.get('skip_rows'):
                        for _ in range(int(map_conf.get('skip_rows', 0))):
                            next(f)
                    reader = csv.DictReader(f)
                    if sheet in ('general' , 'planning', 'tender'):
                        data = self._dictyfy(reader, 'Path')
                    elif sheet == 'data_elements':
                        data = self._dictyfy(reader, 'For mapping')
                    elif sheet == 'data_sources':
                        next(f)
                        data = self._dictyfy(reader, 'Short name')
                    else:
                        data = list(reader)
                    setattr(self, sheet, data)

    def _dictyfy(self, reader, key):
        data = {}
        for row in reader:
            try:
                data[row[key].strip()] = row
            except KeyError:
                import pdb; pdb.set_trace()
        return data

    def get_datasources(self):
        for k, v in self.data_sources.items():
            yield k, v

    def get_data_elements(self, datasource, section):
        for mapping, value in self.data_elements.items():
            if value['Data source'] == datasource and value['Section / Table'].strip() == section:
                yield mapping, value

    def get_combined_columns(self):
        return [d['Data element'] for d in self.data_elements.values() if d['Data element']]

    def get_mapped_general(self):
        return {k: v for k, v in self.general.items() if v['Mapping']}

    def get_mapping_for_sheet(self, sheet_name):
        sheet_cols = [k for k, v in self.data_elements.items() if v['Section / Table'].strip() == sheet_name]
        mapping = {k: v for k, v in self.general.items() if v['Mapping'] in sheet_cols}
        mapping.update({k: v for k, v in self.planning.items() if v['Mapping'] in sheet_cols})
        mapping.update({k: v for k, v in self.tender.items() if v['Mapping'] in sheet_cols})
        return mapping
