import pandas as pd


class Mapping:

    def __init__(self, config):
        self.config = config
        self.data_sources = self.read_sheet('1. Data Sources', skiprows=3)
        self.data_elements = self.read_sheet('2. Data Elements')
        self.mapping = pd.concat([
            self.read_sheet(name, usecols="C,F").assign(stage=stage)
            for name, stage in (
                ('(OCDS) 1. General (all stages)', 'general'),
                ('(OCDS) 2. Planning', 'planning'),
                ('(OCDS) 3. Tender', 'tender'),
                ('(OCDS) 4. Award',  'award'),
                ('(OCDS) 5. Contract', 'contract'),
            )

        ]).dropna(subset=['Mapping'])
        self.mapping['Mapping'] = self.mapping['Mapping'].apply(lambda x: x.replace('  ', ' '))
        # TODO: remove this hack as soon as template is fixed
        self.schema = self.read_sheet('OCDS Schema 1.1.5', skiprows=0)
        #self.load_extensions()

    def read_sheet(self, sheet_name, usecols=None, skiprows=2):
        if usecols:
            return pd.read_excel(self.config.file, sheet_name=sheet_name, skiprows=2, usecols=usecols)
        return pd.read_excel(self.config.file, sheet_name=sheet_name, skiprows=skiprows)

    def get_mapping(self):
        return self.mapping.asdict()

    def get_mapping_for(self, path):
        return self.mapping[self.mapping['Path'] == path].to_dict(orient='records')

    def get_data_sources(self):
        return self.data_sources.asdict()

    def get_paths_for_mapping(self, key):
        return self.mapping[self.mapping['Mapping'] == key].to_dict(orient='records')

    def is_in_array(self, path):
        arrays = self.get_arrays()
        for array in arrays:
            if path['Path'].startswith(array['path']):
                return array
        return False

    def get_arrays(self):
        return self.schema[self.schema['type'] == "array"].to_dict(orient='records')

    def load_extensions(self):
        pass
